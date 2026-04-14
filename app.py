import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from datetime import datetime, timedelta
from functools import wraps
import threading
import time
import traceback
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Text, func, and_, desc, Index
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

# Inisialisasi Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your_secret_key_here')

LOGIN_PASSWORD = os.environ.get('DASHBOARD_PASSWORD', 'OJI2026!')

# Konfigurasi database
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:LDYFsrAvLIHOGLULBRCqvoaylmUSIRTu@interchange.proxy.rlwy.net:46023/railway')
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
db_session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

# --- Model Definitions dengan indeks untuk optimasi ---
class Machine(Base):
    __tablename__ = 'machines'
    machine_id = Column(String(50), primary_key=True)
    status = Column(String(20), default='stopped')
    last_update = Column(DateTime, nullable=True)
    start_time = Column(DateTime, nullable=True)
    total_active_time = Column(Float, default=0.0)
    current_session_start = Column(DateTime, nullable=True)
    last_heartbeat = Column(DateTime, nullable=True)
    completed_treatments = Column(Integer, default=0)
    pump_status = Column(String(20), default='stopped')
    dialysis_session_start = Column(DateTime, nullable=True)
    total_dialysis_time = Column(Float, default=0.0)
    completed_dialysis = Column(Integer, default=0)

class Error(Base):
    __tablename__ = 'errors'
    id = Column(Integer, primary_key=True)
    machine_id = Column(String(50), nullable=False)
    error_code = Column(String(20))
    type = Column(String(50))
    timestamp = Column(DateTime)
    server_received_at = Column(DateTime, default=datetime.now)
    created_at = Column(DateTime, server_default=func.now())
    # OPTIMASI: Tambahkan indeks pada machine_id untuk mempercepat filter dan count
    __table_args__ = (
        Index('idx_errors_machine_id', 'machine_id'),
    )

class Maintenance(Base):
    __tablename__ = 'maintenance'
    id = Column(Integer, primary_key=True)
    machine_id = Column(String(50), nullable=False)
    item = Column(String(50))
    dialysis_count = Column(Integer)
    timestamp = Column(DateTime, default=datetime.now)
    description = Column(Text)
    # OPTIMASI: Indeks composite untuk mempercepat pencarian last maintenance per machine & item
    __table_args__ = (
        Index('idx_maintenance_machine_item', 'machine_id', 'item'),
        Index('idx_maintenance_timestamp', 'timestamp'),
    )

# Buat tabel jika belum ada
Base.metadata.create_all(bind=engine)

# Konfigurasi threshold
MAINTENANCE_THRESHOLDS = {'filter_inlet': 200}
MIN_DIALYSIS_DURATION = 3600
MIN_TREATMENT_DURATION = 5440
HEARTBEAT_TIMEOUT = 390

# --- Helper Functions (yang tidak berubah) ---
def get_maintenance_name(item):
    names = {'filter_inlet': 'Filter Inlet'}
    return names.get(item, item)

def get_maintenance_description(item):
    descriptions = {'filter_inlet': 'Ganti filter endotoksin untuk memastikan kualitas air tetap optimal.'}
    return descriptions.get(item, 'Perlu perawatan rutin.')

def stop_dialysis_session_db(machine: Machine, current_time: datetime):
    if machine.dialysis_session_start:
        session_duration = (current_time - machine.dialysis_session_start).total_seconds()
        machine.total_dialysis_time += session_duration
        if session_duration >= MIN_DIALYSIS_DURATION:
            machine.completed_dialysis += 1
            print(f"Machine {machine.machine_id}: Dialysis completed (duration: {session_duration:.0f}s)")
        machine.dialysis_session_start = None
        machine.pump_status = 'stopped'

# OPTIMASI: Fungsi baru untuk mengambil data semua mesin dalam 3 query saja
def get_all_machines_data():
    """
    Mengembalikan dictionary dengan data semua mesin.
    Hanya melakukan 3 query total, bukan 1 + 2N.
    """
    # Query 1: Ambil semua data mesin
    machines = db_session.query(Machine).all()
    if not machines:
        return {}

    # Query 2: Hitung error count per machine dalam satu query GROUP BY
    error_counts = {}
    error_count_query = db_session.query(
        Error.machine_id, func.count(Error.id).label('count')
    ).group_by(Error.machine_id).all()
    for row in error_count_query:
        error_counts[row.machine_id] = row.count

    # Query 3: Ambil maintenance terakhir per (machine_id, item) menggunakan DISTINCT ON
    # Kita gunakan subquery untuk mendapatkan baris terbaru per machine dan item
    # Karena kita hanya butuh untuk item yang ada di MAINTENANCE_THRESHOLDS
    items = list(MAINTENANCE_THRESHOLDS.keys())
    last_maintenance_subq = (
        db_session.query(
            Maintenance.machine_id,
            Maintenance.item,
            Maintenance.dialysis_count,
            Maintenance.timestamp,
            func.row_number().over(
                partition_by=(Maintenance.machine_id, Maintenance.item),
                order_by=desc(Maintenance.timestamp)
            ).label('rn')
        )
        .filter(Maintenance.item.in_(items))
        .subquery()
    )
    last_maintenance_query = db_session.query(
        last_maintenance_subq.c.machine_id,
        last_maintenance_subq.c.item,
        last_maintenance_subq.c.dialysis_count
    ).filter(last_maintenance_subq.c.rn == 1).all()
    
    # Bangun dictionary untuk last maintenance: key = (machine_id, item) -> dialysis_count
    last_maintenance_dict = {}
    for row in last_maintenance_query:
        last_maintenance_dict[(row.machine_id, row.item)] = row.dialysis_count

    # Siapkan hasil
    current_time = datetime.now()
    result = {}
    for machine in machines:
        machine_id = machine.machine_id
        error_count = error_counts.get(machine_id, 0)
        
        # Hitung maintenance required untuk mesin ini
        maintenance_required = []
        completed_dialysis = machine.completed_dialysis
        for item, threshold in MAINTENANCE_THRESHOLDS.items():
            last_dialysis = last_maintenance_dict.get((machine_id, item), 0)
            if completed_dialysis - last_dialysis >= threshold:
                maintenance_required.append({
                    'item': item,
                    'name': get_maintenance_name(item),
                    'description': get_maintenance_description(item),
                    'threshold': threshold,
                    'treatments_since_last': completed_dialysis - last_dialysis,
                    'last_maintenance_treatment': last_dialysis
                })
        
        # Hitung durasi sesi saat ini
        current_session_duration = 0
        if machine.status == 'running' and machine.current_session_start:
            current_session_duration = (current_time - machine.current_session_start).total_seconds()
        current_dialysis_duration = 0
        if machine.pump_status == 'running' and machine.dialysis_session_start:
            current_dialysis_duration = (current_time - machine.dialysis_session_start).total_seconds()
        
        result[machine_id] = {
            'machine_id': machine_id,
            'status': machine.status,
            'last_update': machine.last_update.isoformat() + 'Z' if machine.last_update else None,
            'total_active_time': machine.total_active_time,
            'current_session_duration': current_session_duration,
            'last_heartbeat': machine.last_heartbeat.isoformat() if machine.last_heartbeat else None,
            'completed_treatments': machine.completed_treatments,
            'error_count': error_count,
            'maintenance_required': maintenance_required,
            'maintenance_count': len(maintenance_required),
            'pump_status': machine.pump_status,
            'total_dialysis_time': machine.total_dialysis_time,
            'completed_dialysis': machine.completed_dialysis,
            'current_dialysis_duration': current_dialysis_duration
        }
    return result

# Fungsi untuk single machine (masih dipakai untuk update individual jika diperlukan, tapi tidak dipakai di /api/machines)
def get_single_machine_data(machine: Machine):
    """Digunakan untuk keperluan lain jika perlu, tidak untuk batch."""
    error_count = db_session.query(Error).filter_by(machine_id=machine.machine_id).count()
    maintenance_required = []
    completed_dialysis = machine.completed_dialysis
    for item, threshold in MAINTENANCE_THRESHOLDS.items():
        last_maintenance = (db_session.query(Maintenance)
                            .filter_by(machine_id=machine.machine_id, item=item)
                            .order_by(desc(Maintenance.timestamp))
                            .first())
        last_dialysis = last_maintenance.dialysis_count if last_maintenance else 0
        if completed_dialysis - last_dialysis >= threshold:
            maintenance_required.append({...})  # sama seperti sebelumnya
    # ... sisanya sama seperti get_machine_data_for_emit lama
    # Namun karena kita sudah tidak memanggilnya di /api/machines, bisa dihapus atau dipertahankan.
    # Saya akan hapus fungsi lama dan ganti dengan ini untuk keperluan internal jika perlu.
    pass

# --- Decorator untuk login (tidak berubah) ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# --- Route Halaman (tidak berubah) ---
@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password')
        if password == LOGIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            return render_template('login.html', error="Password salah")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

# --- API Endpoints ---
@app.route('/api/machines')
def get_machines():
    """Mengembalikan data semua machine dalam bentuk JSON (dioptimasi)."""
    try:
        # OPTIMASI: Panggil fungsi batch yang hanya melakukan 3 query
        result = get_all_machines_data()
        return jsonify(result)
    except Exception as e:
        print(f"Error in /api/machines: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint lain (error-log, update, pump-status, maintenance-done, delete-machine) tetap sama seperti sebelumnya
# Saya tidak mengubahnya karena tidak relevan dengan bottleneck.
# Namun pastikan tidak ada panggilan ke get_machine_data_for_emit di tempat lain.
# Jika ada, ganti dengan get_single_machine_data yang serupa.

# (Kode untuk endpoint lain disalin dari kode Anda, tidak diubah)
# ... (salin semua endpoint lain dari kode asli, kecuali yang sudah diubah)

@app.route('/error-log', methods=['POST'])
def log_error():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        error_code = data.get('error_code')
        error_type = data.get('type')
        if not machine_id or error_code is None:
            return jsonify({'error': 'Missing machine_id or error_code'}), 400

        current_time = datetime.utcnow()
        # Ambil atau buat machine
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)
            db_session.flush()  # untuk mendapatkan ID jika diperlukan

        # Simpan error ke tabel Error
        error_entry = Error(
            machine_id=machine_id,
            error_code=error_code,
            type=error_type,
            timestamp=current_time,
            server_received_at=current_time
        )
        db_session.add(error_entry)

        # Batasi jumlah error per machine menjadi 50 (hapus yang paling lama)
        errors_to_keep = (db_session.query(Error)
                          .filter_by(machine_id=machine_id)
                          .order_by(desc(Error.timestamp))
                          .limit(50)
                          .all())
        # Hapus semua error yang tidak termasuk dalam 50 terbaru
        keep_ids = [e.id for e in errors_to_keep]
        db_session.query(Error).filter(
            and_(Error.machine_id == machine_id, Error.id.notin_(keep_ids))
        ).delete(synchronize_session=False)

        # Update machine
        machine.last_update = current_time
        db_session.commit()

        print(f"Error logged for machine {machine_id}: Code {error_code}, Type: {error_type}")
        return jsonify({'success': True, 'message': 'Error logged successfully'})

    except Exception as e:
        db_session.rollback()
        print(f"Error logging error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/update', methods=['POST'])
def update_machine_status():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        status = data.get('status')
        if not machine_id or not status:
            return jsonify({'error': 'Missing machine_id or status'}), 400

        current_time = datetime.utcnow()
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)

        old_status = machine.status
        machine.last_heartbeat = current_time
        machine.last_update = current_time

        if status == 'running':
            if old_status != 'running':
                machine.status = 'running'
                machine.current_session_start = current_time
                if not machine.start_time:
                    machine.start_time = current_time
                print(f"Machine {machine_id} STARTED new session")
            else:
                if machine.current_session_start:
                    # durasi tidak disimpan, hanya dihitung di frontend
                    pass
        elif status == 'stopped' and old_status == 'running':
            if machine.current_session_start:
                session_duration = (current_time - machine.current_session_start).total_seconds()
                machine.total_active_time += session_duration
                if session_duration >= MIN_TREATMENT_DURATION:
                    machine.completed_treatments += 1
                    print(f"Machine {machine_id} completed treatment #{machine.completed_treatments}")
                machine.current_session_start = None
            machine.status = 'stopped'
            # Jika pompa masih running, stop juga
            if machine.pump_status == 'running':
                stop_dialysis_session_db(machine, current_time)

        db_session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db_session.rollback()
        print(f"Error updating machine status: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/pump-status', methods=['POST'])
def update_pump_status():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        pump_status = data.get('pump_status')
        if not machine_id or not pump_status:
            return jsonify({'error': 'Missing machine_id or pump_status'}), 400

        current_time = datetime.utcnow()
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)

        old_pump_status = machine.pump_status
        machine.last_update = current_time

        if pump_status == 'running':
            if old_pump_status != 'running':
                machine.pump_status = 'running'
                machine.dialysis_session_start = current_time
                print(f"Machine {machine_id}: Pump STARTED dialysis session")
            else:
                if machine.dialysis_session_start:
                    # update durasi tidak disimpan
                    pass
        elif pump_status == 'stopped' and old_pump_status == 'running':
            if machine.dialysis_session_start:
                session_duration = (current_time - machine.dialysis_session_start).total_seconds()
                machine.total_dialysis_time += session_duration
                if session_duration >= MIN_DIALYSIS_DURATION:
                    machine.completed_dialysis += 1
                    print(f"Machine {machine_id} completed dialysis #{machine.completed_dialysis} (duration: {session_duration:.0f}s)")
                machine.dialysis_session_start = None
            machine.pump_status = 'stopped'
            print(f"Machine {machine_id}: Pump STOPPED")

        db_session.commit()
        return jsonify({'success': True, 'message': 'Pump status updated'})
    except Exception as e:
        db_session.rollback()
        print(f"Error updating pump status: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/maintenance-done', methods=['POST'])
def mark_maintenance_done():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        maintenance_item = data.get('maintenance_item')
        if not machine_id or not maintenance_item:
            return jsonify({'error': 'Missing machine_id or maintenance_item'}), 400

        machine = db_session.get(Machine, machine_id)
        if not machine:
            return jsonify({'error': 'Machine not found'}), 404

        # Simpan record maintenance
        maintenance_record = Maintenance(
            machine_id=machine_id,
            item=maintenance_item,
            dialysis_count=machine.completed_dialysis,
            description=f'Maintenance {get_maintenance_name(maintenance_item)} dilakukan',
            timestamp=datetime.now()
        )
        db_session.add(maintenance_record)
        db_session.commit()

        print(f"Maintenance marked as done for {machine_id}: {maintenance_item}")
        return jsonify({'success': True, 'message': 'Maintenance marked as done'})
    except Exception as e:
        db_session.rollback()
        print(f"Error marking maintenance done: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/delete-machine', methods=['POST'])
def delete_machine():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        if not machine_id:
            return jsonify({'error': 'Missing machine_id'}), 400

        machine = db_session.get(Machine, machine_id)
        if not machine:
            return jsonify({'error': 'Machine not found'}), 404

        # Hapus semua error dan maintenance terkait
        db_session.query(Error).filter_by(machine_id=machine_id).delete()
        db_session.query(Maintenance).filter_by(machine_id=machine_id).delete()
        db_session.delete(machine)
        db_session.commit()

        return jsonify({'success': True, 'message': f'Machine {machine_id} deleted'})
    except Exception as e:
        db_session.rollback()
        print(f"Error deleting machine: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

# --- Background Threads ---
def check_machine_timeout():
    """Cek heartbeat timeout dan hentikan sesi yang tidak aktif."""
    while True:
        time.sleep(5)
        try:
            current_time = datetime.utcnow()
            # Cari semua mesin dengan status running dan heartbeat kadaluarsa
            timeout_threshold = current_time - timedelta(seconds=HEARTBEAT_TIMEOUT)
            machines_timeout = db_session.query(Machine).filter(
                Machine.status == 'running',
                Machine.last_heartbeat <= timeout_threshold
            ).all()

            for machine in machines_timeout:
                print(f"Machine {machine.machine_id} timed out, stopping...")
                # Stop treatment session
                if machine.current_session_start:
                    session_duration = (current_time - machine.current_session_start).total_seconds()
                    machine.total_active_time += session_duration
                    if session_duration >= MIN_TREATMENT_DURATION:
                        machine.completed_treatments += 1
                        print(f"Machine {machine.machine_id} completed treatment via timeout")
                    machine.current_session_start = None
                # Stop dialysis session jika running
                if machine.pump_status == 'running':
                    stop_dialysis_session_db(machine, current_time)
                machine.status = 'stopped'
                db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Error in check_machine_timeout: {e}")
            traceback.print_exc()
        finally:
            # Pastikan session ditutup (scoped_session akan handle)
            db_session.remove()

# Mulai background threads
timeout_thread = threading.Thread(target=check_machine_timeout, daemon=True)
timeout_thread.start()

# --- Main ---
if __name__ == '__main__':
    print("Starting Machine Monitoring Server with PostgreSQL (no SocketIO)...")
    app.run(host='0.0.0.0', port=5000, debug=False)
