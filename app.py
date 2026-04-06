import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from datetime import datetime, timedelta, timezone
from functools import wraps
import threading
import time
import traceback
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Text, func, and_, desc
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from sqlalchemy.sql import expression

# Inisialisasi Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your_secret_key_here')

# Password login (gunakan environment variable untuk produksi)
LOGIN_PASSWORD = os.environ.get('DASHBOARD_PASSWORD', 'OJI2026!')

# Konfigurasi database
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:fAfrLTxIvblQAiXDRvllRuJqiGgzYBvx@turntable.proxy.rlwy.net:29037/railway')
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
db_session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

# --- Model Definitions ---
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

class Maintenance(Base):
    __tablename__ = 'maintenance'
    id = Column(Integer, primary_key=True)
    machine_id = Column(String(50), nullable=False)
    item = Column(String(50))
    dialysis_count = Column(Integer)
    timestamp = Column(DateTime, default=datetime.now)
    description = Column(Text)

# Buat tabel jika belum ada (untuk first run)
Base.metadata.create_all(bind=engine)

# Konfigurasi threshold
MAINTENANCE_THRESHOLDS = {'filter_inlet': 5}
MIN_DIALYSIS_DURATION = 60
MIN_TREATMENT_DURATION = 60
HEARTBEAT_TIMEOUT = 90

# --- Helper Functions ---
def get_maintenance_name(item):
    names = {'filter_inlet': 'Filter Inlet'}
    return names.get(item, item)

def get_maintenance_description(item):
    descriptions = {'filter_inlet': 'Ganti filter endotoksin untuk memastikan kualitas air tetap optimal.'}
    return descriptions.get(item, 'Perlu perawatan rutin.')

def calculate_required_maintenance(machine_id):
    """Menghitung daftar maintenance yang diperlukan berdasarkan database."""
    machine = db_session.get(Machine, machine_id)
    if not machine:
        return []
    completed_dialysis = machine.completed_dialysis
    maintenance_required = []
    for item, threshold in MAINTENANCE_THRESHOLDS.items():
        last_maintenance = (db_session.query(Maintenance)
                            .filter_by(machine_id=machine_id, item=item)
                            .order_by(desc(Maintenance.timestamp))
                            .first())
        last_dialysis = last_maintenance.dialysis_count if last_maintenance else 0
        if completed_dialysis - last_dialysis >= threshold:
            maintenance_required.append({
                'item': item,
                'name': get_maintenance_name(item),
                'description': get_maintenance_description(item),
                'threshold': threshold,
                'treatments_since_last': completed_dialysis - last_dialysis,
                'last_maintenance_treatment': last_dialysis
            })
    return maintenance_required

def stop_dialysis_session_db(machine: Machine, current_time: datetime):
    """Menghentikan sesi dialysis dan update database."""
    if machine.dialysis_session_start:
        session_duration = (current_time - machine.dialysis_session_start).total_seconds()
        machine.total_dialysis_time += session_duration
        if session_duration >= MIN_DIALYSIS_DURATION:
            machine.completed_dialysis += 1
            print(f"Machine {machine.machine_id}: Dialysis completed (duration: {session_duration:.0f}s)")
        machine.dialysis_session_start = None
        machine.pump_status = 'stopped'
        # current_dialysis_duration tidak disimpan di DB

def get_machine_data_for_emit(machine: Machine):
    """Mengambil data machine untuk dikirim via socket."""
    # Hitung error count (50 terbaru, karena itu yang disimpan)
    error_count = db_session.query(Error).filter_by(machine_id=machine.machine_id).count()
    # maintenance_required dihitung ulang
    maintenance_required = calculate_required_maintenance(machine.machine_id)
    # Durasi sesi saat ini dihitung dari start time jika running
    current_session_duration = 0
    if machine.status == 'running' and machine.current_session_start:
        current_session_duration = (datetime.now() - machine.current_session_start).total_seconds()
    current_dialysis_duration = 0
    if machine.pump_status == 'running' and machine.dialysis_session_start:
        current_dialysis_duration = (datetime.now() - machine.dialysis_session_start).total_seconds()
    return {
        'machine_id': machine.machine_id,
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



# --- Decorator untuk login ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# --- Route Halaman ---
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
    """Mengembalikan data semua machine dalam bentuk JSON."""
    try:
        machines = db_session.query(Machine).all()
        result = {}
        for machine in machines:
            result[machine.machine_id] = get_machine_data_for_emit(machine)
        return jsonify(result)
    except Exception as e:
        print(f"Error in /api/machines: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

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
    app.run(host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
