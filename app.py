import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_socketio import SocketIO, emit
from datetime import datetime, timedelta
from functools import wraps
import traceback
import threading
import time
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Text, func, and_
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from sqlalchemy.sql import expression

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_here'
socketio = SocketIO(app)

# Password statis (sebaiknya gunakan environment variable)
LOGIN_PASSWORD = "OJI2026!"

# Ambil DATABASE_URL dari environment variable
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:fAfrLTxIvblQAiXDRvllRuJqiGgzYBvx@turntable.proxy.rlwy.net:29037/railway')

# Buat engine dan session
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
db_session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

# Model definitions
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

# Buat tabel jika belum ada
Base.metadata.create_all(bind=engine)

# Konfigurasi threshold maintenance
MAINTENANCE_THRESHOLDS = {
    'filter_inlet': 5, #completed dialysis
}

# Konfigurasi threshold dialysis
MIN_DIALYSIS_DURATION = 60  # detik
HEARTBEAT_TIMEOUT = 90      # detik
MIN_TREATMENT_DURATION = 60 # detik

def get_maintenance_name(item):
    names = {
        'filter_inlet': 'Filter Inlet',
    }
    return names.get(item, item)

def get_maintenance_description(item):
    descriptions = {
        'filter_inlet': 'Ganti filter endotoksin untuk memastikan kualitas air tetap optimal.',
    }
    return descriptions.get(item, 'Perlu perawatan rutin.')

def calculate_required_maintenance(machine_id):
    machine = db_session.get(Machine, machine_id)
    if not machine:
        return []
    completed_dialysis = machine.completed_dialysis
    maintenance_required = []
    for item, threshold in MAINTENANCE_THRESHOLDS.items():
        last_maintenance = Maintenance.query.filter_by(
            machine_id=machine_id, item=item
        ).order_by(Maintenance.timestamp.desc()).first()
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

# Helper untuk mengubah objek Machine ke dictionary untuk dikirim ke frontend
def machine_to_dict(machine: Machine):
    # Hitung current session duration jika mesin running
    current_session_duration = 0
    if machine.status == 'running' and machine.current_session_start:
        current_session_duration = (datetime.now() - machine.current_session_start).total_seconds()
    
    # Hitung current dialysis duration jika pompa running
    current_dialysis_duration = 0
    if machine.pump_status == 'running' and machine.dialysis_session_start:
        current_dialysis_duration = (datetime.now() - machine.dialysis_session_start).total_seconds()
    
    # Ambil error count dari database
    error_count = db_session.query(Error).filter(Error.machine_id == machine.machine_id).count()
    
    # Ambil maintenance required
    maintenance_required = calculate_required_maintenance(machine.machine_id)
    
    return {
        'machine_id': machine.machine_id,
        'status': machine.status,
        'last_update': machine.last_update.isoformat() if machine.last_update else None,
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

# Fungsi untuk stop dialysis session pada objek Machine
def stop_dialysis_session_db(machine: Machine, current_time: datetime):
    if machine.dialysis_session_start:
        session_duration = (current_time - machine.dialysis_session_start).total_seconds()
        machine.total_dialysis_time += session_duration
        if session_duration >= MIN_DIALYSIS_DURATION:
            machine.completed_dialysis += 1
            print(f"Machine {machine.machine_id}: Dialysis completed via timeout (duration: {session_duration:.0f}s)")
        machine.dialysis_session_start = None
        machine.pump_status = 'stopped'

# Decorator login
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Routes
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

@app.route('/api/machines')
def get_machines():
    try:
        machines = db_session.query(Machine).all()
        result = {}
        for machine in machines:
            result[machine.machine_id] = machine_to_dict(machine)
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
        
        current_time = datetime.now()
        
        # Pastikan machine ada, jika tidak buat baru
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)
            db_session.flush()
        
        # Simpan error
        error = Error(
            machine_id=machine_id,
            error_code=error_code,
            type=error_type,
            timestamp=current_time,
            server_received_at=current_time
        )
        db_session.add(error)
        
        # Update machine last_update
        machine.last_update = current_time
        
        # Hapus error lama jika lebih dari 50 per mesin
        # Urutkan berdasarkan id atau timestamp, ambil 50 terakhir
        errors = db_session.query(Error).filter(Error.machine_id == machine_id).order_by(Error.id.desc()).all()
        if len(errors) > 50:
            to_delete = errors[50:]
            for e in to_delete:
                db_session.delete(e)
        
        db_session.commit()
        
        # Kirim update via socket
        socketio.emit('machine_update', machine_to_dict(machine))
        
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

        current_time = datetime.now()
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)
            db_session.flush()

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
                # Update session duration if needed (not stored in DB)
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
        socketio.emit('machine_update', machine_to_dict(machine))
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
        
        current_time = datetime.now()
        machine = db_session.get(Machine, machine_id)
        if not machine:
            machine = Machine(machine_id=machine_id)
            db_session.add(machine)
            db_session.flush()
        
        old_pump_status = machine.pump_status
        machine.last_update = current_time
        
        if pump_status == 'running':
            if old_pump_status != 'running':
                machine.pump_status = 'running'
                machine.dialysis_session_start = current_time
                print(f"Machine {machine_id}: Pump STARTED dialysis session")
        elif pump_status == 'stopped' and old_pump_status == 'running':
            if machine.dialysis_session_start:
                session_duration = (current_time - machine.dialysis_session_start).total_seconds()
                machine.total_dialysis_time += session_duration
                if session_duration >= MIN_DIALYSIS_DURATION:
                    machine.completed_dialysis += 1
                    print(f"Machine {machine_id} completed dialysis #{machine.completed_dialysis} (duration: {session_duration:.0f}s)")
                machine.dialysis_session_start = None
            machine.pump_status = 'stopped'
        
        db_session.commit()
        socketio.emit('machine_update', machine_to_dict(machine))
        return jsonify({'success': True})
        
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
        
        # Simpan maintenance record
        maintenance = Maintenance(
            machine_id=machine_id,
            item=maintenance_item,
            dialysis_count=machine.completed_dialysis,
            description=f'Maintenance {get_maintenance_name(maintenance_item)} dilakukan',
            timestamp=datetime.now()
        )
        db_session.add(maintenance)
        db_session.commit()
        
        print(f"Maintenance marked as done for {machine_id}: {maintenance_item}")
        
        # Kirim update via socket
        socketio.emit('machine_update', machine_to_dict(machine))
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
        db_session.query(Error).filter(Error.machine_id == machine_id).delete()
        db_session.query(Maintenance).filter(Maintenance.machine_id == machine_id).delete()
        db_session.delete(machine)
        db_session.commit()
        
        return jsonify({'success': True, 'message': f'Machine {machine_id} deleted'})
        
    except Exception as e:
        db_session.rollback()
        print(f"Error deleting machine: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

# Background threads
def check_machine_timeout():
    while True:
        time.sleep(5)
        try:
            current_time = datetime.now()
            # Cari machine dengan status 'running' dan last_heartbeat sudah timeout
            machines = db_session.query(Machine).filter(
                Machine.status == 'running',
                Machine.last_heartbeat != None,
                (current_time - Machine.last_heartbeat).total_seconds() > HEARTBEAT_TIMEOUT
            ).all()
            
            for machine in machines:
                # Stop treatment session
                if machine.current_session_start:
                    session_duration = (current_time - machine.current_session_start).total_seconds()
                    machine.total_active_time += session_duration
                    if session_duration >= MIN_TREATMENT_DURATION:
                        machine.completed_treatments += 1
                        print(f"Machine {machine.machine_id} completed treatment via timeout (duration: {session_duration:.0f}s)")
                    machine.current_session_start = None
                
                # Stop dialysis session jika pompa running
                if machine.pump_status == 'running':
                    stop_dialysis_session_db(machine, current_time)
                
                machine.status = 'stopped'
                machine.last_update = current_time
                db_session.commit()
                
                # Emit update via socket
                socketio.emit('machine_update', machine_to_dict(machine))
                print(f"Machine {machine.machine_id} marked as STOPPED due to heartbeat timeout")
        except Exception as e:
            print(f"Error in check_machine_timeout: {e}")
            traceback.print_exc()
            db_session.rollback()

def broadcast_machine_updates():
    while True:
        time.sleep(10)
        try:
            machines = db_session.query(Machine).all()
            for machine in machines:
                # Emit update untuk setiap machine (meskipun tidak ada perubahan, untuk refresh frontend)
                # Tapi kita bisa menghitung durasi di frontend, tapi untuk konsistensi kirim data terbaru
                socketio.emit('machine_update', machine_to_dict(machine))
        except Exception as e:
            print(f"Error in broadcast_machine_updates: {e}")
            traceback.print_exc()

# Start threads
timeout_thread = threading.Thread(target=check_machine_timeout, daemon=True)
timeout_thread.start()

broadcast_thread = threading.Thread(target=broadcast_machine_updates, daemon=True)
broadcast_thread.start()

if __name__ == '__main__':
    print("Starting Machine Monitoring Server with PostgreSQL...")
    socketio.run(app, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
