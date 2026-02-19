from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from datetime import datetime, timedelta
import threading
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_here'
socketio = SocketIO(app)

# Konfigurasi threshold maintenance
MAINTENANCE_THRESHOLDS = {
    'filter_inlet': 5,
    'seal_ring': 10,
}

# Konfigurasi threshold dialysis
MIN_DIALYSIS_DURATION = 60  # detik (untuk testing, bisa disesuaikan)

def get_maintenance_name(item):
    names = {
        'filter_inlet': 'Filter Inlet',
        'seal_ring': 'Seal Ring'
    }
    return names.get(item, item)

def get_maintenance_description(item):
    descriptions = {
        'filter_inlet': 'Ganti filter inlet untuk memastikan kualitas air tetap optimal.',
        'seal_ring': 'Periksa dan ganti seal ring untuk mencegah kebocoran.'
    }
    return descriptions.get(item, 'Perlu perawatan rutin.')

def calculate_required_maintenance(machine_id, machines_dict):
    if machine_id not in machines_dict:
        return []
    
    machine_data = machines_dict[machine_id]
    completed_treatments = machine_data['completed_treatments']
    maintenance_required = []
    
    for item, threshold in MAINTENANCE_THRESHOLDS.items():
        last_maintenance_treatment = 0
        for maintenance in reversed(machine_data['maintenance_history']):
            if maintenance['item'] == item:
                last_maintenance_treatment = maintenance['treatment_count']
                break
        
        if completed_treatments - last_maintenance_treatment >= threshold:
            maintenance_required.append({
                'item': item,
                'name': get_maintenance_name(item),
                'description': get_maintenance_description(item),
                'threshold': threshold,
                'treatments_since_last': completed_treatments - last_maintenance_treatment,
                'last_maintenance_treatment': last_maintenance_treatment
            })
    
    return maintenance_required

# In-memory storage dengan tambahan field untuk dialysis
machines = {
    'HD_999': {
        'status': 'stopped',
        'last_update': None,
        'start_time': None,
        'total_active_time': 0,
        'current_session_start': None,
        'last_heartbeat': None,
        'completed_treatments': 0,
        'current_session_duration': 0,
        'error_history': [],
        'maintenance_history': [],
        'maintenance_required': [],
        # FIELD BARU UNTUK DIALYSIS
        'pump_status': 'stopped',
        'dialysis_session_start': None,
        'total_dialysis_time': 0,
        'completed_dialysis': 0,
        'current_dialysis_duration': 0
    }
}

# Lock untuk thread safety
data_lock = threading.Lock()

# Timeout configuration
HEARTBEAT_TIMEOUT = 90
CLEANUP_INTERVAL = 300
MIN_TREATMENT_DURATION = 60

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/machines')
def get_machines():
    with data_lock:
        for machine_id, data in machines.items():
            # Update current session duration
            if data['status'] == 'running' and data['current_session_start']:
                session_duration = (datetime.now() - data['current_session_start']).total_seconds()
                data['current_session_duration'] = session_duration
            else:
                data['current_session_duration'] = 0
            
            # Update current dialysis duration
            if data['pump_status'] == 'running' and data['dialysis_session_start']:
                dialysis_duration = (datetime.now() - data['dialysis_session_start']).total_seconds()
                data['current_dialysis_duration'] = dialysis_duration
            else:
                data['current_dialysis_duration'] = 0
            
            # Update maintenance required
            data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
                
        return jsonify(machines)

# Endpoint untuk error log
@app.route('/error-log', methods=['POST'])
def log_error():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        error_code = data.get('error_code')
        error_type = data.get('type')
        
        if not machine_id or error_code is None:
            return jsonify({'error': 'Missing machine_id or error_code'}), 400
        
        with data_lock:
            current_time = datetime.now()
            
            if machine_id not in machines:
                machines[machine_id] = create_new_machine(machine_id, current_time)
            
            machine_data = machines[machine_id]
            
            error_entry = {
                'error_code': error_code,
                'type': error_type,
                'timestamp': current_time.isoformat(),
                'server_received_at': current_time.isoformat()
            }
            
            machine_data['error_history'].append(error_entry)
            
            if len(machine_data['error_history']) > 50:
                machine_data['error_history'] = machine_data['error_history'][-50:]
            
            machine_data['last_update'] = current_time
            machine_data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
            
            print(f"Error logged for machine {machine_id}: Code {error_code}, Type: {error_type}")
            
            socketio.emit('machine_update', get_machine_data_for_emit(machine_data, machine_id))
            
            return jsonify({'success': True, 'message': 'Error logged successfully'})
            
    except Exception as e:
        print(f"Error logging error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint update status machine (untuk heartbeat)
@app.route('/update', methods=['POST'])
def update_machine_status():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        status = data.get('status')
        
        if not machine_id or not status:
            return jsonify({'error': 'Missing machine_id or status'}), 400
        
        with data_lock:
            current_time = datetime.now()
            
            if machine_id not in machines:
                machines[machine_id] = create_new_machine(machine_id, current_time)
            
            machine_data = machines[machine_id]
            old_status = machine_data['status']
            
            machine_data['last_heartbeat'] = current_time
            machine_data['last_update'] = current_time
            
            if status == 'running':
                if old_status != 'running':
                    machine_data['status'] = 'running'
                    machine_data['current_session_start'] = current_time
                    machine_data['current_session_duration'] = 0
                    if not machine_data['start_time']:
                        machine_data['start_time'] = current_time
                    print(f"Machine {machine_id} STARTED new session")
                else:
                    if machine_data['current_session_start']:
                        machine_data['current_session_duration'] = (current_time - machine_data['current_session_start']).total_seconds()
            
            elif status == 'stopped' and old_status == 'running':
                if machine_data['current_session_start']:
                    session_duration = (current_time - machine_data['current_session_start']).total_seconds()
                    machine_data['total_active_time'] += session_duration
                    
                    if session_duration >= MIN_TREATMENT_DURATION:
                        machine_data['completed_treatments'] += 1
                        print(f"Machine {machine_id} completed treatment #{machine_data['completed_treatments']} (duration: {session_duration:.0f}s)")
                    
                    machine_data['current_session_start'] = None
                    machine_data['current_session_duration'] = 0
                
                machine_data['status'] = 'stopped'
                print(f"Machine {machine_id} STOPPED")
            
            # Jika mesin mati, hentikan dialysis juga
            if status == 'stopped' and machine_data['pump_status'] == 'running':
                stop_dialysis_session(machine_data, current_time)
            
            machine_data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
            
            socketio.emit('machine_update', get_machine_data_for_emit(machine_data, machine_id))
            
            return jsonify({'success': True, 'message': 'Status updated'})
            
    except Exception as e:
        print(f"Error updating machine status: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint baru untuk status pompa
@app.route('/pump-status', methods=['POST'])
def update_pump_status():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        pump_status = data.get('pump_status')
        
        if not machine_id or not pump_status:
            return jsonify({'error': 'Missing machine_id or pump_status'}), 400
        
        with data_lock:
            current_time = datetime.now()
            
            if machine_id not in machines:
                machines[machine_id] = create_new_machine(machine_id, current_time)
            
            machine_data = machines[machine_id]
            old_pump_status = machine_data['pump_status']
            
            machine_data['last_update'] = current_time
            
            if pump_status == 'running':
                if old_pump_status != 'running':
                    # Pompa baru mulai
                    machine_data['pump_status'] = 'running'
                    machine_data['dialysis_session_start'] = current_time
                    machine_data['current_dialysis_duration'] = 0
                    print(f"Machine {machine_id}: Pump STARTED dialysis session")
                else:
                    # Update duration jika sudah running
                    if machine_data['dialysis_session_start']:
                        machine_data['current_dialysis_duration'] = (current_time - machine_data['dialysis_session_start']).total_seconds()
            
            elif pump_status == 'stopped' and old_pump_status == 'running':
                # Pompa berhenti, hitung dialysis session
                if machine_data['dialysis_session_start']:
                    session_duration = (current_time - machine_data['dialysis_session_start']).total_seconds()
                    machine_data['total_dialysis_time'] += session_duration
                    
                    if session_duration >= MIN_DIALYSIS_DURATION:
                        machine_data['completed_dialysis'] += 1
                        print(f"Machine {machine_id} completed dialysis #{machine_data['completed_dialysis']} (duration: {session_duration:.0f}s)")
                    
                    machine_data['dialysis_session_start'] = None
                    machine_data['current_dialysis_duration'] = 0
                
                machine_data['pump_status'] = 'stopped'
                print(f"Machine {machine_id}: Pump STOPPED")
            
            machine_data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
            
            socketio.emit('machine_update', get_machine_data_for_emit(machine_data, machine_id))
            
            return jsonify({'success': True, 'message': 'Pump status updated'})
            
    except Exception as e:
        print(f"Error updating pump status: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint untuk menandai maintenance sudah dilakukan
@app.route('/maintenance-done', methods=['POST'])
def mark_maintenance_done():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')
        maintenance_item = data.get('maintenance_item')
        
        if not machine_id or not maintenance_item:
            return jsonify({'error': 'Missing machine_id or maintenance_item'}), 400
        
        with data_lock:
            if machine_id not in machines:
                return jsonify({'error': 'Machine not found'}), 404
            
            machine_data = machines[machine_id]
            
            maintenance_record = {
                'item': maintenance_item,
                'treatment_count': machine_data['completed_treatments'],
                'timestamp': datetime.now().isoformat(),
                'description': f'Maintenance {get_maintenance_name(maintenance_item)} dilakukan'
            }
            
            machine_data['maintenance_history'].append(maintenance_record)
            machine_data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
            
            print(f"Maintenance marked as done for {machine_id}: {maintenance_item}")
            
            socketio.emit('machine_update', get_machine_data_for_emit(machine_data, machine_id))
            
            return jsonify({'success': True, 'message': 'Maintenance marked as done'})
            
    except Exception as e:
        print(f"Error marking maintenance done: {e}")
        return jsonify({'error': 'Internal server error'}), 500

#Endpoint hapus mesin
@app.route('/delete-machine', methods=['POST'])
def delete_machine():
    try:
        data = request.get_json()
        machine_id = data.get('machine_id')

        if not machine_id:
            return jsonify({'error': 'Missing machine_id'}), 400

        with data_lock:
            if machine_id not in machines:
                return jsonify({'error': 'Machine not found'}), 404

            del machines[machine_id]

        return jsonify({'success': True, 'message': f'Machine {machine_id} deleted'})

    except Exception as e:
        return jsonify({'error': 'Internal server error'}), 500

# Helper functions
def create_new_machine(machine_id, current_time):
    return {
        'status': 'stopped',
        'last_update': current_time,
        'start_time': None,
        'total_active_time': 0,
        'current_session_start': None,
        'last_heartbeat': current_time,
        'completed_treatments': 0,
        'current_session_duration': 0,
        'error_history': [],
        'maintenance_history': [],
        'maintenance_required': [],
        'pump_status': 'stopped',
        'dialysis_session_start': None,
        'total_dialysis_time': 0,
        'completed_dialysis': 0,
        'current_dialysis_duration': 0
    }

def get_machine_data_for_emit(machine_data, machine_id):
    return {
        'machine_id': machine_id,
        'status': machine_data['status'],
        'last_update': machine_data['last_update'].isoformat() if machine_data['last_update'] else None,
        'total_active_time': machine_data['total_active_time'],
        'current_session_duration': machine_data.get('current_session_duration', 0),
        'last_heartbeat': machine_data['last_heartbeat'].isoformat() if machine_data['last_heartbeat'] else None,
        'completed_treatments': machine_data['completed_treatments'],
        'error_count': len(machine_data['error_history']),
        'maintenance_required': machine_data['maintenance_required'],
        'maintenance_count': len(machine_data['maintenance_required']),
        # Data dialysis
        'pump_status': machine_data['pump_status'],
        'total_dialysis_time': machine_data['total_dialysis_time'],
        'completed_dialysis': machine_data['completed_dialysis'],
        'current_dialysis_duration': machine_data.get('current_dialysis_duration', 0)
    }

def stop_dialysis_session(machine_data, current_time):
    if machine_data['dialysis_session_start']:
        session_duration = (current_time - machine_data['dialysis_session_start']).total_seconds()
        machine_data['total_dialysis_time'] += session_duration
        
        if session_duration >= MIN_DIALYSIS_DURATION:
            machine_data['completed_dialysis'] += 1
            print(f"Machine: Dialysis completed via timeout (duration: {session_duration:.0f}s)")
        
        machine_data['dialysis_session_start'] = None
        machine_data['current_dialysis_duration'] = 0
        machine_data['pump_status'] = 'stopped'

# Background tasks (sama, tapi tambah logic untuk dialysis timeout)
def check_machine_timeout():
    while True:
        time.sleep(5)
        with data_lock:
            current_time = datetime.now()
            machines_to_update = []
            
            for machine_id, data in machines.items():
                # Check heartbeat timeout untuk treatment
                if data['status'] == 'running' and data['last_heartbeat']:
                    time_since_heartbeat = (current_time - data['last_heartbeat']).total_seconds()
                    
                    if time_since_heartbeat > HEARTBEAT_TIMEOUT:
                        machines_to_update.append(machine_id)
            
            for machine_id in machines_to_update:
                machine_data = machines[machine_id]
                
                # Stop treatment session
                if machine_data['current_session_start']:
                    session_duration = (current_time - machine_data['current_session_start']).total_seconds()
                    machine_data['total_active_time'] += session_duration
                    
                    if session_duration >= MIN_TREATMENT_DURATION:
                        machine_data['completed_treatments'] += 1
                        print(f"Machine {machine_id} completed treatment #{machine_data['completed_treatments']} via timeout (duration: {session_duration:.0f}s)")
                    
                    machine_data['current_session_start'] = None
                    machine_data['current_session_duration'] = 0
                
                # Stop dialysis session jika pompa sedang running
                if machine_data['pump_status'] == 'running':
                    stop_dialysis_session(machine_data, current_time)
                
                machine_data['status'] = 'stopped'
                machine_data['maintenance_required'] = calculate_required_maintenance(machine_id, machines)
                
                socketio.emit('machine_update', get_machine_data_for_emit(machine_data, machine_id))
                
                print(f"Machine {machine_id} marked as STOPPED due to heartbeat timeout")


# Start background threads
timeout_thread = threading.Thread(target=check_machine_timeout, daemon=True)
timeout_thread.start()

if __name__ == '__main__':
    print("Starting Machine Monitoring Server...")

    socketio.run(app, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)



