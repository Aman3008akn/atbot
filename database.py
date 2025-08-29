# database.py
import json
import os
from threading import Lock
from datetime import datetime

DB_FILE = 'database.json'
db_lock = Lock()

def load_data():
    """Loads user data from the JSON file."""
    with db_lock:
        if not os.path.exists(DB_FILE):
            return {}
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

def save_data(data):
    """Saves the provided data to the JSON file."""
    with db_lock:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

def get_user_data(user_id):
    """Retrieves data for a specific user, creating it if it doesn't exist."""
    user_id_str = str(user_id)
    data = load_data()
    if user_id_str not in data:
        data[user_id_str] = {
            'username': None,
            'logs': [],
            'is_banned': False, # New: Ban status
            'adbot_status': False,
            'forward_delay': 5,
            'saved_message': None,
            'accounts': {},
            'state': None,
            'has_agreed': False,
            'is_premium': False,
            'start_time': None,
            'stop_time': None,
            'temp_phone_number': None,
            'temp_phone_code_hash': None,
            'temp_otp_digits': ""
        }
        save_data(data)
    return data[user_id_str]

def update_user_data(user_id, key, value):
    """Updates a specific key for a user."""
    user_id_str = str(user_id)
    data = load_data()
    if user_id_str not in data:
        get_user_data(user_id)
        data = load_data()
    data[user_id_str][key] = value
    save_data(data)

def add_log_entry(user_id, log_message):
    """Adds a new timestamped log entry for a user."""
    user_id_str = str(user_id)
    data = load_data()
    if user_id_str in data:
        if 'logs' not in data[user_id_str]:
            data[user_id_str]['logs'] = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data[user_id_str]['logs'] = data[user_id_str]['logs'][-49:] 
        data[user_id_str]['logs'].append(f"[{timestamp}] {log_message}")
        save_data(data)

def delete_user_account(user_id, account_name):
    """Deletes a specific account for a user."""
    user_id_str = str(user_id)
    data = load_data()
    if user_id_str in data and account_name in data[user_id_str].get('accounts', {}):
        del data[user_id_str]['accounts'][account_name]
        save_data(data)
        return True
    return False
