"""
hc_manager.py · Tablero de HC y gestión de horarios para Operaciones.
"""
import os
import re
import csv
import io
import json
import sqlite3
import threading
import unicodedata
from datetime import datetime, timedelta
from flask import Flask, Blueprint, request, jsonify, send_file, send_from_directory

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HC_DB_PATH = os.environ.get('HC_DB_PATH', os.path.join(BASE_DIR, 'hc_manager.db'))
HC_HTML_FILE = 'hc_dashboard.html'

HC_LOCK = threading.RLock()
HC_DB_READY = False

ROSTER_DAYS = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

SCHEMA = """
CREATE TABLE IF NOT EXISTS hc_agents (
    agent_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'Agente',
    is_new_hire INTEGER NOT NULL DEFAULT 0,
    supervisor TEXT NOT NULL DEFAULT '',
    coordinator TEXT NOT NULL DEFAULT '',
    campaign TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'Llamadas',
    status TEXT NOT NULL DEFAULT 'Activo',
    hire_date TEXT,
    termination_date TEXT,
    termination_reason TEXT NOT NULL DEFAULT '',
    lunes TEXT NOT NULL DEFAULT 'DD-DD',
    martes TEXT NOT NULL DEFAULT 'DD-DD',
    miercoles TEXT NOT NULL DEFAULT 'DD-DD',
    jueves TEXT NOT NULL DEFAULT 'DD-DD',
    viernes TEXT NOT NULL DEFAULT 'DD-DD',
    sabado TEXT NOT NULL DEFAULT 'DD-DD',
    domingo TEXT NOT NULL DEFAULT 'DD-DD',
    email TEXT NOT NULL DEFAULT '',
    employee_number TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS hc_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    change_type TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    reason TEXT NOT NULL DEFAULT '',
    effective_date TEXT,
    changed_at TEXT NOT NULL,
    changed_by TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS hc_terminations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    termination_date TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    rehireable INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS hc_transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    from_campaign TEXT NOT NULL,
    to_campaign TEXT NOT NULL,
    from_channel TEXT NOT NULL DEFAULT '',
    to_channel TEXT NOT NULL DEFAULT '',
    from_supervisor TEXT NOT NULL DEFAULT '',
    to_supervisor TEXT NOT NULL DEFAULT '',
    from_coordinator TEXT NOT NULL DEFAULT '',
    to_coordinator TEXT NOT NULL DEFAULT '',
    from_role TEXT NOT NULL DEFAULT '',
    to_role TEXT NOT NULL DEFAULT '',
    effective_date TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS hc_absences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    absence_type TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'approved',
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(agent_id) REFERENCES hc_agents(agent_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hc_people_catalog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role TEXT NOT NULL,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS hc_schedule_overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    schedules_json TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(agent_id) REFERENCES hc_agents(agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hc_campaign ON hc_agents(campaign);
CREATE INDEX IF NOT EXISTS idx_hc_status ON hc_agents(status);
CREATE INDEX IF NOT EXISTS idx_hc_termination ON hc_agents(termination_date);
CREATE INDEX IF NOT EXISTS idx_hc_history_agent ON hc_history(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_term_agent ON hc_terminations(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_transfers_agent ON hc_transfers(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_absences_agent ON hc_absences(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_overrides_agent ON hc_schedule_overrides(agent_id);
"""

def _conn():
    conn = sqlite3.connect(HC_DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=5000')
    return conn

def _init_db():
    global HC_DB_READY
    if HC_DB_READY: return
    with HC_LOCK:
        if HC_DB_READY: return
        conn = _conn()
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.executescript(SCHEMA)
            
            # Migraciones de columnas
            columns = [r[1] for r in conn.execute("PRAGMA table_info(hc_agents)").fetchall()]
            if 'role' not in columns:
                conn.execute("ALTER TABLE hc_agents ADD COLUMN role TEXT NOT NULL DEFAULT 'Agente'")
            if 'is_new_hire' not in columns:
                conn.execute("ALTER TABLE hc_agents ADD COLUMN is_new_hire INTEGER NOT NULL DEFAULT 0")

            t_columns = [r[1] for r in conn.execute("PRAGMA table_info(hc_transfers)").fetchall()]
            if 'from_role' not in t_columns:
                conn.execute("ALTER TABLE hc_transfers ADD COLUMN from_role TEXT NOT NULL DEFAULT ''")
                conn.execute("ALTER TABLE hc_transfers ADD COLUMN to_role TEXT NOT NULL DEFAULT ''")

            abs_columns = [r[1] for r in conn.execute("PRAGMA table_info(hc_absences)").fetchall()]
            if 'status' not in abs_columns:
                conn.execute("ALTER TABLE hc_absences ADD COLUMN status TEXT NOT NULL DEFAULT 'approved'")
                conn.execute("ALTER TABLE hc_absences ADD COLUMN reason TEXT NOT NULL DEFAULT ''")

            conn.commit()
            HC_DB_READY = True
        finally:
            conn.close()

def _today(): return datetime.now().strftime('%Y-%m-%d')
def _now(): return datetime.now().isoformat(timespec='seconds')

def _validate_date(value, field='fecha'):
    raw = str(value or '').strip()[:10]
    try:
        datetime.strptime(raw, '%Y-%m-%d')
        return raw
    except Exception:
        try:
            dt = datetime.strptime(raw, '%d/%m/%Y')
            return dt.strftime('%Y-%m-%d')
        except Exception:
            raise ValueError(f'{field} inválida: "{value}". Usa YYYY-MM-DD o DD/MM/YYYY.')

def _validate_schedule(value):
    raw = str(value or '').strip().upper()
    if not raw or raw in ('NAN', 'NONE', 'DD-DD', 'DESCANSO', 'OFF'): return 'DD-DD'
    m = re.match(r'^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$', raw)
    if not m: raise ValueError(f'Horario inválido: "{value}". Usa HH:MM-HH:MM o DD-DD.')
    h1, m1, h2, m2 = (int(m.group(i)) for i in range(1, 5))
    if not (0 <= h1 <= 23 and 0 <= m1 <= 59 and 0 <= h2 <= 24 and 0 <= m2 <= 59): raise ValueError('Horario fuera de rango.')
    if h2 == 24 and m2 != 0: raise ValueError('La hora final debe ser 24:00.')
    if h1 * 60 + m1 == h2 * 60 + m2: raise ValueError('El horario no puede ser de duración cero.')
    return f'{h1:02d}:{m1:02d}-{h2:02d}:{m2:02d}'

def _normalize_channel(value):
    raw = str(value or '').strip().lower()
    if 'chat' in raw or 'mensaje' in raw: return 'Chat'
    return 'Llamadas'

def _normalize_col(col_name):
    col = unicodedata.normalize('NFKD', str(col_name)).encode('ASCII', 'ignore').decode('utf-8')
    return re.sub(r'[^a-z0-9]', '', col.lower())

def _log_history(conn, agent_id, change_type, before, after, reason='', effective_date=None, actor='operaciones'):
    conn.execute("""INSERT INTO hc_history (agent_id, change_type, before_json, after_json, reason, effective_date, changed_at, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (agent_id, change_type, json.dumps(before, ensure_ascii=False) if before else None, json.dumps(after, ensure_ascii=False) if after else None, str(reason or '')[:500], effective_date, _now(), str(actor or 'operaciones')[:80]))

def _row_to_agent(row):
    if not row: return None
    today = _today()
    d = {
        'agentId': row['agent_id'], 'fullName': row['full_name'] or '', 'role': row['role'] or 'Agente',
        'isNewHire': bool(row['is_new_hire']),
        'supervisor': row['supervisor'] or '', 'coordinator': row['coordinator'] or '', 
        'campaign': row['campaign'] or '', 'channel': row['channel'] or 'Llamadas',
        'status': row['status'] or 'Activo', 'hireDate': row['hire_date'], 'terminationDate': row['termination_date'],
        'terminationReason': row['termination_reason'] or '', 'schedules': {
            'Lunes': row['lunes'] or 'DD-DD', 'Martes': row['martes'] or 'DD-DD', 'Miércoles': row['miercoles'] or 'DD-DD',
            'Jueves': row['jueves'] or 'DD-DD', 'Viernes': row['viernes'] or 'DD-DD', 'Sábado': row['sabado'] or 'DD-DD', 'Domingo': row['domingo'] or 'DD-DD',
        },
        'email': row['email'] or '', 'employeeNumber': row['employee_number'] or '', 'notes': row['notes'] or '',
        'createdAt': row['created_at'], 'updatedAt': row['updated_at'], 'updatedBy': row['updated_by'] or '',
    }
    if row['termination_date']:
        d['displayStatus'] = 'Baja programada' if row['termination_date'] > today else 'Baja'
        d['terminationPending'] = row['termination_date'] > today
    elif str(row['status']).strip().title() == 'Baja':
        d['displayStatus'] = 'Baja'
        d['terminationPending'] = False
    else:
        d['displayStatus'] = 'Activo'
        d['terminationPending'] = False
    return d

def _fetch_agent(conn, agent_id):
    return _row_to_agent(conn.execute("SELECT * FROM hc_agents WHERE agent_id=?", (agent_id,)).fetchone())

def _diff_agents(before, after):
    out = {}
    if not before or not after: return out
    for k in ('fullName', 'supervisor', 'coordinator', 'campaign', 'channel', 'hireDate', 'role', 'isNewHire'):
        if before.get(k) != after.get(k): out[k] = {'before': before.get(k), 'after': after.get(k)}
    if before.get('schedules') != after.get('schedules'): out['schedules'] = {'before': before.get('schedules'), 'after': after.get('schedules')}
    return out

def _ensure_people_catalog(conn, supervisor, coordinator):
    now = _now()
    if supervisor:
        conn.execute("INSERT OR IGNORE INTO hc_people_catalog (role, name, created_at) VALUES ('supervisor', ?, ?)", (supervisor.strip(), now))
    if coordinator:
        conn.execute("INSERT OR IGNORE INTO hc_people_catalog (role, name, created_at) VALUES ('coordinator', ?, ?)", (coordinator.strip(), now))

def _save_agent_tx(conn, old_id, payload, actor='operaciones', allow_create=True):
    new_id = str((payload or {}).get('agentId') or '').strip()
    if not new_id: raise ValueError('El ID del agente es obligatorio.')
    campaign = str((payload or {}).get('campaign') or '').strip()
    if not campaign: raise ValueError('La campaña es obligatoria.')

    before = _fetch_agent(conn, old_id)
    if before is None and not allow_create: raise ValueError('No se encontró el agente.')

    supervisor = str(payload.get('supervisor') or '').strip()[:120]
    coordinator = str(payload.get('coordinator') or '').strip()[:120]
    role = str(payload.get('role') or 'Agente').strip()[:120]
    is_new_hire = 1 if payload.get('isNewHire') else 0
    _ensure_people_catalog(conn, supervisor, coordinator)

    if before and old_id != new_id:
        if conn.execute("SELECT 1 FROM hc_agents WHERE agent_id=?", (new_id,)).fetchone():
            raise ValueError('El nuevo ID de agente ya existe.')
        conn.execute("UPDATE hc_terminations SET agent_id=? WHERE agent_id=?", (new_id, old_id))
        conn.execute("UPDATE hc_transfers SET agent_id=? WHERE agent_id=?", (new_id, old_id))
        conn.execute("UPDATE hc_absences SET agent_id=? WHERE agent_id=?", (new_id, old_id))
        conn.execute("UPDATE hc_schedule_overrides SET agent_id=? WHERE agent_id=?", (new_id, old_id))
        conn.execute("UPDATE hc_history SET agent_id=? WHERE agent_id=?", (new_id, old_id))

    schedules = payload.get('schedules') or {}
    days = {d: _validate_schedule(schedules.get(d, (before or {}).get('schedules', {}).get(d, 'DD-DD'))) for d in ROSTER_DAYS}

    hire_date = payload.get('hireDate')
    if hire_date: 
        hire_date = _validate_date(hire_date, 'fecha de ingreso')
    elif before is None: 
        hire_date = None

    now = _now()
    validity = payload.get('validity') or {}
    is_temp = validity.get('type') == 'temporary'
    
    values = (
        str(payload.get('fullName') or '').strip()[:120], role, is_new_hire, supervisor,
        coordinator, campaign[:120], _normalize_channel(payload.get('channel')),
        hire_date, str(payload.get('email') or '').strip()[:120], str(payload.get('employeeNumber') or '').strip()[:40],
        str(payload.get('notes') or '').strip()[:500], 
        days['Lunes'], days['Martes'], days['Miércoles'], days['Jueves'], days['Viernes'], days['Sábado'], days['Domingo'],
        now, actor,
    )

    if before is None:
        conn.execute("""INSERT INTO hc_agents (agent_id, full_name, role, is_new_hire, supervisor, coordinator, campaign, channel, hire_date, email, employee_number, notes, lunes, martes, miercoles, jueves, viernes, sabado, domingo, status, created_at, updated_at, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Activo', ?, ?, ?)""", (new_id,) + values)
        _log_history(conn, new_id, 'agent_created', None, {'fullName': values[0], 'campaign': values[5], 'channel': values[6], 'supervisor': values[3], 'coordinator': values[4], 'hireDate': hire_date, 'isNewHire': bool(is_new_hire)}, str(payload.get('reason') or 'Alta de agente'), hire_date, actor)
    else:
        if is_temp:
            update_values = values[:10] + (
                before['schedules']['Lunes'], before['schedules']['Martes'], before['schedules']['Miércoles'],
                before['schedules']['Jueves'], before['schedules']['Viernes'], before['schedules']['Sábado'], before['schedules']['Domingo']
            ) + values[17:]
        else:
            update_values = values
            
        conn.execute("""UPDATE hc_agents SET agent_id=?, full_name=?, role=?, is_new_hire=?, supervisor=?, coordinator=?, campaign=?, channel=?, hire_date=?, email=?, employee_number=?, notes=?, lunes=?, martes=?, miercoles=?, jueves=?, viernes=?, sabado=?, domingo=?, updated_at=?, updated_by=? WHERE agent_id=?""", (new_id,) + update_values + (old_id,))
        
        if is_temp:
            start = _validate_date(validity.get('startDate'), 'fecha inicio')
            end = _validate_date(validity.get('endDate'), 'fecha fin')
            if start > end: raise ValueError('La fecha fin no puede ser menor a la fecha inicio.')
            reason = str(validity.get('reason') or 'Cambio temporal')[:500]
            
            conn.execute("""
                INSERT INTO hc_schedule_overrides (agent_id, start_date, end_date, schedules_json, reason, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (new_id, start, end, json.dumps(days), reason, now, actor))
            
            _log_history(conn, new_id, 'temporary_schedule_applied', before, {'startDate': start, 'endDate': end, 'schedules': days}, reason, start, actor)

        after = _fetch_agent(conn, new_id)
        if not is_temp and _diff_agents(before, after): 
            _log_history(conn, new_id, 'agent_updated', before, after, str(payload.get('reason') or 'Actualización'), None, actor)
            
    return _fetch_agent(conn, new_id)

hc_bp = Blueprint('hc_manager', __name__)

@hc_bp.after_request
def _cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Cache-Control'] = 'no-store'
    return response

@hc_bp.route('/', defaults={'path': ''})
@hc_bp.route('/<path:path>')
def _serve_dashboard(path):
    if path.startswith('api/'): return jsonify({'error': 'API no encontrada'}), 404
    if path and os.path.exists(os.path.join(BASE_DIR, path)): return send_from_directory(BASE_DIR, path)
    target = os.path.join(BASE_DIR, HC_HTML_FILE)
    if os.path.exists(target): return send_from_directory(BASE_DIR, HC_HTML_FILE)
    return jsonify({'error': f'No se encontró {HC_HTML_FILE}'}), 404

@hc_bp.route('/api/hc/agents', methods=['GET'])
def hc_list_agents():
    _init_db()
    campaign = request.args.get('campaign', '').strip()
    channel = request.args.get('channel', '').strip()
    supervisor = request.args.get('supervisor', '').strip()
    coordinator = request.args.get('coordinator', '').strip()
    status_filter = request.args.get('status', '').strip()
    search = request.args.get('search', '').strip().lower()
    start_date = request.args.get('startDate', '').strip()
    end_date = request.args.get('endDate', '').strip()

    sql = "SELECT * FROM hc_agents WHERE 1=1"
    args = []
    if campaign:
        sql += " AND campaign=?"
        args.append(campaign)
    if channel:
        sql += " AND channel=?"
        args.append(channel)
    if supervisor:
        sql += " AND supervisor=?"
        args.append(supervisor)
    if coordinator:
        sql += " AND coordinator=?"
        args.append(coordinator)
    if status_filter == 'Activo':
        sql += " AND status='Activo'"
    elif status_filter == 'Baja':
        sql += " AND status='Baja'"
    elif status_filter == 'New Hire':
        sql += " AND status='Activo' AND is_new_hire=1"
    elif status_filter == 'Operativo':
        sql += " AND status='Activo' AND is_new_hire=0"
        
    if start_date and end_date:
        sql += " AND (hire_date IS NULL OR hire_date <= ?) AND (termination_date IS NULL OR termination_date >= ?)"
        args.extend([end_date, start_date])
        
    sql += " ORDER BY campaign, role, supervisor, full_name, agent_id"

    conn = _conn()
    try:
        rows = [_row_to_agent(r) for r in conn.execute(sql, args).fetchall()]
        
        if status_filter in ('Vacación', 'Incapacidad'):
            today = _today()
            absences = {r['agent_id']: r['absence_type'] for r in conn.execute("SELECT agent_id, absence_type FROM hc_absences WHERE start_date <= ? AND end_date >= ? AND status='approved'", (today, today))}
            filtered_rows = []
            for r in rows:
                if r['status'] != 'Activo': continue
                abs_type = absences.get(r['agentId'])
                if status_filter == 'Vacación' and abs_type == 'vacation': filtered_rows.append(r)
                elif status_filter == 'Incapacidad' and abs_type == 'incapacity': filtered_rows.append(r)
            rows = filtered_rows
    finally:
        conn.close()

    if search:
        rows = [r for r in rows if search in f"{r['agentId']} {r['fullName']} {r['role']} {r['supervisor']} {r['coordinator']} {r['campaign']} {r['email']}".lower()]

    return jsonify({'agents': rows, 'count': len(rows)}), 200

@hc_bp.route('/api/hc/agents/<agent_id>', methods=['GET'])
def hc_get_agent(agent_id):
    _init_db()
    conn = _conn()
    try: agent = _fetch_agent(conn, agent_id)
    finally: conn.close()
    if not agent: return jsonify({'error': 'Agente no encontrado.'}), 404
    return jsonify({'agent': agent}), 200

@hc_bp.route('/api/hc/agents', methods=['POST'])
def hc_create_agent():
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.pop('actorMode', 'operaciones'))[:80]
        agent_id = str(payload.get('agentId') or '').strip()
        with HC_LOCK:
            conn = _conn()
            try:
                agent = _save_agent_tx(conn, agent_id, payload, actor, allow_create=True)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        return jsonify({'ok': True, 'agent': agent}), 200
    except sqlite3.IntegrityError: return jsonify({'error': 'Ya existe un agente con ese ID.'}), 409
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo crear: {e}'}), 500

@hc_bp.route('/api/hc/agents/<agent_id>', methods=['PUT'])
def hc_update_agent(agent_id):
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.pop('actorMode', 'operaciones'))[:80]
        with HC_LOCK:
            conn = _conn()
            try:
                agent = _save_agent_tx(conn, agent_id, payload, actor, allow_create=False)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        return jsonify({'ok': True, 'agent': agent}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo actualizar: {e}'}), 500

@hc_bp.route('/api/hc/agents/<agent_id>/terminate', methods=['POST'])
def hc_terminate_agent(agent_id):
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.get('actorMode', 'operaciones'))[:80]
        termination_date = _validate_date(payload.get('terminationDate'), 'fecha de baja')
        reason = str(payload.get('reason') or '')[:500]
        rehireable = 1 if payload.get('rehireable', True) else 0

        with HC_LOCK:
            conn = _conn()
            try:
                before = _fetch_agent(conn, agent_id)
                if not before: raise ValueError('Agente no encontrado.')
                if before.get('terminationDate'): raise ValueError('El agente ya tiene una baja registrada.')
                new_status = 'Baja' if termination_date <= _today() else 'Activo'
                conn.execute("UPDATE hc_agents SET status=?, termination_date=?, termination_reason=?, updated_at=?, updated_by=? WHERE agent_id=?", (new_status, termination_date, reason, _now(), actor, agent_id))
                conn.execute("INSERT INTO hc_terminations (agent_id, termination_date, reason, rehireable, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?)", (agent_id, termination_date, reason, rehireable, _now(), actor))
                _log_history(conn, agent_id, 'agent_terminated', before, {'terminationDate': termination_date, 'reason': reason, 'rehireable': bool(rehireable), 'appliesNow': termination_date <= _today()}, reason, termination_date, actor)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        conn2 = _conn()
        try: agent = _fetch_agent(conn2, agent_id)
        finally: conn2.close()
        return jsonify({'ok': True, 'agent': agent}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo registrar la baja: {e}'}), 500

@hc_bp.route('/api/hc/agents/<agent_id>/reactivate', methods=['POST'])
def hc_reactivate_agent(agent_id):
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.get('actorMode', 'operaciones'))[:80]
        new_hire = payload.get('hireDate')
        with HC_LOCK:
            conn = _conn()
            try:
                before = _fetch_agent(conn, agent_id)
                if not before: raise ValueError('Agente no encontrado.')
                if not before.get('terminationDate') and before.get('status') != 'Baja': raise ValueError('El agente ya está activo.')
                new_hire = _validate_date(new_hire, 'fecha de reingreso') if new_hire else _today()
                conn.execute("UPDATE hc_agents SET status='Activo', termination_date=NULL, termination_reason='', hire_date=?, updated_at=?, updated_by=? WHERE agent_id=?", (new_hire, _now(), actor, agent_id))
                _log_history(conn, agent_id, 'agent_reactivated', before, {'hireDate': new_hire}, str(payload.get('reason') or 'Reactivación'), new_hire, actor)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        conn2 = _conn()
        try: agent = _fetch_agent(conn2, agent_id)
        finally: conn2.close()
        return jsonify({'ok': True, 'agent': agent}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo reactivar: {e}'}), 500

@hc_bp.route('/api/hc/agents/<agent_id>/transfer', methods=['POST'])
def hc_transfer_agent(agent_id):
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.get('actorMode', 'operaciones'))[:80]
        to_campaign = str(payload.get('toCampaign') or '').strip()
        to_channel = _normalize_channel(payload.get('toChannel') or '')
        to_supervisor = str(payload.get('toSupervisor') or '').strip()
        to_coordinator = str(payload.get('toCoordinator') or '').strip()
        to_role = str(payload.get('toRole') or 'Agente').strip()
        effective = _validate_date(payload.get('effectiveDate'), 'fecha efectiva')
        reason = str(payload.get('reason') or '')[:500]

        if not to_campaign: raise ValueError('La campaña destino es obligatoria.')

        with HC_LOCK:
            conn = _conn()
            try:
                before = _fetch_agent(conn, agent_id)
                if not before: raise ValueError('Agente no encontrado.')
                if (before['campaign'] == to_campaign and before['channel'] == to_channel and 
                    before['supervisor'] == to_supervisor and before['coordinator'] == to_coordinator and
                    before['role'] == to_role):
                    raise ValueError('No hay cambios en la asignación actual.')

                _ensure_people_catalog(conn, to_supervisor, to_coordinator)

                conn.execute("UPDATE hc_agents SET campaign=?, channel=?, supervisor=?, coordinator=?, role=?, updated_at=?, updated_by=? WHERE agent_id=?", 
                             (to_campaign, to_channel, to_supervisor, to_coordinator, to_role, _now(), actor, agent_id))
                
                conn.execute("""
                    INSERT INTO hc_transfers (agent_id, from_campaign, to_campaign, from_channel, to_channel, from_supervisor, to_supervisor, from_coordinator, to_coordinator, from_role, to_role, effective_date, reason, created_at, created_by) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (agent_id, before['campaign'], to_campaign, before['channel'], to_channel, before['supervisor'], to_supervisor, before['coordinator'], to_coordinator, before['role'], to_role, effective, reason, _now(), actor))
                
                _log_history(conn, agent_id, 'agent_transferred', before, {
                    'fromCampaign': before['campaign'], 'toCampaign': to_campaign, 
                    'fromChannel': before['channel'], 'toChannel': to_channel,
                    'fromSupervisor': before['supervisor'], 'toSupervisor': to_supervisor,
                    'fromCoordinator': before['coordinator'], 'toCoordinator': to_coordinator,
                    'fromRole': before['role'], 'toRole': to_role,
                    'effectiveDate': effective
                }, reason, effective, actor)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        conn2 = _conn()
        try: agent = _fetch_agent(conn2, agent_id)
        finally: conn2.close()
        return jsonify({'ok': True, 'agent': agent}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo transferir: {e}'}), 500

@hc_bp.route('/api/hc/absences', methods=['GET'])
def hc_list_absences():
    _init_db()
    conn = _conn()
    try:
        rows = [dict(r) for r in conn.execute("""
            SELECT a.*, ag.full_name 
            FROM hc_absences a
            JOIN hc_agents ag ON ag.agent_id = a.agent_id
            ORDER BY a.start_date DESC LIMIT 1000
        """).fetchall()]
        return jsonify({'absences': rows}), 200
    finally:
        conn.close()

@hc_bp.route('/api/hc/absences', methods=['POST'])
def hc_create_absence():
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        agent_id = str(payload.get('agentId') or '').strip()
        abs_type = str(payload.get('absenceType') or '').strip()
        start = _validate_date(payload.get('startDate'), 'fecha inicio')
        end = _validate_date(payload.get('endDate'), 'fecha fin')
        reason = str(payload.get('reason') or '')[:500]
        actor = str(payload.get('actorMode') or 'operaciones')[:80]

        if not agent_id or abs_type not in ('vacation', 'incapacity'):
            raise ValueError('Agente y Tipo son obligatorios.')
        if start > end:
            raise ValueError('La fecha fin no puede ser menor a la fecha inicio.')

        with HC_LOCK:
            conn = _conn()
            try:
                if not _fetch_agent(conn, agent_id):
                    raise ValueError('Agente no encontrado.')
                
                now = _now()
                conn.execute("""
                    INSERT INTO hc_absences (agent_id, absence_type, start_date, end_date, status, reason, created_at, created_by)
                    VALUES (?, ?, ?, ?, 'approved', ?, ?, ?)
                """, (agent_id, abs_type, start, end, reason, now, actor))
                
                _log_history(conn, agent_id, f'{abs_type}_created', None, {
                    'startDate': start, 'endDate': end, 'reason': reason
                }, reason, start, actor)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        return jsonify({'ok': True}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo registrar: {e}'}), 500

@hc_bp.route('/api/hc/people', methods=['GET'])
def hc_list_people():
    _init_db()
    conn = _conn()
    try:
        rows = [dict(r) for r in conn.execute("SELECT role, name FROM hc_people_catalog").fetchall()]
        seen = {(r['role'], r['name']) for r in rows}
        
        # Agregar supervisores de la tabla de agentes
        for r in conn.execute("SELECT DISTINCT supervisor FROM hc_agents WHERE supervisor <> ''"):
            if ('supervisor', r[0]) not in seen:
                rows.append({'role': 'supervisor', 'name': r[0]})
                seen.add(('supervisor', r[0]))
                
        # Agregar coordinadores de la tabla de agentes
        for r in conn.execute("SELECT DISTINCT coordinator FROM hc_agents WHERE coordinator <> ''"):
            if ('coordinator', r[0]) not in seen:
                rows.append({'role': 'coordinator', 'name': r[0]})
                seen.add(('coordinator', r[0]))
                
        rows.sort(key=lambda x: (x['role'], x['name']))
        return jsonify({'people': rows}), 200
    finally:
        conn.close()

@hc_bp.route('/api/hc/people', methods=['POST'])
def hc_create_person():
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        role = str(payload.get('role') or '').strip()
        name = str(payload.get('name') or '').strip()
        actor = str(payload.get('actorMode') or 'operaciones')[:80]

        if role not in ('supervisor', 'coordinator') or not name:
            raise ValueError('Rol y Nombre son obligatorios.')

        with HC_LOCK:
            conn = _conn()
            try:
                now = _now()
                conn.execute("INSERT INTO hc_people_catalog (role, name, created_at, created_by) VALUES (?, ?, ?, ?)", (role, name, now, actor))
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                raise ValueError('Ese nombre ya está registrado.')
            except Exception:
                conn.rollback()
                raise
            finally: conn.close()
        return jsonify({'ok': True}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'No se pudo crear: {e}'}), 500

@hc_bp.route('/api/hc/agents/<agent_id>/history', methods=['GET'])
def hc_agent_history(agent_id):
    _init_db()
    conn = _conn()
    try: rows = conn.execute("SELECT * FROM hc_history WHERE agent_id=? ORDER BY id DESC LIMIT 500", (agent_id,)).fetchall()
    finally: conn.close()
    return jsonify({'history': [{'id': r['id'], 'agentId': r['agent_id'], 'changeType': r['change_type'], 'before': json.loads(r['before_json']) if r['before_json'] else None, 'after': json.loads(r['after_json']) if r['after_json'] else None, 'reason': r['reason'] or '', 'effectiveDate': r['effective_date'], 'changedAt': r['changed_at'], 'changedBy': r['changed_by'] or ''} for r in rows]}), 200

@hc_bp.route('/api/hc/history', methods=['GET'])
def hc_global_history():
    _init_db()
    limit = max(1, min(int(request.args.get('limit', 500)), 2000))
    conn = _conn()
    try: rows = conn.execute("SELECT * FROM hc_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    finally: conn.close()
    return jsonify({'history': [{'id': r['id'], 'agentId': r['agent_id'], 'changeType': r['change_type'], 'before': json.loads(r['before_json']) if r['before_json'] else None, 'after': json.loads(r['after_json']) if r['after_json'] else None, 'reason': r['reason'] or '', 'effectiveDate': r['effective_date'], 'changedAt': r['changed_at'], 'changedBy': r['changed_by'] or ''} for r in rows]}), 200

@hc_bp.route('/api/hc/metrics', methods=['GET'])
def hc_metrics():
    _init_db()
    today = _today()
    default_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    try:
        start = _validate_date(request.args.get('start') or default_start, 'start')
        end = _validate_date(request.args.get('end') or today, 'end')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    if start > end:
        return jsonify({'error': 'La fecha inicial no puede ser posterior a la fecha final.'}), 400

    campaign = request.args.get('campaign', '').strip()
    channel = request.args.get('channel', '').strip()
    supervisor = request.args.get('supervisor', '').strip()
    coordinator = request.args.get('coordinator', '').strip()
    status_filter = request.args.get('status', '').strip()
    search = request.args.get('search', '').strip().lower()

    filters = []
    args = []
    if campaign:
        filters.append("a.campaign=?")
        args.append(campaign)
    if channel:
        filters.append("a.channel=?")
        args.append(channel)
    if supervisor:
        filters.append("a.supervisor=?")
        args.append(supervisor)
    if coordinator:
        filters.append("a.coordinator=?")
        args.append(coordinator)
    if status_filter == 'Activo':
        filters.append("a.status='Activo'")
    elif status_filter == 'Baja':
        filters.append("a.status='Baja'")
    if search:
        filters.append("(lower(a.agent_id) LIKE ? OR lower(a.full_name) LIKE ?)")
        args.extend([f"%{search}%", f"%{search}%"])
    
    agent_where = (" AND " + " AND ".join(filters)) if filters else ""

    conn = _conn()
    try:
        active = conn.execute(
            "SELECT COUNT(*) FROM hc_agents a WHERE a.status='Activo' "
            "AND (a.termination_date IS NULL OR a.termination_date > ?)" + agent_where,
            [today] + args
        ).fetchone()[0]

        active_new_hires = conn.execute(
            "SELECT COUNT(*) FROM hc_agents a WHERE a.status='Activo' AND a.is_new_hire=1 "
            "AND (a.termination_date IS NULL OR a.termination_date > ?)" + agent_where,
            [today] + args
        ).fetchone()[0]

        term_query_parts = []
        term_args = []
        
        q1 = "SELECT t.agent_id AS agent_id FROM hc_terminations t JOIN hc_agents a ON a.agent_id = t.agent_id WHERE t.termination_date BETWEEN ? AND ?"
        q1_args = [start, end]
        if agent_where:
            q1 += agent_where
            q1_args.extend(args)
        term_query_parts.append(q1)
        term_args.extend(q1_args)

        q2 = "SELECT a.agent_id AS agent_id FROM hc_agents a WHERE a.termination_date BETWEEN ? AND ?"
        q2_args = [start, end]
        if agent_where:
            q2 += agent_where
            q2_args.extend(args)
        term_query_parts.append(q2)
        term_args.extend(q2_args)

        month_start = datetime.now().strftime('%Y-%m-01')
        next_month_start = (datetime.now().replace(day=28) + timedelta(days=4)).replace(day=1).strftime('%Y-%m-%d')
        if start < next_month_start and end >= month_start:
            q3 = "SELECT a.agent_id AS agent_id FROM hc_agents a WHERE a.status = 'Baja' AND a.termination_date IS NULL"
            q3_args = []
            if agent_where:
                q3 += agent_where
                q3_args.extend(args)
            term_query_parts.append(q3)
            term_args.extend(q3_args)

        termination_sql = "SELECT COUNT(DISTINCT agent_id) FROM (" + " UNION ".join(term_query_parts) + ")"
        terminations = conn.execute(termination_sql, term_args).fetchone()[0]

        new_hires = conn.execute(
            "SELECT COUNT(*) FROM hc_agents a WHERE a.hire_date BETWEEN ? AND ?" + agent_where,
            [start, end] + args
        ).fetchone()[0]

        transfer_filters = []
        transfer_args = [start, end]
        if campaign:
            transfer_filters.append("(t.from_campaign=? OR t.to_campaign=?)")
            transfer_args.extend([campaign, campaign])
        if channel:
            transfer_filters.append("(t.from_channel=? OR t.to_channel=?)")
            transfer_args.extend([channel, channel])
        if supervisor:
            transfer_filters.append("(t.from_supervisor=? OR t.to_supervisor=?)")
            transfer_args.extend([supervisor, supervisor])
        if coordinator:
            transfer_filters.append("(t.from_coordinator=? OR t.to_coordinator=?)")
            transfer_args.extend([coordinator, coordinator])
        if search:
            transfer_filters.append("(lower(a.agent_id) LIKE ? OR lower(a.full_name) LIKE ?)")
            transfer_args.extend([f"%{search}%", f"%{search}%"])
        
        transfer_sql = "SELECT COUNT(*) FROM hc_transfers t LEFT JOIN hc_agents a ON a.agent_id=t.agent_id WHERE t.effective_date BETWEEN ? AND ?"
        if transfer_filters:
            transfer_sql += " AND " + " AND ".join(transfer_filters)
        transfers = conn.execute(transfer_sql, transfer_args).fetchone()[0]

        by_campaign = [dict(r) for r in conn.execute(
            "SELECT a.campaign, a.channel, COUNT(*) AS count FROM hc_agents a "
            "WHERE a.status='Activo' AND (a.termination_date IS NULL OR a.termination_date > ?)"
            + agent_where + " GROUP BY a.campaign, a.channel ORDER BY count DESC",
            [today] + args
        ).fetchall()]

        active_at_start = active + terminations - new_hires
        if active_at_start <= 0:
            active_at_start = active
            
        attrition = round((float(terminations) / float(active_at_start) * 100.0), 2) if active_at_start > 0 else 0.0
        net_change = new_hires - terminations

    finally:
        conn.close()

    return jsonify({
        'active': active, 'activeAtStart': active_at_start, 'byCampaign': by_campaign,
        'activeNewHires': active_new_hires,
        'terminations': terminations, 'newHires': new_hires, 'transfers': transfers,
        'attrition': attrition, 'attritionPercent': attrition, 'netChange': net_change, 'periodStart': start,
        'periodEnd': end, 'asOf': today
    }), 200

@hc_bp.route('/api/hc/campaigns', methods=['GET'])
def hc_list_campaigns():
    _init_db()
    conn = _conn()
    try: rows = [r['campaign'] for r in conn.execute("SELECT DISTINCT campaign FROM hc_agents WHERE campaign <> '' ORDER BY campaign").fetchall()]
    finally: conn.close()
    return jsonify({'campaigns': rows}), 200

@hc_bp.route('/api/hc/supervisors', methods=['GET'])
def hc_list_supervisors():
    _init_db()
    conn = _conn()
    try: 
        rows = [r[0] for r in conn.execute("""
            SELECT name FROM hc_people_catalog WHERE role='supervisor'
            UNION
            SELECT DISTINCT supervisor FROM hc_agents WHERE supervisor <> ''
            ORDER BY 1
        """).fetchall()]
    finally: conn.close()
    return jsonify({'supervisors': rows}), 200

@hc_bp.route('/api/hc/template', methods=['GET'])
def hc_download_template():
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['ID Agente', 'Nombre Completo', 'Puesto', 'New Hire', 'Supervisor', 'Coordinador', 'Campaña', 'Canal', 'Estado', 'Fecha Ingreso', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
    
    hoy_ddmmyyyy = datetime.now().strftime('%d/%m/%Y')
    
    writer.writerow([
        'A-00001', 'Nombre Apellido', 'Agente', 'No', 'Supervisor 1', 'Coordinador 1', 
        'Coppel Servicios', 'Llamadas', 'Activo', hoy_ddmmyyyy, 
        '08:00-17:00', '08:00-17:00', '08:00-17:00', '08:00-17:00', '08:00-17:00', 
        'DD-DD', 'DD-DD'
    ])
    return send_file(io.BytesIO(buffer.getvalue().encode('utf-8-sig')), mimetype='text/csv; charset=utf-8', as_attachment=True, download_name='plantilla_hc.csv')

@hc_bp.route('/api/hc/export', methods=['GET'])
def hc_export_csv():
    _init_db()
    conn = _conn()
    try: rows = [_row_to_agent(r) for r in conn.execute("SELECT * FROM hc_agents ORDER BY campaign, full_name, agent_id").fetchall()]
    finally: conn.close()
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['ID Agente', 'Nombre Completo', 'Puesto', 'New Hire', 'Supervisor', 'Coordinador', 'Campaña', 'Canal', 'Estado', 'Fecha Ingreso', 'Fecha Baja', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo', 'Email', 'Número Empleado', 'Notas'])
    for r in rows:
        s = r['schedules']
        writer.writerow([r['agentId'], r['fullName'], r['role'], 'Si' if r['isNewHire'] else 'No', r['supervisor'], r['coordinator'], r['campaign'], r['channel'], r['displayStatus'], r['hireDate'] or '', r['terminationDate'] or '', s['Lunes'], s['Martes'], s['Miércoles'], s['Jueves'], s['Viernes'], s['Sábado'], s['Domingo'], r['email'], r['employeeNumber'], r['notes']])
    return send_file(io.BytesIO(buffer.getvalue().encode('utf-8-sig')), mimetype='text/csv; charset=utf-8', as_attachment=True, download_name=f'HC_export_{_today()}.csv')

@hc_bp.route('/api/hc/import', methods=['POST'])
def hc_import():
    _init_db()
    if 'file' not in request.files: return jsonify({'error': 'Selecciona un archivo CSV o XLSX.'}), 400
    f = request.files['file']
    actor = str(request.form.get('actorMode', 'operaciones'))[:80]
    
    try:
        file_bytes = f.read()
        is_xlsx = file_bytes.startswith(b'PK\x03\x04')
        
        if HAS_PANDAS:
            if is_xlsx:
                df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
            else:
                try:
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding='utf-8-sig', sep=None, engine='python')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding='latin-1', sep=None, engine='python')
        else:
            if is_xlsx:
                return jsonify({'error': 'Para leer Excel instala pandas y openpyxl.'}), 400
            else:
                try:
                    raw = file_bytes.decode('utf-8-sig')
                except UnicodeDecodeError:
                    raw = file_bytes.decode('latin-1')
                sep = ';' if ';' in raw.split('\n')[0] else ','
                df = [dict(r) for r in csv.DictReader(io.StringIO(raw), delimiter=sep)]
    except Exception as e:
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}'}), 400

    imported = 0
    errors = []
    
    if HAS_PANDAS:
        records = df.fillna('').to_dict('records')
    else:
        records = list(df)
    
    normalized_records = []
    for r in records:
        new_r = {}
        for k, v in r.items():
            new_r[_normalize_col(k)] = v
        normalized_records.append(new_r)
    records = normalized_records

    with HC_LOCK:
        conn = _conn()
        try:
            conn.execute('BEGIN')
            for idx, row in enumerate(records):
                try:
                    def get(*keys):
                        for k in keys:
                            nk = _normalize_col(k)
                            if nk in row and row[nk] is not None and str(row[nk]).strip().lower() not in ('', 'nan', 'none'): 
                                return str(row[nk]).strip()
                        return ''
                    
                    agent_id = get('ID Agente', 'agent_id', 'id')
                    campaign = get('Campaña', 'campana')
                    supervisor = get('Supervisor')
                    coordinator = get('Coordinador')
                    role = get('Puesto', 'Rol', 'role') or 'Agente'
                    nh_raw = get('New Hire', 'Nuevo Ingreso').lower()
                    is_new_hire = 1 if nh_raw in ('si', 'sí', '1', 'true', 'yes') else 0
                    
                    if not agent_id or not campaign:
                        raise ValueError('ID y Campaña son obligatorios.')
                        
                    _ensure_people_catalog(conn, supervisor, coordinator)
                        
                    hire_date_raw = get('Fecha Ingreso')
                    hire_date = None
                    if hire_date_raw:
                        try:
                            hire_date = _validate_date(hire_date_raw, 'fecha ingreso')
                        except ValueError:
                            pass 
                            
                    schedules = {}
                    for d in ROSTER_DAYS:
                        val = get(d)
                        try:
                            schedules[d] = _validate_schedule(val)
                        except ValueError:
                            schedules[d] = 'DD-DD'

                    raw_status = get('Estado').lower()
                    is_baja = raw_status in ('baja', 'inactivo')
                    status = 'Baja' if is_baja else 'Activo'
                    term_date = _today() if is_baja else None

                    now = _now()
                    
                    conn.execute("""
                        INSERT INTO hc_agents (
                            agent_id, full_name, role, is_new_hire, supervisor, coordinator, campaign, channel, 
                            hire_date, email, employee_number, notes, 
                            lunes, martes, miercoles, jueves, viernes, sabado, domingo, 
                            status, termination_date, created_at, updated_at, updated_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(agent_id) DO UPDATE SET
                            full_name=excluded.full_name,
                            role=excluded.role,
                            is_new_hire=excluded.is_new_hire,
                            supervisor=excluded.supervisor,
                            coordinator=excluded.coordinator,
                            campaign=excluded.campaign,
                            channel=excluded.channel,
                            hire_date=excluded.hire_date,
                            email=excluded.email,
                            employee_number=excluded.employee_number,
                            notes=excluded.notes,
                            lunes=excluded.lunes,
                            martes=excluded.martes,
                            miercoles=excluded.miercoles,
                            jueves=excluded.jueves,
                            viernes=excluded.viernes,
                            sabado=excluded.sabado,
                            domingo=excluded.domingo,
                            status=excluded.status,
                            termination_date=excluded.termination_date,
                            updated_at=excluded.updated_at,
                            updated_by=excluded.updated_by
                    """, (
                        agent_id, get('Nombre Completo', 'nombre'), role, is_new_hire, supervisor, coordinator,
                        campaign, _normalize_channel(get('Canal')),
                        hire_date, get('Email'), get('Número Empleado'), get('Notas'),
                        schedules['Lunes'], schedules['Martes'], schedules['Miércoles'], schedules['Jueves'],
                        schedules['Viernes'], schedules['Sábado'], schedules['Domingo'],
                        status, term_date,
                        now, now, actor
                    ))

                    if is_baja:
                        conn.execute("DELETE FROM hc_terminations WHERE agent_id = ?", (agent_id,))
                        conn.execute("""
                            INSERT INTO hc_terminations (agent_id, termination_date, reason, rehireable, created_at, created_by)
                            VALUES (?, ?, 'Importación CSV', 1, ?, ?)
                        """, (agent_id, term_date, now, actor))

                    imported += 1
                except Exception as e:
                    errors.append({'row': idx + 2, 'error': str(e)})
                    if len(errors) >= 20: 
                        break 
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            return jsonify({'error': f'Error de base de datos: {str(e)}'}), 500
        finally: 
            conn.close()
            
    return jsonify({'ok': True, 'imported': imported, 'errors': errors}), 200

@hc_bp.route('/api/hc/agents/bulk-terminate', methods=['POST'])
def hc_bulk_terminate():
    _init_db()
    try:
        payload = request.get_json(force=True, silent=False) or {}
        actor = str(payload.get('actorMode', 'operaciones'))[:80]
        ids = payload.get('agentIds') or []
        date = _validate_date(payload.get('terminationDate'), 'fecha de baja')
        reason = str(payload.get('reason') or 'Baja masiva')[:500]
        rehireable = 1 if payload.get('rehireable', True) else 0
        results = []
        with HC_LOCK:
            conn = _conn()
            try:
                for aid in ids:
                    try:
                        aid = str(aid).strip()
                        before = _fetch_agent(conn, aid)
                        if not before: results.append({'agentId': aid, 'status': 'skipped', 'reason': 'No encontrado'}); continue
                        if before.get('terminationDate'): results.append({'agentId': aid, 'status': 'skipped', 'reason': 'Ya tiene baja'}); continue
                        new_status = 'Baja' if date <= _today() else 'Activo'
                        conn.execute("UPDATE hc_agents SET status=?, termination_date=?, termination_reason=?, updated_at=?, updated_by=? WHERE agent_id=?", (new_status, date, reason, _now(), actor, aid))
                        conn.execute("DELETE FROM hc_terminations WHERE agent_id = ?", (aid,))
                        conn.execute("INSERT INTO hc_terminations (agent_id, termination_date, reason, rehireable, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?)", (aid, date, reason, rehireable, _now(), actor))
                        _log_history(conn, aid, 'agent_terminated', before, {'terminationDate': date, 'reason': reason}, reason, date, actor)
                        results.append({'agentId': aid, 'status': 'ok'})
                    except Exception as e: results.append({'agentId': aid, 'status': 'error', 'reason': str(e)})
                conn.commit()
            except Exception: conn.rollback(); raise
            finally: conn.close()
        return jsonify({'ok': True, 'processed': len(results), 'succeeded': sum(1 for r in results if r['status'] == 'ok'), 'results': results}), 200
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e: return jsonify({'error': f'Error en baja masiva: {e}'}), 500

def create_hc_app():
    app = Flask(__name__, static_folder=None)
    app.register_blueprint(hc_bp)
    return app

if __name__ == '__main__':
    _init_db()
    app = create_hc_app()
    port = int(os.environ.get('HC_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('HC_DEBUG', '0') == '1')
