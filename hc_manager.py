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
    created_at TEXT NOT NULL,
    FOREIGN KEY(agent_id) REFERENCES hc_agents(agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hc_campaign ON hc_agents(campaign);
CREATE INDEX IF NOT EXISTS idx_hc_status ON hc_agents(status);
CREATE INDEX IF NOT EXISTS idx_hc_termination ON hc_agents(termination_date);
CREATE INDEX IF NOT EXISTS idx_hc_history_agent ON hc_history(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_term_agent ON hc_terminations(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_transfers_agent ON hc_transfers(agent_id);
CREATE INDEX IF NOT EXISTS idx_hc_absences_agent ON hc_absences(agent_id);
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
        raise ValueError(f'{field} inválida: "{value}". Usa YYYY-MM-DD.')

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

def _log_history(conn, agent_id, change_type, before, after, reason='', effective_date=None, actor='operaciones'):
    conn.execute("""INSERT INTO hc_history (agent_id, change_type, before_json, after_json, reason, effective_date, changed_at, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (agent_id, change_type, json.dumps(before, ensure_ascii=False) if before else None, json.dumps(after, ensure_ascii=False) if after else None, str(reason or '')[:500], effective_date, _now(), str(actor or 'operaciones')[:80]))

def _row_to_agent(row):
    if not row: return None
    today = _today()
    d = {
        'agentId': row['agent_id'], 'fullName': row['full_name'] or '', 'supervisor': row['supervisor'] or '',
        'coordinator': row['coordinator'] or '', 'campaign': row['campaign'] or '', 'channel': row['channel'] or 'Llamadas',
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
    else:
        d['displayStatus'] = 'Activo'
        d['terminationPending'] = False
    return d

def _fetch_agent(conn, agent_id):
    return _row_to_agent(conn.execute("SELECT * FROM hc_agents WHERE agent_id=?", (agent_id,)).fetchone())

def _diff_agents(before, after):
    out = {}
    if not before or not after: return out
    for k in ('fullName', 'supervisor', 'coordinator', 'campaign', 'channel', 'hireDate'):
        if before.get(k) != after.get(k): out[k] = {'before': before.get(k), 'after': after.get(k)}
    if before.get('schedules') != after.get('schedules'): out['schedules'] = {'before': before.get('schedules'), 'after': after.get('schedules')}
    return out

def _save_agent_tx(conn, payload, actor='operaciones', allow_create=True):
    agent_id = str((payload or {}).get('agentId') or '').strip()
    if not agent_id: raise ValueError('El ID del agente es obligatorio.')
    campaign = str((payload or {}).get('campaign') or '').strip()
    if not campaign: raise ValueError('La campaña es obligatoria.')

    before = _fetch_agent(conn, agent_id)
    if before is None and not allow_create: raise ValueError('No se encontró el agente.')

    schedules = payload.get('schedules') or {}
    days = {d: _validate_schedule(schedules.get(d, (before or {}).get('schedules', {}).get(d, 'DD-DD'))) for d in ROSTER_DAYS}

    hire_date = payload.get('hireDate')
    if hire_date: hire_date = _validate_date(hire_date, 'fecha de ingreso')
    elif before is None: hire_date = _today()

    now = _now()
    values = (
        str(payload.get('fullName') or '').strip()[:120], str(payload.get('supervisor') or '').strip()[:120],
        str(payload.get('coordinator') or '').strip()[:120], campaign[:120], _normalize_channel(payload.get('channel')),
        hire_date, str(payload.get('email') or '').strip()[:120], str(payload.get('employeeNumber') or '').strip()[:40],
        str(payload.get('notes') or '').strip()[:500], days['Lunes'], days['Martes'], days['Miércoles'],
        days['Jueves'], days['Viernes'], days['Sábado'], days['Domingo'], now, actor,
    )

    if before is None:
        conn.execute("""INSERT INTO hc_agents (agent_id, full_name, supervisor, coordinator, campaign, channel, hire_date, email, employee_number, notes, lunes, martes, miercoles, jueves, viernes, sabado, domingo, status, created_at, updated_at, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Activo', ?, ?, ?)""", (agent_id,) + values)
        _log_history(conn, agent_id, 'agent_created', None, {'fullName': values[0], 'campaign': values[3], 'channel': values[4], 'supervisor': values[1], 'coordinator': values[2], 'hireDate': hire_date}, str(payload.get('reason') or 'Alta de agente'), hire_date, actor)
    else:
        conn.execute("""UPDATE hc_agents SET full_name=?, supervisor=?, coordinator=?, campaign=?, channel=?, hire_date=?, email=?, employee_number=?, notes=?, lunes=?, martes=?, miercoles=?, jueves=?, viernes=?, sabado=?, domingo=?, updated_at=?, updated_by=? WHERE agent_id=?""", values + (agent_id,))
        after = _fetch_agent(conn, agent_id)
        if _diff_agents(before, after): _log_history(conn, agent_id, 'agent_updated', before, after, str(payload.get('reason') or 'Actualización'), None, actor)
    return _fetch_agent(conn, agent_id)

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
    if status_filter == 'Activo':
        sql += " AND status='Activo'"
    elif status_filter == 'Baja':
        sql += " AND status='Baja'"
        
    if start_date and end_date:
        sql += " AND (hire_date IS NULL OR hire_date <= ?) AND (termination_date IS NULL OR termination_date >= ?)"
        args.extend([end_date, start_date])
        
    sql += " ORDER BY campaign, supervisor, full_name, agent_id"

    conn = _conn()
    try:
        rows = [_row_to_agent(r) for r in conn.execute(sql, args).fetchall()]
        
        # Filtrado especial para Vacaciones e Incapacidades
        if status_filter in ('Vacación', 'Incapacidad'):
            today = _today()
            absences = {r['agent_id']: r['absence_type'] for r in conn.execute("SELECT agent_id, absence_type FROM hc_absences WHERE start_date <= ? AND end_date >= ?", (today, today))}
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
        rows = [r for r in rows if search in f"{r['agentId']} {r['fullName']} {r['supervisor']} {r['coordinator']} {r['campaign']} {r['email']}".lower()]

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
        with HC_LOCK:
            conn = _conn()
            try:
                agent = _save_agent_tx(conn, payload, actor, allow_create=True)
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
        payload['agentId'] = agent_id
        actor = str(payload.pop('actorMode', 'operaciones'))[:80]
        with HC_LOCK:
            conn = _conn()
            try:
                agent = _save_agent_tx(conn, payload, actor, allow_create=False)
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
                if not before.get('terminationDate'): raise ValueError('El agente ya está activo.')
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
        if not to_campaign: raise ValueError('La campaña destino es obligatoria.')
        effective = _validate_date(payload.get('effectiveDate'), 'fecha efectiva')
        reason = str(payload.get('reason') or '')[:500]
        to_channel = _normalize_channel(payload.get('toChannel') or '')

        with HC_LOCK:
            conn = _conn()
            try:
                before = _fetch_agent(conn, agent_id)
                if not before: raise ValueError('Agente no encontrado.')
                if before['campaign'] == to_campaign and before['channel'] == to_channel: raise ValueError('El agente ya está en esa campaña y canal.')
                conn.execute("UPDATE hc_agents SET campaign=?, channel=?, updated_at=?, updated_by=? WHERE agent_id=?", (to_campaign, to_channel, _now(), actor, agent_id))
                conn.execute("INSERT INTO hc_transfers (agent_id, from_campaign, to_campaign, from_channel, to_channel, effective_date, reason, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (agent_id, before['campaign'], to_campaign, before['channel'], to_channel, effective, reason, _now(), actor))
                _log_history(conn, agent_id, 'campaign_transferred', before, {'fromCampaign': before['campaign'], 'toCampaign': to_campaign, 'fromChannel': before['channel'], 'toChannel': to_channel, 'effectiveDate': effective}, reason, effective, actor)
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

@hc_bp.route('/api/hc/terminations', methods=['GET'])
def hc_list_terminations():
    _init_db()
    conn = _conn()
    try: rows = [dict(r) for r in conn.execute("SELECT t.*, a.full_name, a.campaign, a.channel, a.supervisor FROM hc_terminations t LEFT JOIN hc_agents a ON a.agent_id=t.agent_id ORDER BY t.termination_date DESC LIMIT 1000").fetchall()]
    finally: conn.close()
    return jsonify({'terminations': rows}), 200

@hc_bp.route('/api/hc/transfers', methods=['GET'])
def hc_list_transfers():
    _init_db()
    conn = _conn()
    try: rows = [dict(r) for r in conn.execute("SELECT t.*, a.full_name, a.supervisor FROM hc_transfers t LEFT JOIN hc_agents a ON a.agent_id=t.agent_id ORDER BY t.effective_date DESC LIMIT 1000").fetchall()]
    finally: conn.close()
    return jsonify({'transfers': rows}), 200

@hc_bp.route('/api/hc/metrics', methods=['GET'])
def hc_metrics():
    _init_db()
    today = _today()
    default_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    start = _validate_date(request.args.get('start', default_start), 'start')
    end = _validate_date(request.args.get('end', today), 'end')

    conn = _conn()
    try:
        active = conn.execute("SELECT COUNT(*) FROM hc_agents WHERE status='Activo' AND (termination_date IS NULL OR termination_date > ?)", (today,)).fetchone()[0]
        active_at_start = conn.execute("SELECT COUNT(*) FROM hc_agents WHERE (hire_date IS NULL OR hire_date <= ?) AND (termination_date IS NULL OR termination_date > ?)", (start, start)).fetchone()[0]
        by_campaign = [dict(r) for r in conn.execute("SELECT campaign, channel, COUNT(*) AS count FROM hc_agents WHERE status='Activo' AND (termination_date IS NULL OR termination_date > ?) GROUP BY campaign, channel ORDER BY count DESC", (today,)).fetchall()]
        terminations = conn.execute("SELECT COUNT(*) FROM hc_terminations WHERE termination_date BETWEEN ? AND ?", (start, end)).fetchone()[0]
        new_hires = conn.execute("SELECT COUNT(*) FROM hc_agents WHERE hire_date BETWEEN ? AND ?", (start, end)).fetchone()[0]
        transfers = conn.execute("SELECT COUNT(*) FROM hc_transfers WHERE effective_date BETWEEN ? AND ?", (start, end)).fetchone()[0]
        attrition = round((terminations / active_at_start * 100), 2) if active_at_start > 0 else 0.0
        net_change = new_hires - terminations
    finally: conn.close()

    return jsonify({'active': active, 'activeAtStart': active_at_start, 'byCampaign': by_campaign, 'terminations': terminations, 'newHires': new_hires, 'transfers': transfers, 'attrition': attrition, 'netChange': net_change, 'periodStart': start, 'periodEnd': end, 'asOf': today}), 200

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
    try: rows = [r['supervisor'] for r in conn.execute("SELECT DISTINCT supervisor FROM hc_agents WHERE supervisor <> '' ORDER BY supervisor").fetchall()]
    finally: conn.close()
    return jsonify({'supervisors': rows}), 200

@hc_bp.route('/api/hc/template', methods=['GET'])
def hc_download_template():
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['ID Agente', 'Nombre Completo', 'Supervisor', 'Coordinador', 'Campaña', 'Canal', 'Estado', 'Fecha Ingreso', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo', 'Email', 'Número Empleado', 'Notas'])
    writer.writerow(['A-00001', 'Nombre Apellido', 'Supervisor 1', 'Coordinador 1', 'Coppel Servicios', 'Llamadas', 'Activo', _today(), '08:00-17:00', '08:00-17:00', '08:00-17:00', '08:00-17:00', '08:00-17:00', 'DD-DD', 'DD-DD', '', '', ''])
    return send_file(io.BytesIO(buffer.getvalue().encode('utf-8-sig')), mimetype='text/csv; charset=utf-8', as_attachment=True, download_name='plantilla_hc.csv')

@hc_bp.route('/api/hc/export', methods=['GET'])
def hc_export_csv():
    _init_db()
    conn = _conn()
    try: rows = [_row_to_agent(r) for r in conn.execute("SELECT * FROM hc_agents ORDER BY campaign, full_name, agent_id").fetchall()]
    finally: conn.close()
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['ID Agente', 'Nombre Completo', 'Supervisor', 'Coordinador', 'Campaña', 'Canal', 'Estado', 'Fecha Ingreso', 'Fecha Baja', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo', 'Email', 'Número Empleado', 'Notas'])
    for r in rows:
        s = r['schedules']
        writer.writerow([r['agentId'], r['fullName'], r['supervisor'], r['coordinator'], r['campaign'], r['channel'], r['displayStatus'], r['hireDate'] or '', r['terminationDate'] or '', s['Lunes'], s['Martes'], s['Miércoles'], s['Jueves'], s['Viernes'], s['Sábado'], s['Domingo'], r['email'], r['employeeNumber'], r['notes']])
    return send_file(io.BytesIO(buffer.getvalue().encode('utf-8-sig')), mimetype='text/csv; charset=utf-8', as_attachment=True, download_name=f'HC_export_{_today()}.csv')

@hc_bp.route('/api/hc/import', methods=['POST'])
def hc_import():
    _init_db()
    if 'file' not in request.files: return jsonify({'error': 'Selecciona un archivo CSV o XLSX.'}), 400
    f = request.files['file']
    name = (f.filename or '').lower()
    actor = str(request.form.get('actorMode', 'operaciones'))[:80]
    try:
        if name.endswith('.csv'):
            raw = f.read().decode('utf-8-sig')
            df = pd.read_csv(io.StringIO(raw)) if HAS_PANDAS else [dict(r) for r in csv.DictReader(io.StringIO(raw))]
        elif name.endswith('.xlsx'):
            if not HAS_PANDAS: return jsonify({'error': 'Instala pandas y openpyxl.'}), 400
            df = pd.read_excel(f, engine='openpyxl')
        else: return jsonify({'error': 'Formato no soportado.'}), 400
    except Exception as e: return jsonify({'error': f'Error al leer archivo: {e}'}), 400
    imported = 0; errors = []
    records = df.to_dict('records') if hasattr(df, 'to_dict') else list(df)
    with HC_LOCK:
        conn = _conn()
        try:
            for idx, row in enumerate(records):
                try:
                    def get(*keys):
                        for k in keys:
                            if k in row and row[k] is not None and str(row[k]).strip().lower() not in ('', 'nan', 'none'): return row[k]
                        return ''
                    payload = {'agentId': str(get('ID Agente', 'agent_id', 'id')).strip(), 'fullName': str(get('Nombre Completo', 'nombre')).strip(), 'supervisor': str(get('Supervisor')).strip(), 'coordinator': str(get('Coordinador')).strip(), 'campaign': str(get('Campaña', 'campana')).strip(), 'channel': str(get('Canal')).strip() or 'Llamadas', 'hireDate': str(get('Fecha Ingreso')).strip() or None, 'email': str(get('Email')).strip(), 'employeeNumber': str(get('Número Empleado')).strip(), 'notes': str(get('Notas')).strip(), 'reason': f'Importación fila {idx+2}', 'schedules': {'Lunes': str(get('Lunes')).strip() or 'DD-DD', 'Martes': str(get('Martes')).strip() or 'DD-DD', 'Miércoles': str(get('Miércoles', 'Miercoles')).strip() or 'DD-DD', 'Jueves': str(get('Jueves')).strip() or 'DD-DD', 'Viernes': str(get('Viernes')).strip() or 'DD-DD', 'Sábado': str(get('Sábado', 'Sabado')).strip() or 'DD-DD', 'Domingo': str(get('Domingo')).strip() or 'DD-DD'}}
                    if not payload['agentId'] or not payload['campaign']: raise ValueError('ID y Campaña obligatorios.')
                    _save_agent_tx(conn, payload, actor, allow_create=True); imported += 1
                except Exception as e:
                    errors.append({'row': idx + 2, 'error': str(e)})
                    if len(errors) >= 50: break
            conn.commit()
        except Exception:
            conn.rollback(); raise
        finally: conn.close()
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
