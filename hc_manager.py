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
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding='utf-8-sig')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding='latin-1')
        else:
            if is_xlsx:
                return jsonify({'error': 'Para leer Excel instala pandas y openpyxl.'}), 400
            else:
                try:
                    raw = file_bytes.decode('utf-8-sig')
                except UnicodeDecodeError:
                    raw = file_bytes.decode('latin-1')
                df = [dict(r) for r in csv.DictReader(io.StringIO(raw))]
    except Exception as e:
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}'}), 400

    imported = 0
    errors = []
    records = df.fillna('').to_dict('records') if HAS_PANDAS else list(df)
    
    with HC_LOCK:
        conn = _conn()
        try:
            # Aceleramos la transacción
            conn.execute('BEGIN')
            
            for idx, row in enumerate(records):
                try:
                    def get(*keys):
                        for k in keys:
                            if k in row and row[k] is not None and str(row[k]).strip().lower() not in ('', 'nan', 'none'): 
                                return str(row[k]).strip()
                        return ''
                    
                    agent_id = get('ID Agente', 'agent_id', 'id')
                    campaign = get('Campaña', 'campana')
                    
                    if not agent_id or not campaign:
                        raise ValueError('ID y Campaña son obligatorios.')
                        
                    # Validar fecha de ingreso
                    hire_date_raw = get('Fecha Ingreso')
                    hire_date = _today()
                    if hire_date_raw:
                        try:
                            hire_date = _validate_date(hire_date_raw, 'fecha ingreso')
                        except ValueError:
                            pass 
                            
                    # Validar horarios (si fallan, ponemos DD-DD por defecto)
                    schedules = {}
                    for d in ROSTER_DAYS:
                        val = get(d, d.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u'))
                        try:
                            schedules[d] = _validate_schedule(val)
                        except ValueError:
                            schedules[d] = 'DD-DD'

                    now = _now()
                    
                    # Inserción ultra rápida usando UPSERT de SQLite
                    conn.execute("""
                        INSERT INTO hc_agents (
                            agent_id, full_name, supervisor, coordinator, campaign, channel, 
                            hire_date, email, employee_number, notes, 
                            lunes, martes, miercoles, jueves, viernes, sabado, domingo, 
                            status, created_at, updated_at, updated_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Activo', ?, ?, ?)
                        ON CONFLICT(agent_id) DO UPDATE SET
                            full_name=excluded.full_name,
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
                            updated_at=excluded.updated_at,
                            updated_by=excluded.updated_by
                    """, (
                        agent_id, get('Nombre Completo', 'nombre'), get('Supervisor'), get('Coordinador'),
                        campaign, _normalize_channel(get('Canal')),
                        hire_date, get('Email'), get('Número Empleado'), get('Notas'),
                        schedules['Lunes'], schedules['Martes'], schedules['Miércoles'], schedules['Jueves'],
                        schedules['Viernes'], schedules['Sábado'], schedules['Domingo'],
                        now, actor
                    ))
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
