import streamlit as st
import sys, io
sys.path.insert(0, ".")
import pandas as pd
from models.database import init_db
import services.electoral as svc

init_db()

st.set_page_config(page_title="Sistema Electoral", page_icon="🗳️", layout="wide")
st.markdown("""
<style>
[data-testid="stSidebar"] { background-color: #f0f2f6; }
.metric-card { border-radius:10px;padding:16px;color:white;text-align:center;margin-bottom:10px; }
</style>
""", unsafe_allow_html=True)

# ── SIDEBAR ──────────────────────────────────────────────────────
st.sidebar.title("🗳️ Sistema Electoral")
pagina = st.sidebar.radio("Navegación", [
    "📊 Dashboard",
    "📋 Censo Electoral",
    "🗳️ Registro de Votantes",
    "📥 Carga Masiva",
    "📈 Estadísticos",
    "👥 Líderes",
    "🗑️ Gestión / Borrado",
    "🔍 Consulta de Cédula",
], index=0)

st.sidebar.divider()
_sc = svc.stats_censo()
hay_censo    = _sc["total_padron"] > 0
hay_votantes = _sc["total_votantes"] > 0

st.sidebar.metric("📋 Padrón", f"{_sc['total_padron']:,}")
cs1, cs2 = st.sidebar.columns(2)
cs1.metric("✅ Disponibles", f"{_sc['disponibles']:,}")
cs2.metric("🚫 Ya votaron",  f"{_sc['inhabilitadas']:,}")
st.sidebar.metric("🗳️ Votantes reg.", f"{_sc['total_votantes']:,}")
if hay_censo:
    st.sidebar.progress(_sc["cobertura_pct"]/100, text=f"Cobertura {_sc['cobertura_pct']}%")
else:
    st.sidebar.warning("⚠️ Sin censo cargado")


# ═══════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════
if pagina == "📊 Dashboard":
    st.title("📊 Dashboard Electoral")

    if not hay_censo:
        st.markdown("""<div style="background:#fff3cd;border:2px solid #ffc107;border-radius:10px;padding:20px;margin-bottom:20px">
<h4 style="margin:0;color:#856404">⚠️ Paso 1: Carga el Censo Electoral</h4>
<p style="margin:8px 0 0;color:#856404">Ve a <b>📋 Censo Electoral</b> y carga el padrón con cédulas, nombres y líderes.</p>
</div>""", unsafe_allow_html=True)
    elif not hay_votantes:
        st.markdown("""<div style="background:#d1ecf1;border:2px solid #17a2b8;border-radius:10px;padding:20px;margin-bottom:20px">
<h4 style="margin:0;color:#0c5460">✅ Censo listo — Paso 2: Registra Votantes</h4>
<p style="margin:8px 0 0;color:#0c5460">Ve a <b>🗳️ Registro de Votantes</b> y digita cédulas para registrar uno a uno.</p>
</div>""", unsafe_allow_html=True)

    k1,k2,k3,k4 = st.columns(4)
    k1.markdown(f'<div class="metric-card" style="background:#1e3a5f"><div style="font-size:2rem;font-weight:bold">{_sc["total_votantes"]:,}</div><div>Total Votantes</div></div>', unsafe_allow_html=True)
    k2.markdown(f'<div class="metric-card" style="background:#2d6a4f"><div style="font-size:2rem;font-weight:bold">{_sc["disponibles"]:,}</div><div>Disponibles</div></div>', unsafe_allow_html=True)
    k3.markdown(f'<div class="metric-card" style="background:#c0392b"><div style="font-size:2rem;font-weight:bold">{_sc["inhabilitadas"]:,}</div><div>Ya Votaron</div></div>', unsafe_allow_html=True)
    lideres_all = svc.listar_lideres()
    k4.markdown(f'<div class="metric-card" style="background:#7f4f24"><div style="font-size:2rem;font-weight:bold">{len(lideres_all)}</div><div>Líderes</div></div>', unsafe_allow_html=True)

    if hay_censo:
        st.progress(_sc["cobertura_pct"]/100,
                    text=f"Cobertura: {_sc['cobertura_pct']}% — {_sc['inhabilitadas']:,} de {_sc['total_padron']:,}")

    if lideres_all:
        st.divider()
        st.subheader("🏆 Ranking de líderes")
        stats_l = svc.estadisticos_por_lider()
        for i, l in enumerate(stats_l, 1):
            tc    = l["total_censo"] or 1
            pct_b = l["registrados"] / tc * 100
            med   = {1:"🥇",2:"🥈",3:"🥉"}.get(i, f"#{i}")
            color = "#1e3a5f" if i<=3 else "#4a7fa5"
            st.markdown(f"""<div style="margin-bottom:8px;padding:10px 14px;background:#f8f9fa;border-radius:8px;border-left:4px solid {color}">
  <div style="display:flex;justify-content:space-between;margin-bottom:5px">
    <span style="font-weight:600;font-size:0.88rem">{med} {l["nombre"]}</span>
    <span style="font-weight:700;color:{color}">{l["registrados"]}<span style="font-weight:400;color:#666">/{l["total_censo"]}</span>
      <span style="font-size:0.72rem;color:#999;margin-left:6px">{l["pct_avance"]}%</span>
    </span>
  </div>
  <div style="background:#dde3ea;border-radius:4px;height:6px"><div style="background:{color};width:{pct_b:.1f}%;height:6px;border-radius:4px"></div></div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# CENSO ELECTORAL
# ═══════════════════════════════════════════════════════════════════
elif pagina == "📋 Censo Electoral":
    st.title("📋 Censo Electoral")
    st.caption("**Paso 1 obligatorio.** Carga el padrón completo con cédula, nombre y líder. Las cédulas entran como DISPONIBLES.")

    sc = svc.stats_censo()
    k1,k2,k3,k4 = st.columns(4)
    k1.markdown(f'<div class="metric-card" style="background:#1e3a5f"><div style="font-size:2rem;font-weight:bold">{sc["total_padron"]:,}</div><div>Total padrón</div></div>', unsafe_allow_html=True)
    k2.markdown(f'<div class="metric-card" style="background:#2d6a4f"><div style="font-size:2rem;font-weight:bold">{sc["disponibles"]:,}</div><div>✅ Disponibles</div></div>', unsafe_allow_html=True)
    k3.markdown(f'<div class="metric-card" style="background:#c0392b"><div style="font-size:2rem;font-weight:bold">{sc["inhabilitadas"]:,}</div><div>🚫 Ya votaron</div></div>', unsafe_allow_html=True)
    k4.markdown(f'<div class="metric-card" style="background:#7f4f24"><div style="font-size:2rem;font-weight:bold">{sc["cobertura_pct"]}%</div><div>Cobertura</div></div>', unsafe_allow_html=True)

    if sc["total_padron"] > 0:
        st.markdown("<br>", unsafe_allow_html=True)
        st.progress(sc["cobertura_pct"]/100,
                    text=f"🗳️ {sc['inhabilitadas']:,} de {sc['total_padron']:,} ya votaron ({sc['cobertura_pct']}%)")

    st.divider()
    tab_cargar, tab_borrar, tab_dup, tab_buscar = st.tabs(["⬆️ Cargar Padrón", "🗑️ Borrar Líder del Censo", "⚠️ Duplicados", "🔍 Buscar Cédula"])

    # ── TAB CARGAR ────────────────────────────────────────────────
    with tab_cargar:
        st.subheader("Cargar padrón electoral completo")
        st.info("📌 El archivo debe tener columnas: **cédula**, **nombre** y **líder**. Los líderes se crean automáticamente.")

        arch_c = st.file_uploader("Selecciona archivo CSV o Excel", type=["csv","xlsx","xls"], key="up_censo")

        if arch_c:
            try:
                raw   = arch_c.read()
                narch = arch_c.name.lower()

                if narch.endswith((".xlsx",".xls")):
                    xf = pd.ExcelFile(io.BytesIO(raw))
                    mejor, mejor_n = None, 0
                    for h in xf.sheet_names:
                        try:
                            t = pd.read_excel(io.BytesIO(raw), sheet_name=h, nrows=3, dtype=str)
                            if len(t) > mejor_n: mejor_n, mejor = len(t), h
                        except: pass
                    df_c = pd.read_excel(io.BytesIO(raw), sheet_name=mejor, dtype=str)
                    st.caption(f"Excel · hoja: **{mejor}**")
                else:
                    sep2 = st.selectbox("Separador", [",",";","|"], key="sep_censo")
                    df_c = pd.read_csv(io.BytesIO(raw), sep=sep2, dtype=str)

                # Normalizar columnas
                cols_n, cnt = [], {}
                for c in df_c.columns:
                    cn = c.strip().lower()
                    if cn in cnt: cnt[cn]+=1; cn=f"{cn}_{cnt[cn]}"
                    else: cnt[cn]=0
                    cols_n.append(cn)
                df_c.columns = cols_n
                df_c = df_c.dropna(how="all").reset_index(drop=True)

                alias_ced = {"cedula","cc","documento","id","num_doc"}
                alias_nom = {"nombre","nombre_votante","nombres","name","nombre completo"}
                alias_lid = {"lider","lider_nombre","nombre_lider","leader"}
                alias_src = {"source.name","source_name"}

                col_ced = next((c for c in df_c.columns if c in alias_ced), None)
                col_nom = next((c for c in df_c.columns if c in alias_nom), None)
                col_lid = next((c for c in df_c.columns if c in alias_lid), None)
                col_src = next((c for c in df_c.columns if c in alias_src), None)

                if not col_ced:
                    st.error(f"❌ No se encontró columna de cédula. Columnas disponibles: {list(df_c.columns)}")
                    st.stop()
                if not col_nom:
                    st.error(f"❌ No se encontró columna de nombre. Columnas disponibles: {list(df_c.columns)}")
                    st.stop()
                if not col_lid and not col_src:
                    st.error(f"❌ No se encontró columna de líder. Columnas disponibles: {list(df_c.columns)}")
                    st.stop()

                st.success(f"✅ cédula:`{col_ced}` · nombre:`{col_nom}` · líder:`{col_src or col_lid}`")
                st.dataframe(df_c.head(8), use_container_width=True, hide_index=True)

                # Detectar líderes únicos
                if col_src:
                    lids_unicos = sorted({svc.extraer_nombre_lider(v)
                                          for v in df_c[col_src].dropna().unique()
                                          if str(v).strip().lower() not in ("nan","none","")})
                else:
                    lids_unicos = sorted(df_c[col_lid].str.strip().dropna().unique().tolist())

                sis_existentes = {l["nombre"].lower() for l in svc.listar_lideres()}
                nuevos_lids = [n for n in lids_unicos if n.lower() not in sis_existentes]

                cv1, cv2, cv3 = st.columns(3)
                cv1.metric("Total filas", f"{len(df_c):,}")
                cv2.metric("Líderes existentes", len(lids_unicos)-len(nuevos_lids))
                cv3.metric("Líderes nuevos", len(nuevos_lids), delta=f"+{len(nuevos_lids)}" if nuevos_lids else None)

                if nuevos_lids:
                    with st.expander(f"👥 {len(lids_unicos)} líderes detectados"):
                        for n in lids_unicos:
                            st.markdown(f"{'🟢' if n.lower() in sis_existentes else '🆕'} {n}")

                st.divider()
                conf_c = st.checkbox("Confirmo cargar este padrón electoral.")
                if st.button(f"📋 Cargar {len(df_c):,} registros al censo",
                             disabled=not conf_c, type="primary", use_container_width=True):

                    # Crear líderes nuevos
                    lider_id_map = {}
                    if nuevos_lids:
                        with st.spinner(f"Creando {len(nuevos_lids)} líderes..."):
                            for n in nuevos_lids: svc.crear_lider(n)
                    for l in svc.listar_lideres():
                        lider_id_map[l["nombre"].strip().lower()] = l["id"]

                    # Preparar filas
                    barra = st.progress(0, text="Cargando censo...")
                    total_f = len(df_c); LOTE=500
                    acc = {"nuevas":0,"ya_existe":0,"invalidas":0}
                    batch_results = []

                    for ini in range(0, total_f, LOTE):
                        fin  = min(ini+LOTE, total_f)
                        lote_filas = []
                        for _, row in df_c.iloc[ini:fin].iterrows():
                            ced = str(row[col_ced]).strip().replace(".0","")
                            nom = str(row[col_nom]).strip() if col_nom else ""
                            if col_src:
                                sv = str(row[col_src]).strip()
                                lid_nombre = svc.extraer_nombre_lider(sv) if sv.lower() not in ("nan","none","") else ""
                            else:
                                lid_nombre = str(row[col_lid]).strip() if col_lid else ""
                            lid_id = lider_id_map.get(lid_nombre.lower())
                            lote_filas.append({"cedula":ced,"nombre":nom,"lider_id":lid_id})

                        res = svc.cargar_censo_masivo(lote_filas)
                        batch_results.append(res)
                        for k in acc: acc[k] += res[k]
                        barra.progress(int(fin/total_f*100), text=f"Procesando {fin:,}/{total_f:,}...")

                    barra.progress(100, text="✅ Completado")
                    st.divider()
                    ra, rb, rc = st.columns(3)
                    ra.metric("✅ Nuevas en padrón", f"{acc['nuevas']:,}")
                    rb.metric("ℹ️ Ya existían", f"{acc['ya_existe']:,}")
                    rc.metric("⚠️ Inválidas", f"{acc['invalidas']:,}")
                    # Guardar duplicados en session_state para la pestaña
                    todos_dup = []
                    for batch_res in batch_results:
                        todos_dup.extend(batch_res.get("duplicados", []))
                    st.session_state["ultimo_censo_duplicados"] = todos_dup

                    if acc["nuevas"] > 0:
                        st.success(f"🎉 {acc['nuevas']:,} cédulas cargadas. {len(lids_unicos)} líderes listos.")
                        if todos_dup:
                            st.warning(f"⚠️ {len(todos_dup)} duplicados detectados — revísalos en la pestaña **⚠️ Duplicados**.")
                        st.balloons()
                    else:
                        st.info("No se agregaron registros nuevos.")

            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

    # ── TAB BORRAR LÍDER DEL CENSO ─────────────────────────────────
    with tab_borrar:
        st.subheader("Borrar un líder completo del censo")
        st.warning("⚠️ Esta acción borra el líder, todas sus cédulas del padrón y todos sus votantes registrados.")

        lideres = svc.listar_lideres()
        if not lideres:
            st.info("No hay líderes en el sistema.")
        else:
            lider_sel_censo = st.selectbox(
                "Seleccionar líder a eliminar del censo",
                [(l["id"], l["nombre"], l["total_votantes"]) for l in lideres],
                format_func=lambda x: f"{x[1]}  ({x[2]} votantes registrados)"
            )

            # Contar cédulas en el censo de este líder
            # contar cedulas de ese lider en el censo
            from sqlalchemy import select as _sel
            from models.database import ControlCedula as _CC, SessionLocal as _SL
            from sqlalchemy import func as _func
            with _SL() as _s:
                n_cedulas = _s.execute(
                    _sel(_func.count(_CC.cedula)).where(_CC.lider_id==lider_sel_censo[0])
                ).scalar() or 0

            st.markdown(f"""<div style="background:#f8d7da;border:2px solid #dc3545;border-radius:10px;padding:16px;margin:12px 0">
<h4 style="color:#721c24;margin:0">🚨 Se eliminará todo lo referenciado a <b>{lider_sel_censo[1]}</b></h4>
<ul style="color:#721c24;margin:8px 0 0 0">
<li>{n_cedulas:,} cédulas del padrón electoral</li>
<li>{lider_sel_censo[2]:,} votantes registrados</li>
<li>El líder mismo</li>
</ul>
</div>""", unsafe_allow_html=True)

            conf_borrar_censo = st.checkbox(f"Confirmo eliminar completamente a {lider_sel_censo[1]} del sistema")
            if st.button("🗑️ Eliminar líder del censo", disabled=not conf_borrar_censo,
                         type="primary", use_container_width=True):
                r = svc.borrar_lider_del_censo(lider_sel_censo[0])
                if r.ok: st.success(r.mensaje); st.rerun()
                else: st.error(r.mensaje)

    # ── TAB DUPLICADOS ───────────────────────────────────────────
    with tab_dup:
        st.subheader("⚠️ Cédulas duplicadas de la última carga")
        st.caption("Estas cédulas ya existían en el sistema cuando cargaste el censo. Puedes revisarlas y decidir si las liberas a DISPONIBLE.")

        dup_list = st.session_state.get("ultimo_censo_duplicados", [])

        if not dup_list:
            st.info("No hay duplicados registrados. Carga un censo para ver si hay duplicados.")
        else:
            df_dup = pd.DataFrame(dup_list)[["cedula","nombre","lider","estado","motivo"]]
            df_dup.columns = ["Cédula","Nombre","Líder","Estado actual","Motivo"]

            # Métricas rápidas
            d1,d2,d3 = st.columns(3)
            d1.metric("Total duplicados", len(df_dup))
            d2.metric("INHABILITADAS", len(df_dup[df_dup["Estado actual"]=="INHABILITADA"]))
            d3.metric("DISPONIBLES", len(df_dup[df_dup["Estado actual"]=="DISPONIBLE"]))

            st.dataframe(df_dup, use_container_width=True, hide_index=True)

            # Exportar
            st.download_button(
                "⬇️ Exportar duplicados CSV",
                df_dup.to_csv(index=False).encode("utf-8"),
                "duplicados_censo.csv", "text/csv"
            )

            # Solo mostrar opción de liberar si hay inhabilitadas
            inhabilitadas = [r["cedula"] for r in dup_list if r["estado"] == "INHABILITADA"]
            if inhabilitadas:
                st.divider()
                st.markdown(f"**{len(inhabilitadas)} cédulas INHABILITADAS** — ¿Deseas liberarlas a DISPONIBLE?")
                st.caption("Esto las habilitará para ser registradas como votantes nuevamente.")
                if st.button(f"🔓 Liberar {len(inhabilitadas)} cédulas a DISPONIBLE",
                             type="primary", use_container_width=True, key="btn_liberar_dup"):
                    r = svc.liberar_cedulas_a_disponible(inhabilitadas)
                    if r.ok:
                        st.success(r.mensaje)
                        # Actualizar estado en session_state
                        for item in st.session_state["ultimo_censo_duplicados"]:
                            if item["cedula"] in inhabilitadas:
                                item["estado"] = "DISPONIBLE"
                        st.rerun()
                    else:
                        st.error(r.mensaje)
            else:
                st.success("✅ Todos los duplicados ya están en estado DISPONIBLE.")

    # ── TAB BUSCAR ────────────────────────────────────────────────
    with tab_buscar:
        st.subheader("Consultar cédula en el padrón")
        ced_b = st.text_input("Número de cédula", placeholder="Ej: 1090123456", key="ced_censo_buscar")
        if st.button("🔍 Consultar", key="btn_censo_b"):
            if not ced_b.strip():
                st.warning("Ingresa una cédula.")
            else:
                info = svc.buscar_cedula(ced_b.strip())
                ctrl = info["control"]; vot = info["votante"]
                bc1, bc2 = st.columns(2)
                with bc1:
                    st.markdown("#### 📋 En el padrón")
                    if ctrl["existe"]:
                        st.markdown(f"**Nombre:** {ctrl['nombre']}")
                        lmap = {l["id"]:l["nombre"] for l in svc.listar_lideres()}
                        st.markdown(f"**Líder:** {lmap.get(ctrl['lider_id'],'N/A')}")
                        if ctrl["estado"] == "DISPONIBLE":
                            st.success("🟢 **DISPONIBLE** — Habilitada para votar")
                        else:
                            st.error("🔴 **INHABILITADA** — Ya fue utilizada")
                            if ctrl["fecha_inhabilitacion"]: st.caption(f"Fecha: {ctrl['fecha_inhabilitacion']}")
                    else:
                        st.warning("⚠️ No está en el padrón electoral")
                with bc2:
                    st.markdown("#### 🗳️ Como votante")
                    if vot["registrado"]:
                        st.error("🚫 Ya registrada como votante")
                        st.markdown(f"**Fecha:** {vot['fecha_registro']}")
                    else:
                        st.success("✅ Pendiente de registrar")


# ═══════════════════════════════════════════════════════════════════
# REGISTRO DE VOTANTES
# ═══════════════════════════════════════════════════════════════════
elif pagina == "🗳️ Registro de Votantes":
    st.title("🗳️ Registro Individual de Votantes")
    st.caption("Digita la cédula — el sistema trae nombre y líder del censo automáticamente.")

    if not hay_censo:
        st.warning("⚠️ Primero debes cargar el **Censo Electoral**.")
        st.stop()

    col_form, _, col_rank = st.columns([2, 0.15, 2])

    with col_form:
        st.markdown("### 📝 Formulario de registro")

        cedula_input = st.text_input(
            "🪪 Número de cédula",
            placeholder="Digita la cédula...",
            max_chars=20, key="ced_reg"
        )

        info_censo = None
        cedula_disponible = False
        nombre_auto   = ""
        lider_auto_id = None
        lider_auto_nom = ""

        if cedula_input.strip():
            info_censo = svc.buscar_en_censo(cedula_input.strip())
            if info_censo["encontrado"]:
                if info_censo["estado"] == "DISPONIBLE":
                    cedula_disponible = True
                    nombre_auto    = info_censo["nombre"]
                    lider_auto_id  = info_censo["lider_id"]
                    lider_auto_nom = info_censo["lider_nombre"]
                    st.success(f"✅ **{nombre_auto}** — Líder: **{lider_auto_nom}** — 🟢 DISPONIBLE")
                else:
                    st.error(f"🚫 Cédula **{cedula_input}** ya está INHABILITADA.")
            else:
                st.warning(f"⚠️ Cédula **{cedula_input}** no está en el censo electoral.")

        # Campos solo lectura — se llenan desde el censo
        st.text_input("👤 Nombre completo", value=nombre_auto,
                      placeholder="Se autocompletará desde el censo...", disabled=True,
                      key="reg_nombre_display")

        lideres_activos = svc.listar_lideres(solo_activos=True)
        lider_id_sel = None
        if lideres_activos:
            lider_opts = {f"{l['nombre']}  ({l['total_votantes']} votos)": l["id"] for l in lideres_activos}
            lider_keys = list(lider_opts.keys())
            default_idx = 0
            if lider_auto_id:
                for i, k in enumerate(lider_keys):
                    if lider_opts[k] == lider_auto_id: default_idx = i; break
            lider_sel    = st.selectbox("🏅 Líder", lider_keys, index=default_idx,
                                         disabled=True, key="reg_lider_display")
            lider_id_sel = lider_opts[lider_sel]
        else:
            st.info("Sin líderes activos. Carga el censo primero.")

        st.divider()
        confirmar = st.checkbox("✅ Confirmo el registro. Esta acción es irreversible.",
                                 key="chk_reg")

        # Botón — se habilita solo cuando cédula DISPONIBLE + confirmación
        if st.button("🗳️ Registrar Votante", use_container_width=True,
                     type="primary",
                     disabled=not (cedula_disponible and confirmar and lider_id_sel)):
            r = svc.registrar_votante(cedula_input.strip(), nombre_auto, lider_auto_id)
            if r.ok:
                d = r.datos
                st.success(f"✅ **{d['nombre']}** registrado exitosamente")
                st.markdown(f"| Campo | Valor |\n|---|---|\n| 🪪 Cédula | `{d['cedula']}` |\n| 🏅 Líder | {d['lider']} |\n| 📊 Total líder | **{d['total_lider']} votantes** |")
                st.balloons()
            else:
                st.error(r.mensaje)
        st.divider()
        st.markdown("### 🕐 Últimos 10 registros")
        recientes = []
        for g in svc.consolidado_por_lider():
            for v in g["votantes"]:
                recientes.append({
                    "Fecha": v["fecha_registro"].strftime("%d/%m %H:%M") if v["fecha_registro"] else "",
                    "Cédula": v["cedula"], "Nombre": v["nombre"], "Líder": g["lider_nombre"]
                })
        if recientes:
            st.dataframe(pd.DataFrame(recientes).sort_values("Fecha", ascending=False).head(10),
                         use_container_width=True, hide_index=True)
        else:
            st.caption("Sin registros aún.")

    with col_rank:
        st.markdown("### 🏆 Ranking de líderes")
        lideres_activos = svc.listar_lideres(solo_activos=True)
        if not lideres_activos:
            st.info("Sin líderes aún.")
        else:
            total_v = sum(l["total_votantes"] for l in lideres_activos)
            maximo  = max((l["total_votantes"] for l in lideres_activos), default=1) or 1
            st.markdown(f'<div style="background:#1e3a5f;border-radius:10px;padding:14px;color:white;text-align:center;margin-bottom:16px"><div style="font-size:2.2rem;font-weight:bold">{total_v:,}</div><div style="opacity:.85">Total votantes registrados</div></div>', unsafe_allow_html=True)
            stats_l = svc.estadisticos_por_lider()
            ITEMS=10; total_l=len(stats_l); total_p=max(1,-(-total_l//ITEMS))
            pag_r = st.number_input(f"Página ({total_l} líderes)", min_value=1, max_value=total_p, value=1) if total_p>1 else 1
            ini=(pag_r-1)*ITEMS; fin=ini+ITEMS
            for i,l in enumerate(stats_l[ini:fin], ini+1):
                tc    = l["total_censo"] or 1
                pct_b = l["registrados"] / tc * 100
                med   = {1:"🥇",2:"🥈",3:"🥉"}.get(i, f"#{i}")
                color = "#1e3a5f" if i<=3 else ("#4a7fa5" if i<=10 else "#7aaecc")
                st.markdown(f"""<div style="margin-bottom:8px;padding:8px 12px;background:#f8f9fa;border-radius:8px;border-left:4px solid {color}">
  <div style="display:flex;justify-content:space-between;margin-bottom:4px">
    <span style="font-weight:600;font-size:0.82rem">{med} {l["nombre"]}</span>
    <span style="font-weight:700;color:{color};font-size:0.88rem">{l["registrados"]}<span style="font-weight:400;color:#666;font-size:0.8rem">/{l["total_censo"]}</span>
      <span style="font-size:0.68rem;color:#999;margin-left:4px">{l["pct_avance"]}%</span>
    </span>
  </div>
  <div style="background:#dde3ea;border-radius:4px;height:5px"><div style="background:{color};width:{pct_b:.1f}%;height:5px;border-radius:4px"></div></div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# CARGA MASIVA
# ═══════════════════════════════════════════════════════════════════
elif pagina == "📥 Carga Masiva":
    st.title("📥 Carga Masiva de Votantes")
    st.caption("Inhabilita en bloque todas las cédulas de un archivo. Útil para validar la lista completa de un líder de una vez.")

    if not hay_censo:
        st.warning("⚠️ Primero debes cargar el **Censo Electoral**.")
        st.stop()

    st.info("📌 Sube el archivo del líder. El sistema buscará cada cédula en el censo — si está DISPONIBLE la registra, si ya está inhabilitada la salta.")

    archivo = st.file_uploader("Selecciona archivo CSV o Excel", type=["csv","xlsx","xls"])
    es_excel = archivo is not None and archivo.name.lower().endswith((".xlsx",".xls"))
    if archivo and not es_excel:
        sep_csv = st.selectbox("Separador", [",",";","|"], format_func=lambda x:{",":" Coma",";":"Punto y coma","|":"Pipe"}[x])
        enc_csv = st.selectbox("Codificación", ["utf-8","utf-8-sig","latin-1"])
    else:
        sep_csv, enc_csv = ",", "utf-8"

    def leer_archivo_masivo(arch, sep, enc):
        raw=arch.read(); n=arch.name.lower()
        if n.endswith((".xlsx",".xls")):
            xf=pd.ExcelFile(io.BytesIO(raw)); mejor,mejor_n=None,0
            for h in xf.sheet_names:
                try:
                    t=pd.read_excel(io.BytesIO(raw),sheet_name=h,nrows=3,dtype=str)
                    if len(t)>mejor_n: mejor_n,mejor=len(t),h
                except: pass
            df=pd.read_excel(io.BytesIO(raw),sheet_name=mejor,dtype=str)
            cols_n,cnt=[],{}
            for c in df.columns:
                cn=c.strip().lower()
                if cn in cnt: cnt[cn]+=1; cn=f"{cn}_{cnt[cn]}"
                else: cnt[cn]=0
                cols_n.append(cn)
            df.columns=cols_n; df=df.dropna(how="all").reset_index(drop=True)
            if not any(c in {"lider","lider_nombre","nombre_lider","leader","source.name","source_name"} for c in df.columns):
                df["lider"]=mejor.strip()
            return df, f"Excel · hoja **{mejor}**"
        else:
            df=pd.read_csv(io.BytesIO(raw),sep=sep,encoding=enc,dtype=str)
            cols_n,cnt=[],{}
            for c in df.columns:
                cn=c.strip().lower()
                if cn in cnt: cnt[cn]+=1; cn=f"{cn}_{cnt[cn]}"
                else: cnt[cn]=0
                cols_n.append(cn)
            df.columns=cols_n
            return df.dropna(how="all").reset_index(drop=True),"CSV"

    if archivo:
        try:
            df_raw, info_fmt = leer_archivo_masivo(archivo, sep_csv, enc_csv)
            st.caption(f"Formato: {info_fmt} · **{len(df_raw):,} filas**")
            st.dataframe(df_raw.head(8), use_container_width=True, hide_index=True)
            st.divider()
            st.warning(f"Se procesarán **{len(df_raw):,} cédulas**. Las DISPONIBLES se registrarán. Las ya inhabilitadas se saltarán.")
            conf = st.checkbox(f"Confirmo procesar {len(df_raw):,} registros")
            if st.button(f"🚀 Iniciar carga masiva ({len(df_raw):,})", disabled=not conf,
                         type="primary", use_container_width=True):
                barra = st.progress(0, text="Procesando..."); LOTE=100
                total_f=len(df_raw); ok_l=[]; err_l=[]
                for ini in range(0, total_f, LOTE):
                    fin=min(ini+LOTE, total_f)
                    r=svc.cargar_votantes_masivo(df_raw.iloc[ini:fin].copy())
                    ok_l.extend(r.exitosos); err_l.extend(r.fallidos)
                    barra.progress(int(fin/total_f*100),
                                   text=f"Procesando {fin:,}/{total_f:,} · ✅{len(ok_l):,} · ❌{len(err_l):,}")
                barra.progress(100, text="✅ Completado")
                st.divider()
                cr1,cr2,cr3 = st.columns(3)
                cr1.metric("Total", f"{total_f:,}")
                cr2.metric("✅ Registrados", f"{len(ok_l):,}")
                cr3.metric("⚠️ Saltados/Error", f"{len(err_l):,}")
                t1,t2 = st.tabs([f"✅ Exitosos ({len(ok_l):,})", f"⚠️ Saltados ({len(err_l):,})"])
                with t1:
                    if ok_l:
                        df_ok=pd.DataFrame(ok_l)[["fila","cedula","nombre","lider"]]
                        st.dataframe(df_ok, use_container_width=True, hide_index=True)
                        st.download_button("⬇️ Exportar", df_ok.to_csv(index=False).encode(), "exitosos.csv","text/csv")
                    else: st.info("Ningún registro nuevo.")
                with t2:
                    if err_l:
                        df_e=pd.DataFrame(err_l)[["fila","cedula","nombre","lider","error"]]
                        st.dataframe(df_e, use_container_width=True, hide_index=True)
                        st.download_button("⬇️ Exportar", df_e.to_csv(index=False).encode(), "saltados.csv","text/csv")
                    else: st.success("🎉 Todos procesados sin errores.")
                if ok_l: st.balloons()
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# ESTADÍSTICOS
# ═══════════════════════════════════════════════════════════════════
elif pagina == "📈 Estadísticos":
    st.title("📈 Estadísticos Electorales")
    st.caption("Análisis completo del avance del proceso electoral por líder.")

    if not hay_censo:
        st.warning("⚠️ Carga el Censo Electoral primero.")
        st.stop()

    import plotly.express as px
    import plotly.graph_objects as go

    sc    = svc.stats_censo()
    stats = svc.estadisticos_por_lider()

    # ── KPIs generales ───────────────────────────────────────────
    k1,k2,k3,k4,k5 = st.columns(5)
    k1.markdown(f'''<div class="metric-card" style="background:#1e3a5f">
<div style="font-size:1.8rem;font-weight:bold">{sc["total_padron"]:,}</div>
<div style="font-size:0.8rem">Total padrón</div></div>''', unsafe_allow_html=True)
    k2.markdown(f'''<div class="metric-card" style="background:#2d6a4f">
<div style="font-size:1.8rem;font-weight:bold">{sc["disponibles"]:,}</div>
<div style="font-size:0.8rem">Disponibles</div></div>''', unsafe_allow_html=True)
    k3.markdown(f'''<div class="metric-card" style="background:#c0392b">
<div style="font-size:1.8rem;font-weight:bold">{sc["inhabilitadas"]:,}</div>
<div style="font-size:0.8rem">Ya votaron</div></div>''', unsafe_allow_html=True)
    k4.markdown(f'''<div class="metric-card" style="background:#7f4f24">
<div style="font-size:1.8rem;font-weight:bold">{sc["cobertura_pct"]}%</div>
<div style="font-size:0.8rem">Cobertura</div></div>''', unsafe_allow_html=True)
    lids_con_votos = sum(1 for l in stats if l["registrados"] > 0)
    k5.markdown(f'''<div class="metric-card" style="background:#4a7fa5">
<div style="font-size:1.8rem;font-weight:bold">{lids_con_votos}/{len(stats)}</div>
<div style="font-size:0.8rem">Líderes activos</div></div>''', unsafe_allow_html=True)

    st.progress(sc["cobertura_pct"]/100,
                text=f"Avance total: {sc['inhabilitadas']:,} de {sc['total_padron']:,} votantes registrados ({sc['cobertura_pct']}%)")

    st.divider()

    df_stats = pd.DataFrame(stats)

    # ── Gráficas ─────────────────────────────────────────────────
    tab_bar, tab_pie, tab_tabla, tab_pendientes = st.tabs([
        "📊 Barras por líder", "🥧 Distribución", "📋 Tabla detallada", "⏳ Pendientes"
    ])

    with tab_bar:
        st.subheader("Votantes registrados vs cupo por líder")
        df_plot = df_stats[df_stats["total_censo"] > 0].copy()
        df_plot = df_plot.sort_values("registrados", ascending=True)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Disponibles",
            y=df_plot["nombre"],
            x=df_plot["disponibles"],
            orientation="h",
            marker_color="#dde3ea",
            text=[f"{v}" for v in df_plot["disponibles"]],
            textposition="inside",
        ))
        fig.add_trace(go.Bar(
            name="Registrados",
            y=df_plot["nombre"],
            x=df_plot["registrados"],
            orientation="h",
            marker_color="#1e3a5f",
            text=[f"{r}/{t}" for r,t in zip(df_plot["registrados"], df_plot["total_censo"])],
            textposition="inside",
        ))
        fig.update_layout(
            barmode="stack",
            height=max(400, len(df_plot)*28),
            margin=dict(l=10,r=10,t=30,b=10),
            legend=dict(orientation="h", y=1.05),
            xaxis_title="Votantes",
            yaxis_title="",
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab_pie:
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.subheader("Distribución de votantes registrados")
            df_pie = df_stats[df_stats["registrados"] > 0]
            if df_pie.empty:
                st.info("Sin votantes registrados aún.")
            else:
                fig_pie = px.pie(df_pie, values="registrados", names="nombre",
                                  color_discrete_sequence=px.colors.sequential.Blues_r)
                fig_pie.update_traces(textposition="inside", textinfo="percent+label")
                fig_pie.update_layout(showlegend=False, height=420,
                                       margin=dict(l=10,r=10,t=10,b=10))
                st.plotly_chart(fig_pie, use_container_width=True)
        with col_p2:
            st.subheader("Distribución del padrón (cupos)")
            fig_pie2 = px.pie(df_stats, values="total_censo", names="nombre",
                               color_discrete_sequence=px.colors.sequential.Teal)
            fig_pie2.update_traces(textposition="inside", textinfo="percent+label")
            fig_pie2.update_layout(showlegend=False, height=420,
                                    margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig_pie2, use_container_width=True)

    with tab_tabla:
        st.subheader("Tabla detallada por líder")
        df_tabla = df_stats[["nombre","total_censo","registrados","disponibles","pct_avance"]].copy()
        df_tabla.columns = ["Líder","Cupo en censo","Registrados","Disponibles","% Avance"]
        df_tabla = df_tabla.sort_values("Registrados", ascending=False).reset_index(drop=True)
        df_tabla.index += 1

        # Totales
        totales = pd.DataFrame([{
            "Líder": "🔢 TOTAL",
            "Cupo en censo": df_tabla["Cupo en censo"].sum(),
            "Registrados": df_tabla["Registrados"].sum(),
            "Disponibles": df_tabla["Disponibles"].sum(),
            "% Avance": round(df_tabla["Registrados"].sum()/df_tabla["Cupo en censo"].sum()*100,1) if df_tabla["Cupo en censo"].sum()>0 else 0,
        }])

        st.dataframe(df_tabla, use_container_width=True)
        st.markdown("**Totales:**")
        st.dataframe(totales, use_container_width=True, hide_index=True)

        # Exportar
        csv_exp = df_tabla.to_csv(index=True).encode("utf-8")
        st.download_button("⬇️ Exportar tabla CSV", csv_exp, "estadisticos_lideres.csv", "text/csv")

    with tab_pendientes:
        st.subheader("⏳ Líderes con votantes pendientes")
        df_pend = df_stats[df_stats["disponibles"] > 0].copy()
        df_pend = df_pend.sort_values("pct_avance", ascending=True)
        if df_pend.empty:
            st.success("🎉 ¡Todos los votantes han sido registrados!")
        else:
            st.caption(f"{len(df_pend)} líderes con cédulas aún disponibles")
            for _, row in df_pend.iterrows():
                tc   = row["total_censo"] or 1
                pct  = row["pct_avance"]
                color = "#c0392b" if pct < 30 else ("#e67e22" if pct < 70 else "#2d6a4f")
                st.markdown(f'''<div style="margin-bottom:8px;padding:10px 14px;background:#f8f9fa;border-radius:8px;border-left:4px solid {color}">
  <div style="display:flex;justify-content:space-between;margin-bottom:5px">
    <span style="font-weight:600;font-size:0.88rem">{row["nombre"]}</span>
    <span style="font-weight:700;color:{color}">{row["registrados"]}/{row["total_censo"]}
      <span style="font-size:0.72rem;margin-left:4px">({pct}%)</span>
      <span style="font-size:0.72rem;color:#999;margin-left:6px">faltan {row["disponibles"]}</span>
    </span>
  </div>
  <div style="background:#dde3ea;border-radius:4px;height:6px">
    <div style="background:{color};width:{pct:.1f}%;height:6px;border-radius:4px"></div>
  </div>
</div>''', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# LÍDERES
# ═══════════════════════════════════════════════════════════════════
elif pagina == "👥 Líderes":
    st.title("👥 Gestión de Líderes")
    col_a, col_b = st.columns([1,1])
    with col_a:
        st.subheader("Crear nuevo líder")
        with st.form("form_lider"):
            nombre_lider = st.text_input("Nombre del líder", placeholder="Ej: Pedro Ramírez")
            if st.form_submit_button("➕ Crear Líder", use_container_width=True, type="primary"):
                r = svc.crear_lider(nombre_lider)
                if r.ok: st.success(r.mensaje); st.rerun()
                else: st.error(r.mensaje)
        st.divider()
        st.subheader("Cambiar estado")
        lideres = svc.listar_lideres()
        if lideres:
            sel = st.selectbox("Líder", [(l["id"],l["nombre"]) for l in lideres], format_func=lambda x:x[1])
            nuevo = st.radio("Estado", ["ACTIVO","INACTIVO"], horizontal=True)
            if st.button("Actualizar", use_container_width=True):
                r = svc.cambiar_estado_lider(sel[0], nuevo)
                if r.ok: st.success(r.mensaje); st.rerun()
                else: st.error(r.mensaje)
    with col_b:
        lideres = svc.listar_lideres()
        st.subheader(f"Todos los líderes ({len(lideres)})")
        if not lideres: st.info("No hay líderes.")
        else:
            st.dataframe(pd.DataFrame([{"Nombre":l["nombre"],"Votantes":l["total_votantes"],"Estado":l["estado"]} for l in lideres]),
                         use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════
# GESTIÓN / BORRADO
# ═══════════════════════════════════════════════════════════════════
elif pagina == "🗑️ Gestión / Borrado":
    st.title("🗑️ Gestión y Borrado de Votantes")
    st.caption("Estas operaciones borran votantes registrados y liberan sus cédulas al censo como DISPONIBLES.")

    tab_lider, tab_todos = st.tabs(["🏅 Por Líder", "☢️ Borrar Todo"])

    with tab_lider:
        lideres = svc.listar_lideres()
        if not lideres:
            st.info("No hay líderes.")
        else:
            lider_sel_b = st.selectbox(
                "Seleccionar líder",
                [(l["id"],l["nombre"],l["total_votantes"]) for l in lideres],
                format_func=lambda x: f"{x[1]}  ({x[2]} votantes registrados)"
            )
            st.markdown(f"""<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:14px;margin:10px 0">
⚠️ Se borrarán <b>{lider_sel_b[2]}</b> votantes registrados de <b>{lider_sel_b[1]}</b>.<br>
Sus cédulas volverán a DISPONIBLE en el censo. El líder permanece.
</div>""", unsafe_allow_html=True)
            conf_v = st.checkbox("Confirmo borrar los votantes registrados de este líder")
            if st.button("🗑️ Borrar votantes del líder", disabled=not conf_v,
                         use_container_width=True, type="primary"):
                r = svc.borrar_votantes_de_lider(lider_sel_b[0])
                if r.ok: st.success(r.mensaje); st.rerun()
                else: st.error(r.mensaje)

    with tab_todos:
        sc_now = svc.stats_censo()
        st.markdown(f"""<div style="background:#f8d7da;border:2px solid #dc3545;border-radius:10px;padding:20px;margin:12px 0">
<h4 style="color:#721c24;margin:0">🚨 ACCIÓN MUY DESTRUCTIVA</h4>
<p style="color:#721c24;margin:8px 0 0">
Se eliminarán <b>{sc_now['total_votantes']:,} votantes</b> de todos los líderes.<br>
Los líderes y el censo permanecen. Las cédulas vuelven a DISPONIBLE.
</p></div>""", unsafe_allow_html=True)
        conf_t1 = st.checkbox("Entiendo que se borrarán TODOS los votantes")
        conf_t2 = st.checkbox("Confirmo continuar")
        texto   = st.text_input("Escribe BORRAR TODO para confirmar", placeholder="BORRAR TODO")
        if st.button("☢️ Ejecutar Borrado Total", disabled=not (conf_t1 and conf_t2),
                     use_container_width=True, type="primary"):
            if texto.strip().upper() != "BORRAR TODO":
                st.error("❌ Escribe exactamente: BORRAR TODO")
            else:
                r = svc.borrar_todos_los_votantes()
                if r.ok: st.success(r.mensaje); st.rerun()
                else: st.error(r.mensaje)


# ═══════════════════════════════════════════════════════════════════
# CONSULTA DE CÉDULA
# ═══════════════════════════════════════════════════════════════════
elif pagina == "🔍 Consulta de Cédula":
    st.title("🔍 Consulta de Estado de Cédula")
    ced_q = st.text_input("Número de cédula", max_chars=20)
    if st.button("🔍 Consultar"):
        if not ced_q.strip():
            st.warning("Ingresa una cédula.")
        else:
            res  = svc.buscar_cedula(ced_q)
            ctrl = res["control"]; vot = res["votante"]
            c1,c2 = st.columns(2)
            with c1:
                st.subheader("📋 En el padrón")
                if ctrl["existe"]:
                    st.markdown(f"**Nombre:** {ctrl['nombre']}")
                    lmap = {l["id"]:l["nombre"] for l in svc.listar_lideres()}
                    st.markdown(f"**Líder:** {lmap.get(ctrl['lider_id'],'N/A')}")
                    if ctrl["estado"]=="DISPONIBLE": st.success("🟢 DISPONIBLE")
                    else:
                        st.error("🔴 INHABILITADA")
                        if ctrl["fecha_inhabilitacion"]: st.caption(f"Fecha: {ctrl['fecha_inhabilitacion']}")
                else:
                    st.info("No está en el padrón electoral.")
            with c2:
                st.subheader("🗳️ Como Votante")
                if vot["registrado"]:
                    st.error("🚫 Ya registrada como votante.")
                    st.markdown(f"**Fecha:** {vot['fecha_registro']}")
                    lmap = {l["id"]:l["nombre"] for l in svc.listar_lideres()}
                    st.markdown(f"**Líder:** {lmap.get(vot['lider_id'],'Desconocido')}")
                else:
                    st.success("✅ Pendiente de registrar.")
