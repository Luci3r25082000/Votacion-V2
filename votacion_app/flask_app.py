from __future__ import annotations

import io
import json
import os
import tempfile
import uuid
from pathlib import Path

import pandas as pd
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from sqlalchemy import func, select
from werkzeug.utils import secure_filename

import services.electoral as svc
from models.database import (
    ControlCedula,
    EstadoLider,
    Lider,
    SessionLocal,
    Votante,
    database_diagnostics,
    init_db,
)


def _env_flag(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _tmp_dir() -> Path:
    base = Path(tempfile.gettempdir()) / "votacion_app"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _save_upload(file_storage, prefix: str) -> Path:
    name = secure_filename(file_storage.filename or "")
    suffix = Path(name).suffix.lower()
    token = uuid.uuid4().hex
    out = _tmp_dir() / f"{prefix}_{token}{suffix}"
    file_storage.save(out)
    return out


def _read_upload_to_df(path: Path, sep: str = ",", encoding: str = "utf-8") -> tuple[pd.DataFrame, str]:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str)
        fmt = f"Excel ({suffix})"
    else:
        df = pd.read_csv(path, sep=sep, encoding=encoding, dtype=str)
        fmt = f"CSV ({sep}, {encoding})"

    df.columns = [str(c).strip().lower() for c in df.columns]
    return df.fillna(""), fmt


def _recent_registrations(limit: int = 10) -> list[dict]:
    with SessionLocal() as s:
        rows = (
            s.execute(select(Votante).order_by(Votante.fecha_registro.desc()).limit(limit))
            .scalars()
            .all()
        )
        out = []
        for v in rows:
            lider = s.get(Lider, v.lider_id)
            out.append(
                {
                    "fecha": v.fecha_registro,
                    "cedula": v.cedula,
                    "nombre": v.nombre,
                    "lider": lider.nombre if lider else "",
                }
            )
        return out


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.secret_key = (os.getenv("SECRET_KEY") or "").strip() or os.urandom(32)

    # Intentar inicializar DB sin tumbar el import (útil en Vercel)
    try:
        init_db()
        app.config["DB_INIT_ERROR"] = None
        app.config["DB_DIAG"] = database_diagnostics()
    except Exception as e:  # pragma: no cover
        app.config["DB_INIT_ERROR"] = str(e)
        app.config["DB_DIAG"] = database_diagnostics()

    @app.before_request
    def _guard_db_error():
        if request.path.startswith("/static"):
            return None
        if app.config.get("DB_INIT_ERROR"):
            return (
                render_template(
                    "db_error.html",
                    message=f"No se pudo inicializar la base de datos: {app.config['DB_INIT_ERROR']}",
                    diag=app.config.get("DB_DIAG"),
                ),
                500,
            )
        return None

    @app.get("/")
    def root():
        return redirect(url_for("dashboard"))

    @app.get("/dashboard")
    def dashboard():
        sc = svc.stats_censo()
        lideres = svc.listar_lideres()
        ranking = svc.estadisticos_por_lider()
        return render_template("dashboard.html", sc=sc, lideres=lideres, ranking=ranking)

    # ─────────────────────────────────────────────────────────────
    # Líderes
    # ─────────────────────────────────────────────────────────────
    @app.get("/leaders")
    def leaders():
        lideres = svc.listar_lideres()
        return render_template("leaders.html", lideres=lideres)

    @app.post("/leaders/create")
    def leaders_create():
        nombre = (request.form.get("nombre") or "").strip()
        r = svc.crear_lider(nombre)
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("leaders"))

    @app.post("/leaders/state")
    def leaders_state():
        lider_id = int(request.form.get("lider_id") or 0)
        estado = (request.form.get("estado") or "").strip().upper()
        if estado not in {EstadoLider.ACTIVO, EstadoLider.INACTIVO, "ACTIVO", "INACTIVO"}:
            flash("Estado inválido.", "danger")
            return redirect(url_for("leaders"))
        r = svc.cambiar_estado_lider(lider_id, estado)
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("leaders"))

    @app.get("/export/leaders.csv")
    def export_leaders_csv():
        df = pd.DataFrame(svc.listar_lideres())
        bio = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        return send_file(bio, mimetype="text/csv", as_attachment=True, download_name="leaders.csv")

    # ─────────────────────────────────────────────────────────────
    # Censo
    # ─────────────────────────────────────────────────────────────
    @app.get("/census")
    def census():
        sc = svc.stats_censo()
        tab = (request.args.get("tab") or "upload").strip().lower()
        if tab not in {"upload", "delete", "dup", "search"}:
            tab = "upload"

        lideres = svc.listar_lideres() if tab == "delete" else []
        leader_info = None
        if tab == "delete":
            leader_id = int(request.args.get("leader_id") or 0)
            if leader_id:
                with SessionLocal() as s:
                    l = s.get(Lider, leader_id)
                    if l:
                        ced_count = (
                            s.execute(
                                select(func.count(ControlCedula.cedula)).where(ControlCedula.lider_id == leader_id)
                            ).scalar()
                            or 0
                        )
                        vot_count = (
                            s.execute(select(func.count(Votante.id)).where(Votante.lider_id == leader_id)).scalar()
                            or 0
                        )
                        leader_info = {
                            "id": l.id,
                            "nombre": l.nombre,
                            "total_votantes": vot_count,
                            "cedulas": ced_count,
                        }

        upload_path = session.get("censo_upload_path")
        upload_info = None
        preview_rows = None
        if tab == "upload" and upload_path and Path(upload_path).exists():
            try:
                df, info_fmt = _read_upload_to_df(
                    Path(upload_path),
                    sep=session.get("censo_sep", ","),
                    encoding=session.get("censo_enc", "utf-8"),
                )
                upload_info = {"fmt": info_fmt, "rows": len(df), "cols": list(df.columns)}
                preview_rows = df.head(8).fillna("").to_dict(orient="records")
            except Exception as e:
                flash(f"No se pudo leer el archivo cargado: {e}", "warning")
                session.pop("censo_upload_path", None)

        dup_token = session.get("censo_dup_token")
        dup_rows = None
        dup_summary = None
        if tab == "dup" and dup_token:
            dup_path = _tmp_dir() / f"censo_dup_{dup_token}.json"
            if dup_path.exists():
                data = json.loads(dup_path.read_text(encoding="utf-8"))
                rows = data.get("duplicados", [])
                dup_rows = rows[:200]
                inh = sum(1 for r in rows if r.get("estado") == "INHABILITADA")
                disp = sum(1 for r in rows if r.get("estado") == "DISPONIBLE")
                dup_summary = {"total": len(rows), "inhabilitadas": inh, "disponibles": disp}

        cedula_q = (request.args.get("cedula") or "").strip()
        cedula_info = svc.buscar_cedula(cedula_q) if (tab == "search" and cedula_q) else None

        return render_template(
            "census.html",
            sc=sc,
            tab=tab,
            lideres=lideres,
            leader_info=leader_info,
            upload_info=upload_info,
            preview_rows=preview_rows,
            dup_token=dup_token,
            dup_rows=dup_rows,
            dup_summary=dup_summary,
            cedula_q=cedula_q,
            cedula_info=cedula_info,
        )

    @app.post("/census/upload")
    def census_upload():
        f = request.files.get("file")
        if not f or not (f.filename or "").strip():
            flash("Selecciona un archivo CSV o Excel.", "warning")
            return redirect(url_for("census", tab="upload"))
        sep = (request.form.get("sep") or ",").strip()
        enc = (request.form.get("encoding") or "utf-8").strip()
        path = _save_upload(f, "censo")
        session["censo_upload_path"] = str(path)
        session["censo_sep"] = sep
        session["censo_enc"] = enc
        flash("Archivo cargado. Revisa el preview y ejecuta la carga.", "info")
        return redirect(url_for("census", tab="upload"))

    @app.post("/census/execute")
    def census_execute():
        upload_path = session.get("censo_upload_path")
        if not upload_path or not Path(upload_path).exists():
            flash("No hay archivo cargado para ejecutar.", "warning")
            return redirect(url_for("census", tab="upload"))

        try:
            df, _ = _read_upload_to_df(
                Path(upload_path),
                sep=session.get("censo_sep", ","),
                encoding=session.get("censo_enc", "utf-8"),
            )

            alias_ced = {"cedula", "cc", "documento", "id", "num_doc"}
            alias_nom = {"nombre", "nombre_votante", "nombres", "name", "nombre completo"}
            alias_lid = {"lider", "lider_nombre", "nombre_lider", "leader"}
            alias_src = {"source.name", "source_name"}

            col_ced = next((c for c in df.columns if c in alias_ced), None)
            col_nom = next((c for c in df.columns if c in alias_nom), None)
            col_lid = next((c for c in df.columns if c in alias_lid), None)
            col_src = next((c for c in df.columns if c in alias_src), None)

            if not col_ced:
                raise ValueError(f"No se encontró columna de cédula. Columnas: {list(df.columns)}")
            if not col_nom:
                raise ValueError(f"No se encontró columna de nombre. Columnas: {list(df.columns)}")
            if not col_lid and not col_src:
                raise ValueError(f"No se encontró columna de líder. Columnas: {list(df.columns)}")

            # Líderes únicos
            if col_src:
                leaders_unique = sorted(
                    {
                        svc.extraer_nombre_lider(v)
                        for v in df[col_src]
                        if str(v).strip().lower() not in ("nan", "none", "")
                    }
                )
            else:
                leaders_unique = sorted(
                    {
                        str(v).strip()
                        for v in df[col_lid]
                        if str(v).strip().lower() not in ("nan", "none", "")
                    }
                )

            existing = {l["nombre"].strip().lower() for l in svc.listar_lideres()}
            for name in leaders_unique:
                if name and name.strip().lower() not in existing:
                    svc.crear_lider(name)

            leader_id_map = {l["nombre"].strip().lower(): l["id"] for l in svc.listar_lideres()}

            total_rows = len(df)
            batch_size = 500
            acc = {"nuevas": 0, "ya_existe": 0, "invalidas": 0}
            dups: list[dict] = []

            for start in range(0, total_rows, batch_size):
                end = min(start + batch_size, total_rows)
                batch = []
                for _, row in df.iloc[start:end].iterrows():
                    ced = str(row.get(col_ced, "")).strip().replace(".0", "")
                    nom = str(row.get(col_nom, "")).strip()
                    if col_src:
                        src = str(row.get(col_src, "")).strip()
                        leader_name = svc.extraer_nombre_lider(src) if src else ""
                    else:
                        leader_name = str(row.get(col_lid, "")).strip()
                    leader_id = leader_id_map.get(leader_name.strip().lower()) if leader_name else None
                    batch.append({"cedula": ced, "nombre": nom, "lider_id": leader_id})

                res = svc.cargar_censo_masivo(batch)
                for k in acc:
                    acc[k] += int(res.get(k, 0))
                dups.extend(res.get("duplicados", []))

            token = uuid.uuid4().hex
            dup_path = _tmp_dir() / f"censo_dup_{token}.json"
            dup_path.write_text(json.dumps({"duplicados": dups}, ensure_ascii=False), encoding="utf-8")
            session["censo_dup_token"] = token

            flash(
                f"Censo cargado: {acc['nuevas']} nuevas, {acc['ya_existe']} duplicadas, {acc['invalidas']} inválidas.",
                "success",
            )
            return redirect(url_for("census", tab="dup"))
        except Exception as e:
            flash(f"Error al cargar censo: {e}", "danger")
            return redirect(url_for("census", tab="upload"))

    @app.post("/census/delete_leader")
    def census_delete_leader():
        lider_id = int(request.form.get("lider_id") or 0)
        confirmar = (request.form.get("confirm") or "").strip().lower() == "on"
        if not confirmar:
            flash("Debes confirmar la eliminación.", "warning")
            return redirect(url_for("census", tab="delete", leader_id=lider_id))
        r = svc.borrar_lider_del_censo(lider_id)
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("census", tab="delete"))

    @app.post("/census/dup/liberar")
    def census_dup_liberar():
        token = session.get("censo_dup_token")
        if not token:
            flash("No hay duplicados cargados.", "warning")
            return redirect(url_for("census", tab="dup"))
        dup_path = _tmp_dir() / f"censo_dup_{token}.json"
        if not dup_path.exists():
            flash("El archivo de duplicados ya no existe (Vercel /tmp es efímero).", "warning")
            return redirect(url_for("census", tab="dup"))

        data = json.loads(dup_path.read_text(encoding="utf-8"))
        dup_rows = data.get("duplicados", [])
        inhabilitadas = [r["cedula"] for r in dup_rows if r.get("estado") == "INHABILITADA"]
        r = svc.liberar_cedulas_a_disponible(inhabilitadas)
        flash(r.mensaje, "success" if r.ok else "danger")

        # actualizar cache
        inhabil_set = set(inhabilitadas)
        for item in dup_rows:
            if item.get("cedula") in inhabil_set:
                item["estado"] = "DISPONIBLE"

        dup_path.write_text(json.dumps({"duplicados": dup_rows}, ensure_ascii=False), encoding="utf-8")
        return redirect(url_for("census", tab="dup"))

    @app.get("/export/duplicados.csv")
    def export_duplicados_csv():
        token = request.args.get("token") or session.get("censo_dup_token")
        if not token:
            flash("No hay duplicados para exportar.", "warning")
            return redirect(url_for("census", tab="dup"))
        dup_path = _tmp_dir() / f"censo_dup_{token}.json"
        if not dup_path.exists():
            flash("Duplicados no disponibles (archivo expiró).", "warning")
            return redirect(url_for("census", tab="dup"))
        data = json.loads(dup_path.read_text(encoding="utf-8"))
        df = pd.DataFrame(data.get("duplicados", []))
        bio = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        return send_file(bio, mimetype="text/csv", as_attachment=True, download_name="duplicados_censo.csv")

    # ─────────────────────────────────────────────────────────────
    # Registro individual de votantes
    # ─────────────────────────────────────────────────────────────
    @app.get("/voters")
    def voters():
        sc = svc.stats_censo()
        cedula = (request.args.get("cedula") or "").strip()
        info_censo = svc.buscar_en_censo(cedula) if cedula else None
        recientes = _recent_registrations(10)
        return render_template("voters.html", sc=sc, cedula=cedula, info_censo=info_censo, recientes=recientes)

    @app.post("/voters/register")
    def voters_register():
        cedula = (request.form.get("cedula") or "").strip()
        if not cedula:
            flash("Cédula vacía.", "warning")
            return redirect(url_for("voters"))

        info = svc.buscar_en_censo(cedula)
        if not info.get("encontrado"):
            flash(f"Cédula {cedula} no está en el censo electoral.", "warning")
            return redirect(url_for("voters", cedula=cedula))
        if info.get("estado") != "DISPONIBLE":
            flash(f"Cédula {cedula} ya está INHABILITADA.", "danger")
            return redirect(url_for("voters", cedula=cedula))

        if not _env_flag("ALLOW_REGISTER_WITHOUT_CONFIRM"):
            if (request.form.get("confirm") or "").strip().lower() != "on":
                flash("Debes confirmar el registro.", "warning")
                return redirect(url_for("voters", cedula=cedula))

        r = svc.registrar_votante(cedula, info.get("nombre") or "", info.get("lider_id"))
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("voters", cedula=cedula))

    @app.get("/export/plantilla.csv")
    def export_plantilla():
        csv_text = svc.generar_csv_plantilla()
        bio = io.BytesIO(csv_text.encode("utf-8"))
        return send_file(bio, mimetype="text/csv", as_attachment=True, download_name="plantilla_votantes.csv")

    # ─────────────────────────────────────────────────────────────
    # Carga masiva de votantes
    # ─────────────────────────────────────────────────────────────
    @app.get("/bulk")
    def bulk():
        sc = svc.stats_censo()
        upload_path = session.get("bulk_upload_path")
        upload_info = None
        preview_rows = None

        if upload_path and Path(upload_path).exists():
            try:
                df, info_fmt = _read_upload_to_df(
                    Path(upload_path),
                    sep=session.get("bulk_sep", ","),
                    encoding=session.get("bulk_enc", "utf-8"),
                )
                upload_info = {"fmt": info_fmt, "rows": len(df), "cols": list(df.columns)}
                preview_rows = df.head(8).fillna("").to_dict(orient="records")
            except Exception as e:
                flash(f"No se pudo leer el archivo cargado: {e}", "warning")
                session.pop("bulk_upload_path", None)

        res_token = session.get("bulk_res_token")
        res_summary = None
        if res_token:
            meta_path = _tmp_dir() / f"bulk_res_{res_token}.json"
            if meta_path.exists():
                res_summary = json.loads(meta_path.read_text(encoding="utf-8"))

        return render_template(
            "bulk.html",
            sc=sc,
            upload_info=upload_info,
            preview_rows=preview_rows,
            res_token=res_token,
            res_summary=res_summary,
        )

    @app.post("/bulk/upload")
    def bulk_upload():
        f = request.files.get("file")
        if not f or not (f.filename or "").strip():
            flash("Selecciona un archivo CSV o Excel.", "warning")
            return redirect(url_for("bulk"))
        sep = (request.form.get("sep") or ",").strip()
        enc = (request.form.get("encoding") or "utf-8").strip()
        path = _save_upload(f, "bulk")
        session["bulk_upload_path"] = str(path)
        session["bulk_sep"] = sep
        session["bulk_enc"] = enc
        flash("Archivo cargado. Revisa el preview y ejecuta la carga.", "info")
        return redirect(url_for("bulk"))

    @app.post("/bulk/execute")
    def bulk_execute():
        upload_path = session.get("bulk_upload_path")
        if not upload_path or not Path(upload_path).exists():
            flash("No hay archivo cargado para ejecutar.", "warning")
            return redirect(url_for("bulk"))
        try:
            df, _ = _read_upload_to_df(
                Path(upload_path),
                sep=session.get("bulk_sep", ","),
                encoding=session.get("bulk_enc", "utf-8"),
            )

            r = svc.cargar_votantes_masivo(df.copy())
            ok_l = list(r.exitosos)
            err_l = list(r.fallidos)

            token = uuid.uuid4().hex
            ok_path = _tmp_dir() / f"bulk_ok_{token}.csv"
            err_path = _tmp_dir() / f"bulk_err_{token}.csv"
            pd.DataFrame(ok_l).to_csv(ok_path, index=False)
            pd.DataFrame(err_l).to_csv(err_path, index=False)

            meta_path = _tmp_dir() / f"bulk_res_{token}.json"
            meta_path.write_text(
                json.dumps(
                    {
                        "total": int(getattr(r, "total", len(df))),
                        "ok": len(ok_l),
                        "err": len(err_l),
                        "ok_file": ok_path.name,
                        "err_file": err_path.name,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            session["bulk_res_token"] = token

            flash(f"Carga masiva completada: ✅{len(ok_l)} · ⚠️{len(err_l)}", "success")
            return redirect(url_for("bulk"))
        except Exception as e:
            flash(f"Error en carga masiva: {e}", "danger")
            return redirect(url_for("bulk"))

    @app.get("/export/bulk_ok.csv")
    def export_bulk_ok():
        token = request.args.get("token") or session.get("bulk_res_token")
        if not token:
            flash("No hay resultado de carga masiva.", "warning")
            return redirect(url_for("bulk"))
        meta_path = _tmp_dir() / f"bulk_res_{token}.json"
        if not meta_path.exists():
            flash("Resultado no disponible (archivo expiró).", "warning")
            return redirect(url_for("bulk"))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        ok_path = _tmp_dir() / meta["ok_file"]
        return send_file(ok_path, mimetype="text/csv", as_attachment=True, download_name="exitosos.csv")

    @app.get("/export/bulk_err.csv")
    def export_bulk_err():
        token = request.args.get("token") or session.get("bulk_res_token")
        if not token:
            flash("No hay resultado de carga masiva.", "warning")
            return redirect(url_for("bulk"))
        meta_path = _tmp_dir() / f"bulk_res_{token}.json"
        if not meta_path.exists():
            flash("Resultado no disponible (archivo expiró).", "warning")
            return redirect(url_for("bulk"))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        err_path = _tmp_dir() / meta["err_file"]
        return send_file(err_path, mimetype="text/csv", as_attachment=True, download_name="saltados.csv")

    # ─────────────────────────────────────────────────────────────
    # Gestión / borrado
    # ─────────────────────────────────────────────────────────────
    @app.get("/admin")
    def admin():
        sc = svc.stats_censo()
        lideres = svc.listar_lideres()
        return render_template("admin.html", sc=sc, lideres=lideres)

    @app.post("/admin/delete_by_leader")
    def admin_delete_by_leader():
        lider_id = int(request.form.get("lider_id") or 0)
        if (request.form.get("confirm") or "").strip().lower() != "on":
            flash("Debes confirmar el borrado.", "warning")
            return redirect(url_for("admin"))
        r = svc.borrar_votantes_de_lider(lider_id)
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("admin"))

    @app.post("/admin/delete_all")
    def admin_delete_all():
        if (request.form.get("c1") or "").strip().lower() != "on" or (request.form.get("c2") or "").strip().lower() != "on":
            flash("Debes marcar ambas confirmaciones.", "warning")
            return redirect(url_for("admin"))
        texto = (request.form.get("texto") or "").strip().upper()
        if texto != "BORRAR TODO":
            flash("Escribe exactamente: BORRAR TODO", "danger")
            return redirect(url_for("admin"))
        r = svc.borrar_todos_los_votantes()
        flash(r.mensaje, "success" if r.ok else "danger")
        return redirect(url_for("admin"))

    # ─────────────────────────────────────────────────────────────
    # Consulta de cédula
    # ─────────────────────────────────────────────────────────────
    @app.get("/cedula")
    def cedula():
        ced = (request.args.get("cedula") or "").strip()
        res = svc.buscar_cedula(ced) if ced else None
        lider_map = {l["id"]: l["nombre"] for l in svc.listar_lideres()} if ced else {}
        return render_template("cedula.html", cedula=ced, res=res, lider_map=lider_map)

    return app


app = create_app()
