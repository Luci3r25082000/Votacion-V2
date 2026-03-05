"""Servicio transaccional - Sistema Electoral"""
import re
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, func
from models.database import (
    SessionLocal, Lider, Votante, ControlCedula,
    EstadoCedula, EstadoLider
)


class Resultado:
    def __init__(self, ok, mensaje, datos=None):
        self.ok = ok; self.mensaje = mensaje; self.datos = datos
    def __bool__(self): return self.ok


# ═══════════════════════════════════════════════════════════════════
# LÍDERES
# ═══════════════════════════════════════════════════════════════════

def crear_lider(nombre: str) -> Resultado:
    nombre = nombre.strip()
    if not nombre: return Resultado(False, "Nombre vacío.")
    with SessionLocal() as s:
        if s.execute(select(Lider).where(func.lower(Lider.nombre)==nombre.lower())).scalar_one_or_none():
            return Resultado(False, f"Ya existe el líder '{nombre}'.")
        l = Lider(nombre=nombre, estado=EstadoLider.ACTIVO, total_votantes=0)
        s.add(l); s.commit(); s.refresh(l)
        return Resultado(True, f"Líder '{nombre}' creado.", {"id": l.id})

def listar_lideres(solo_activos=False):
    with SessionLocal() as s:
        q = select(Lider)
        if solo_activos: q = q.where(Lider.estado==EstadoLider.ACTIVO)
        return [{"id":l.id,"nombre":l.nombre,"total_votantes":l.total_votantes,
                 "estado":l.estado,"fecha_creacion":l.fecha_creacion}
                for l in s.execute(q.order_by(Lider.total_votantes.desc())).scalars().all()]

def cambiar_estado_lider(lider_id, nuevo_estado) -> Resultado:
    with SessionLocal() as s:
        l = s.get(Lider, lider_id)
        if not l: return Resultado(False, "Líder no encontrado.")
        l.estado = nuevo_estado; s.commit()
        return Resultado(True, f"Líder actualizado a {nuevo_estado}.")


# ═══════════════════════════════════════════════════════════════════
# CENSO ELECTORAL
# ═══════════════════════════════════════════════════════════════════

def cargar_censo_masivo(filas: list) -> dict:
    """
    Carga el padrón completo como DISPONIBLE.
    Cada fila: {"cedula": str, "nombre": str, "lider_id": int}
    NO registra votantes.
    Retorna detalle de duplicados para revisión.
    """
    nuevas = invalidas = 0
    duplicados = []   # detalle completo de las que ya existían
    vistas = set()
    lider_cache = {}
    with SessionLocal() as s:
        # Precarga nombres de líderes para el reporte
        for l in s.execute(select(Lider)).scalars().all():
            lider_cache[l.id] = l.nombre
        for fila in filas:
            c = str(fila.get("cedula","")).strip().replace(".0","")
            if not c or c.lower() in ("nan","none",""):
                invalidas += 1; continue
            if c in vistas:
                ex = s.get(ControlCedula, c)
                duplicados.append({
                    "cedula": c, "nombre": fila.get("nombre",""),
                    "lider": lider_cache.get(fila.get("lider_id"),""),
                    "estado": ex.estado if ex else "DUPLICADO_EN_LOTE",
                    "motivo": "Duplicado dentro del mismo archivo",
                })
                continue
            vistas.add(c)
            ex = s.get(ControlCedula, c)
            if ex:
                duplicados.append({
                    "cedula": c, "nombre": ex.nombre or fila.get("nombre",""),
                    "lider": lider_cache.get(ex.lider_id,""),
                    "estado": ex.estado,
                    "motivo": "Ya existía en el sistema",
                })
                continue
            s.add(ControlCedula(
                cedula=c,
                nombre=fila.get("nombre","").strip(),
                lider_id=fila.get("lider_id"),
                estado=EstadoCedula.DISPONIBLE,
            ))
            nuevas += 1
        s.commit()
    return {
        "nuevas": nuevas,
        "ya_existe": len(duplicados),
        "invalidas": invalidas,
        "total": nuevas + len(duplicados) + invalidas,
        "duplicados": duplicados,
    }

def stats_censo() -> dict:
    with SessionLocal() as s:
        total  = s.execute(select(func.count(ControlCedula.cedula))).scalar() or 0
        disp   = s.execute(select(func.count(ControlCedula.cedula))
                   .where(ControlCedula.estado==EstadoCedula.DISPONIBLE)).scalar() or 0
        inh    = total - disp
        votan  = s.execute(select(func.count(Votante.id))).scalar() or 0
        cob    = round(inh/total*100, 2) if total > 0 else 0.0
    return {"total_padron":total,"disponibles":disp,"inhabilitadas":inh,
            "total_votantes":votan,"cobertura_pct":cob}

def stats_cedulas():
    sc = stats_censo()
    return {"total":sc["total_padron"],"disponibles":sc["disponibles"],"inhabilitadas":sc["inhabilitadas"]}

def buscar_en_censo(cedula: str) -> dict:
    """Busca cédula en el padrón y retorna nombre y líder si existe."""
    cedula = cedula.strip()
    with SessionLocal() as s:
        ctrl = s.get(ControlCedula, cedula)
        if ctrl:
            lider = s.get(Lider, ctrl.lider_id) if ctrl.lider_id else None
            return {
                "encontrado": True,
                "cedula": cedula,
                "nombre": ctrl.nombre or "",
                "lider_id": ctrl.lider_id,
                "lider_nombre": lider.nombre if lider else "",
                "estado": ctrl.estado,
            }
        return {"encontrado": False, "cedula": cedula, "nombre": "",
                "lider_id": None, "lider_nombre": "", "estado": "NO_REGISTRADA"}

def buscar_cedula(cedula: str) -> dict:
    """Consulta completa: estado en censo + si está registrado como votante."""
    cedula = cedula.strip()
    with SessionLocal() as s:
        ctrl  = s.get(ControlCedula, cedula)
        votan = s.execute(select(Votante).where(Votante.cedula==cedula)).scalar_one_or_none()
        return {
            "cedula": cedula,
            "control": {
                "existe": ctrl is not None,
                "estado": ctrl.estado if ctrl else "NO REGISTRADA",
                "nombre": ctrl.nombre if ctrl else "",
                "lider_id": ctrl.lider_id if ctrl else None,
                "fecha_inhabilitacion": ctrl.fecha_inhabilitacion if ctrl else None,
            },
            "votante": {
                "registrado": votan is not None,
                "nombre": votan.nombre if votan else None,
                "lider_id": votan.lider_id if votan else None,
                "fecha_registro": votan.fecha_registro if votan else None,
            },
        }

def borrar_lider_del_censo(lider_id: int) -> Resultado:
    """
    Borra un líder y TODO lo referenciado a él:
    - Sus cédulas del padrón (control_cedula)
    - Sus votantes registrados
    - El líder mismo
    """
    with SessionLocal() as s:
        lider = s.get(Lider, lider_id)
        if not lider: return Resultado(False, "Líder no encontrado.")
        nombre = lider.nombre
        # Borrar votantes
        votantes = s.execute(select(Votante).where(Votante.lider_id==lider_id)).scalars().all()
        n_vot = len(votantes)
        for v in votantes: s.delete(v)
        # Borrar cédulas del censo
        cedulas = s.execute(select(ControlCedula).where(ControlCedula.lider_id==lider_id)).scalars().all()
        n_ced = len(cedulas)
        for c in cedulas: s.delete(c)
        # Borrar líder
        s.delete(lider)
        s.commit()
        return Resultado(True,
            f"✅ Líder '{nombre}' eliminado del censo. {n_ced} cédulas y {n_vot} votantes borrados.")


# ═══════════════════════════════════════════════════════════════════
# REGISTRO TRANSACCIONAL DE VOTANTE
# ═══════════════════════════════════════════════════════════════════

def registrar_votante(cedula: str, nombre: str, lider_id: int) -> Resultado:
    cedula = cedula.strip(); nombre = nombre.strip()
    if not cedula: return Resultado(False, "Cédula vacía.")
    if not nombre: return Resultado(False, "Nombre vacío.")
    if not lider_id: return Resultado(False, "Seleccione un líder.")
    with SessionLocal() as s:
        try:
            lider = s.execute(select(Lider).where(Lider.id==lider_id).with_for_update()).scalar_one_or_none()
            if not lider: return Resultado(False, "Líder no existe.")
            if lider.estado != EstadoLider.ACTIVO: return Resultado(False, "Líder inactivo.")
            if s.execute(select(Votante).where(Votante.cedula==cedula)).scalar_one_or_none():
                return Resultado(False, f"⚠️ Cédula {cedula} ya registrada como votante.")
            ctrl = s.execute(select(ControlCedula).where(ControlCedula.cedula==cedula).with_for_update()).scalar_one_or_none()
            if ctrl and ctrl.estado == EstadoCedula.INHABILITADA:
                return Resultado(False, f"🚫 Cédula {cedula} ya inhabilitada.")
            if ctrl is None:
                return Resultado(False, f"⚠️ Cédula {cedula} no está en el censo electoral.")
            # Registrar votante
            s.add(Votante(cedula=cedula, nombre=nombre, lider_id=lider_id, fecha_registro=datetime.utcnow()))
            s.flush()
            # Inhabilitar en el censo
            ctrl.estado = EstadoCedula.INHABILITADA
            ctrl.fecha_inhabilitacion = datetime.utcnow()
            lider.total_votantes += 1
            s.commit()
            return Resultado(True,
                f"✅ '{nombre}' registrado bajo '{lider.nombre}'.",
                {"cedula":cedula,"nombre":nombre,"lider":lider.nombre,"total_lider":lider.total_votantes})
        except IntegrityError:
            s.rollback(); return Resultado(False, f"⚠️ Cédula {cedula} duplicada.")
        except Exception as e:
            s.rollback(); return Resultado(False, f"❌ Error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# CARGA MASIVA (inhabilitar bloque de un líder)
# ═══════════════════════════════════════════════════════════════════

class ResultadoCargaMasiva:
    def __init__(self):
        self.exitosos=[]; self.fallidos=[]; self.total=0
    @property
    def ok(self): return len(self.exitosos)>0

def extraer_nombre_lider(source_name: str) -> str:
    s = source_name.strip()
    s = re.sub(r"\.xlsx.*","",s,flags=re.IGNORECASE)
    s = re.sub(r"^\d+(\.\d+)*\s+","",s)
    s = re.sub(r"\s*-\s*ZONA\s*\d+\b","",s,flags=re.IGNORECASE)
    s = re.sub(r"\s*-?\s*(COMPLETO\s+VERIFICADO)\s*$","",s,flags=re.IGNORECASE)
    s = re.sub(r"\b\d{4}\b","",s)
    s = re.sub(r"\s*\([^)]*\)\s*"," ",s)
    s = re.sub(r"\.+","",s).strip().strip("-").strip()
    return re.sub(r"\s+"," ",s).title()

def cargar_votantes_masivo(df) -> ResultadoCargaMasiva:
    """
    Carga masiva: busca cada cédula en el censo.
    Si está DISPONIBLE → registra votante e inhabilita.
    Si ya INHABILITADA → salta.
    Si no está en el censo → error.
    """
    resultado = ResultadoCargaMasiva()
    df.columns = [c.strip().lower() for c in df.columns]

    alias_c = {"cedula","documento","cc","id_votante"}
    alias_n = {"nombre","nombre_votante","name","nombres","nombre completo"}
    alias_l = {"lider","lider_nombre","nombre_lider","leader"}
    alias_s = {"source.name","source_name","fuente","archivo"}

    col_c = next((c for c in df.columns if c in alias_c), None)
    col_n = next((c for c in df.columns if c in alias_n), None)
    col_l = next((c for c in df.columns if c in alias_l), None)
    col_s = next((c for c in df.columns if c in alias_s), None)

    # Cache de líderes
    cache = {}
    with SessionLocal() as s:
        for l in s.execute(select(Lider)).scalars().all():
            cache[l.nombre.strip().lower()] = l.id
            cache[str(l.id)] = l.id

    for idx, row in df.iterrows():
        fila = idx+2; resultado.total += 1
        ced  = str(row[col_c]).strip() if col_c else ""
        nom  = str(row[col_n]).strip() if col_n else ""
        lid  = str(row[col_l]).strip() if col_l else ""

        if col_s:
            sv = str(row[col_s]).strip()
            if sv and sv.lower() not in ("nan","none",""): lid = extraer_nombre_lider(sv)

        if not ced or ced.lower() in ("nan","none",""):
            resultado.fallidos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid,"error":"Cédula vacía."}); continue

        # Buscar en censo para obtener nombre y líder si no vienen en el archivo
        info_censo = buscar_en_censo(ced)
        if not info_censo["encontrado"]:
            resultado.fallidos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid,
                                        "error":"Cédula no está en el censo electoral."}); continue
        if info_censo["estado"] == EstadoCedula.INHABILITADA:
            resultado.fallidos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid,
                                        "error":"Ya inhabilitada."}); continue

        # Usar nombre y líder del censo si no vienen en el archivo
        if not nom or nom.lower() in ("nan","none",""): nom = info_censo["nombre"]
        lider_id = info_censo["lider_id"]
        if not lider_id:
            lider_id = cache.get(lid.lower()) if lid else None
        if not lider_id:
            resultado.fallidos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid,
                                        "error":"Líder no encontrado."}); continue

        r = registrar_votante(ced, nom, lider_id)
        if r.ok: resultado.exitosos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid})
        else: resultado.fallidos.append({"fila":fila,"cedula":ced,"nombre":nom,"lider":lid,"error":r.mensaje})

    return resultado

def generar_csv_plantilla():
    return ("cedula,nombre,lider\n1090100001,Ana Maria Torres,Pedro Ramirez\n"
            "1090100002,Luis Gomez Perez,Pedro Ramirez\n1090100003,Sofia Martinez,Carlos Mendez\n")


# ═══════════════════════════════════════════════════════════════════
# CONSULTAS
# ═══════════════════════════════════════════════════════════════════

def consolidado_por_lider():
    with SessionLocal() as s:
        lideres = s.execute(select(Lider).order_by(Lider.total_votantes.desc())).scalars().all()
        return [{"lider_id":l.id,"lider_nombre":l.nombre,"total_votantes":l.total_votantes,
                 "votantes":[{"cedula":v.cedula,"nombre":v.nombre,"fecha_registro":v.fecha_registro}
                              for v in s.execute(select(Votante).where(Votante.lider_id==l.id)
                                         .order_by(Votante.fecha_registro.desc())).scalars().all()]}
                for l in lideres]

def total_votantes_registrados():
    with SessionLocal() as s:
        return s.execute(select(func.count(Votante.id))).scalar() or 0

def buscar_cedula_en_votantes(cedula: str) -> dict:
    """Compatibilidad — ahora busca en el censo."""
    return buscar_en_censo(cedula)


# ═══════════════════════════════════════════════════════════════════
# BORRADO
# ═══════════════════════════════════════════════════════════════════

def borrar_votantes_de_lider(lider_id: int) -> Resultado:
    with SessionLocal() as s:
        lider = s.get(Lider, lider_id)
        if not lider: return Resultado(False, "Líder no encontrado.")
        votantes = s.execute(select(Votante).where(Votante.lider_id==lider_id)).scalars().all()
        count = len(votantes)
        for v in votantes:
            ctrl = s.get(ControlCedula, v.cedula)
            if ctrl:
                ctrl.estado = EstadoCedula.DISPONIBLE
                ctrl.fecha_inhabilitacion = None
            s.delete(v)
        lider.total_votantes = 0
        s.commit()
        return Resultado(True, f"✅ {count} votantes borrados. Cédulas liberadas en el censo.")

def borrar_lider_completo(lider_id: int) -> Resultado:
    with SessionLocal() as s:
        lider = s.get(Lider, lider_id)
        if not lider: return Resultado(False, "Líder no encontrado.")
        nombre = lider.nombre
        votantes = s.execute(select(Votante).where(Votante.lider_id==lider_id)).scalars().all()
        count = len(votantes)
        for v in votantes:
            ctrl = s.get(ControlCedula, v.cedula)
            if ctrl:
                ctrl.estado = EstadoCedula.DISPONIBLE
                ctrl.fecha_inhabilitacion = None
            s.delete(v)
        s.delete(lider)
        s.commit()
        return Resultado(True, f"✅ Líder '{nombre}' y {count} votantes borrados. Cédulas liberadas.")

def borrar_todos_los_votantes() -> Resultado:
    with SessionLocal() as s:
        votantes = s.execute(select(Votante)).scalars().all()
        count = len(votantes)
        for v in votantes:
            ctrl = s.get(ControlCedula, v.cedula)
            if ctrl:
                ctrl.estado = EstadoCedula.DISPONIBLE
                ctrl.fecha_inhabilitacion = None
            s.delete(v)
        for l in s.execute(select(Lider)).scalars().all():
            l.total_votantes = 0
        s.commit()
        return Resultado(True, f"✅ {count} votantes borrados. Censo reseteado a DISPONIBLE.")


# ═══════════════════════════════════════════════════════════════════
# ESTADÍSTICOS
# ═══════════════════════════════════════════════════════════════════

def estadisticos_por_lider() -> list:
    """
    Por cada líder retorna:
    - total_censo: cédulas asignadas en el padrón
    - total_votantes: votantes ya registrados
    - disponibles: pendientes de registrar
    - pct_avance: porcentaje de avance
    """
    from sqlalchemy import func as _func
    with SessionLocal() as s:
        # Conteo de cédulas en el censo por líder
        censo_por_lider = dict(s.execute(
            select(ControlCedula.lider_id, _func.count(ControlCedula.cedula))
            .group_by(ControlCedula.lider_id)
        ).all())

        lideres = s.execute(select(Lider).order_by(Lider.total_votantes.desc())).scalars().all()
        result = []
        for l in lideres:
            total_censo = censo_por_lider.get(l.id, 0)
            registrados = l.total_votantes
            disponibles = total_censo - registrados
            pct = round(registrados / total_censo * 100, 1) if total_censo > 0 else 0.0
            result.append({
                "id": l.id,
                "nombre": l.nombre,
                "estado": l.estado,
                "total_censo": total_censo,
                "registrados": registrados,
                "disponibles": disponibles,
                "pct_avance": pct,
            })
        return result

def liberar_cedulas_a_disponible(cedulas: list) -> Resultado:
    """Fuerza el estado de las cédulas indicadas a DISPONIBLE."""
    if not cedulas:
        return Resultado(False, "Lista vacía.")
    liberadas = 0
    with SessionLocal() as s:
        for c in cedulas:
            ctrl = s.get(ControlCedula, c)
            if ctrl:
                ctrl.estado = EstadoCedula.DISPONIBLE
                ctrl.fecha_inhabilitacion = None
                liberadas += 1
        s.commit()
    return Resultado(True, f"✅ {liberadas} cédulas liberadas como DISPONIBLES.")
