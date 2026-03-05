"""
Tests del sistema electoral — Pruebas mínimas requeridas
Ejecutar: python tests/test_electoral.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Usar DB de prueba en memoria
os.environ["TEST_MODE"] = "1"

from models.database import Base, engine, SessionLocal, init_db
from services import electoral as svc

# Limpiar y recrear tablas para tests
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)


def seed_censo(cedulas, lider_id, nombre_prefix="Votante"):
    filas = []
    for i, ced in enumerate(cedulas, 1):
        filas.append({"cedula": str(ced), "nombre": f"{nombre_prefix} {i}", "lider_id": lider_id})
    return svc.cargar_censo_masivo(filas)


def separador(titulo):
    print(f"\n{'='*60}")
    print(f"  {titulo}")
    print("="*60)


def test_crear_lider():
    separador("TEST 1: Crear líder")
    r = svc.crear_lider("Pedro Ramírez")
    assert r.ok, f"Falló: {r.mensaje}"
    print(f"✅ {r.mensaje}")
    print(f"   ID asignado: {r.datos['id']}")


def test_crear_lider_duplicado():
    separador("TEST 2: Crear líder duplicado")
    svc.crear_lider("Ana Torres")
    r = svc.crear_lider("Ana Torres")
    assert not r.ok, "Debería fallar por duplicado"
    print(f"✅ Duplicado rechazado correctamente: {r.mensaje}")


def test_registrar_votante_exitoso():
    separador("TEST 3: Registrar votante exitosamente")
    svc.crear_lider("Carlos Mendez")
    lideres = svc.listar_lideres(solo_activos=True)
    lider_id = next(l["id"] for l in lideres if l["nombre"] == "Carlos Mendez")

    seed_censo(["1090111222"], lider_id, nombre_prefix="María González")

    r = svc.registrar_votante("1090111222", "María González", lider_id)
    assert r.ok, f"Falló: {r.mensaje}"
    print(f"✅ {r.mensaje}")
    print(f"   Datos: {r.datos}")


def test_cedula_duplicada():
    separador("TEST 4: Intentar registrar cédula duplicada")
    lideres = svc.listar_lideres(solo_activos=True)
    lider_id = lideres[0]["id"]

    seed_censo(["9999999999"], lider_id, nombre_prefix="Votante Original")

    # Primer registro
    svc.registrar_votante("9999999999", "Votante Original", lider_id)

    # Intento duplicado
    r = svc.registrar_votante("9999999999", "Votante Falso", lider_id)
    assert not r.ok, "Debería fallar por cédula duplicada"
    print(f"✅ Duplicado rechazado: {r.mensaje}")


def test_contador_lider():
    separador("TEST 5: Verificar aumento del contador del líder")
    svc.crear_lider("Líder Contador")
    lideres = svc.listar_lideres(solo_activos=True)
    lider = next(l for l in lideres if l["nombre"] == "Líder Contador")
    lider_id = lider["id"]
    inicial = lider["total_votantes"]

    seed_censo(["1111111111", "2222222222", "3333333333"], lider_id, nombre_prefix="Votante")

    svc.registrar_votante("1111111111", "Votante A", lider_id)
    svc.registrar_votante("2222222222", "Votante B", lider_id)
    svc.registrar_votante("3333333333", "Votante C", lider_id)

    lideres_actualizados = svc.listar_lideres()
    lider_act = next(l for l in lideres_actualizados if l["id"] == lider_id)

    esperado = inicial + 3
    assert lider_act["total_votantes"] == esperado, \
        f"Esperado {esperado}, obtenido {lider_act['total_votantes']}"
    print(f"✅ Contador correcto: {inicial} → {lider_act['total_votantes']} (+3)")


def test_cedula_dada_de_baja():
    separador("TEST 6: Verificar cédula inhabilitada tras registro")
    lideres = svc.listar_lideres(solo_activos=True)
    lider_id = lideres[0]["id"]

    cedula_test = "5555555555"

    seed_censo([cedula_test], lider_id, nombre_prefix="Votante Prueba Baja")
    svc.registrar_votante(cedula_test, "Votante Prueba Baja", lider_id)

    info = svc.buscar_cedula(cedula_test)
    assert info["control"]["estado"] == "INHABILITADA", "Cédula debería estar inhabilitada"
    assert info["votante"]["registrado"] == True, "Votante debería estar registrado"
    print(f"✅ Cédula {cedula_test} inhabilitada correctamente")
    print(f"   Fecha inhabilitación: {info['control']['fecha_inhabilitacion']}")


def test_lider_inexistente():
    separador("TEST 7: Registrar votante con líder inexistente")
    r = svc.registrar_votante("7777777777", "Fantasma", lider_id=99999)
    assert not r.ok, "Debería fallar por líder inexistente"
    print(f"✅ Líder inexistente rechazado: {r.mensaje}")


def test_consolidado():
    separador("TEST 8: Consolidado por líder")
    consolidado = svc.consolidado_por_lider()
    assert isinstance(consolidado, list)
    for grupo in consolidado:
        assert "lider_nombre" in grupo
        assert "votantes" in grupo
        assert grupo["total_votantes"] == len(grupo["votantes"])
    print(f"✅ Consolidado correcto: {len(consolidado)} líderes")
    for g in consolidado:
        print(f"   {g['lider_nombre']}: {g['total_votantes']} votantes")


if __name__ == "__main__":
    print("\n🗳️  SUITE DE PRUEBAS — Sistema Electoral")
    print("=" * 60)

    tests = [
        test_crear_lider,
        test_crear_lider_duplicado,
        test_registrar_votante_exitoso,
        test_cedula_duplicada,
        test_contador_lider,
        test_cedula_dada_de_baja,
        test_lider_inexistente,
        test_consolidado,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ FALLÓ: {e}")
            failed += 1
        except Exception as e:
            print(f"💥 ERROR INESPERADO: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"  RESULTADO: {passed} ✅ pasaron  |  {failed} ❌ fallaron")
    print("=" * 60)
