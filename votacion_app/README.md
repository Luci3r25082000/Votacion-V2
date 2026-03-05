# 🗳️ Sistema de Registro Electoral

Aplicación transaccional en Python + Streamlit para gestión de líderes y votantes con garantías de atomicidad, consistencia y antifraude.

---

## 📁 Estructura del Proyecto

```
votacion_app/
├── app.py                    # Aplicación Streamlit principal
├── requirements.txt          # Dependencias
├── models/
│   ├── __init__.py
│   └── database.py           # Modelos SQLAlchemy + Engine
├── services/
│   ├── __init__.py
│   └── electoral.py          # Lógica de negocio transaccional
└── tests/
    └── test_electoral.py     # Suite de pruebas
```

---

## 🚀 Instalación y Ejecución

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Ejecutar la aplicación

```bash
cd votacion_app
streamlit run app.py
```

La app abre en: **http://localhost:8501**

### 3. Ejecutar las pruebas

```bash
cd votacion_app
python tests/test_electoral.py
```

---

## 🗂️ Modelo de Datos

| Tabla | Descripción |
|-------|-------------|
| `lider` | Líderes con contador atómico |
| `votante` | Votantes con cédula UNIQUE |
| `control_cedula` | Censo electoral y estado de cédulas |

---

## 🔐 Garantías del Sistema

| Garantía | Implementación |
|----------|----------------|
| **Atomicidad** | Transacción única: INSERT votante + UPDATE cédula + UPDATE líder |
| **Unicidad** | UNIQUE constraint en `votante.cedula` |
| **Antifraude** | Doble validación: `ControlCedula` + `Votante` antes de insertar |
| **Concurrencia** | `SELECT FOR UPDATE` en líder y cédula + `busy_timeout` SQLite |
| **Rollback** | `session.rollback()` ante cualquier excepción |

---

## 📌 Flujo Transaccional (CORE)

```
INICIO TRANSACCIÓN
  │
  ├─ [1] Bloquear fila del líder (FOR UPDATE)
  ├─ [2] Verificar líder activo
  ├─ [3] Verificar cédula no registrada como votante
  ├─ [4] Verificar cédula no inhabilitada en control
  ├─ [5] INSERT en VOTANTE
  ├─ [6] UPDATE CONTROL_CEDULA → INHABILITADA
  ├─ [7] lider.total_votantes += 1
  │
  └─ COMMIT ──── o ──── ROLLBACK TOTAL
```

---

## 🧪 Tests incluidos

1. ✅ Crear líder
2. ✅ Rechazar líder duplicado
3. ✅ Registrar votante exitosamente
4. ✅ Rechazar cédula duplicada
5. ✅ Verificar incremento del contador del líder
6. ✅ Verificar cédula dada de baja
7. ✅ Rechazar líder inexistente
8. ✅ Consolidado por líder
