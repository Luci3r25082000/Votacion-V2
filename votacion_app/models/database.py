"""Modelos de base de datos - Sistema Electoral

Mantiene SQLAlchemy como ORM y expone:
- DATABASE_URL
- engine
- SessionLocal
- Base
- init_db()

Soporte de configuración por entorno:
1) TEST_MODE=1  -> SQLite en memoria (StaticPool)
2) DATABASE_URL -> Usa esa URL (normalizada para psycopg3/Supabase)
3) En Vercel sin DATABASE_URL -> SQLite en /tmp/votacion.db
4) Default -> SQLite local ./votacion.db
"""

from __future__ import annotations

import enum
import os
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.pool import StaticPool


def _env_flag(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _is_vercel() -> bool:
    return bool(os.getenv("VERCEL") or os.getenv("VERCEL_ENV"))


def normalize_database_url(url: str) -> str:
    """Normaliza URLs para Postgres/Supabase.

    - Acepta postgres:// y postgresql://
    - Fuerza psycopg3: postgresql+psycopg://
    - Reescribe postgresql+psycopg2:// -> postgresql+psycopg://
    - Si el host es Supabase, agrega sslmode=require si no está
    """

    u = (url or "").strip()
    if not u:
        return u

    # Arreglos de esquemas comunes
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    if u.startswith("postgresql+psycopg2://"):
        u = "postgresql+psycopg://" + u[len("postgresql+psycopg2://") :]

    parsed = urlparse(u)
    scheme = parsed.scheme

    if scheme == "postgresql":
        scheme = "postgresql+psycopg"
    elif scheme == "postgres":
        scheme = "postgresql+psycopg"
    elif scheme == "postgresql+psycopg2":
        scheme = "postgresql+psycopg"

    hostname = (parsed.hostname or "").lower()
    is_supabase = "supabase" in hostname

    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if is_supabase and "sslmode" not in {k.lower() for k in query_items.keys()}:
        query_items["sslmode"] = "require"

    # Mantener orden estable no es crítico, pero ayuda en debugging
    new_query = urlencode(query_items, doseq=True)
    rebuilt = parsed._replace(scheme=scheme, query=new_query)
    return urlunparse(rebuilt)


def _resolve_database_url() -> str:
    if _env_flag("TEST_MODE"):
        return "sqlite+pysqlite:///:memory:"

    env_url = (os.getenv("DATABASE_URL") or "").strip()
    if env_url:
        return normalize_database_url(env_url)

    if _is_vercel():
        return "sqlite+pysqlite:////tmp/votacion.db"

    return "sqlite+pysqlite:///./votacion.db"


DATABASE_URL = _resolve_database_url()


def database_url_source() -> str:
    if _env_flag("TEST_MODE"):
        return "TEST_MODE"
    if (os.getenv("DATABASE_URL") or "").strip():
        return "ENV_DATABASE_URL"
    if _is_vercel():
        return "VERCEL_TMP_SQLITE"
    return "LOCAL_SQLITE"


def redact_database_url(url: str) -> str:
    """Devuelve una versión segura para logs/UI (sin password)."""
    try:
        p = urlparse(url)
        if p.scheme.startswith("sqlite"):
            return url
        # userinfo redacted
        netloc = p.hostname or ""
        if p.port:
            netloc = f"{netloc}:{p.port}"
        return urlunparse(p._replace(netloc=netloc))
    except Exception:
        return "<unparseable>"


def database_diagnostics() -> dict:
    """Info no sensible para depurar despliegues serverless."""
    diag: dict = {
        "source": database_url_source(),
        "redacted_url": redact_database_url(DATABASE_URL),
    }
    try:
        p = urlparse(DATABASE_URL)
        diag.update(
            {
                "scheme": p.scheme,
                "host": p.hostname,
                "port": p.port,
                "db": (p.path or "").lstrip("/") or None,
            }
        )
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        diag["sslmode"] = q.get("sslmode")
    except Exception:
        pass
    return diag


def _create_engine(db_url: str):
    if db_url.startswith("sqlite"):
        if _env_flag("TEST_MODE") and ":memory:" in db_url:
            return create_engine(
                db_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        return create_engine(db_url, connect_args={"check_same_thread": False})

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "300")),
    )


engine = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class EstadoCedula(str, enum.Enum):
    DISPONIBLE   = "DISPONIBLE"
    INHABILITADA = "INHABILITADA"


class EstadoLider(str, enum.Enum):
    ACTIVO   = "ACTIVO"
    INACTIVO = "INACTIVO"


class Lider(Base):
    __tablename__ = "lider"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    nombre          = Column(String(200), nullable=False, unique=True)
    estado          = Column(String(20),  default=EstadoLider.ACTIVO)
    total_votantes  = Column(Integer, default=0)
    fecha_creacion  = Column(DateTime, default=datetime.utcnow)
    votantes        = relationship("Votante", back_populates="lider")
    cedulas_censo   = relationship("ControlCedula", back_populates="lider")


class ControlCedula(Base):
    """
    Padrón electoral completo.
    Cada fila = una persona habilitada para votar.
    """
    __tablename__ = "control_cedula"
    cedula                = Column(String(20),  primary_key=True)
    nombre                = Column(String(200), nullable=True)   # nombre del votante del padrón
    lider_id              = Column(Integer, ForeignKey("lider.id"), nullable=True)
    estado                = Column(String(20),  default=EstadoCedula.DISPONIBLE)
    fecha_inhabilitacion  = Column(DateTime, nullable=True)
    lider                 = relationship("Lider", back_populates="cedulas_censo")


class Votante(Base):
    """Registro efectivo de voto — solo se crea cuando alguien vota."""
    __tablename__ = "votante"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    cedula         = Column(String(20), unique=True, nullable=False)
    nombre         = Column(String(200), nullable=False)
    lider_id       = Column(Integer, ForeignKey("lider.id"), nullable=False)
    fecha_registro = Column(DateTime, default=datetime.utcnow)
    lider          = relationship("Lider", back_populates="votantes")


def init_db():
    Base.metadata.create_all(bind=engine)
