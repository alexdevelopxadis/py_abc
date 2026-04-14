from __future__ import annotations

import argparse
import sys

from .db import load_hibernate_config, open_session
from .principal_abc import PrincipalABC


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ejecutor del proceso ABC en Python")
    parser.add_argument("proceso", help="T | GM | GMI | AM | AMI")
    parser.add_argument(
        "--hibernate-config",
        default="py_abc/hibernate.cfg.xml",
        help="Ruta al hibernate.cfg.xml",
    )
    args = parser.parse_args(argv)

    proceso = args.proceso.replace("-", "").upper()
    if proceso not in {"T", "GM", "GMI", "AM", "AMI"}:
        raise ValueError(f"Proceso no valido: {proceso}")

    cfg = load_hibernate_config(args.hibernate_config)
    session = open_session(cfg)

    print("Proceso Iniciado")
    try:
        PrincipalABC().ejecutar_abc(session, proceso)
    except Exception:
        print("INFO: la aplicacion fallo")
        raise
    finally:
        print("se cierran los recursos")
        session.conn.close()

    print("Proceso finalizado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
