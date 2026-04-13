from __future__ import annotations

from datetime import datetime

from .db import DBSession
from .utils import add_days


class DaoGeneral:
    def obtener_semanas_evaluar(self, semanas: int) -> list[tuple[datetime, datetime]]:
        fecha_hasta = datetime.now()
        out: list[tuple[datetime, datetime]] = []
        for _ in range(semanas):
            fecha_desde = add_days(fecha_hasta, -1)
            fecha_hasta = add_days(fecha_desde, -6)
            out.append((fecha_desde, fecha_hasta))
        return out

    def obtener_empresas_activas(self, session: DBSession) -> list[int]:
        rows = session.query_list("SELECT e.id FROM cat_empresa e WHERE e.activo=1")
        return [int(x) for x in rows]
