from __future__ import annotations

from datetime import datetime, timedelta


def fmt(fecha: datetime, formato: str) -> str:
    py_fmt = (
        formato.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
    )
    return fecha.strftime(py_fmt)


def add_days(fecha: datetime, dias: int) -> datetime:
    return fecha + timedelta(days=dias)
