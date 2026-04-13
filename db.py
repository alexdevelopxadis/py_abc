from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DBConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


class DBSession:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def execute_update(self, sql: str) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            self.conn.commit()  # 🔥 SIEMPRE commit
            return cur.rowcount if cur.rowcount is not None else 0
        finally:
            cur.close()

    def query_list(self, sql: str) -> list[Any]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                return []
            if len(rows[0]) == 1:
                return [r[0] for r in rows]
            return rows
        finally:
            cur.close()

    def query_one(self, sql: str) -> Any:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                return None
            if len(row) == 1:
                return row[0]
            return row
        finally:
            cur.close()


def _parse_jdbc_url(url: str) -> tuple[str, int, str]:
    m = re.match(r"jdbc:mysql://([^/:]+)(?::(\d+))?/([^?]+)", url.strip())
    if not m:
        raise ValueError(f"No se pudo parsear JDBC URL: {url}")
    host = m.group(1)
    port = int(m.group(2) or 3306)
    db = m.group(3)
    return host, port, db


def load_hibernate_config(path: str | Path = "src/hibernate.cfg.xml") -> DBConfig:
    tree = ET.parse(str(path))
    root = tree.getroot()

    props: dict[str, str] = {}
    for p in root.findall(".//property"):
        name = p.attrib.get("name")
        value = (p.text or "").strip()
        if name:
            props[name] = value

    url = props.get("connection.url")
    user = props.get("connection.username")
    password = props.get("connection.password")
    if not url or not user:
        raise ValueError("Faltan propiedades de conexion en hibernate.cfg.xml")

    host, port, database = _parse_jdbc_url(url)
    return DBConfig(
        host=host,
        port=port,
        database=database,
        username=user,
        password=password or "",
    )


def open_session(cfg: DBConfig) -> DBSession:
    import mysql.connector

    conn = mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.username,
        password=cfg.password,
        database=cfg.database,
        autocommit=True,
    )
    return DBSession(conn)
