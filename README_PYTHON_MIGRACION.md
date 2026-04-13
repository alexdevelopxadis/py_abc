# Migracion ABC Java -> Python

Se migro el flujo principal del ABC a Python conservando el orden y SQL original:

1. `procesa_sustitutos`
2. `obtener_ventas_individual`
3. `obtener_ventas_resumida`
4. `inicializar_ventas_categorias`
5. `procesar_abc` (`GENERAL_MARCA`, `GENERAL_MARCA_INDIV`, `ALMACEN_MARCA`, `ALMACEN_MARCA_INDIV`)

## Estructura

- `py_abc/db.py`: conexion MySQL y lectura de `src/hibernate.cfg.xml`
- `py_abc/dao_general.py`: empresas activas y semanas a evaluar
- `py_abc/procesos_adicionales.py`: logica de sustitutos/ventas/abc
- `py_abc/principal_abc.py`: orquestacion del proceso
- `py_abc/run_abc.py`: punto de entrada CLI

## Instalacion

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecucion

```bash
python3 -m py_abc.run_abc T
```

Procesos soportados:

- `T` (total)
- `GM`
- `GMI`
- `AM`
- `AMI`

Opcional:

```bash
python3 -m py_abc.run_abc T --hibernate-config src/hibernate.cfg.xml
```

## Nota

Esta migracion mantiene SQL nativo MySQL para preservar comportamiento del Java original.
