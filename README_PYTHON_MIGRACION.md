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

## Guia rapida para diferencias de decimales y sustitucion

Si recibes un reporte de diferencias como el que compartiste (ventas por semana distintas, diferencias en `UND`, `MAD SKU`, `MAD COMBO` o `MAD GEN`), revisa **estos puntos exactos**:

### 1) Redondeo semanal y de totales

En `procesos_adicionales.py`, metodo `obtener_sql_ventas_individual_almacenes`, se redondea por semana y tambien los totales con `ROUND(...,2)`:

- Semanas `S1..S24` (ventas y consumos)
- `venta_total`, `venta_minima`, `venta_maxima`
- Formulas de `mad`, `mad_sku`, `mad_combo`

Si Produccion usa mas/menos precision, cambia esos `2` (y en MAD el `5`) para igualar comportamiento.

### 2) Agrupacion de cadenas de sustitucion

En `procesos_adicionales.py`, metodo `obtener_sql_ventas_resumidas`, la agrupacion de sustitutos ocurre con:

- `solo_sustitutos=True`
- filtro `v.vigente is not null`
- `GROUP BY v.empresa_id, v.vigente, v.numalm, ...` (o sin `numalm` en general)

Si necesitas excluir el SKU vigente de su propio grupo (evitar mezclar "vigente + historicos"), el ajuste se hace en este query (por ejemplo agregando `AND v.numart <> v.vigente` en el caso de sustitutos).

### 3) Carga del mapeo vigente/historico

En `procesa_sustitutos` se genera `config_reemplazo_abc` y luego en `obtener_ventas_individual` se actualiza `v.vigente` con join a esa tabla.

Si el mapeo tiene cadenas incompletas o duplicadas, cualquier diferencia de ventas semanales se propaga a ABC y MAD.

### 4) Recomendacion para validar rapido contra Produccion

1. Compara el contenido de `config_reemplazo_abc` entre ambos ambientes.
2. Valida una muestra de SKUs con query semanal (`S1..S24`) antes y despues de aplicar sustitucion.
3. Verifica que ambos lados usen la misma precision de `ROUND` en todas las sumas intermedias.
