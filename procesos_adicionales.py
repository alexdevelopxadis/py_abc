from __future__ import annotations

from datetime import datetime

from .dao_general import DaoGeneral
from .db import DBSession
from .utils import fmt


class ProcesosAdicionales:
    def actualizar_mad_combo(self, session: DBSession, empresa_id: int, fecha: str) -> int:
        sql = (
            " UPDATE abc_generado ag_hijo "
            " INNER JOIN rel_combo_articulo r "
            "   ON ag_hijo.numart = r.numart "
            "  AND ag_hijo.empresa_id = r.empresa_id "
            " INNER JOIN abc_generado ag_padre "
            "   ON r.cod_combo = ag_padre.numart "
            "  AND ag_padre.empresa_id = r.empresa_id "
            f"  AND DATE(ag_padre.fecha_creacion) = DATE('{fecha}') "
            "  AND ag_padre.tipo_abc = ag_hijo.tipo_abc "
            "  AND ag_padre.numalm = ag_hijo.numalm "
            " SET ag_hijo.mad_combo = ag_padre.mad_sku "
            f" WHERE ag_hijo.empresa_id = {empresa_id} "
            f"   AND DATE(ag_hijo.fecha_creacion) = DATE('{fecha}') "
            "   AND ag_hijo.tipo_abc IN ('20', '21', '18', '19') "
        )
        return session.execute_update(sql)

    def sumar_mad_combo_sku_por_ids(
        self,
        session: DBSession,
        empresa_id: int,
        fecha: str,
        lote_size: int = 500,
    ) -> int:
        sql_ids = (
            " SELECT id_abc "
            " FROM abc_generado "
            f" WHERE empresa_id = {empresa_id} "
            f"   AND DATE(fecha_creacion) = DATE('{fecha}') "
            "   AND tipo_abc IN ('20', '21', '18', '19') "
            "   AND (mad_combo IS NOT NULL OR mad_sku IS NOT NULL) "
            " ORDER BY id_abc "
        )
        ids = [int(x) for x in session.query_list(sql_ids)]
        if not ids:
            return 0

        total = 0
        for i in range(0, len(ids), lote_size):
            chunk = ids[i : i + lote_size]
            ids_str = ",".join(str(x) for x in chunk)
            sql_update = (
                " UPDATE abc_generado "
                " SET mad = COALESCE(mad_combo, 0) + COALESCE(mad_sku, 0) "
                f" WHERE empresa_id = {empresa_id} "
                f"   AND id_abc IN ({ids_str}) "
            )
            total += session.execute_update(sql_update)
        return total

    def get_numart_con_mad_incorrecto(self, session: DBSession, empresa_id: int) -> list[str]:
        sql = (
            " SELECT DISTINCT hijo.numart "
            " FROM reabasto_calculos_combos hijo "
            " INNER JOIN reabasto_calculos_combos maestro "
            "   ON maestro.cod_combo = hijo.cod_combo "
            "  AND maestro.cod_combo = maestro.numart "
            f" WHERE hijo.empresa_id = {empresa_id} "
            "   AND maestro.empresa_id = hijo.empresa_id "
            "   AND hijo.numart <> hijo.cod_combo "
            "   AND hijo.mad_combo <> maestro.mad_sku "
        )
        return [str(x) for x in session.query_list(sql)]

    def actualizar_mad_combo_correcto(self, session: DBSession, empresa_id: int, numart: str) -> int:
        safe_numart = str(numart).replace("'", "''")
        sql = (
            " UPDATE reabasto_calculos_combos hijo "
            " INNER JOIN reabasto_calculos_combos maestro "
            "   ON maestro.cod_combo = hijo.cod_combo "
            "  AND maestro.cod_combo = maestro.numart "
            "  AND maestro.empresa_id = hijo.empresa_id "
            " SET hijo.mad_combo = maestro.mad_sku "
            f" WHERE hijo.empresa_id = {empresa_id} "
            f"   AND hijo.numart = '{safe_numart}' "
            "   AND hijo.mad_combo <> maestro.mad_sku "
        )
        return session.execute_update(sql)

    def actualizar_mad_combo_reabasto(self, session: DBSession, empresa_id: int) -> int:
        sql = (
            " UPDATE reabasto_calculos_combos ag "
            " SET ag.mad_combo_receta = ag.mad_combo * ag.cantidad_receta "
            f" WHERE ag.empresa_id = {empresa_id} "
            "   AND ag.cantidad_receta > 0 "
        )
        return session.execute_update(sql)

    def actualizar_mad_combo_planificado_reabasto(self, session: DBSession, empresa_id: int) -> int:
        sql = (
            " UPDATE reabasto_calculos_combos ag_hijo "
            " INNER JOIN ( "
            "   SELECT numart, empresa_id, SUM(mad_combo_receta) AS suma_mad_sku "
            "   FROM reabasto_calculos_combos "
            f"   WHERE empresa_id = {empresa_id} "
            "     AND numart IS NOT NULL "
            "   GROUP BY numart, empresa_id "
            " ) subquery_suma "
            "   ON ag_hijo.numart = subquery_suma.numart "
            "  AND ag_hijo.empresa_id = subquery_suma.empresa_id "
            " SET ag_hijo.mad_mes_receta = subquery_suma.suma_mad_sku "
            f" WHERE ag_hijo.empresa_id = {empresa_id} "
            "   AND ag_hijo.cantidad_receta > 0 "
        )
        return session.execute_update(sql)

    def sumar_mad_combo_sku_por_ids_reabasto(self, session: DBSession, empresa_id: int) -> int:
        sql = (
            " UPDATE reabasto_calculos_combos "
            " SET mad = COALESCE(mad_mes_receta, 0) + COALESCE(mad_sku, 0) "
            f" WHERE empresa_id = {empresa_id} "
            "   AND (mad_mes_receta IS NOT NULL OR mad_sku IS NOT NULL) "
            "   AND cantidad_receta > 0 "
        )
        return session.execute_update(sql)

    def actualizar_mad_combo_desde_reabasto(self, session: DBSession, empresa_id: int, fecha: str) -> int:
        sql = (
            " UPDATE abc_generado ag "
            " INNER JOIN ( "
            "   SELECT numart, empresa_id, MAX(mad_mes_receta) AS mad_mes_receta "
            "   FROM reabasto_calculos_combos "
            f"   WHERE empresa_id = {empresa_id} "
            "   GROUP BY numart, empresa_id "
            " ) rcc "
            "   ON ag.numart = rcc.numart "
            "  AND ag.empresa_id = rcc.empresa_id "
            " SET ag.mad_combo = rcc.mad_mes_receta "
            f" WHERE ag.empresa_id = {empresa_id} "
            f"   AND DATE(ag.fecha_creacion) = DATE('{fecha}') "
            "   AND ag.tipo_abc IN ('18') "
        )
        return session.execute_update(sql)

    def sumar_mad_combo_sku_por_ids_correcto(
        self,
        session: DBSession,
        empresa_id: int,
        fecha: str,
        lote_size: int = 500,
    ) -> int:
        sql_ids = (
            " SELECT id_abc "
            " FROM abc_generado "
            f" WHERE empresa_id = {empresa_id} "
            f"   AND DATE(fecha_creacion) = DATE('{fecha}') "
            "   AND tipo_abc IN ('18') "
            "   AND (mad_combo IS NOT NULL OR mad_sku IS NOT NULL) "
            " ORDER BY id_abc "
        )
        ids = [int(x) for x in session.query_list(sql_ids)]
        if not ids:
            return 0

        total = 0
        for i in range(0, len(ids), lote_size):
            chunk = ids[i : i + lote_size]
            ids_str = ",".join(str(x) for x in chunk)
            sql_update = (
                " UPDATE abc_generado "
                " SET mad = COALESCE(mad_combo, 0) + COALESCE(mad_sku, 0) "
                f" WHERE empresa_id = {empresa_id} "
                f"   AND id_abc IN ({ids_str}) "
            )
            total += session.execute_update(sql_update)
        return total

    def eliminar_tipo_abc_por_lotes(self, session, table, codigo_abc, batch_size=100):
        import time

        total = 0
        codigo_abc = str(codigo_abc).strip().replace("'", "''")

        while True:
            sql = f"""
            DELETE FROM {table}
            WHERE tipo_abc = '{codigo_abc}'
            LIMIT {batch_size}
            """

            print("SQL A EJECUTAR:")
            print(" ".join(sql.split()))

            eliminados = session.execute_update(sql)
            total += eliminados

            print(f"lote: {eliminados}, total: {total}")

            if eliminados == 0:
                break

            time.sleep(0.1)

        return total
    def limpiar_tablas_ventas(self, session: DBSession, empresa_id: int) -> None:
        _ = empresa_id
        session.execute_update("TRUNCATE TABLE abc_ventas_individual")
        session.execute_update("TRUNCATE TABLE abc_ventas")

    def inicializar_tablas_abc(self, session: DBSession, empresa_id: int) -> None:
        _ = empresa_id
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** INICIALIZANDO TABLAS ABC ***")
        session.execute_update("TRUNCATE TABLE abc_generado")

    def inicializar_ventas_categorias(self, session: DBSession, empresa_id: int) -> None:
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** INICIALIZANDO TABLAS VENTAS X CATEGORIAS ***")

        session.execute_update("TRUNCATE TABLE abc_ventas_categoria")
        session.execute_update(
            " INSERT INTO abc_ventas_categoria "
            " SELECT v.numalm, a.cod_categoria10, ROUND(SUM(v.venta_total),6) AS total_categoria "
            " FROM abc_ventas v "
            " INNER JOIN cat_articulo a "
            "   ON a.empresa_id = v.empresa_id "
            "  AND a.numart = v.numart "
            "  AND a.estatus = 1 "
            "  AND a.cod_categoria10 NOT IN('') "
            "  AND a.tipo_almacenamiento IN('P', 'U', 'O') "
            f" WHERE v.empresa_id = {empresa_id} "
            " GROUP BY v.numalm, a.cod_categoria10 "
        )

        session.execute_update("TRUNCATE TABLE abc_ventas_individual_categoria")
        session.execute_update(
            " INSERT INTO abc_ventas_individual_categoria "
            " SELECT v.numalm, a.cod_categoria10, ROUND(SUM(v.venta_total),6) AS total_categoria "
            " FROM abc_ventas_individual v "
            " INNER JOIN cat_articulo a FORCE INDEX(idx_01) "
            "   ON a.empresa_id = v.empresa_id "
            "  AND a.numart = v.numart "
            f" WHERE v.empresa_id = {empresa_id} "
            " GROUP BY v.numalm, a.cod_categoria10 "
        )

    def relacion_articulos_precios(self, session: DBSession, empresa_id: int, almacen: str, estatus: int) -> None:
        sql = (
            " INSERT INTO rel_articulo_precios("
            " numalm, numart, fecha_ultima_compra, fecha_ultima_venta, precio_compra,precio_venta, costo_fob, "
            " empresa_id, usuario_creacion, fecha_actualizacion_erp, fecha_actualizacion, fecha_creacion"
            " )"
            f" SELECT '{almacen}', numart, NULL, NULL, NULL, NULL, NULL, 1, '1', '0000-00-00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP "
            " FROM cat_articulo ca "
            f" WHERE ca.empresa_id = {empresa_id} "
            + (f" AND ca.estatus = {estatus} " if estatus > 0 else "")
            + " AND NOT EXISTS ("
            "   SELECT 'X' FROM rel_articulo_precios ra "
            "   WHERE ra.empresa_id = ca.empresa_id "
            "     AND ra.numart = ca.numart "
            f"     AND ra.numalm = '{almacen}'"
            " )"
        )
        r = session.execute_update(sql)
        print(f"respuesta: {r}")

    def obtener_almacenes(self, session: DBSession, empresa_id: int) -> list[str]:
        sql = (
            " SELECT DISTINCT x.numalm "
            " FROM ( "
            "   SELECT a.numalm "
            "   FROM cat_almacen a "
            "   INNER JOIN rel_almacen_familia af ON af.empresa_id = a.empresa_id AND af.numalm = a.numalm "
            "   WHERE a.activo = 1 "
            f"     AND a.empresa_id = {empresa_id} "
            "   UNION "
            "   SELECT v.numalm "
            "   FROM mov_ventas_erp v "
            f"   WHERE v.empresa_id = {empresa_id} "
            "   UNION "
            "   SELECT c.numalm "
            "   FROM mov_consumo_erp c "
            f"   WHERE c.empresa_id = {empresa_id} "
            " ) x "
            " WHERE IFNULL(x.numalm, '') <> '' "
            " ORDER BY x.numalm "
        )
        return [str(x) for x in session.query_list(sql)]

    def obtener_categorias(self, session: DBSession, empresa_id: int) -> list[str]:
        sql = (
            " SELECT cod_categoria10 "
            " FROM cat_articulo "
            " WHERE cod_categoria10 NOT IN('') "
            f" AND empresa_id={empresa_id} "
            " GROUP BY cod_categoria10 "
            " ORDER BY cod_categoria10 ASC"
        )
        return [str(x) for x in session.query_list(sql)]

    def obtener_almacenes_categorias(self, session: DBSession, empresa_id: int, almacen: str) -> list[str]:
        filtro_general = " AND v.cod_categoria10 NOT IN ('') " if almacen.upper() == "GENERAL" else ""
        sql = (
            " SELECT DISTINCT c.cod_categoria10 "
            " FROM cat_articulo c "
            " INNER JOIN abc_ventas_individual_categoria v "
            "   ON v.cod_categoria10 = c.cod_categoria10 "
            f"  AND v.numalm = '{almacen}' "
            f" {filtro_general} "
            f" WHERE c.empresa_id = {empresa_id}"
        )
        return [str(x) for x in session.query_list(sql)]

    def procesa_sustitutos(self, session: DBSession, id_empresa: int) -> None:
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** PROCESANDO SUSTITUTOS ***")

        session.execute_update("TRUNCATE TABLE config_reemplazo_abc")

        str_query_sustitutos = (
            " SELECT CONCAT(p.numart,'-',p.numart_historico) "
            " FROM config_reemplazo_sku p "
            " WHERE p.estatus = 1 "
            " ORDER BY p.numart "
        )

        ls = [str(x) for x in session.query_list(str_query_sustitutos)]
        array_reemplazo: list[str] = []
        vigente: dict[str, str] = {}
        str_values: list[str] = []

        max_id = 0
        resultado = 0

        for item in ls:
            parts = item.split("-")
            articulo_vigente = parts[0]
            cadena_sustitutos = parts[1] if len(parts) > 1 else ""

            array_reemplazo.clear()
            array_reemplazo.append(articulo_vigente)
            vigente[articulo_vigente] = articulo_vigente
            str_values.append(f"({id_empresa},'{articulo_vigente}','{articulo_vigente}',1)")

            if cadena_sustitutos.strip():
                for sust in [x.strip() for x in cadena_sustitutos.split(",") if x.strip()]:
                    if sust not in array_reemplazo and sust not in vigente:
                        array_reemplazo.append(sust)
                        vigente[sust] = articulo_vigente
                        str_values.append(f"({id_empresa},'{sust}','{articulo_vigente}',1)")

            max_id += 1
            if max_id % 1000 == 0 and str_values:
                resultado = session.execute_update(
                    " INSERT INTO config_reemplazo_abc VALUES " + ",".join(str_values)
                )
                str_values = []

        if str_values:
            resultado = session.execute_update(
                " INSERT INTO config_reemplazo_abc VALUES " + ",".join(str_values)
            )

        print(f"Insertados Final: {resultado} = maxId: {max_id}")
        session.execute_update("commit")

        str_query_sustitutos_niveles = (
            " SELECT CONCAT(r.numartvigente,'-',p.numart_historico) "
            " FROM config_reemplazo_abc r "
            " INNER JOIN config_reemplazo_sku p "
            "   ON p.numart = r.numart "
            "  AND p.estatus = 0 "
            "  AND LENGTH(TRIM(p.numart_historico)) > 0 "
            f" WHERE r.empresa_id = {id_empresa} "
            " AND r.numart <> r.numartvigente "
            " AND r.nivel = {0} "
            " ORDER BY r.numartvigente ASC "
        )

        ejecuta_nivel = 1
        fin_niveles = False
        while not fin_niveles:
            q = str_query_sustitutos_niveles.replace("{0}", str(ejecuta_nivel))
            ls_nivel = [str(x) for x in session.query_list(q)]
            if not ls_nivel:
                fin_niveles = True
                continue

            ejecuta_nivel += 1
            str_values = []
            for item in ls_nivel:
                parts = item.split("-")
                articulo_vigente = parts[0]
                cadena = parts[1] if len(parts) > 1 else ""
                for sust in [x.strip() for x in cadena.split(",") if x.strip()]:
                    str_values.append(f"({id_empresa},'{sust}','{articulo_vigente}',{ejecuta_nivel})")

            if str_values:
                resultado = session.execute_update(
                    " INSERT IGNORE INTO config_reemplazo_abc VALUES " + ",".join(str_values)
                )
                print(f"Insertados: {resultado} = nivel: {ejecuta_nivel}")

        str_query_ajustes = (
            " INSERT IGNORE INTO cat_articulo(numart) "
            " SELECT c.numart FROM cat_articulo c "
            f" WHERE c.empresa_id = {id_empresa} "
            " AND c.estatus = 0 "
            " ON DUPLICATE KEY UPDATE estatus = 1 "
        )
        resultado = session.execute_update(str_query_ajustes)
        print(f"sustitutos a vigentes: {resultado}")

        str_query_ajustes = (
            " INSERT IGNORE INTO cat_articulo(numart) "
            " SELECT c.numart FROM cat_articulo c "
            " INNER JOIN config_reemplazo_abc r "
            "   ON r.empresa_id = c.empresa_id "
            "  AND r.numart = c.numart "
            "  AND r.numart <> r.numartvigente "
            " ON DUPLICATE KEY UPDATE estatus = 0, fecha_actualizacion = CURRENT_TIMESTAMP "
        )
        resultado = session.execute_update(str_query_ajustes)
        print(f"sustitutos los articulos procesados de la lista: {resultado}")

        session.execute_update("commit")
        print("Fin procesaSustitutos")

    def obtener_sql_ventas_individual_almacenes(self, session: DBSession, id_empresa: int, semanas_evaluar: int) -> str:
        dao = DaoGeneral()
        lista_rangos_semanas = dao.obtener_semanas_evaluar(semanas_evaluar)

        str_query_ventas = ""
        str_query_consumos = ""
        str_query_inventarios = ""
        str_query_fechas = ""

        fecha_desde = None
        fecha_hasta = None
        str_query_semanas_ventas = ""
        str_query_semanas_consumo = ""
        str_semanas_general = ""

        for i, rango in enumerate(lista_rangos_semanas):
            r_hasta = rango[0]
            r_desde = rango[1]

            if fecha_hasta is None and i == 0:
                fecha_hasta = r_hasta
            fecha_desde = r_desde

            str_query_semanas_ventas += (
                " ,ROUND(SUM(CASE WHEN v.fecha_venta >= '"
                + fmt(r_desde, "yyyy-MM-dd")
                + "' AND v.fecha_venta <= '"
                + fmt(r_hasta, "yyyy-MM-dd")
                + "' THEN v.sumqty ELSE 0 END),2) AS S"
                + str(i + 1)
            )
            str_query_semanas_consumo += (
                " ,ROUND(SUM(CASE WHEN v.fecha_consumo >= '"
                + fmt(r_desde, "yyyy-MM-dd")
                + "' AND v.fecha_consumo <= '"
                + fmt(r_hasta, "yyyy-MM-dd")
                + "' THEN v.cantidad ELSE 0 END),2) AS S"
                + str(i + 1)
            )
            str_semanas_general += " ,SUM(X.S" + str(i + 1) + ")"

        if fecha_desde is None or fecha_hasta is None:
            raise RuntimeError("No se pudieron calcular rangos de semanas")

        f_desde = fmt(fecha_desde, "yyyy-MM-dd")
        f_hasta = fmt(fecha_hasta, "yyyy-MM-dd")

        str_query_ventas += " Select v.numart, v.empresa_id, v.numalm, v.fecha_venta AS fecha_venta "
        str_query_ventas += str_query_semanas_ventas
        str_query_ventas += (
            " ,ROUND(SUM(IFNULL(v.sumqty,0)),2) AS venta_total"
            " ,ROUND(MIN(IFNULL(v.sumqty,0)),2) AS venta_minima"
            " ,ROUND(MAX(IFNULL(v.sumqty,0)),2) AS venta_maxima"
            " ,(SELECT IFNULL(SUM(vc.sumqty * c.cantidad),0)"
            "   FROM mov_ventas_erp vc, rel_combo_articulo c"
            "  WHERE vc.numalm = v.numalm"
            "    AND vc.numart = c.cod_combo"
            "    AND c.numart = v.numart"
            "    AND vc.empresa_id = v.empresa_id"
            f"    AND vc.fecha_venta >= '{f_desde}' AND vc.fecha_venta <= '{f_hasta}') AS venta_total_combo"
            " ,0 AS inventario"
            " ,NULL AS ultima_compra_jde"
            " ,NULL AS ultima_venta_jde"
            " FROM mov_ventas_erp v"
            f" WHERE v.empresa_id = {id_empresa}"
            "   AND v.tipo_movimiento IN('1','2')"
            "   AND v.numalm = '{ALMACEN}'"
            f"   AND v.fecha_venta >= '{f_desde}' AND v.fecha_venta <= '{f_hasta}'"
            " GROUP BY v.numart, v.empresa_id, v.numalm"
        )

        str_query_consumos += " Select v.numart, v.empresa_id, v.numalm, v.fecha_consumo AS fecha_venta "
        str_query_consumos += str_query_semanas_consumo
        str_query_consumos += (
            " ,ROUND(SUM(IFNULL(v.cantidad,0)),2) AS venta_total"
            " ,ROUND(MIN(IFNULL(v.cantidad,0)),2) AS venta_minima"
            " ,ROUND(MAX(IFNULL(v.cantidad,0)),2) AS venta_maxima"
            " ,(SELECT IFNULL(SUM(vc.cantidad * c.cantidad),0)"
            "   FROM mov_consumo_erp vc, rel_combo_articulo c"
            "  WHERE vc.numalm = v.numalm"
            "    AND vc.numart = c.cod_combo"
            "    AND c.numart = v.numart"
            "    AND vc.empresa_id = v.empresa_id"
            f"    AND vc.fecha_consumo >= '{f_desde}' AND vc.fecha_consumo <= '{f_hasta}') AS venta_total_combo"
            " ,0 AS inventario"
            " ,NULL AS ultima_compra_jde"
            " ,NULL AS ultima_venta_jde"
            " FROM mov_consumo_erp v"
            f" WHERE v.empresa_id = {id_empresa}"
            "   AND v.tipo_movimiento IN('1','2')"
            "   AND v.numalm = '{ALMACEN}'"
            f"   AND v.fecha_consumo >= '{f_desde}' AND v.fecha_consumo <= '{f_hasta}'"
            " GROUP BY v.numart, v.empresa_id, v.numalm"
        )

        str_query_inventarios = (
            " SELECT i.numart,i.empresa_id,i.numalm,NULL,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,IFNULL(SUM(i.cantidad),0),NULL,NULL"
            " FROM mov_inventario i"
            " INNER JOIN cat_articulo a ON a.numart=i.numart AND a.categoria10 NOT IN ('')"
            " WHERE i.tipo_movimiento=0 AND i.cantidad>0"
            f" AND i.empresa_id={id_empresa} AND i.numalm='{{ALMACEN}}'"
            " GROUP BY i.numart"
        )

        str_query_fechas = (
            " SELECT ap.numart,ap.empresa_id,ap.numalm,NULL,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,MAX(ap.fecha_ultima_compra),MAX(ap.fecha_ultima_venta)"
            " FROM rel_articulo_precios ap"
            " WHERE ap.numalm = '{ALMACEN}'"
            " GROUP BY ap.numart"
        )

        query_general = (
            " SELECT @rownum:=@rownum+1"
            " ,x.numalm"
            " ,x.numart"
            f" ,'{f_desde}'"
            f" ,'{f_hasta}'"
            + str_semanas_general
            + " ,ROUND(SUM(x.venta_total),2)"
            " ,ROUND(MIN(x.venta_minima),2)"
            " ,ROUND(MAX(x.venta_maxima),2)"
            " ,ROUND(SUM(x.venta_total_combo),2)"
            " ,ROUND((((ROUND(SUM(x.venta_total),2) + ROUND(SUM(x.venta_total_combo),2)) / 24) * 52) / 12,5)"
            " ,ROUND(((ROUND(SUM(x.venta_total),2) / 24) * 52) / 12,5)"
            " ,ROUND(((ROUND(SUM(x.venta_total_combo),2) / 24) * 52) / 12,5)"
            " ,MAX(ultima_compra_jde)"
            " ,MAX(ultima_venta_jde)"
            " ,MAX(fecha_venta)"
            " ,X.empresa_id"
            " ,CURRENT_TIMESTAMP"
            " ,ROUND(SUM(x.inventario),2)"
            " ,NULL"
            " FROM (SELECT @rownum:={MAXID}) r, ("
            + str_query_ventas
            + " UNION ALL "
            + str_query_consumos
            + " UNION ALL "
            + str_query_inventarios
            + " UNION ALL "
            + str_query_fechas
            + " ) X"
            " GROUP BY X.numart, X.empresa_id, X.numalm"
            " ORDER BY 1 ASC"
        )

        return query_general

    def obtener_ventas_individual(self, session: DBSession, id_empresa: int, semanas_evaluar: int) -> None:
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** OBTENIENDO VENTAS INDIVIDUALES ***")
        self.limpiar_tablas_ventas(session, id_empresa)

        table = "abc_ventas_individual"
        query_ventas = self.obtener_sql_ventas_individual_almacenes(session, id_empresa, semanas_evaluar)
        ls_almacenes = self.obtener_almacenes(session, id_empresa)

        resultado = 0
        max_id = 0
        for almacen in ls_almacenes:
            max_id = int(session.query_one(f"SELECT CONCAT(IFNULL(MAX(id_venta),0),'') FROM {table}") or 0)
            query_final = query_ventas.replace("{ALMACEN}", almacen).replace("{MAXID}", str(max_id))
            resultado = session.execute_update(f"INSERT INTO `{table}` {query_final}")

        print(f"Total Insertados VENTA-SUCURSAL-DETALLADO: {max_id + resultado}")

        query_final = (
            f" UPDATE `{table}` v "
            " INNER JOIN config_reemplazo_abc r ON r.empresa_id = v.empresa_id AND r.numart = v.numart "
            " SET v.vigente = r.numartvigente "
        )
        resultado = session.execute_update(query_final)
        print(f"Sustitutos Actualizados: {resultado}")
        session.execute_update("commit")

        max_id = int(session.query_one(f"SELECT CONCAT(IFNULL(MAX(id_venta),0),'') FROM {table}") or 0)
        query_final = self.obtener_sql_ventas_resumidas(session, id_empresa, "GENERAL_DETALLADO", False)
        query_final = f"INSERT INTO `{table}` " + query_final.replace("{MAXID}", str(max_id))
        resultado = session.execute_update(query_final)
        print(f"Total Insertados VENTA-GENERAL-DETALLADO: {resultado}")
        session.execute_update("commit")

    def obtener_sql_ventas_resumidas(self, session: DBSession, id_empresa: int, tipo_general: str, solo_sustitutos: bool) -> str:
        _ = id_empresa
        table = "abc_ventas_individual"
        query_cabecera = (
            " ,v.fecha_inicio"
            " ,v.fecha_final"
            " ,SUM(v.semana_1),SUM(v.semana_2),SUM(v.semana_3),SUM(v.semana_4),SUM(v.semana_5),SUM(v.semana_6)"
            " ,SUM(v.semana_7),SUM(v.semana_8),SUM(v.semana_9),SUM(v.semana_10),SUM(v.semana_11),SUM(v.semana_12)"
            " ,SUM(v.semana_13),SUM(v.semana_14),SUM(v.semana_15),SUM(v.semana_16),SUM(v.semana_17),SUM(v.semana_18)"
            " ,SUM(v.semana_19),SUM(v.semana_20),SUM(v.semana_21),SUM(v.semana_22),SUM(v.semana_23),SUM(v.semana_24)"
            " ,SUM(v.venta_total),MIN(v.venta_minima),MAX(v.venta_maxima),SUM(v.venta_total_combo)"
            " ,ROUND((((ROUND(SUM(venta_total),2) + ROUND(SUM(venta_total_combo),2)) / 24) * 52) / 12,5) AS mad"
            " ,ROUND(((ROUND(SUM(venta_total),2) / 24) * 52) / 12,5) AS mad_sku"
            " ,ROUND(((ROUND(SUM(venta_total_combo),2) / 24) * 52) / 12,5) AS mad_combo"
            " ,MAX(v.fecha_ultima_compra_jde) AS fecha_ultima_compra_jde"
            " ,MAX(v.fecha_ultima_venta_jde) AS fecha_ultima_venta_jde"
            " ,MAX(v.fecha_ultima_venta_xadis) AS fecha_ultima_venta_xadis"
            " ,v.empresa_id"
            " ,CURRENT_TIMESTAMP AS fecha_creacion"
            " ,SUM(v.inventario)"
        )

        if tipo_general.upper() == "GENERAL_DETALLADO":
            return (
                " SELECT @rownum:=@rownum+1"
                " ,'GENERAL'"
                " ,v.numart"
                + query_cabecera
                + " ,NULL"
                f" FROM (SELECT @rownum:={{MAXID}}) r, {table} v"
                " WHERE v.numalm != 'GENERAL'"
                " GROUP BY v.empresa_id, v.numart, v.fecha_inicio, v.fecha_final"
            )

        if tipo_general.upper() == "GENERAL_ALMACEN_RESUMIDO":
            return (
                " SELECT @rownum:=@rownum+1"
                " ,v.numalm"
                + (" ,v.vigente " if solo_sustitutos else " ,v.numart ")
                + query_cabecera
                + (" ,'S' " if solo_sustitutos else " ,NULL")
                + f" FROM (SELECT @rownum:={{MAXID}}) r, {table} v"
                + " WHERE v.numalm != 'GENERAL'"
                + (" AND v.vigente is not null " if solo_sustitutos else " AND v.vigente is null ")
                + (
                    " GROUP BY v.empresa_id, v.vigente, v.numalm, v.fecha_inicio, v.fecha_final"
                    if solo_sustitutos
                    else " GROUP BY v.empresa_id, v.numart, v.numalm, v.fecha_inicio, v.fecha_final"
                )
            )

        if tipo_general.upper() == "GENERAL_RESUMIDO":
            return (
                " SELECT @rownum:=@rownum+1"
                " ,'GENERAL'"
                + (" ,v.vigente " if solo_sustitutos else " ,v.numart ")
                + query_cabecera
                + (" ,'S' " if solo_sustitutos else " ,NULL")
                + f" FROM (SELECT @rownum:={{MAXID}}) r, {table} v"
                + " WHERE v.numalm != 'GENERAL'"
                + (" AND v.vigente is not null " if solo_sustitutos else " AND v.vigente is null ")
                + (
                    " GROUP BY v.empresa_id, v.vigente, v.fecha_inicio, v.fecha_final"
                    if solo_sustitutos
                    else " GROUP BY v.empresa_id, v.numart, v.fecha_inicio, v.fecha_final"
                )
            )

        raise ValueError(f"TipoGeneral no soportado: {tipo_general}")

    def obtener_ventas_resumida(self, session: DBSession, id_empresa: int, semanas_evaluar: int) -> None:
        _ = semanas_evaluar
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** OBTENIENDO VENTAS GENERALES ***")

        table = "abc_ventas"
        max_id = 0

        query_final = self.obtener_sql_ventas_resumidas(session, id_empresa, "GENERAL_ALMACEN_RESUMIDO", False)
        query_final = f"INSERT INTO `{table}` " + query_final.replace("{MAXID}", str(max_id))
        resultado = session.execute_update(query_final)
        print(f"Total Insertados VENTA-GENERAL_ALMACEN_RESUMIDO (no reemplazos): {resultado}")

        max_id = int(session.query_one(f"SELECT CONCAT(IFNULL(MAX(id_venta),0),'') FROM {table}") or 0)
        query_final = self.obtener_sql_ventas_resumidas(session, id_empresa, "GENERAL_ALMACEN_RESUMIDO", True)
        query_final = f"INSERT INTO `{table}` " + query_final.replace("{MAXID}", str(max_id))
        resultado = session.execute_update(query_final)
        print(f"Total Insertados VENTA-GENERAL_ALMACEN_RESUMIDO (si reemplazos): {resultado}")

        max_id = int(session.query_one(f"SELECT CONCAT(IFNULL(MAX(id_venta),0),'') FROM {table}") or 0)
        query_final = self.obtener_sql_ventas_resumidas(session, id_empresa, "GENERAL_RESUMIDO", False)
        query_final = f"INSERT INTO `{table}` " + query_final.replace("{MAXID}", str(max_id))
        resultado = session.execute_update(query_final)
        print(f"Total Insertados VENTA-GENERAL_RESUMIDO (no reemplazos): {resultado}")

        max_id = int(session.query_one(f"SELECT CONCAT(IFNULL(MAX(id_venta),0),'') FROM {table}") or 0)
        query_final = self.obtener_sql_ventas_resumidas(session, id_empresa, "GENERAL_RESUMIDO", True)
        query_final = f"INSERT INTO `{table}` " + query_final.replace("{MAXID}", str(max_id))
        resultado = session.execute_update(query_final)
        print(f"Total Insertados VENTA-GENERAL_RESUMIDO (si reemplazos): {resultado}")

    def obtener_sql_abc(
        self,
        session: DBSession,
        id_empresa: int,
        almacen: str,
        categoria: str,
        periodo: str,
        codigo_abc: str,
        status_sku: str,
        usuario_abc: str,
        is_individual: bool,
    ) -> str:
        _ = status_sku
        table_ventas = "abc_ventas_individual" if is_individual else "abc_ventas"
        table_categoria = "abc_ventas_individual_categoria" if is_individual else "abc_ventas_categoria"
        max_id = int(session.query_one("SELECT CONCAT(IFNULL(MAX(id_abc),0),'') FROM abc_generado") or 0)

        filtro_general = "" if is_individual else " AND a.estatus=1 AND a.tipo_almacenamiento IN('P', 'U', 'O')"

        return f"""
SELECT @rownum:=@rownum+1 AS id_abc,
       x.empresa_id,
       x.numart,
       x.numalm,
       x.cod_categoria10 AS cod_categoria10,
       '{codigo_abc}' AS tipo_abc,
       '{periodo}' AS aniomes,
       x.inventario,
       x.fecha_ultima_compra_jde,
       x.fecha_ultima_venta_jde,
       x.semana_1,x.semana_2,x.semana_3,x.semana_4,x.semana_5,x.semana_6,
       x.semana_7,x.semana_8,x.semana_9,x.semana_10,x.semana_11,x.semana_12,
       x.semana_13,x.semana_14,x.semana_15,x.semana_16,x.semana_17,x.semana_18,
       x.semana_19,x.semana_20,x.semana_21,x.semana_22,x.semana_23,x.semana_24,
       x.venta_total,
       x.mad_sku,
       x.mad_combo,
       x.mad,
       x.dias AS dias,
       @abc_clasificacion:=CASE
           WHEN ISNULL(x.fecha_ultima_venta_jde) && x.dias < 365.0000 THEN 'F'
           WHEN ISNULL(x.dias) && x.inventario > 0 THEN 'K'
           WHEN ISNULL(x.dias) THEN 'N'
           WHEN x.dias <= 168.0000 THEN 'AaE'
           WHEN x.dias <= 365.0000 THEN 'F1'
           WHEN x.dias <= 730.0000 THEN 'G'
           WHEN x.dias <= 1095.0000 THEN 'H'
           WHEN x.dias <= 1460.0000 THEN 'J'
           ELSE 'K'
       END AS abc_clasificacion,
       IFNULL(x.porc_und,0) AS porc_und,
       @porc_acum:=ROUND(@porc_acum + IFNULL(x.porc_und,0),6) AS porc_acumulado,
       @abc_und:=CASE
           WHEN @porc_acum <= 70.000000 THEN 'A'
           WHEN @porc_acum <= 85.000000 THEN 'B'
           WHEN @porc_acum <= 95.000000 THEN 'C'
           WHEN @porc_acum <= 98.000000 THEN 'D'
           ELSE 'E'
       END AS abc_und,
       x.frecuencia,
       @porc_frecuencia:=ROUND((x.frecuencia/24)*100,6) AS porc_frecuencia,
       @abc_frec:=CASE WHEN @porc_frecuencia <= 20.000000 THEN 'C' WHEN @porc_frecuencia <= 50.000000 THEN 'B' ELSE 'A' END AS abc_frec,
       @abc_comb:=CONCAT(@abc_und,@abc_frec) AS abc_comb,
       @abc_int:=CASE
           WHEN @abc_comb='AA' THEN 'A' WHEN @abc_comb='AB' THEN 'A' WHEN @abc_comb='AC' THEN 'B'
           WHEN @abc_comb='BA' THEN 'A' WHEN @abc_comb='BB' THEN 'B' WHEN @abc_comb='BC' THEN 'C'
           WHEN @abc_comb='CA' THEN 'B' WHEN @abc_comb='CB' THEN 'C' WHEN @abc_comb='CC' THEN 'D'
           WHEN @abc_comb='DA' THEN 'C' WHEN @abc_comb='DB' THEN 'D' WHEN @abc_comb='DC' THEN 'E'
           WHEN @abc_comb='EA' THEN 'D' WHEN @abc_comb='EB' THEN 'E' WHEN @abc_comb='EC' THEN 'E'
           ELSE 'E'
       END AS abc_integrado,
       CASE WHEN @abc_clasificacion='AaE' THEN @abc_int ELSE @abc_clasificacion END AS abc_calculado,
       CURRENT_TIMESTAMP AS fecha_creacion,
       '{usuario_abc}' AS user
FROM (SELECT @porc_acum:=0, @rownum:={max_id}) r,
(
    SELECT a.empresa_id,
           a.numart,
           IFNULL(v.numalm,'{almacen}') AS numalm,
           a.cod_categoria10,
           v.fecha_ultima_compra_jde,
           v.fecha_ultima_venta_jde,
           CASE WHEN ISNULL(v.fecha_ultima_compra_jde) && ISNULL(v.fecha_ultima_venta_jde)
                THEN NULL
                ELSE DATEDIFF(DATE_SUB(CURDATE(), INTERVAL 1 DAY), IFNULL(v.fecha_ultima_venta_jde, v.fecha_ultima_compra_jde))
           END AS dias,
           IFNULL(v.inventario,0) AS inventario,
           IFNULL(v.venta_total,0) AS venta_total,
           ROUND(IFNULL(v.mad_sku,0),6) AS mad_sku,
           ROUND(IFNULL(v.mad_combo,0),6) AS mad_combo,
           ROUND(IFNULL(v.mad,0),6) AS mad,
           @ventas_categoria:=IFNULL((SELECT c.total_categoria FROM {table_categoria} c WHERE c.numalm='{almacen}' AND c.cod_categoria10=a.cod_categoria10),0) AS total_categoria,
           @porc:=CASE WHEN IFNULL(@ventas_categoria,0) >= 0 THEN ROUND((IFNULL(v.venta_total,0) / @ventas_categoria) * 100,6) ELSE 0 END AS porc_und,
           IFNULL(v.semana_1,0) AS semana_1, IFNULL(v.semana_2,0) AS semana_2, IFNULL(v.semana_3,0) AS semana_3, IFNULL(v.semana_4,0) AS semana_4,
           IFNULL(v.semana_5,0) AS semana_5, IFNULL(v.semana_6,0) AS semana_6, IFNULL(v.semana_7,0) AS semana_7, IFNULL(v.semana_8,0) AS semana_8,
           IFNULL(v.semana_9,0) AS semana_9, IFNULL(v.semana_10,0) AS semana_10, IFNULL(v.semana_11,0) AS semana_11, IFNULL(v.semana_12,0) AS semana_12,
           IFNULL(v.semana_13,0) AS semana_13, IFNULL(v.semana_14,0) AS semana_14, IFNULL(v.semana_15,0) AS semana_15, IFNULL(v.semana_16,0) AS semana_16,
           IFNULL(v.semana_17,0) AS semana_17, IFNULL(v.semana_18,0) AS semana_18, IFNULL(v.semana_19,0) AS semana_19, IFNULL(v.semana_20,0) AS semana_20,
           IFNULL(v.semana_21,0) AS semana_21, IFNULL(v.semana_22,0) AS semana_22, IFNULL(v.semana_23,0) AS semana_23, IFNULL(v.semana_24,0) AS semana_24,
           @frecuencia:=ROUND(
               (CASE WHEN IFNULL(v.semana_1,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_2,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_3,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_4,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_5,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_6,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_7,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_8,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_9,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_10,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_11,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_12,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_13,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_14,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_15,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_16,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_17,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_18,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_19,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_20,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_21,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_22,0)>0 THEN 1 ELSE 0 END +
                CASE WHEN IFNULL(v.semana_23,0)>0 THEN 1 ELSE 0 END + CASE WHEN IFNULL(v.semana_24,0)>0 THEN 1 ELSE 0 END),2
           ) AS frecuencia
    FROM cat_articulo a
    LEFT JOIN {table_ventas} v FORCE INDEX (idx_01)
      ON v.empresa_id = a.empresa_id
     AND v.numart = a.numart
     AND v.numalm = '{almacen}'
    WHERE a.empresa_id = {id_empresa}
      AND a.cod_categoria10 = '{categoria}'
      {filtro_general}
) X
ORDER BY x.venta_total DESC, x.fecha_ultima_venta_jde DESC, (x.numart*1) ASC
"""

    def procesar_abc(self, session: DBSession, id_empresa: int, tipo_abc: str, fecha_desde: str, fecha_hasta: str, is_abc_general: bool) -> None:
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} ==> *** PROCESANDO ABC: {tipo_abc} ***")

        codigo_abc = "0"
        status_sku = "1"
        usuario_abc = "PMS"
        table = "abc_generado"
        log_id = 0
        periodo = fmt(datetime.now(), "yyyyMM")
        is_individual = tipo_abc.upper() in ("GENERAL_MARCA_INDIV", "ALMACEN_MARCA_INDIV")

        try:
            config = session.query_one(
                " SELECT m.orden, m.valor"
                " FROM config_modulo m"
                f" WHERE m.empresa_id = {id_empresa}"
                " AND m.modulo = 'abc'"
                f" AND m.variable = '{tipo_abc}'"
            )
            if not config:
                raise RuntimeError(f"No se pudo obtener la configuracion para el ABC: {tipo_abc}")

            codigo_abc = str(config[0])
            status_sku = str(config[1])

            log_id = self.inicia_log_proceso(session, id_empresa, tipo_abc, codigo_abc, fecha_desde, fecha_hasta)
            # print("ANTES DELETE")
            # # eliminados = self.eliminar_tipo_abc_por_lotes(session, table, codigo_abc)
            # # print(f"tipo_abc:{codigo_abc} registros_eliminados:{eliminados}")
            # print("DESPUES DELETE")
            ls_almacenes = ["GENERAL"] if is_abc_general else self.obtener_almacenes(session, id_empresa)

            for almacen in ls_almacenes:
                ls_categorias = self.obtener_almacenes_categorias(session, id_empresa, almacen)
                for categoria in ls_categorias:
                    query_final = self.obtener_sql_abc(
                        session,
                        id_empresa,
                        almacen,
                        categoria,
                        periodo,
                        codigo_abc,
                        status_sku,
                        usuario_abc,
                        is_individual,
                    )
                    resultado = session.execute_update(f"INSERT IGNORE INTO {table} {query_final}")
                    print(f"almacen:{almacen} categoria:{categoria} resultado:{resultado}")

            self.actualiza_log_proceso(session, log_id, "1", "Exito")
        except Exception as exc:
            self.actualiza_log_proceso(session, log_id, "0", str(exc))
            raise

    def obtener_log_proceso(self, session: DBSession, empresa_id: int, proceso: str, fecha_ejecucion: str, estatus: str) -> int:
        value = session.query_one(
            " SELECT CONCAT(IFNULL(MAX(idlog),0),'')"
            " FROM abc_control"
            f" WHERE proceso = '{proceso}'"
            f" AND fecha_ejecucion = '{fecha_ejecucion}'"
            f" AND empresa_id = {empresa_id}"
            f" AND estatus = {estatus}"
        )
        return int(value or 0)

    def inicia_log_proceso(
        self,
        session: DBSession,
        empresa_id: int,
        proceso: str,
        codigo_abc: str,
        fecha_abc_desde: str,
        fecha_abc_hasta: str,
    ) -> int:
        fecha_ejecucion = fmt(datetime.now(), "yyyy-MM-dd")
        session.execute_update(
            f"DELETE FROM abc_control WHERE proceso = '{proceso}' AND fecha_ejecucion < (CURRENT_DATE - INTERVAL 30 DAY)"
        )
        log_id = int(session.query_one("SELECT CONCAT(IFNULL(MAX(idlog),0)+1,'') FROM abc_control") or 1)

        session.execute_update(
            " INSERT INTO abc_control ("
            " idlog, fecha_ejecucion, empresa_id, proceso, fecha_inicio, fecha_fin, estatus, log, fecha_abc_desde, fecha_abc_hasta, tipo_abc"
            " ) VALUES ("
            f" {log_id},'{fecha_ejecucion}',{empresa_id},'{proceso}',CURRENT_TIMESTAMP,NULL,0,'','{fecha_abc_desde}','{fecha_abc_hasta}','{codigo_abc}'"
            " )"
        )
        return log_id

    def actualiza_log_proceso(self, session: DBSession, log_id: int, estatus: str, respuesta: str) -> None:
        safe = (respuesta or "").replace("'", " ")
        session.execute_update(
            " UPDATE abc_control"
            f" SET estatus='{estatus}', fecha_fin=CURRENT_TIMESTAMP, log='{safe}'"
            f" WHERE idlog = {log_id}"
        )
