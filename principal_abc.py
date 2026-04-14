from __future__ import annotations

from datetime import datetime

from .dao_general import DaoGeneral
from .db import DBSession
from .procesos_adicionales import ProcesosAdicionales
from .utils import fmt


class PrincipalABC:
    def ejecutar_abc(self, session: DBSession, proceso: str) -> None:
        dao = DaoGeneral()
        proc = ProcesosAdicionales()
        semanas_evaluar = 24
        proceso_ventas = "PROCESO_VENTAS"
        fecha_ejecucion = fmt(datetime.now(), "yyyy-MM-dd")
        fecha_desde = ""
        fecha_hasta = ""
        # print("test")
        # return 
        is_ejecucion_total = proceso.upper() == "T"
        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} - *** INICIO PROCESO ABC ***")

        for codigo_empresa in dao.obtener_empresas_activas(session):
            print(f"******* Procesa empresa: {codigo_empresa} ********")

            log_id = proc.obtener_log_proceso(session, codigo_empresa, proceso_ventas, fecha_ejecucion, "1")
            if is_ejecucion_total or log_id <= 0:
                try:
                    lista = dao.obtener_semanas_evaluar(semanas_evaluar)
                    fecha_desde = fmt(lista[len(lista) - 1][1], "yyyy-MM-dd")
                    fecha_hasta = fmt(lista[0][0], "yyyy-MM-dd")

                    log_id = proc.inicia_log_proceso(session, codigo_empresa, proceso_ventas, "0", fecha_desde, fecha_hasta)

                    proc.procesa_sustitutos(session, codigo_empresa)
                    proc.obtener_ventas_individual(session, codigo_empresa, semanas_evaluar)
                    proc.obtener_ventas_resumida(session, codigo_empresa, semanas_evaluar)
                    proc.inicializar_ventas_categorias(session, codigo_empresa)

                    proc.actualiza_log_proceso(session, log_id, "1", "Exito")
                except Exception as exc:
                    proc.actualiza_log_proceso(session, log_id, "0", str(exc))
                    raise

            if is_ejecucion_total:
                proc.inicializar_tablas_abc(session, codigo_empresa)

            if is_ejecucion_total or proceso.upper() == "GM":
                proc.procesar_abc(session, codigo_empresa, "GENERAL_MARCA", fecha_desde, fecha_hasta, True)

            if is_ejecucion_total or proceso.upper() == "GMI":
                proc.procesar_abc(session, codigo_empresa, "GENERAL_MARCA_INDIV", fecha_desde, fecha_hasta, True)

            if is_ejecucion_total or proceso.upper() == "AM":
                proc.procesar_abc(session, codigo_empresa, "ALMACEN_MARCA", fecha_desde, fecha_hasta, False)

            if is_ejecucion_total or proceso.upper() == "AMI":
                proc.procesar_abc(session, codigo_empresa, "ALMACEN_MARCA_INDIV", fecha_desde, fecha_hasta, False)

        print(f"{fmt(datetime.now(), 'dd-MM-yyyy HH:mm:ss')} - *** FIN PROCESO ABC ***")
