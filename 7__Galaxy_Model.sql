/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 7. Galaxy Schema — Modelo Dimensional
   DESCRIPCIÓN   : Construye el Galaxy Schema (Capa 3 — capa física) a partir de las
                   vistas MART. Crea las tablas de dimensiones (DIM) y tablas de hechos
                   (FCT) listas para ser consumidas por Power BI.
                   El modelo tiene múltiples tablas de hechos compartiendo dimensiones
                   comunes, lo que lo clasifica como Galaxy Schema (en oposición al
                   esquema estrella simple).
   TABLA FUENTE  : MART_T1_creditos, MART_T2_estado_resultados, MART_T3_balance_general, MART_T4_pagos_cuotas, MART_T5_clientes
   TABLAS CRUZADAS: Todas las vistas MART
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : Tablas RAW cargadas y scripts anteriores ejecutados.
============================================================================================================================ */

USE RiesgoCrediticioProyecto2;
GO

/* ============================================================================================================================
   DIM 1 — CLIENTE
   Fuente: MART_T1_creditos
   Cardinalidad: 1 fila por id_cliente (estado actual — SCD Tipo 1).
   Se toma la combinación más reciente por fecha_corte para garantizar unicidad.
============================================================================================================================ */


DROP TABLE IF EXISTS DIM_cliente;

SELECT
	ROW_NUMBER() OVER (ORDER BY id_cliente) AS id_cliente_PK,
		id_cliente,
        tipo_persona,
        sector_economico,
        segmento,
        zona_geografica,
		oficial_credito
INTO DIM_cliente
FROM (
    SELECT DIStinct
        id_cliente,
        tipo_persona,
        sector_economico,
        segmento,
        zona_geografica,
		oficial_credito
	from MART_T1_creditos
) AS base;

/* ============================================================================================================================
   2. DIM_Status_T5 
   Fuente: MART_T5_clientes
============================================================================================================================ */
DROP TABLE IF EXISTS DIM_Status_T5;

SELECT
	ROW_NUMBER() OVER (ORDER BY es_cliente_preferente) AS id_status_t5,
        es_cliente_preferente,
        tiene_credito_en_mora
INTO DIM_Status_T5
FROM (
    SELECT DIStinct
        es_cliente_preferente,
        tiene_credito_en_mora
	from MART_T5_clientes
) AS base;


/* ============================================================================================================================
   DIM 3 — DIM_Calificacion
   Fuente: MART_T1_creditos
============================================================================================================================ */

DROP TABLE IF EXISTS DIM_Calificacion;

SELECT 
    ROW_NUMBER() OVER(ORDER BY calificacion) AS id_calificacion_sk,
    calificacion
INTO DIM_Calificacion
FROM (
    SELECT DISTINCT calificacion_sbs as calificacion FROM MART_T1_creditos 
    UNION
    SELECT DISTINCT calificacion_consolidada FROM MART_T5_clientes 
    UNION
    SELECT DISTINCT calificacion_al_pago FROM MART_T4_pagos_cuotas 
) AS u;


/* ============================================================================================================================
   DIM 4 — DIM_Producto
   Fuente: MART_T1_creditos
============================================================================================================================ */

DROP TABLE IF EXISTS DIM_Producto;

SELECT
    ROW_NUMBER() OVER (ORDER BY producto_crediticio)     AS id_producto_PK,
	producto_crediticio, tipo_garantia
INTO DIM_Producto
FROM (
    SELECT DISTINCT producto_crediticio, tipo_garantia
    FROM MART_T1_creditos
) AS combinaciones;
GO
/* ============================================================================================================================
   DIM 4 — DIM_Estado
   Fuente: MART_T4_pagos_cuotas
   -- Relacion 
============================================================================================================================ */
DROP TABLE IF EXISTS DIM_Estado;
SELECT
    ROW_NUMBER() OVER (ORDER BY estado_pago)     AS id_estado_PK,
	estado_pago, canal_pago
INTO DIM_Estado
FROM (
    SELECT DISTINCT estado_pago, canal_pago
    FROM MART_T4_pagos_cuotas
) AS combinaciones;

/* ============================================================================================================================
   FCT_creditos — T1
   Fuente: MART_T1_creditos
   Granularidad: 1 fila por crédito.
   Campos exactos según diagrama Galaxy acordado.
   fecha_desembolso  → relación ACTIVA   con DIM_calendario en Power BI
   fecha_vencimiento → relación INACTIVA (USERELATIONSHIP)
   fecha_corte       → relación INACTIVA (USERELATIONSHIP)
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_creditos_t1;

SELECT

	-- 1. IDENTIFICADORES
	t.id_credito,
	t.id_cliente,

	t.oficial_credito,

	-- 1.1. Llaves 
	cl.id_cliente_PK,
	cal.id_calificacion_sk,
	pr.id_producto_PK,
	-- 2. PRODUCTO

	-- 3. FECHAS
	t.fecha_desembolso,
	t.fecha_vencimiento,
	t.fecha_corte,
	t.plazo_meses,

	-- 4. MONTOS
	t.monto_aprobado,
	t.monto_desembolsado,
	t.saldo_capital,
	t.saldo_interes_devengado,
	t.saldo_mora,
	t.saldo_total_exposicion,

	-- 5. TASAS
	t.tasa_nominal_anual_pct,
	t.tasa_efectiva_anual_pct,
	t.spread_pct,
	t.costo_fondos_pct,

	-- 6. MORA Y CALIFICACIÓN
	t.numero_cuotas_vencidas,

	-- 7. PROVISIONES
	t.tasa_provision_pct,
	t.provision_requerida,
	t.provision_constituida,

	-- 8. GARANTÍAS
	t.valor_garantia,
	t.cobertura_garantia_ratio,
	t.ltv_loan_to_value,
	t.ratio_cobertura_total,

	-- 9. MODELOS INTERNOS BASILEA II
	t.score_crediticio,
	t.pd_probabilidad_default,
	t.lgd_loss_given_default,
	t.el_expected_loss,

	-- 10. RATIOS FINANCIEROS DEL CLIENTE
	t.dscr_cobertura_servicio_deuda,
	t.ratio_endeudamiento,
	t.ratio_liquidez_corriente,
	t.ingresos_anuales,
	t.cuota_mensual,

	-- 11. COMPORTAMIENTO CREDITICIO
	t.numero_refinanciaciones,
	t.es_refinanciado,
	t.veces_pago_anticipado,
	t.numero_creditos_banco,
	t.numero_creditos_sistema,
	t.antiguedad_cliente_meses,
	t.edad_empresa_anos,

	-- 12. CASTIGOS
	t.en_castigo,
	t.monto_castigado


INTO FCT_creditos_t1
FROM MART_T1_creditos AS t

LEFT JOIN DIM_cliente AS cl on
t.id_cliente = cl.id_cliente and t.oficial_credito = cl.oficial_credito and t.sector_economico = cl.sector_economico 
	and t.segmento = cl.segmento and t.tipo_persona = cl.tipo_persona and t.zona_geografica = cl.zona_geografica

LEFT JOIN DIM_calificacion as cal on
t.calificacion_sbs = cal.calificacion

Left Join DIM_Producto as pr on
t.producto_crediticio = pr.producto_crediticio and t.tipo_garantia = pr.tipo_garantia;


select COUNT(*) from MART_T1_creditos;
select * from FCT_creditos_t1;
/* ============================================================================================================================
   FCT_historial_pagos — T4
   Fuente: MART_T4_pagos_cuotas
   Granularidad: 1 fila por cuota (id_pago).
   Campos exactos según diagrama Galaxy acordado.
   fecha_pago_real         → relación ACTIVA   con DIM_calendario en Power BI
   fecha_vencimiento_cuota → relación INACTIVA (USERELATIONSHIP)
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_historial_pagos_t4;

SELECT
    -- Llaves
    t.id_pago,
    t.id_credito,              -- navegación lógica a FCT_creditos
    t.id_cliente,
    cal.id_calificacion_sk,
	est.id_estado_PK,
	cl.id_cliente_PK,


    -- Fechas (FKs hacia DIM_calendario en Power BI)
    t.fecha_pago_real,            -- relación activa
    t.fecha_vencimiento_cuota,    -- relación inactiva (USERELATIONSHIP)

    -- Métricas de la cuota
    t.monto_capital_cuota,
    t.monto_interes_cuota,
    t.monto_pagado,
    t.saldo_pendiente_cuota,

    -- Comportamiento de pago
    t.dias_retraso_pago

INTO FCT_historial_pagos_t4
FROM MART_T4_pagos_cuotas AS t
OUTER APPLY (
    SELECT TOP 1 cl_sub.id_cliente_PK
    FROM DIM_cliente AS cl_sub
    WHERE t.id_cliente = cl_sub.id_cliente 
      AND t.segmento = cl_sub.segmento 
      AND t.zona_geografica = cl_sub.zona_geografica
      AND t.oficial_credito = cl_sub.oficial_credito
    -- LÓGICA DE PRIORIDAD REAL (Solo con columnas de DIM_cliente):
    ORDER BY 
        -- 1. Priorizamos filas que tengan el Sector Económico informado
        CASE WHEN cl_sub.sector_economico IS NOT NULL THEN 0 ELSE 1 END ASC,
        -- 2. Priorizamos filas que tengan el Tipo de Persona informado
        CASE WHEN cl_sub.tipo_persona IS NOT NULL THEN 0 ELSE 1 END ASC,
        -- 3. Priorizar filas sin nulos zona geografica
		case when cl_sub.zona_geografica is not null then 0 else 1 end asc,
        -- 3. Si todo lo demás es igual, usamos el registro más reciente
        cl_sub.id_cliente_PK DESC 
) AS cl
LEFT JOIN DIM_calificacion as cal on
t.calificacion_al_pago = cal.calificacion
left join DIM_Estado as est on
t.estado_pago = est.estado_pago and t.canal_pago = est.canal_pago
;



select COUNT(*) from MART_T4_pagos_cuotas;
select COUNT(*) from FCT_historial_pagos_t4;

/* ============================================================================================================================
   FCT_clientes_consol — T5
   Fuente: MART_T5_clientes
   Granularidad: 1 fila por cliente (snapshot).
   Campos exactos según diagrama Galaxy acordado.
   fecha_primer_credito → relación ACTIVA con DIM_calendario en Power BI.
   Antigüedad: DATEDIFF(FCT_clientes_consol[fecha_primer_credito], MAX(DIM_calendario[Date]), MONTH)
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_clientes_consol_t5;

SELECT
    -- Llaves
    t.id_cliente,
    cl.id_cliente_PK,
	st5.id_status_t5,
	cf.id_calificacion_sk,

    -- Fecha (FK hacia DIM_calendario en Power BI — relación activa)
    t.fecha_primer_credito,
	t.fecha_ultimo_corte,

    -- Portafolio
    t.num_creditos_activos,
    t.num_creditos_sistema,
    t.num_refinanciaciones,

    -- Riesgo
    t.score_crediticio,
    t.max_dias_atraso,
    t.tiene_credito_en_mora,
    t.calificacion_consolidada,
    t.provision_total_cliente,
    t.el_total_cliente,

    -- Exposición
    t.saldo_total_deuda,
    t.saldo_mora_total,
    t.tasa_mora_cliente_pct,
    t.ingresos_anuales,
    t.es_cliente_preferente,
    t.edad_empresa_anos,
    t.tasa_endeudamiento_sistema_pct

INTO FCT_clientes_consol_t5
FROM MART_T5_clientes AS t

LEFT JOIN DIM_cliente AS cl ON  
t.id_cliente = cl.id_cliente and t.oficial_credito = cl.oficial_credito and t.sector_economico = cl.sector_economico
and t.segmento = cl.segmento and t.tipo_persona = cl.tipo_persona and t.zona_geografica = cl.zona_geografica
left join DIM_Status_T5 st5 on
st5.es_cliente_preferente = t.es_cliente_preferente and st5.tiene_credito_en_mora = t.tiene_credito_en_mora
left join DIM_Calificacion cf on
t.calificacion_consolidada = cf.calificacion;
GO

select COUNT(*) from MART_T5_clientes;
select COUNT(*) from FCT_clientes_consol_t5;
/* ============================================================================================================================
   FCT_estado_financiero — T2
   Fuente: MART_T2_estado_resultados
   Granularidad: 1 fila por mes (nivel institución).
   Campos exactos según diagrama Galaxy acordado.
   fecha_mes → relación ACTIVA con DIM_calendario en Power BI.
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_estado_financiero_t2;

SELECT
    -- Fecha (FK hacia DIM_calendario en Power BI — relación activa)
    t.fecha_mes,

    -- Ingresos financieros
    t.ing_intereses_cartera,
    t.ing_intereses_inversiones,
    t.ing_intereses_interbancarios,
    t.ing_comisiones_ganadas,
    t.ing_utilidades_financieras,
    t.ing_servicios,
    t.total_ingresos,

    -- Gastos financieros
    t.gasto_intereses_depositos,
    t.gasto_intereses_obligaciones,
    t.gasto_comisiones_pagadas,
    t.total_gastos_financieros,

    -- Gastos operativos
    t.gasto_provisiones_periodo,
    t.gasto_personal,
    t.gasto_honorarios,
    t.gasto_instalaciones,
    t.gasto_publicidad,
    t.total_gastos_operativos,

    -- Distribución de utilidades
    t.participacion_trabajadores_15pct,
    t.impuesto_renta_25pct,

    -- Resultados
    t.margen_financiero_bruto,
    t.margen_financiero_neto,
    t.utilidad_neta,

    -- Recuperaciones y castigos
    t.recuperaciones_periodo,
    t.castigos_periodo

INTO FCT_estado_financiero_t2
FROM MART_T2_estado_resultados AS t;
GO

/* ============================================================================================================================
   FCT_balance_general — T3
   Fuente: MART_T3_balance_general
   Granularidad: 1 fila por mes (nivel institución).
   Campos exactos según diagrama Galaxy acordado.
   fecha_mes → relación ACTIVA con DIM_calendario en Power BI.
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_balance_general_t3;

SELECT
    -- Fecha (FK hacia DIM_calendario en Power BI — relación activa)
    t.fecha_mes,

    -- Activos
    t.fondos_disponibles,
    t.operaciones_interbancarias_activo,
    t.inversiones,
    t.cartera_bruta,
	t.cartera_vigente,
    t.provisiones_acumuladas,
    t.cartera_neta,
    t.bienes_realizables_adjudicados,
    t.propiedades_y_equipos,
    t.otros_activos,
    t.total_activos,

    -- Pasivos — captaciones
    t.depositos_vista,
    t.depositos_plazo,
    t.depositos_ahorro,
    t.total_obligaciones_publico,
    t.operaciones_interbancarias_pasivo,
    t.obligaciones_financieras_bde_cfn,
    t.cuentas_por_pagar,
    t.total_pasivos,

    -- Patrimonio
    t.capital_social,
    t.reserva_legal,
    t.reserva_especial,
    t.superavit_valuaciones,
    t.utilidad_acumulada_ejercicio,
    t.total_patrimonio,

    -- Indicadores prudenciales
    t.activos_ponderados_riesgo,
    t.indice_solvencia_pct,
    t.indice_liquidez_pct,
    t.aporte_cosede,

    -- Métricas de mora (derivadas de T1 consolidadas)
    t.cartera_mora_t1,
    t.tasa_mora_pct_t1,
    t.provision_requerida_t1,
    t.provision_constituida_t1,
    t.cobertura_provision_pct_t1,
    t.el_total_t1

INTO FCT_balance_general_t3
FROM MART_T3_balance_general AS t;
GO

/* ============================================================================================================================
   INSTRUCCIONES DAX — DIM_calendario y relaciones en Power BI
   Copiar y ejecutar en el editor DAX de Power BI Desktop.

   ── CREAR DIM_calendario ────────────────────────────────────────────────────
   DIM_calendario = CALENDAR(DATE(2022,1,1), DATE(2025,12,31))

   Columnas calculadas sobre DIM_calendario:
     Año           = YEAR([Date])
     Mes           = MONTH([Date])
     Nombre mes    = FORMAT([Date], "MMMM", "es-EC")
     Mes abreviado = FORMAT([Date], "MMM",  "es-EC")
     Trimestre     = "T" & QUARTER([Date])
     Año-Mes       = FORMAT([Date], "yyyy-MM")

   ── RELACIONES ACTIVAS ───────────────────────────────────────────────────────
   DIM_calendario.Date        → FCT_creditos.fecha_desembolso            1:N  Activa
   DIM_calendario.Date        → FCT_historial_pagos.fecha_pago_real      1:N  Activa
   DIM_calendario.Date        → FCT_clientes_consol.fecha_primer_credito 1:N  Activa
   DIM_calendario.Date        → FCT_estado_financiero.fecha_mes          1:N  Activa
   DIM_calendario.Date        → FCT_balance_general.fecha_mes            1:N  Activa

   DIM_cliente.id_cliente_sk  → FCT_creditos.id_cliente_sk               1:N
   DIM_cliente.id_cliente_sk  → FCT_historial_pagos.id_cliente_sk        1:N
   DIM_cliente.id_cliente_sk  → FCT_clientes_consol.id_cliente_sk        1:N

   DIM_oficial.id_oficial     → FCT_creditos.id_oficial                  1:N
   DIM_oficial.id_oficial     → FCT_historial_pagos.id_oficial           1:N
   DIM_oficial.id_oficial     → FCT_clientes_consol.id_oficial           1:N

   DIM_canal_pago.id_canal    → FCT_historial_pagos.id_canal             1:N

   FCT_creditos.id_credito    → FCT_historial_pagos.id_credito           1:N  (satélite)

   ── RELACIONES INACTIVAS (activar con USERELATIONSHIP) ──────────────────────
   DIM_calendario.Date        → FCT_creditos.fecha_vencimiento           1:N  Inactiva
   DIM_calendario.Date        → FCT_creditos.fecha_corte                 1:N  Inactiva
   DIM_calendario.Date        → FCT_historial_pagos.fecha_vencimiento_cuota 1:N Inactiva

