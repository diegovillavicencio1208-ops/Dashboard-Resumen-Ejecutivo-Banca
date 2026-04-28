/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 6. Limpieza — T1, T2, T3, T4, T5
   DESCRIPCIÓN   : Capa de limpieza y estandarización: implementa todas las correcciones
                   identificadas en el EDA como vistas MART (capa intermedia).
                   Cada vista aplica: conversión de tipos, corrección de negativos,
                   normalización de catálogos, imputación de nulos y recálculo de
                   variables derivadas erróneas detectadas en los scripts 1–5.
                   Las vistas MART son la única fuente de verdad para el modelo Galaxy.
   TABLA FUENTE  : T1_creditos_RAW, T2_estado_resultados_RAW, T3_balance_general_RAW, T4_pagos_cuotas_RAW, T5_clientes_RAW
   TABLAS CRUZADAS: Todas las tablas RAW + cruce T1↔T3 para cartera bruta y cartera vigente
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : Tablas RAW cargadas y scripts anteriores ejecutados.
============================================================================================================================ */

use RiesgoCrediticioProyecto2;
GO
--========================================================================================================================
-- Limpieza T1_creditos_RAW 
--========================================================================================================================
CREATE OR ALTER VIEW MART_T1_creditos AS
WITH Cartera_Limpia AS (
    SELECT 
        *,
        -- Centralización de conversiones (Casteo único para performance)
        CAST(dias_atraso AS INT) AS dias_atraso_int,
        CAST(saldo_capital AS DECIMAL(18,2)) AS saldo_num,
        CAST(tasa_nominal_anual_pct AS DECIMAL(18,4)) AS tasa_num,
        CAST(score_crediticio AS DECIMAL(18,2)) AS score_crediticio_num,
        CAST(monto_aprobado AS DECIMAL(18,2)) AS monto_num,
        CAST(plazo_meses AS INT) AS plazo_int
    FROM T1_creditos_RAW
    WHERE 
        -- id_log 11: Exclusión por inconsistencia de fechas
        CAST(fecha_vencimiento AS DATE) > CAST(fecha_desembolso AS DATE)
        
        -- id_log 13 y 14: Exclusiones por Política
        AND (flag_error NOT IN ('EXCLUIR_PROCESO_JUDICIAL', 'EXCLUIR_CREDITO_EMPLEADO') 
             OR flag_error IS NULL)
             
        -- id_log 7: excluir tasa fuera de rango: 0% < tasa <= 30%
        AND CAST(tasa_nominal_anual_pct AS DECIMAL(18,4)) > 0
        AND CAST(tasa_nominal_anual_pct AS DECIMAL(18,4)) <= 30.0
        
        -- id_log 8: Excluir score fuera de cartera institucional (200- 850)
        AND CAST(score_crediticio AS DECIMAL(18,2)) BETWEEN 200 AND 850 
        
        -- id_log 10: Excluir saldo_capital menor a 0 
        AND CAST(saldo_capital AS DECIMAL(18,2)) >= 0
)
SELECT 
    -- 1. IDENTIFICADORES 
    CAST(id_credito AS VARCHAR(50)) AS id_credito,
    CAST(id_cliente AS VARCHAR(50)) AS id_cliente,
    CAST(tipo_persona AS VARCHAR(20)) AS tipo_persona,
    CAST(segmento AS VARCHAR(50)) AS segmento,
    CAST(zona_geografica AS VARCHAR(100)) AS zona_geografica,
    CAST(oficial_credito AS VARCHAR(100)) AS oficial_credito,

	-- 2. PRODUCTO
    CAST(producto_crediticio AS VARCHAR(100)) AS producto_crediticio,
    CAST(moneda AS VARCHAR(10)) AS moneda,

    -- 3. FECHAS 
    CAST(fecha_desembolso AS DATE) AS fecha_desembolso,
    CAST(fecha_vencimiento AS DATE) AS fecha_vencimiento,
    CAST(fecha_corte AS DATE) AS fecha_corte,
	CAST(plazo_int AS INT) AS plazo_meses,

    -- 4. MONTOS
    CAST(monto_num AS DECIMAL(18,2)) AS monto_aprobado,
	CAST(monto_desembolsado as decimal (18,2)) as monto_desembolsado,
    CAST(saldo_num AS DECIMAL(18,2)) AS saldo_capital,
	CAST(saldo_interes_devengado AS DECIMAL(18,2)) AS saldo_interes_devengado,
	CAST(saldo_mora AS DECIMAL(18,2)) AS saldo_mora,
	CAST(saldo_total_exposicion AS DECIMAL(18,2)) AS saldo_total_exposicion,

	-- 5. TASAS
    CAST(tasa_num AS DECIMAL(18,4)) AS tasa_nominal_anual_pct,
	CAST(tasa_efectiva_anual_pct AS DECIMAL(18,4)) AS tasa_efectiva_anual_pct,
	CAST(spread_pct AS DECIMAL(18,4)) AS spread_pct,
	CAST(costo_fondos_pct AS DECIMAL(18,4)) AS costo_fondos_pct,

	-- 6. MORA Y CALIFICACIÓN

	CAST(numero_cuotas_vencidas AS DECIMAL(18,2)) AS numero_cuotas_vencidas,
	CAST(calificacion_sbs AS VARCHAR(100)) AS calificacion_sbs,

	-- 7. PROVISIONES 
	CAST(tasa_provision_pct AS DECIMAL(18,2)) AS tasa_provision_pct,
	CAST(provision_requerida AS DECIMAL(18,2)) AS provision_requerida,
	CAST(provision_constituida AS DECIMAL(18,2)) AS provision_constituida,

	-- 8. GARANTÍAS
	
    CAST(tipo_garantia AS VARCHAR(100)) AS tipo_garantia,
	CAST(valor_garantia AS DECIMAL(18,2)) AS valor_garantia,
    CAST(cobertura_garantia_ratio AS DECIMAL(18,2)) AS cobertura_garantia_ratio,
	CAST(ltv_loan_to_value AS DECIMAL(18,4)) AS ltv_loan_to_value,
	CAST(ratio_cobertura_total AS DECIMAL(18,2)) AS ratio_cobertura_total,

	-- 9. MODELOS INTERNOS BASILEA II

	CAST(score_crediticio_num AS DECIMAL(18,2)) AS score_crediticio,
	CAST(pd_probabilidad_default AS DECIMAL(18,4)) AS pd_probabilidad_default,
	CAST(lgd_loss_given_default AS DECIMAL(18,4)) AS lgd_loss_given_default,
	CAST(el_expected_loss AS DECIMAL(18,2)) AS el_expected_loss,

	-- 10. RATIOS FINANCIEROS DEL CLIENTE
	CAST(dscr_cobertura_servicio_deuda AS DECIMAL(18,2)) AS dscr_cobertura_servicio_deuda,
	CAST(ratio_endeudamiento AS DECIMAL(18,4)) AS ratio_endeudamiento,
	CAST(ratio_liquidez_corriente AS DECIMAL(18,4)) AS ratio_liquidez_corriente,
	CAST(ingresos_anuales AS DECIMAL(18,2)) AS ingresos_anuales,
	CAST(cuota_mensual AS DECIMAL(18,2)) AS cuota_mensual,

	--- 11. COMPORTAMIENTO CREDITICIO
	CAST(numero_refinanciaciones AS DECIMAL(18,2)) AS numero_refinanciaciones,
	CAST(es_refinanciado AS DECIMAL(18,2)) AS es_refinanciado,
	CAST(veces_pago_anticipado AS DECIMAL(18,2)) AS veces_pago_anticipado,
	CAST(numero_creditos_banco AS DECIMAL(18,2)) AS numero_creditos_banco,
	CAST(numero_creditos_sistema AS INT) AS numero_creditos_sistema,
	CAST(antiguedad_cliente_meses AS INT) AS antiguedad_cliente_meses,
	CAST(edad_empresa_anos AS DECIMAL(18,2)) as edad_empresa_anos,

	-- 12. CASTIGOS 
	CAST(en_castigo AS DECIMAL(18,2)) as en_castigo,
	CAST(monto_castigado AS DECIMAL(18,2)) as monto_castigado,



    --Eestandarizar valores negativos a 0
    CAST(CASE WHEN dias_atraso_int < 0 THEN 0 ELSE dias_atraso_int END AS INT) AS dias_atraso,

    -- Normalizar sector_economico (VARCHAR)
    CAST(CASE
        WHEN sector_economico LIKE '%agr%' THEN 'Agricultura'
        WHEN sector_economico LIKE '%comercio%' THEN 'Comercio'
        WHEN sector_economico LIKE '%const%' THEN 'Construcción'
        WHEN sector_economico LIKE '%exp%' THEN 'Exportación'
        WHEN sector_economico LIKE '%ind%' THEN 'Industria'
        WHEN sector_economico LIKE '%pes%' THEN 'Pesca'
        WHEN sector_economico LIKE '%serv%' THEN 'Servicios'
        WHEN sector_economico LIKE '%trans%' THEN 'Transporte'
        WHEN sector_economico LIKE '%turis%' THEN 'Turismo'
        ELSE 'No Definido' 
    END AS VARCHAR(50)) AS sector_economico
FROM Cartera_Limpia;
Go

--========================================================================================================================
-- Limpieza T2_estado_resultados_RAW 
--========================================================================================================================
CREATE OR ALTER VIEW MART_T2_estado_resultados AS
WITH MART_T2_CAST AS (
    SELECT
        CAST(fecha_mes AS DATE) AS fecha_mes,

        -- INGRESOS (CAST a DECIMAL 18,4)
        CAST(ing_intereses_cartera AS DECIMAL(18,4)) AS ing_intereses_cartera,
        CAST(ing_intereses_inversiones AS DECIMAL(18,4)) AS ing_intereses_inversiones,
        CAST(ing_intereses_interbancarios AS DECIMAL(18,4)) AS ing_intereses_interbancarios,
        CAST(ing_comisiones_ganadas AS DECIMAL(18,4)) AS ing_comisiones_ganadas,
        CAST(ing_utilidades_financieras AS DECIMAL(18,4)) AS ing_utilidades_financieras,
        CAST(ing_servicios AS DECIMAL(18,4)) AS ing_servicios,
        CAST(otros_ingresos AS DECIMAL(18,4)) AS otros_ingresos,
        CAST(total_ingresos AS DECIMAL(18,4)) AS total_ingresos,

        -- GASTOS FINANCIEROS Y OPERATIVOS (CAST a DECIMAL 18,4)
        CAST(gasto_intereses_depositos AS DECIMAL(18,4)) AS gasto_intereses_depositos,
        CAST(gasto_intereses_obligaciones AS DECIMAL(18,4)) AS gasto_intereses_obligaciones,
        CAST(gasto_comisiones_pagadas AS DECIMAL(18,4)) AS gasto_comisiones_pagadas,
        CAST(total_gastos_financieros AS DECIMAL(18,4)) AS total_gastos_financieros,
        
        -- Limpieza: Valores Negativos Erróneos (ABS)
        ABS(CAST(gasto_provisiones_periodo AS DECIMAL(18,4))) AS gasto_provisiones_periodo,
        ABS(CAST(resultado_antes_impuestos AS DECIMAL(18,4))) AS resultado_antes_impuestos,
        
        CAST(gasto_personal AS DECIMAL(18,4)) AS gasto_personal,
        CAST(gasto_honorarios AS DECIMAL(18,4)) AS gasto_honorarios,
        CAST(gasto_instalaciones AS DECIMAL(18,4)) AS gasto_instalaciones,
        CAST(gasto_publicidad AS DECIMAL(18,4)) AS gasto_publicidad,
        CAST(gasto_sistemas_tecnologia AS DECIMAL(18,4)) AS gasto_sistemas_tecnologia,
        CAST(total_gastos_operativos AS DECIMAL(18,4)) AS total_gastos_operativos,

        -- IMPUESTOS Y OTROS
        -- recalculo de participacion_trabajadores_15pct
		CASE 
            WHEN CAST(resultado_antes_impuestos AS DECIMAL(18,2)) > 0 
            THEN (CAST(resultado_antes_impuestos AS DECIMAL(18,2)) * 0.15)
            ELSE 0 
        END AS participacion_trabajadores_15pct,

        CAST(impuesto_renta_25pct AS DECIMAL(18,4)) AS impuesto_renta_25pct,
        CAST(recuperaciones_periodo AS DECIMAL(18,4)) AS recuperaciones_periodo,
        CAST(castigos_periodo AS DECIMAL(18,4)) AS castigos_periodo
    FROM T2_estado_resultados_RAW
)
Select
	*,
	-- Margen Financiero Bruto: Ingresos - Gastos Financieros
	(total_ingresos - total_gastos_financieros) as margen_financiero_bruto,

	-- Margen Financiero Neto: Bruto - Provisiones
	((total_ingresos - total_gastos_financieros) - gasto_provisiones_periodo) as margen_financiero_neto,

	-- Utilidad Neta: Resultado antes de impuestos - Impuestos/Part.
	(resultado_antes_impuestos - participacion_trabajadores_15pct - impuesto_renta_25pct) AS utilidad_neta

from MART_T2_CAST;
Go

/* ============================================================================================================
    VISTA FINAL: MART_T3 - BALANCE GENERAL SANEADO
============================================================================================================ */

CREATE OR ALTER VIEW MART_T3_balance_general AS

WITH MART_T3_CAST AS (
    SELECT 
        CAST(fecha_mes as date) as fecha_mes,
        -- Activos (Componentes detallados)
        CAST(fondos_disponibles AS DECIMAL(18,2)) AS fondos_disponibles,
        CAST(operaciones_interbancarias_activo AS DECIMAL(18,2)) AS operaciones_interbancarias_activo,
        CAST(inversiones AS DECIMAL(18,2)) AS inversiones,
        CAST(provisiones_acumuladas AS DECIMAL(18,2)) AS provisiones_acumuladas,
        CAST(cuentas_por_cobrar AS DECIMAL(18,2)) AS cuentas_por_cobrar,
        CAST(bienes_realizables_adjudicados AS DECIMAL(18,2)) AS bienes_realizables_adjudicados,
        CAST(propiedades_y_equipos AS DECIMAL(18,2)) AS propiedades_y_equipos,
        CAST(otros_activos AS DECIMAL(18,2)) AS otros_activos,
        
        -- Pasivos (Campos íntegros)
		CAST(depositos_vista AS DECIMAL(18,2)) AS depositos_vista,
		CAST(depositos_ahorro AS DECIMAL(18,2)) AS depositos_ahorro,
		CAST(depositos_plazo AS DECIMAL(18,2)) AS depositos_plazo,
		CAST(depositos_restringidos AS DECIMAL(18,2)) AS depositos_restringidos,
        CAST(total_obligaciones_publico AS DECIMAL(18,2)) AS total_obligaciones_publico,
		CAST(operaciones_interbancarias_pasivo AS DECIMAL(18,2)) AS operaciones_interbancarias_pasivo,
		CAST(obligaciones_financieras_bde_cfn AS DECIMAL(18,2)) AS obligaciones_financieras_bde_cfn,
		CAST(cuentas_por_pagar AS DECIMAL(18,2)) AS cuentas_por_pagar,
        CAST(otros_pasivos AS DECIMAL(18,2)) AS otros_pasivos,
        CAST(total_pasivos AS DECIMAL(18,2)) AS total_pasivos,
		CAST(cartera_neta AS DECIMAL(18,2)) AS cartera_neta,

        -- Patrimonio (Componentes para reconstrucción)
        CAST(capital_social AS DECIMAL(18,2)) AS capital_social,
        CAST(reserva_legal AS DECIMAL(18,2)) AS reserva_legal,
        CAST(reserva_especial AS DECIMAL(18,2)) AS reserva_especial,
        CAST(superavit_valuaciones AS DECIMAL(18,2)) As superavit_valuaciones,
        CAST(utilidad_acumulada_ejercicio AS DECIMAL(18,2)) AS utilidad_acumulada_ejercicio,

		-- INDICADORES REGULATORIOS SBS ECUADOR
		CAST(activos_ponderados_riesgo AS decimal (18,2)) AS activos_ponderados_riesgo,
		CAST(indice_solvencia_pct AS decimal (18,2)) AS indice_solvencia_pct,
		CAST(indice_liquidez_pct AS decimal (18,2)) AS indice_liquidez_pct,

		-- MÉTRICAS DE CARTERA
		CAST(cartera_mora_t1 AS decimal (18,2)) AS cartera_mora_t1,
		CAST(tasa_mora_pct_t1 AS decimal (18,2)) AS tasa_mora_pct_t1,
		CAST(provision_requerida_t1 AS decimal (18,2)) AS provision_requerida_t1,
		CAST(provision_constituida_t1 AS decimal (18,2)) AS provision_constituida_t1,
		CAST(cobertura_provision_pct_t1 AS decimal (18,2)) AS cobertura_provision_pct_t1,
		CAST(el_total_t1 AS decimal (18,2)) AS el_total_t1

    FROM T3_balance_general_RAW
	
),
t1_cartera_bruta as (
select 
	SUM(CAST(saldo_capital as decimal(18,2))) as cartera_bruta,
	CAST(fecha_corte as date) as fecha_corte
from T1_creditos_RAW
group by CAST(fecha_corte as date)
),
t1_cartera_vigente as (
select 
	SUM(CAST(saldo_capital as decimal(18,2))) as cartera_vigente,
	CAST(fecha_corte as date) as fecha_corte
from T1_creditos_RAW
where calificacion_sbs in ('Normal','Vigente')
group by CAST(fecha_corte as date)
)
SELECT 
	T3.*,
	t1.cartera_bruta, 
	t1v.cartera_vigente,
	-- Recalculo total Patrimonio
	(capital_social + reserva_legal + reserva_especial + superavit_valuaciones + utilidad_acumulada_ejercicio) AS total_patrimonio,
	-- Recalculo Total activos
	(fondos_disponibles + operaciones_interbancarias_activo + inversiones + (t1.cartera_bruta - provisiones_acumuladas) + 
     cuentas_por_cobrar + bienes_realizables_adjudicados + propiedades_y_equipos + otros_activos) AS total_activos,
	-- Recalculo de aporte_cosede
	-- Cast por seguridad para fijar los decimales presentados 
	CAST((total_obligaciones_publico * 0.00065) / 6 AS DECIMAL(18,4)) AS aporte_cosede

FROM MART_T3_CAST T3
LEFT JOIN t1_cartera_bruta T1 
ON T3.fecha_mes = T1.fecha_corte
Left join t1_cartera_vigente t1v
on t3.fecha_mes = t1v.fecha_corte;
GO
Select COUNT(*) from T3_balance_general_RAW
Select COUNT(*) from MART_T3_balance_general
select * from T3_balance_general_RAW
--========================================================================================================================
-- Limpieza T4_pagos_cuotas_RAW 
--========================================================================================================================
CREATE OR ALTER VIEW MART_T4_pagos_cuotas AS
with t1_monto_pactado as (
    select 
        id_credito as id_credito_t1, 
        CAST(cuota_mensual as decimal(18,2)) as cuota_mensual
    from (
        select id_credito, cuota_mensual, 
            ROW_NUMBER() over(partition by id_credito order by CAST(fecha_corte as date) desc) as rn
        from T1_creditos_RAW
    ) as sub 
    where rn = 1
),
Pagos_Limpia_t4 AS (
select
	-- IDENTIFICACIÓN
	CAST(id_pago as VARCHAR (50)) as id_pago,
	CAST(id_credito as VARCHAR (50)) as id_credito,
	CAST(id_cliente as VARCHAR (50)) as id_cliente,
	CAST(segmento as VARCHAR (50)) as segmento,
	CAST(oficial_credito as VARCHAR (50)) as oficial_credito,
	CAST(zona_geografica as VARCHAR (50)) as zona_geografica,
	CAST(monto_interes_cuota as decimal(18,2)) as monto_interes_cuota,

	-- CUOTA

	CAST(numero_cuota AS INT) as numero_cuota,
	CAST(fecha_pago_real AS DATE) as fecha_pago_real,
	CAST(fecha_vencimiento_cuota AS DATE) as fecha_vencimiento_cuota,
	CAST(cuota_pactada as decimal(18,2)) as cuota_pactada_pre,
	CAST(monto_capital_cuota as decimal(18,2)) as monto_capital_cuota,
		-- Absoluto para valores negativos
	ABS(CAST(monto_pagado as decimal(18,2))) as monto_pagado,
	CAST(saldo_pendiente_cuota as decimal(18,2)) as saldo_pendiente_cuota,
	CAST(dias_retraso_pago as INT) as dias_retraso_pago_int,

	-- COMPORTAMIENTO 

	CAST(estado_pago as VARCHAR (50)) as estado_pago,
	CAST(canal_pago as VARCHAR (50)) as canal_pago_null,
	CAST(calificacion_al_pago as VARCHAR (50)) as calificacion_al_pago

FROM T4_pagos_cuotas_RAW
)
select
	t4.*,
	-- Normalizacion NULOS canal_pago 
	case
		when canal_pago_null is null and monto_pagado > 0 then 'No Identificado' 
		when canal_pago_null is null and monto_pagado <= 0 then 'No Aplica (Sin Pago)'
		else canal_pago_null
	end as canal_pago,
	-- Normalizar negativos dias_retraso_pago
	case    
        when t4.dias_retraso_pago_int < 0 then 
            case when t4.monto_pagado = 0 then 180 else 0 end
        else t4.dias_retraso_pago_int
    end as dias_retraso_pago,
	-- Normalizar es_pago_anticipado
	CAST(
		case
			when dias_retraso_pago_int < 0 and monto_pagado > 0 then 1
			else 0
		end as BIT
	) as es_pago_anticipado,
	-- Imputar cuota_pactada erroneas de t1 
	case 
		when  t4.cuota_pactada_pre <> t1.cuota_mensual  then t1.cuota_mensual
		else t4.cuota_pactada_pre
	end as cuota_pactada
from Pagos_Limpia_t4 as t4
left join t1_monto_pactado as t1 on t4.id_credito = t1.id_credito_t1; 
GO

--========================================================================================================================
-- Limpieza T5_clientes_RAW 
--========================================================================================================================
CREATE OR ALTER VIEW MART_T5_clientes AS
WITH t1_agregado_por_corte AS (
    -- Consolidamos todo lo que depende de la FECHA DE CORTE en una sola pasada
    SELECT 
        id_cliente,
        fecha_corte,
        SUM(TRY_CAST(provision_constituida AS DECIMAL(18,2))) AS provisiones_t1,
        SUM(TRY_CAST(el_expected_loss AS DECIMAL(18,2))) AS expected_loss_t1,
        SUM(TRY_CAST(saldo_total_exposicion AS DECIMAL(18,2))) AS saldo_exposicion_total_t1,
        SUM(TRY_CAST(ingresos_anuales AS DECIMAL(18,2))) AS ingreso_reciente_t1,
        -- Ponderación de tasa
        SUM(TRY_CAST(saldo_capital AS DECIMAL(18,2)) * TRY_CAST(tasa_nominal_anual_pct AS DECIMAL(18,4))) 
        / NULLIF(SUM(TRY_CAST(saldo_capital AS DECIMAL(18,2))), 0) AS tasa_ponderada_t1,
        -- Conteo de créditos activos en este corte
        COUNT(*) AS creditos_activos_t1,
        -- Calificación (asumimos que es consistente por corte/cliente)
        MAX(calificacion_sbs) AS calificacion_sbs_corte 
    FROM T1_creditos_RAW
    GROUP BY id_cliente, fecha_corte
),
t1_maestra AS (
    -- Filtramos la "última foto" y calculamos antigüedad histórica
    SELECT 
        id_cliente, provisiones_t1, expected_loss_t1, tasa_ponderada_t1,
        saldo_exposicion_total_t1, ingreso_reciente_t1, 
        creditos_activos_t1, calificacion_sbs_corte,
        -- Antigüedad calculada sobre el grupo histórico
        DATEDIFF(MONTH, MIN(fecha_corte) OVER(PARTITION BY id_cliente), fecha_corte) AS meses_antiguedad_t1,
        ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY CAST(fecha_corte AS DATE) DESC) as ranking
    FROM t1_agregado_por_corte
),
t1_historico_ingresos AS (
    -- Mantenemos el primer registro para ingresos (Diccionario ASC)
    SELECT id_cliente, ingreso_inicial_t1
    FROM (
        SELECT id_cliente,
               TRY_CAST(ingresos_anuales AS DECIMAL(18,2)) AS ingreso_inicial_t1,
               ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY CAST(fecha_corte AS DATE) ASC) as ranking
        FROM T1_creditos_RAW
    ) AS sub WHERE ranking = 1
),
t5_base AS (
    -- Tipado de datos inicial
    SELECT 
		CAST(fecha_primer_credito as date) as fecha_primer_credito,
		CAST(fecha_ultimo_corte as date) as fecha_ultimo_corte,
		-- IDENTIFICACIÓN
        CAST(id_cliente AS VARCHAR(50)) as id_cliente,
		CAST(tipo_persona AS VARCHAR(50)) as tipo_persona,
        CAST(segmento AS VARCHAR(50)) as segmento,
		CAST(sector_economico AS VARCHAR(50)) as sector_economico,
		CAST(zona_geografica AS VARCHAR(50)) as zona_geografica,
		CAST(oficial_credito AS VARCHAR(50)) as oficial_credito,
		CAST(edad_empresa_anos AS DECIMAL(18,2)) as edad_empresa_anos,
		CAST(antiguedad_cliente_meses AS INT) as antiguedad_cliente_meses,
	
		-- POSICIÓN CREDITICIA CONSOLIDADA (dic - 2025) 
        CAST(num_creditos_activos AS INT) as num_creditos_activos,
		CAST(num_creditos_sistema AS INT) as num_creditos_sistema,
		CAST(num_refinanciaciones AS INT) as num_refinanciaciones,
		CAST(calificacion_consolidada AS VARCHAR(50)) as calificacion_consolidada,
		CAST(score_crediticio AS INT) as score_crediticio,
		CAST(max_dias_atraso AS INT) as max_dias_atraso,
		CAST(tiene_credito_en_mora as BIT) as tiene_credito_en_mora,

		-- EXPOSICIÓN FINANCIERA CONSOLIDADA
        ABS(CAST(saldo_total_deuda AS DECIMAL(18,2))) as saldo_total_deuda,
        ABS(CAST(saldo_mora_total AS DECIMAL(18,2))) as saldo_mora_total,
        CAST(tasa_mora_cliente_pct AS DECIMAL(18,2)) as tasa_mora_cliente_pct,
        CAST(provision_total_cliente AS DECIMAL(18,2)) as prov_t5,
        CAST(el_total_cliente AS DECIMAL(18,2)) as expected_loss_t5,
        CAST(tasa_nominal_promedio AS DECIMAL(18,4)) as tasa_t5,
        CAST(ingresos_anuales AS DECIMAL(18,2)) as ingresos_t5
       
    FROM T5_clientes_RAW
)
SELECT 
	t5.fecha_primer_credito, t5.fecha_ultimo_corte,
    t5.id_cliente, t5.tipo_persona, t5.segmento, t5.zona_geografica, t5.antiguedad_cliente_meses, 
	t5.num_creditos_activos, t5.num_creditos_sistema, t5.num_refinanciaciones,
	t5.score_crediticio, t5.max_dias_atraso, t5.tiene_credito_en_mora,t5.saldo_total_deuda,
	t5.saldo_mora_total, t5.tasa_mora_cliente_pct, 
    -- 1. CALIFICACIÓN (Imputación por mora)
    COALESCE(t5.calificacion_consolidada, 
        CASE 
            WHEN t5.max_dias_atraso = 0 THEN 'Normal'
            WHEN t5.max_dias_atraso <= 15 THEN 'Potencial'
            WHEN t5.max_dias_atraso <= 45 THEN 'Deficiente'
            WHEN t5.max_dias_atraso <= 90 THEN 'Dudoso'
			WHEN t5.max_dias_atraso > 90 THEN 'Pérdida'
            ELSE t5.calificacion_consolidada
        END) AS calificacion_consolidada,


    -- 2. OFICIAL (Prioriza T5, rellena con T1)
    COALESCE(t5.oficial_credito, t1_of.oficial_credito) AS oficial_credito,

    -- 3. SECTOR (Normalización)
    CASE 
        WHEN t5.sector_economico LIKE '%agr%' THEN 'Agricultura'
        WHEN t5.sector_economico LIKE '%comercio%' THEN 'Comercio'
        WHEN t5.sector_economico LIKE '%const%' THEN 'Construcción'
        WHEN t5.sector_economico LIKE '%exp%' THEN 'Exportación'
        WHEN t5.sector_economico LIKE '%ind%' THEN 'Industria'
        WHEN t5.sector_economico LIKE '%pes%' THEN 'Pesca'
        WHEN t5.sector_economico LIKE '%serv%' THEN 'Servicios'
        WHEN t5.sector_economico LIKE '%trans%' THEN 'Transporte'
        WHEN t5.sector_economico LIKE '%turis%' THEN 'Turismo'
        ELSE 'No Definido' 
    END AS sector_economico,

    -- 4. EDAD EMPRESA
    CASE WHEN t5.tipo_persona = 'Natural' THEN 0.0 ELSE t5.edad_empresa_anos END AS edad_empresa_anos,

    -- 5. REEMPLAZOS DE T1 (Verdad Financiera)
    CASE WHEN ABS(t5.prov_t5 - m.provisiones_t1) > 0.01 THEN m.provisiones_t1 ELSE t5.prov_t5 END AS provision_total_cliente,
    CASE WHEN ABS(t5.expected_loss_t5 - m.expected_loss_t1) > 0.01 THEN m.expected_loss_t1 ELSE t5.expected_loss_t5 END AS el_total_cliente,
    CASE WHEN ABS(t5.tasa_t5 - m.tasa_ponderada_t1) > 0.01 THEN m.tasa_ponderada_t1 ELSE t5.tasa_t5 END AS tasa_nominal_promedio,
    CASE WHEN ABS(t5.ingresos_t5 - hist.ingreso_inicial_t1) > 0.01 THEN hist.ingreso_inicial_t1 ELSE t5.ingresos_t5 END AS ingresos_anuales,

    -- 6. CÁLCULOS DERIVADOS
    (m.saldo_exposicion_total_t1 / NULLIF(m.ingreso_reciente_t1, 0)) * 100 AS tasa_endeudamiento_sistema_pct,

    CASE 
        WHEN m.calificacion_sbs_corte = 'Normal' AND m.meses_antiguedad_t1 > 24 AND m.creditos_activos_t1 > 1 THEN 1 
        ELSE 0 
    END AS es_cliente_preferente

FROM t5_base t5
LEFT JOIN (SELECT id_cliente, MAX(oficial_credito) as oficial_credito FROM T1_creditos_RAW GROUP BY id_cliente) t1_of 
    ON t5.id_cliente = t1_of.id_cliente
LEFT JOIN t1_maestra m ON t5.id_cliente = m.id_cliente AND m.ranking = 1
LEFT JOIN t1_historico_ingresos hist ON t5.id_cliente = hist.id_cliente
WHERE t5.saldo_total_deuda NOT IN (251958.69, 3099540.33) 
  AND t5.tasa_mora_cliente_pct <= 100 AND
   score_crediticio >= 200 and score_crediticio <= 850;
GO

select es_cliente_preferente from MART_T5_clientes group by es_cliente_preferente



   