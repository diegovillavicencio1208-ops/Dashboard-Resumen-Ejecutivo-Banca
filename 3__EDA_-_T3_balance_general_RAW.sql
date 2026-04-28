/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 3. EDA — T3_balance_general_RAW
   DESCRIPCIÓN   : Análisis Exploratorio de Datos sobre el Balance General mensual.
                   Valida la estructura de Activos, Pasivos y Patrimonio, detecta
                   negativos anómalos (patrimonio e índice de solvencia negativo),
                   audita la coherencia de los totales y cruza la cartera bruta
                   con T1 para garantizar consistencia entre estados financieros.
   TABLA FUENTE  : T3_balance_general_RAW
   TABLAS CRUZADAS: T1_creditos_RAW (validación cartera bruta)
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : Tablas RAW cargadas y scripts anteriores ejecutados.
============================================================================================================================ */

Use RiesgoCrediticioProyecto2;
Go
/*============================================================================================================
    1. EXPLORACIÓN DE INTEGRIDAD TÉCNICA - T3_balance_general_RAW
============================================================================================================*/

SELECT 
    '1. INTEGRIDAD (BASURA)' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T3_balance_general_RAW
CROSS APPLY (VALUES 
    -- ACTIVOS
    ('fondos_disponibles', fondos_disponibles),
    ('operaciones_interbancarias_activo', operaciones_interbancarias_activo),
    ('inversiones', inversiones),
    ('cartera_bruta', cartera_bruta),
    ('provisiones_accumuladas', provisiones_acumuladas),
    ('cuentas_por_cobrar', cuentas_por_cobrar),
    ('bienes_realizables_adjudicados', bienes_realizables_adjudicados),
    ('propiedades_y_equipos', propiedades_y_equipos),
    ('otros_activos', otros_activos),
    ('total_activos', total_activos),

    -- PASIVOS
    ('depositos_vista', depositos_vista),
    ('depositos_ahorro', depositos_ahorro),
    ('depositos_plazo', depositos_plazo),
    ('depositos_restringidos', depositos_restringidos),
    ('total_obligaciones_publico', total_obligaciones_publico),
    ('operaciones_interbancarias_pasivo', operaciones_interbancarias_pasivo),
    ('obligaciones_financieras_bde_cfn', obligaciones_financieras_bde_cfn),
    ('cuentas_por_pagar', cuentas_por_pagar),
    ('otros_pasivos', otros_pasivos),
    ('total_pasivos', total_pasivos),

    -- PATRIMONIO
    ('capital_social', capital_social),
    ('reserva_legal', reserva_legal),
    ('reserva_especial', reserva_especial),
    ('superavit_valuaciones', superavit_valuaciones),
    ('utilidad_acumulada_ejercicio', utilidad_acumulada_ejercicio),
    ('total_patrimonio', total_patrimonio),

    -- INDICADORES Y MÉTRICAS T1
    ('activos_ponderados_riesgo', activos_ponderados_riesgo),
    ('indice_solvencia_pct', indice_solvencia_pct),
    ('indice_liquidez_pct', indice_liquidez_pct),
    ('aporte_cosede', aporte_cosede),
    ('cartera_mora_t1', cartera_mora_t1),
    ('tasa_mora_pct_t1', tasa_mora_pct_t1),
    ('provision_requerida_t1', provision_requerida_t1),
    ('provision_constituida_t1', provision_constituida_t1),
    ('cobertura_provision_pct_t1', cobertura_provision_pct_t1),
    ('el_total_t1', el_total_t1)
) AS v(campo, valor_sucio)
WHERE TRY_CAST(v.valor_sucio AS DECIMAL(18,4)) IS NULL 
  AND v.valor_sucio IS NOT NULL 
  AND v.valor_sucio <> ''
GROUP BY v.campo, v.valor_sucio;

-- Limpieza: Ninguna, no hay errores en los valores de esta tabla 

/* ============================================================================================================
    2. EXPLORACIÓN DE NULOS Y VACÍOS - T3_BALANCE_GENERAL_RAW
    Objetivo: Identificar ausencia de datos en cuentas críticas del balance y métricas SBS.
============================================================================================================ */
SELECT 
    '2. NULOS' AS Paso,
    v.campo,
    SUM(v.es_nulo) AS total_nulos,
    COUNT(*) AS registros_totales,
    CAST(SUM(v.es_nulo) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_nulos
FROM T3_balance_general_RAW
CROSS APPLY (VALUES
    -- Totales Estructurales (Métricas Base para Ratios)
    ('total_activos',                CASE WHEN total_activos IS NULL OR total_activos = '' THEN 1 ELSE 0 END),
    ('total_pasivos',                CASE WHEN total_pasivos IS NULL OR total_pasivos = '' THEN 1 ELSE 0 END),
    ('total_patrimonio',             CASE WHEN total_patrimonio IS NULL OR total_patrimonio = '' THEN 1 ELSE 0 END),
    
    -- Componentes Críticos del Activo
    ('cartera_bruta',                CASE WHEN cartera_bruta IS NULL OR cartera_bruta = '' THEN 1 ELSE 0 END),
    ('fondos_disponibles',           CASE WHEN fondos_disponibles IS NULL OR fondos_disponibles = '' THEN 1 ELSE 0 END),
    ('inversiones',                  CASE WHEN inversiones IS NULL OR inversiones = '' THEN 1 ELSE 0 END),
    ('provisiones_acumuladas',       CASE WHEN provisiones_acumuladas IS NULL OR provisiones_acumuladas = '' THEN 1 ELSE 0 END),
    
    -- Componentes Críticos del Pasivo (Fondeo)
    ('total_obligaciones_publico',   CASE WHEN total_obligaciones_publico IS NULL OR total_obligaciones_publico = '' THEN 1 ELSE 0 END),
    ('depositos_plazo',              CASE WHEN depositos_plazo IS NULL OR depositos_plazo = '' THEN 1 ELSE 0 END),
    
    -- Patrimonio y Resultados
    ('capital_social',               CASE WHEN capital_social IS NULL OR capital_social = '' THEN 1 ELSE 0 END),
    ('utilidad_acumulada_ejercicio', CASE WHEN utilidad_acumulada_ejercicio IS NULL OR utilidad_acumulada_ejercicio = '' THEN 1 ELSE 0 END),
    
    -- Indicadores SBS
    ('indice_solvencia_pct',         CASE WHEN indice_solvencia_pct IS NULL OR indice_solvencia_pct = '' THEN 1 ELSE 0 END),
    ('indice_liquidez_pct',          CASE WHEN indice_liquidez_pct IS NULL OR indice_liquidez_pct = '' THEN 1 ELSE 0 END)
) AS v(campo, es_nulo)
GROUP BY v.campo
ORDER BY pct_nulos DESC;
-- Limpieza: Ninguna, no hay nulos en las variables 

/* ============================================================================================================
    3. UNICIDAD (Duplicados) - T3_BALANCE_GENERAL_RAW
    Objetivo: Asegurar que solo exista un reporte de Balance por cada cierre de mes.
============================================================================================================ */
SELECT 
    '3. DUPLICADOS' AS Paso,
    fecha_mes, 
    COUNT(*) AS registros_encontrados,
    -- Concatenamos los años/meses para verificar si el error es de carga repetida
    MAX(ano) AS ano_reportado,
    MAX(mes) AS mes_reportado
FROM T3_balance_general_RAW
GROUP BY fecha_mes
HAVING COUNT(*) > 1;
-- Limpieza, Ninguna al no existir presencia de duplicados
/* ============================================================================================================
    4. EXPLORACIÓN DE VALORES NEGATIVOS - T3_BALANCE_GENERAL_RAW
    Objetivo: Detectar montos menores a cero en el balance que puedan indicar errores de signo o registros inusuales.
============================================================================================================ */
SELECT 
    '1.1 NEGATIVOS' AS Paso,
    v.campo,
    COUNT(*) AS frecuencia
FROM T3_balance_general_RAW
CROSS APPLY (VALUES 
    -- ACTIVOS (Grupo 1)
    ('fondos_disponibles',             TRY_CAST(fondos_disponibles AS DECIMAL(18,4))),
    ('operaciones_interbancarias_act', TRY_CAST(operaciones_interbancarias_activo AS DECIMAL(18,4))),
    ('inversiones',                    TRY_CAST(inversiones AS DECIMAL(18,4))),
    ('cartera_bruta',                  TRY_CAST(cartera_bruta AS DECIMAL(18,4))),
    ('provisiones_acumuladas',         TRY_CAST(provisiones_acumuladas AS DECIMAL(18,4))), -- Suele ser negativo en contabilidad, pero aquí verificamos consistencia
    ('cuentas_por_cobrar',             TRY_CAST(cuentas_por_cobrar AS DECIMAL(18,4))),
    ('bienes_realizables_adjudicados', TRY_CAST(bienes_realizables_adjudicados AS DECIMAL(18,4))),
    ('propiedades_y_equipos',          TRY_CAST(propiedades_y_equipos AS DECIMAL(18,4))),
    ('otros_activos',                  TRY_CAST(otros_activos AS DECIMAL(18,4))),
    ('total_activos',                  TRY_CAST(total_activos AS DECIMAL(18,4))),

    -- PASIVOS (Grupo 2)
    ('depositos_vista',                TRY_CAST(depositos_vista AS DECIMAL(18,4))),
    ('depositos_ahorro',               TRY_CAST(depositos_ahorro AS DECIMAL(18,4))),
    ('depositos_plazo',                TRY_CAST(depositos_plazo AS DECIMAL(18,4))),
    ('total_obligaciones_publico',     TRY_CAST(total_obligaciones_publico AS DECIMAL(18,4))),
    ('operaciones_interbancarias_pas', TRY_CAST(operaciones_interbancarias_pasivo AS DECIMAL(18,4))),
    ('obligaciones_financieras_bde',   TRY_CAST(obligaciones_financieras_bde_cfn AS DECIMAL(18,4))),
    ('total_pasivos',                  TRY_CAST(total_pasivos AS DECIMAL(18,4))),

    -- PATRIMONIO (Grupo 3)
    ('capital_social',                 TRY_CAST(capital_social AS DECIMAL(18,4))),
    ('reserva_legal',                  TRY_CAST(reserva_legal AS DECIMAL(18,4))),
    ('total_patrimonio',               TRY_CAST(total_patrimonio AS DECIMAL(18,4))), -- Valor negativo
    ('utilidad_acumulada_ejercicio',   TRY_CAST(utilidad_acumulada_ejercicio AS DECIMAL(18,4))), 

    -- INDICADORES REGULATORIOS Y T1
    ('indice_solvencia_pct',           TRY_CAST(indice_solvencia_pct AS DECIMAL(18,4))), -- Valor negativo
    ('indice_liquidez_pct',            TRY_CAST(indice_liquidez_pct AS DECIMAL(18,4))),
    ('cartera_mora_t1',                TRY_CAST(cartera_mora_t1 AS DECIMAL(18,4))),
    ('el_total_t1',                    TRY_CAST(el_total_t1 AS DECIMAL(18,4)))
) AS v(campo, valor_encontrado)
WHERE v.valor_encontrado < 0 
GROUP BY v.campo;
-- Valores negativos en total_patrimonio y indice_solvencia_pct

-- Revision individual
SELECT 
    fecha_mes,
    total_patrimonio,
    indice_solvencia_pct,
    capital_social,
    reserva_legal,
    utilidad_acumulada_ejercicio
FROM T3_balance_general_RAW
WHERE TRY_CAST(total_patrimonio AS DECIMAL(18,4)) < 0
   OR TRY_CAST(indice_solvencia_pct AS DECIMAL(18,4)) < 0;
-- Recalculo de 
WITH T3_Recalculo AS (
    SELECT 
        fecha_mes,
        TRY_CAST(capital_social AS DECIMAL(18,2)) AS capital_social,
        TRY_CAST(reserva_legal AS DECIMAL(18,2)) AS reserva_legal,
        TRY_CAST(reserva_especial AS DECIMAL(18,2)) AS reserva_especial,
        TRY_CAST(superavit_valuaciones AS DECIMAL(18,2)) AS superavit_valuaciones,
        TRY_CAST(utilidad_acumulada_ejercicio AS DECIMAL(18,2)) AS utilidad_acumulada_ejercicio,
        TRY_CAST(total_patrimonio AS DECIMAL(18,2)) AS total_patrimonio_reportada
    FROM T3_balance_general_RAW
    WHERE fecha_mes = '2024-09-30'
)
SELECT 
    fecha_mes,
    total_patrimonio_reportada,
    (capital_social + reserva_legal +  reserva_especial + superavit_valuaciones + utilidad_acumulada_ejercicio) AS patrimonio_recalculado,
	-- Patrimonio recalculo
    ABS(total_patrimonio_reportada - (capital_social + reserva_legal + reserva_especial + superavit_valuaciones + utilidad_acumulada_ejercicio)) AS diferencia_error
FROM T3_Recalculo;
-- Limpieza: Recalcular total_patrimonio producto del error apreciado en 2024-09-30, patrominio reportado -3007840.36, recalculo (37598004.45)

/* ============================================================================================================
    5. CONSISTENCIA DE VALORES NUMÉRICOS (Reglas de Negocio - T3)
    Objetivo: Validar que los Totales Reportados coincidan con la suma de sus componentes.
============================================================================================================ */
-- Revision cartera_bruta 
-- Coincidencia con saldo_capital en T1
with t1 as (
	select 
		SUM(CAST(saldo_capital as decimal(18,2))) as cartera_bruta,
		CAST(fecha_corte as date) as fecha_corte
	from T1_creditos_RAW
	group by fecha_corte
)
select
	t3.fecha_mes,
	t1.fecha_corte,
	CAST(t3.cartera_bruta as decimal(18,2)) as cartera_bruta_t3,
	t1.cartera_bruta as cartera_bruta_t1,
	CAST(t3.cartera_bruta as decimal(18,2)) - t1.cartera_bruta  as diferencia
from T3_balance_general_RAW t3
left join t1 as t1 on 
t3.fecha_mes = t1.fecha_corte
where t3.cartera_bruta <> t1.cartera_bruta

--- Recalcular cartera bruta t3 y Crear cartera vigente t3
-------------------------------------------------------------------------------------------------------------------
-- Revision de Patrimonio
WITH T3_Patrimonio_Check AS (
    SELECT 
        fecha_mes,
        TRY_CAST(total_patrimonio AS DECIMAL(18,2)) AS total_patrimonio_reportado,
        -- Suma de los componentes que tenemos en el diccionario
        (TRY_CAST(capital_social AS DECIMAL(18,2)) + 
         TRY_CAST(reserva_legal AS DECIMAL(18,2)) + 
         TRY_CAST(reserva_especial AS DECIMAL(18,2)) + 
         TRY_CAST(superavit_valuaciones AS DECIMAL(18,2)) + 
         TRY_CAST(utilidad_acumulada_ejercicio AS DECIMAL(18,2))) AS patrimonio_recalculado
    FROM T3_balance_general_RAW
)
SELECT 
    fecha_mes,
    total_patrimonio_reportado,
    patrimonio_recalculado,
    (total_patrimonio_reportado - patrimonio_recalculado) AS diferencia_oculta
FROM T3_Patrimonio_Check
ORDER BY fecha_mes;

-- Limpieza: Recalculo de total_patrimonio, 2024-09-30 patrominio negativo -3007840.36 y no se valida a recalcular 37598004.45
------------------------------------------------------------------------------------------------------------------------
-- Revision total_activos
WITH T3_Conversio_Datos AS (
    SELECT 
        fecha_mes,
        -- Conversión limpia de tipos de datos
        TRY_CAST(total_activos AS DECIMAL(18,2)) AS total_activos_reportada,
        TRY_CAST(fondos_disponibles AS DECIMAL(18,2)) AS fondos_disponibles,
        TRY_CAST(operaciones_interbancarias_activo AS DECIMAL(18,2)) AS operaciones_interbancarias_activo,
        TRY_CAST(inversiones AS DECIMAL(18,2)) AS inversiones,
        TRY_CAST(cartera_bruta AS DECIMAL(18,2)) AS cartera_bruta,
        TRY_CAST(provisiones_acumuladas AS DECIMAL(18,2)) AS provisiones_acumuladas,
        TRY_CAST(cuentas_por_cobrar AS DECIMAL(18,2)) AS cuentas_por_cobrar,
        TRY_CAST(bienes_realizables_adjudicados AS DECIMAL(18,2)) AS bienes_realizables_adjudicados,
        TRY_CAST(propiedades_y_equipos AS DECIMAL(18,2)) AS propiedades_y_equipos,
        TRY_CAST(otros_activos AS DECIMAL(18,2)) AS otros_activos
    FROM T3_balance_general_RAW
)
SELECT 
    fecha_mes,
    total_activos_reportada,
	-- activos recalculo
	fondos_disponibles + operaciones_interbancarias_activo + inversiones + cartera_bruta + provisiones_acumuladas + (cartera_bruta - provisiones_acumuladas) +
	cuentas_por_cobrar + bienes_realizables_adjudicados + propiedades_y_equipos + otros_activos AS total_activos_recalculado,
    -- Cálculo de la diferencia
    (total_activos_reportada - (fondos_disponibles + operaciones_interbancarias_activo + inversiones + (cartera_bruta - provisiones_acumuladas) + 
     cuentas_por_cobrar + bienes_realizables_adjudicados + propiedades_y_equipos + otros_activos)) AS diferencia_activo
FROM T3_Conversio_Datos
ORDER BY fecha_mes;

-- Recalcular activos 2025-07-31 (618361812.30) y 2024-12-31 (548271782.38) se encuentran inflados el resto de calculos son correctos.

----------------------------------------------------------------------------------------------------------------------------
WITH T3_Pasivo_Casteo AS (
    SELECT 
        fecha_mes,
        TRY_CAST(total_pasivos AS DECIMAL(18,2)) AS total_pasivos_reportada,
        TRY_CAST(total_obligaciones_publico AS DECIMAL(18,2)) AS total_obligaciones_publico,
        TRY_CAST(depositos_vista AS DECIMAL(18,2)) AS depositos_vista,
        TRY_CAST(depositos_plazo AS DECIMAL(18,2)) AS depositos_plazo,
        TRY_CAST(operaciones_interbancarias_pasivo AS DECIMAL(18,2)) AS operaciones_interbancarias_pasivo,
        TRY_CAST(obligaciones_financieras_bde_cfn AS DECIMAL(18,2)) AS obligaciones_financieras,
        TRY_CAST(cuentas_por_pagar AS DECIMAL(18,2)) AS cuentas_por_pagar,
        TRY_CAST(otros_pasivos AS DECIMAL(18,2)) AS otros_pasivos
    FROM T3_balance_general_RAW
)
SELECT 
    fecha_mes,
    total_pasivos_reportada,
    -- Sumatoria de los componentes del Pasivo
    (total_obligaciones_publico + operaciones_interbancarias_pasivo + obligaciones_financieras + cuentas_por_pagar + 
     otros_pasivos) AS pasivo_recalculado,
    -- Cálculo de la diferencia interna del pasivo
    (total_pasivos_reportada - (total_obligaciones_publico + operaciones_interbancarias_pasivo + obligaciones_financieras + 
      cuentas_por_pagar + otros_pasivos)) AS diferencia_pasivo
FROM T3_Pasivo_Casteo
ORDER BY fecha_mes;
-- Limpieza: Sin errores, usar variable intacta

-----------------------------------------------------------------------------------------------------------------------
-- Verificaccion total_obligaciones_publico
WITH T3_Casteo_Obligaciones AS (
    SELECT 
        fecha_mes,
        -- Campo 19: Total Reportado
        TRY_CAST(total_obligaciones_publico AS DECIMAL(18,2)) AS total_obligaciones_RAW,
        -- Suma de Componentes (Campos 15 al 18)
        (TRY_CAST(depositos_vista AS DECIMAL(18,2)) + 
         TRY_CAST(depositos_ahorro AS DECIMAL(18,2)) + 
         TRY_CAST(depositos_plazo AS DECIMAL(18,2)) + 
         TRY_CAST(depositos_restringidos AS DECIMAL(18,2))) AS suma_componentes_15_18
    FROM T3_balance_general_RAW
)
SELECT 
    fecha_mes,
    total_obligaciones_RAW,
    suma_componentes_15_18,
    -- Diferencia de cuadre
    (total_obligaciones_RAW - suma_componentes_15_18) AS diferencia_cuadre_captaciones
FROM T3_Casteo_Obligaciones
ORDER BY diferencia_cuadre_captaciones DESC;
--Limpieza: Ninguna, todo cuadra

---------------------------------------------------------------------------------------------------------------------------
-- Verificar aporte_cosede
WITH T3_Auditoria_COSEDE AS (
    SELECT 
        fecha_mes,
        CAST(total_obligaciones_publico AS DECIMAL(18,4)) AS obligaciones_publico,
        CAST(aporte_cosede AS DECIMAL(18,4)) AS cosede_reportado,
        -- Cálculo según tu diccionario: (Monto * 0.065%) / 6
        CAST((CAST(total_obligaciones_publico AS DECIMAL(18,4)) * 0.00065) / 6 AS DECIMAL(18,4)) AS cosede_teorico
    FROM T3_balance_general_RAW
)
SELECT 
    fecha_mes,
    obligaciones_publico,
    cosede_reportado,
    cosede_teorico,
    (cosede_reportado - cosede_teorico) AS diferencia_cosede,
    CASE 
        WHEN ABS(cosede_reportado - cosede_teorico) < 1.00 THEN 'CORRECTO'
        ELSE 'ERROR_CALCULO'
    END AS estatus_cosede
FROM T3_Auditoria_COSEDE
ORDER BY fecha_mes;
-- Errores de calculo, cosede_reportado no fue calculado utilizando el divisor 6 haciendo que se pagen 6 veces mas lo esperado
