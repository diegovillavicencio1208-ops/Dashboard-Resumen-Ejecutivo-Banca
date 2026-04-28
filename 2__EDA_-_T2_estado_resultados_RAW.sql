/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 2. EDA — T2_estado_resultados_RAW
   DESCRIPCIÓN   : Análisis Exploratorio de Datos sobre el Estado de Resultados mensual.
                   Valida la calidad del dato financiero en 6 dimensiones y audita la
                   coherencia matemática de los cálculos contables reportados
                   (márgenes, participación trabajadores, utilidad neta, totales de
                   ingresos, gastos financieros y gastos operativos).
   TABLA FUENTE  : T2_estado_resultados_RAW
   TABLAS CRUZADAS: T1_creditos_RAW (validación de castigos)
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : T2_estado_resultados_RAW cargada en la base de datos.
============================================================================================================================ */

Use RiesgoCrediticioProyecto2;
Go

/* ============================================================================================================================
   PASO 1: INTEGRIDAD TÉCNICA — Detección de "basura" en campos financieros
   Objetivo : Identificar valores de texto en columnas que deben ser numéricas.
              Un valor no convertible a DECIMAL indica un problema de carga o formato.
   Columnas : Toda la cuenta de Ingresos, Gastos y Resultados del Estado de Resultados.
   Resultado: 0 filas esperadas (ninguna basura en valores numéricos).
============================================================================================================================ */
SELECT 
    '1. INTEGRIDAD' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T2_estado_resultados_RAW
CROSS APPLY (VALUES 
    -- INGRESOS
    ('ing_intereses_cartera', ing_intereses_cartera),
    ('ing_intereses_inversiones', ing_intereses_inversiones),
    ('ing_intereses_interbancarios', ing_intereses_interbancarios),
    ('ing_comisiones_ganadas', ing_comisiones_ganadas),
    ('ing_utilidades_financieras', ing_utilidades_financieras),
    ('ing_servicios', ing_servicios),
    ('otros_ingresos', otros_ingresos),
    ('total_ingresos', total_ingresos),

    -- GASTOS FINANCIEROS Y MARGEN
    ('gasto_intereses_depositos', gasto_intereses_depositos),
    ('gasto_intereses_obligaciones', gasto_intereses_obligaciones),
    ('gasto_comisiones_pagadas', gasto_comisiones_pagadas),
    ('total_gastos_financieros', total_gastos_financieros),
    ('margen_financiero_bruto', margen_financiero_bruto),
    ('gasto_provisiones_periodo', gasto_provisiones_periodo),

    -- GASTOS OPERATIVOS (ADMINISTRATIVOS)
    ('gasto_personal', gasto_personal),
    ('gasto_honorarios', gasto_honorarios),
    ('gasto_instalaciones', gasto_instalaciones),
    ('gasto_publicidad', gasto_publicidad),
    ('gasto_sistemas_tecnologia', gasto_sistemas_tecnologia),
    ('gasto_depreciacion', gasto_depreciacion),
    ('gasto_amortizacion', gasto_amortizacion),
    ('otros_gastos_operativos', otros_gastos_operativos),
    ('total_gastos_operativos', total_gastos_operativos),

    -- OTROS Y RESULTADOS FINALES
    ('castigos_periodo', castigos_periodo),
    ('recuperaciones_periodo', recuperaciones_periodo),
    ('otras_perdidas_operacionales', otras_perdidas_operacionales),
    ('otros_gastos', otros_gastos),
    ('resultado_antes_impuestos', resultado_antes_impuestos),
    ('participacion_trabajadores_15pct', participacion_trabajadores_15pct),
    ('impuesto_renta_25pct', impuesto_renta_25pct),
    ('utilidad_neta', utilidad_neta)
) AS v(campo, valor_sucio)
WHERE TRY_CAST(v.valor_sucio AS DECIMAL(18,4)) IS NULL 
  AND v.valor_sucio IS NOT NULL AND v.valor_sucio <> ''
GROUP BY v.campo, v.valor_sucio;

-- Limpieza: NADA, sin valores texto dentro de los variables numericas

/* ============================================================================================================================
   PASO 1.1: VALORES NEGATIVOS — Posibles distorsiones en rentabilidad (ROE/ROA)
   Objetivo : Detectar montos menores a cero que puedan distorsionar el cálculo
              de rentabilidad y márgenes financieros.
   Notas importantes sobre negativos "esperados":
     - utilidad_neta            : Puede ser negativo (pérdida del período). Se deja.
     - resultado_antes_impuestos: Puede ser negativo (comportamiento normal del banco).
     - gasto_provisiones_periodo: Valores negativos detectados → se corregirán con ABS() en limpieza.
============================================================================================================================ */
SELECT 
    '1.1 NEGATIVOS' AS Paso,
    v.campo,
    COUNT(*) AS frecuencia
FROM T2_estado_resultados_RAW
CROSS APPLY (VALUES 
    -- INGRESOS
    ('ing_intereses_cartera',           TRY_CAST(ing_intereses_cartera AS DECIMAL(18,4))),
    ('ing_intereses_inversiones',       TRY_CAST(ing_intereses_inversiones AS DECIMAL(18,4))),
    ('ing_intereses_interbancarios',    TRY_CAST(ing_intereses_interbancarios AS DECIMAL(18,4))),
    ('ing_comisiones_ganadas',          TRY_CAST(ing_comisiones_ganadas AS DECIMAL(18,4))),
    ('ing_utilidades_financieras',      TRY_CAST(ing_utilidades_financieras AS DECIMAL(18,4))),
    ('ing_servicios',                   TRY_CAST(ing_servicios AS DECIMAL(18,4))),
    ('otros_ingresos',                  TRY_CAST(otros_ingresos AS DECIMAL(18,4))),
    ('total_ingresos',                  TRY_CAST(total_ingresos AS DECIMAL(18,4))),

    -- GASTOS FINANCIEROS Y OPERATIVOS
    ('gasto_intereses_depositos',       TRY_CAST(gasto_intereses_depositos AS DECIMAL(18,4))),
    ('gasto_intereses_obligaciones',    TRY_CAST(gasto_intereses_obligaciones AS DECIMAL(18,4))),
    ('gasto_comisiones_pagadas',        TRY_CAST(gasto_comisiones_pagadas AS DECIMAL(18,4))),
    ('total_gastos_financieros',        TRY_CAST(total_gastos_financieros AS DECIMAL(18,4))),
    ('gasto_provisiones_periodo',       TRY_CAST(gasto_provisiones_periodo AS DECIMAL(18,4))), -- Valores negativos → corregir con ABS()
    ('gasto_personal',                  TRY_CAST(gasto_personal AS DECIMAL(18,4))),
    ('gasto_honorarios',                TRY_CAST(gasto_honorarios AS DECIMAL(18,4))),
    ('gasto_instalaciones',             TRY_CAST(gasto_instalaciones AS DECIMAL(18,4))),
    ('gasto_publicidad',                TRY_CAST(gasto_publicidad AS DECIMAL(18,4))),
    ('gasto_sistemas_tecnologia',       TRY_CAST(gasto_sistemas_tecnologia AS DECIMAL(18,4))),
    ('total_gastos_operativos',         TRY_CAST(total_gastos_operativos AS DECIMAL(18,4))),

    -- RESULTADOS
    ('margen_financiero_bruto',         TRY_CAST(margen_financiero_bruto AS DECIMAL(18,4))),
    ('utilidad_neta',                   TRY_CAST(utilidad_neta AS DECIMAL(18,4))),            -- Negativo válido (pérdida)
    ('resultado_antes_impuestos',       TRY_CAST(resultado_antes_impuestos AS DECIMAL(18,4))),-- Negativo válido
    ('recuperaciones_periodo',          TRY_CAST(recuperaciones_periodo AS DECIMAL(18,4))),
    ('castigos_periodo',                TRY_CAST(castigos_periodo AS DECIMAL(18,4)))
) AS v(campo, valor_encontrado)
WHERE v.valor_encontrado < 0 
GROUP BY v.campo;

-- utilidad_neta: Valores negativos; se deja fuera ya que se recalculará en el script de limpieza.
-- resultado_antes_impuestos: Valores negativos, comportamiento normal del banco.
-- gasto_provisiones_periodo: Valores negativos → se corregirán con ABS() en limpieza (script 6).

/* ============================================================================================================================
   PASO 2: COMPLETITUD — Nulos y vacíos en cuentas que definen la utilidad del período
   Objetivo : Identificar vacíos en las cuentas base para el cálculo de márgenes (ROA/ROE).
   Resultado esperado: 0 nulos en todas las variables críticas.
============================================================================================================================ */
SELECT 
    '2. NULOS' AS Paso,
    v.campo,
    SUM(v.es_nulo) AS total_nulos,
    COUNT(*) AS registros_totales,
    CAST(SUM(v.es_nulo) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_nulos
FROM T2_estado_resultados_RAW
CROSS APPLY (VALUES
    -- Totales de Ingresos y Gastos (Métricas Base)
    ('total_ingresos',               CASE WHEN total_ingresos IS NULL OR total_ingresos = '' THEN 1 ELSE 0 END),
    ('total_gastos_financieros',     CASE WHEN total_gastos_financieros IS NULL OR total_gastos_financieros = '' THEN 1 ELSE 0 END),
    ('total_gastos_operativos',      CASE WHEN total_gastos_operativos IS NULL OR total_gastos_operativos = '' THEN 1 ELSE 0 END),
    
    -- Componentes de Ingresos (Principales)
    ('ing_intereses_cartera',        CASE WHEN ing_intereses_cartera IS NULL OR ing_intereses_cartera = '' THEN 1 ELSE 0 END),
    ('ing_comisiones_ganadas',       CASE WHEN ing_comisiones_ganadas IS NULL OR ing_comisiones_ganadas = '' THEN 1 ELSE 0 END),
    ('ing_servicios',                CASE WHEN ing_servicios IS NULL OR ing_servicios = '' THEN 1 ELSE 0 END),
    
    -- Componentes de Gasto
    ('gasto_intereses_depositos',    CASE WHEN gasto_intereses_depositos IS NULL OR gasto_intereses_depositos = '' THEN 1 ELSE 0 END),
    ('gasto_provisiones_periodo',    CASE WHEN gasto_provisiones_periodo IS NULL OR gasto_provisiones_periodo = '' THEN 1 ELSE 0 END),
    ('gasto_personal',               CASE WHEN gasto_personal IS NULL OR gasto_personal = '' THEN 1 ELSE 0 END),
    
    -- Resultados y Margen
    ('margen_financiero_bruto',      CASE WHEN margen_financiero_bruto IS NULL OR margen_financiero_bruto = '' THEN 1 ELSE 0 END),
    ('resultado_antes_impuestos',    CASE WHEN resultado_antes_impuestos IS NULL OR resultado_antes_impuestos = '' THEN 1 ELSE 0 END),
    ('utilidad_neta',                CASE WHEN utilidad_neta IS NULL OR utilidad_neta = '' THEN 1 ELSE 0 END)
) AS v(campo, es_nulo)
GROUP BY v.campo
ORDER BY pct_nulos DESC;

-- Accion: Ninguna — 0 valores nulos

/* ============================================================================================================================
   PASO 3: UNICIDAD — Duplicados por mes
   Objetivo : Confirmar que solo exista un cierre contable mensual por fecha.
              La tabla debe tener exactamente 24 filas (24 meses de histórico).
   Resultado esperado: 0 filas (ningún mes duplicado).
============================================================================================================================ */
SELECT 
    '3. DUPLICADOS' AS Paso,
    fecha_mes, 
    COUNT(*) AS registros_encontrados,
    -- Sumatoria de utilidad: si el duplicado es exacto, el resultado se dobla
    SUM(TRY_CAST(utilidad_neta AS DECIMAL(18,2))) AS suma_utilidad_en_mes 
FROM T2_estado_resultados_RAW
GROUP BY fecha_mes
HAVING COUNT(*) > 1;

-- Accion: 0 duplicados

/* ============================================================================================================================
   PASO 4: CONSISTENCIA DE FECHAS
   Objetivo : Validar que fecha_mes, mes y año sean coherentes entre sí.
              Un error típico de carga es que el campo año/mes no coincida
              con la fecha completa registrada.
   Resultado esperado: 0 filas (ninguna inconsistencia).
============================================================================================================================ */
SELECT 
	fecha_mes,
	mes,
	ano
from T2_estado_resultados_RAW
where 
	(YEAR(try_cast(fecha_mes as date)) <> ano) AND
	(MONTH(try_cast(fecha_mes as date)) <> mes)

-- Accion: Ninguna — 0 inconsistencias de fechas

/* ============================================================================================================================
   PASO 5: CONSISTENCIA DE VALORES NUMÉRICOS — Auditoría de reglas de negocio
   Técnica  : CTE para conversión limpia de tipos (VARCHAR → DECIMAL) antes de los cálculos.
              Esto evita repetir TRY_CAST en cada comparación y mejora la legibilidad.

   5.1. Verificación de resultado_antes_impuestos
        Fórmula esperada: margen_financiero_neto - total_gastos_operativos
                          - otras_perdidas_operacionales - otros_gastos
============================================================================================================================ */
with Verificar_Resultado As(
	select 
		fecha_mes,
		CAST(resultado_antes_impuestos AS decimal(18,2)) AS resultado_antes_impuestos,
		CAST(margen_financiero_neto AS decimal(18,2)) AS margen_financiero_neto,
		CAST(total_gastos_operativos AS decimal(18,2)) AS total_gastos_operativos,
		CAST(otras_perdidas_operacionales AS decimal(18,2)) AS otras_perdidas_operacionales,
		CAST(otros_gastos AS decimal(18,2)) As otros_gastos
	from T2_estado_resultados_RAW
)
select
	fecha_mes,
	-- resultado_antes_impuestos: Verificacion 
	resultado_antes_impuestos AS resultado_antes_impuestos_reportado,
	margen_financiero_neto - total_gastos_operativos - otras_perdidas_operacionales - otros_gastos AS resultado_antes_impuestos_recalculado,
	(resultado_antes_impuestos - (margen_financiero_neto - total_gastos_operativos - otras_perdidas_operacionales - otros_gastos)) AS Diferencia
from Verificar_Resultado
WHERE
	(resultado_antes_impuestos - (margen_financiero_neto - total_gastos_operativos - otras_perdidas_operacionales - otros_gastos)) > 0
-- Accion a tomar: Ninguna, calculo correcto

/* ============================================================================================================================
   5.2. Auditoría de participación_trabajadores (15%) y utilidad_neta
        Norma ecuatoriana: las utilidades se distribuyen así:
          - 15% para trabajadores (solo si resultado_antes_impuestos > 0)
          - 25% de impuesto a la renta sobre la base imponible
          - utilidad_neta = resultado - participación - impuesto
        Se detectaron errores en 2025-01-31 y 2025-04-30.
        Decisión: recalcular utilidad_neta en el dashboard (script 6).
============================================================================================================================ */

-- Auditoría de participacion_trabajadores_15pct
WITH T2_Auditoria_Participacion AS (
    SELECT 
        fecha_mes,
        -- Resultado antes de impuestos (Base para el cálculo)
        CAST(resultado_antes_impuestos AS DECIMAL(18,2)) AS resultado_antes_impuestos_RAW,
        -- Valor que la IA puso en el reporte
        CAST(participacion_trabajadores_15pct AS DECIMAL(18,2)) AS participacion_reportada,
        -- Cálculo teórico (Solo si la utilidad es positiva)
        CASE 
            WHEN CAST(resultado_antes_impuestos AS DECIMAL(18,2)) > 0 
            THEN (CAST(resultado_antes_impuestos AS DECIMAL(18,2)) * 0.15)
            ELSE 0 
        END AS participacion_trabajadores_15pct_recalculada
    FROM T2_estado_resultados_RAW
)
SELECT 
    fecha_mes,
    resultado_antes_impuestos_RAW,
    participacion_reportada,
    participacion_trabajadores_15pct_recalculada,
    -- Diferencia de auditoría
    (participacion_reportada - participacion_trabajadores_15pct_recalculada) AS diferencia_participacion,
    -- Validación de regla
    CASE 
        WHEN ABS(participacion_reportada - participacion_trabajadores_15pct_recalculada) < 0.05 THEN 'CORRECTO'
        ELSE 'ERROR_CALCULO'
    END AS estatus_validacion
FROM T2_Auditoria_Participacion
ORDER BY fecha_mes;

--Limpieza: recalcular participacion_trabajadores_15pct_recalculada antes de calcular utilidad neta

-------------------------------------------------------------------------------------------------------------------------
-- Verificación de utilidad_neta reportada
-- Fórmula: resultado_antes_impuestos - participacion_trabajadores - impuesto_renta
WITH T2_Calculo AS (
    SELECT 
        fecha_mes,
        CAST(resultado_antes_impuestos AS DECIMAL(18,2)) AS resultado_antes_impuestos,
        CAST(participacion_trabajadores_15pct AS DECIMAL(18,2)) AS participacion_trabajadores_15pct,
        CAST(impuesto_renta_25pct AS DECIMAL(18,2)) AS impuesto_renta_25pct,
        CAST(utilidad_neta AS DECIMAL(18,2)) AS utilidad_neta_reportada
    FROM T2_estado_resultados_RAW
)
SELECT 
    fecha_mes,
    resultado_antes_impuestos AS resultado_antes_impuestos,
    participacion_trabajadores_15pct AS participacion_trabajadores_15pct,
    impuesto_renta_25pct AS impuesto_renta_25pct,
    utilidad_neta_reportada AS utilidad_neta_reportada,
    (resultado_antes_impuestos - participacion_trabajadores_15pct - impuesto_renta_25pct) AS Utilidad_neta_recalculo,
    ABS(utilidad_neta_reportada - (resultado_antes_impuestos - participacion_trabajadores_15pct - impuesto_renta_25pct)) AS Diferencia
FROM T2_Calculo
WHERE ABS(utilidad_neta_reportada - (resultado_antes_impuestos - participacion_trabajadores_15pct - impuesto_renta_25pct)) > 1; -- Solo muestra si hay error > 1 USD

-- Accion tomada: Para el dashboard se recalculara manualmente la utilidad neta ya que se observaron errores en 2025-01-31 y 2025-04-30

/* ============================================================================================================================
   5.2. Validación de sumatoria de INGRESOS (campos 4 al 11)
        Confirmar que total_ingresos = suma de sus 7 componentes individuales.
        Resultado esperado: 0 filas (ninguna diferencia > 1 USD).
============================================================================================================================ */
WITH T2_Ingresos_Detalle AS (
    SELECT 
        fecha_mes,
        -- Componentes individuales (Campos 4 al 10)
        CAST(ing_intereses_cartera AS DECIMAL(18,2)) AS ing_intereses_cartera,
        CAST(ing_intereses_inversiones AS DECIMAL(18,2)) AS ing_intereses_inversiones,
        CAST(ing_intereses_interbancarios AS DECIMAL(18,2)) AS ing_intereses_interbancarios,
        CAST(ing_comisiones_ganadas AS DECIMAL(18,2)) AS ing_comisiones_ganadas,
        CAST(ing_utilidades_financieras AS DECIMAL(18,2)) AS ing_utilidades_financieras,
        CAST(ing_servicios AS DECIMAL(18,2)) AS ing_servicios,
        CAST(otros_ingresos AS DECIMAL(18,2)) AS otros_ingresos,
        -- Total reportado (Campo 11)
        CAST(total_ingresos AS DECIMAL(18,2)) AS total_ingresos_reportado
    FROM T2_estado_resultados_RAW
)
Select
	-- Calculo presentado
	total_ingresos_reportado  as  total_ingresos_presentado,
	-- Recalculo
	ing_intereses_cartera + ing_intereses_inversiones + ing_intereses_interbancarios   + ing_comisiones_ganadas   +
	ing_utilidades_financieras   + ing_servicios + otros_ingresos    AS  Total_ingresos_recalculado,
	-- Diferencia
	ABS(total_ingresos_reportado - (ing_intereses_cartera + ing_intereses_inversiones + ing_intereses_interbancarios + 
	ing_comisiones_ganadas + ing_utilidades_financieras + ing_servicios + otros_ingresos)) AS Diferencia
from T2_Ingresos_Detalle
where ABS(total_ingresos_reportado  - (ing_intereses_cartera + ing_intereses_inversiones + ing_intereses_interbancarios   + ing_comisiones_ganadas   +
	ing_utilidades_financieras   + ing_servicios + otros_ingresos)) > 1

-- Verificación puntual para meses específicos
select fecha_mes, utilidad_neta from T2_estado_resultados_RAW where fecha_mes = '2024-08-31' or fecha_mes = '2025-05-31'
-- Sin errores en los calculos

/* ============================================================================================================================
   5.3. Validación de sumatoria de GASTOS FINANCIEROS (campos 12 al 14)
        Fórmula: total_gastos_financieros = depósitos + obligaciones + comisiones
        Resultado esperado: 0 filas.
============================================================================================================================ */
WITH T2_Gastos_Fin_Validacion AS (
    SELECT 
        fecha_mes,
        CAST(gasto_intereses_depositos AS DECIMAL(18,2)) AS gasto_intereses_depositos, -- 12
        CAST(gasto_intereses_obligaciones AS DECIMAL(18,2)) AS gasto_intereses_obligaciones, -- 13
        CAST(gasto_comisiones_pagadas AS DECIMAL(18,2)) AS gasto_comisiones_pagadas, -- 14
        CAST(total_gastos_financieros AS DECIMAL(18,2)) AS total_gastos_financieros_reportado --15
    FROM T2_estado_resultados_RAW
)
SELECT
    fecha_mes,
    total_gastos_financieros_reportado AS total_gastos_financieros_presentado,
    (gasto_intereses_depositos + gasto_intereses_obligaciones + gasto_comisiones_pagadas) AS total_gastos_financieros_recalculado,
    ABS(total_gastos_financieros_reportado - (gasto_intereses_depositos + gasto_intereses_obligaciones + gasto_comisiones_pagadas)) AS Diferencia
FROM T2_Gastos_Fin_Validacion
WHERE ABS(total_gastos_financieros_reportado - (gasto_intereses_depositos + gasto_intereses_obligaciones + gasto_comisiones_pagadas)) > 1;

-- Accion: Ninguna, sin errores

/* ============================================================================================================================
   5.4. Validación de MÁRGENES financieros
        Margen Bruto = total_ingresos - total_gastos_financieros
        Margen Neto  = Margen Bruto - gasto_provisiones_periodo
        Nota: gasto_provisiones puede ser negativo en algunos meses; se tomará
              el valor absoluto en el script de limpieza.
============================================================================================================================ */
WITH T2_Márgenes_Limpia AS (
    SELECT 
        fecha_mes,
        CAST(total_ingresos AS DECIMAL(18,2)) AS total_ingresos,
        CAST(total_gastos_financieros AS DECIMAL(18,2)) AS total_gastos_financieros,
        CAST(margen_financiero_bruto AS DECIMAL(18,2)) AS margen_financiero_bruto,
        CAST(gasto_provisiones_periodo AS DECIMAL(18,2)) AS gasto_provisiones_periodo,
        CAST(margen_financiero_neto AS DECIMAL(18,2)) AS margen_financiero_neto
    FROM T2_estado_resultados_RAW
)
SELECT 
	-- Validación 1: Margen Bruto (Ingresos - Gastos Financieros)
    fecha_mes,
    total_ingresos AS [Ingresos],
    total_gastos_financieros AS [Gastos_Fin],
    margen_financiero_bruto AS [Bruto_Reportado],
    (total_ingresos - total_gastos_financieros) AS [Bruto_Calculado],
	ABS(margen_financiero_bruto - (total_ingresos - total_gastos_financieros))AS [Diferencia_Bruto],
	-- Validación 2: Margen Neto (Margen Bruto - Provisiones)
    gasto_provisiones_periodo AS [Valor_Provision],
    margen_financiero_neto AS [Neto_Reportado],
    (margen_financiero_bruto - gasto_provisiones_periodo) AS [Neto_Calculado],
    ABS(margen_financiero_neto - (margen_financiero_bruto - gasto_provisiones_periodo)) AS Diferencia_Neto
FROM T2_Márgenes_Limpia
WHERE 
    ABS(margen_financiero_bruto - (total_ingresos - total_gastos_financieros)) > 1 OR 
    ABS(margen_financiero_neto - (margen_financiero_bruto - gasto_provisiones_periodo)) > 1;

-- IMPORTANTE:
-- gasto_provisiones_periodo: Transformar todo a positivo (asumir que siempre es un egreso).
-- margen_financiero_bruto: Recalcularlo desde cero (ingresos - gastos_financieros).

/* ============================================================================================================================
   5.5. Validación de GASTOS OPERATIVOS (campos 19 al 27)
        Fórmula: total_gastos_operativos = personal + honorarios + instalaciones
                 + publicidad + sistemas + depreciación + amortización + otros
        Resultado esperado: 0 filas.
============================================================================================================================ */
WITH T2_Gastos_Operativos AS (
    SELECT 
        fecha_mes,
        CAST(gasto_personal AS DECIMAL(18,2)) AS gasto_personal,                       -- 19
        CAST(gasto_honorarios AS DECIMAL(18,2)) AS gasto_honorarios,                   -- 20
        CAST(gasto_instalaciones AS DECIMAL(18,2)) AS gasto_instalaciones,             -- 21
        CAST(gasto_publicidad AS DECIMAL(18,2)) AS gasto_publicidad,                   -- 22
        CAST(gasto_sistemas_tecnologia AS DECIMAL(18,2)) AS gasto_sistemas_tecnologia, -- 23
        CAST(gasto_depreciacion AS DECIMAL(18,2)) AS gasto_depreciacion,               -- 24
        CAST(gasto_amortizacion AS DECIMAL(18,2)) AS gasto_amortizacion,               -- 25
        CAST(otros_gastos_operativos AS DECIMAL(18,2)) AS otros_gastos_operativos,     -- 26
        CAST(total_gastos_operativos AS DECIMAL(18,2)) AS total_gastos_operativos_rep  -- 27
    FROM T2_estado_resultados_RAW
)
SELECT 
    fecha_mes,
    total_gastos_operativos_rep AS [Reportado_en_Tabla],
    (gasto_personal + gasto_honorarios + gasto_instalaciones + gasto_publicidad + 
     gasto_sistemas_tecnologia + gasto_depreciacion + gasto_amortizacion + otros_gastos_operativos) AS Recalculo_Gastos_Operativos,
    ABS(total_gastos_operativos_rep - (gasto_personal + gasto_honorarios + gasto_instalaciones + 
     gasto_publicidad + gasto_sistemas_tecnologia + gasto_depreciacion + gasto_amortizacion + otros_gastos_operativos)) AS Diferencia
FROM T2_Gastos_Operativos
WHERE ABS(total_gastos_operativos_rep - (gasto_personal + gasto_honorarios + gasto_instalaciones + 
     gasto_publicidad + gasto_sistemas_tecnologia + gasto_depreciacion + gasto_amortizacion + otros_gastos_operativos)) > 1;

-- Accion: Ninguna, sin errores

/* ============================================================================================================================
   5.5. Validación cruzada de CASTIGOS (T2 vs. T1)
        Objetivo : El gasto contable por castigos en T2 debe coincidir con la
                   suma de monto_castigado en T1 para el mismo mes.
        Cruce    : T2.castigos_periodo ↔ SUM(T1.monto_castigado) por fecha_corte.
        Resultado esperado: 0 filas.
============================================================================================================================ */
WITH T1_Agrupada AS (
    SELECT 
        fecha_corte,
        SUM(CAST(monto_castigado AS DECIMAL(18,2))) AS total_castigos_T1
    FROM T1_creditos_RAW
    GROUP BY fecha_corte
),
T2_Castigos AS (
    SELECT 
        fecha_mes,
        CAST(castigos_periodo AS DECIMAL(18,2)) AS castigos_periodo_T2
    FROM T2_estado_resultados_RAW
)
SELECT 
    T2.fecha_mes,
    T2.castigos_periodo_T2 AS [Castigos_Reportados_T2],
    ISNULL(T1.total_castigos_T1, 0) AS [Suma_Monto_Castigado_T1],
    ABS(T2.castigos_periodo_T2 - ISNULL(T1.total_castigos_T1, 0)) AS Diferencia
FROM T2_Castigos T2
LEFT JOIN T1_Agrupada T1 ON T2.fecha_mes = T1.fecha_corte
WHERE ABS(T2.castigos_periodo_T2 - ISNULL(T1.total_castigos_T1, 0)) > 1;

-- Accion: Ninguna, sin errores

/* ============================================================================================================================
   5.6. Validación de RECUPERACIONES
        Regla de negocio: las recuperaciones deben estar entre el 8% y 35%
        de los castigos del período. Fuera de ese rango es inusual y debe revisarse.
============================================================================================================================ */
WITH T2_Castigos_Recup_Limpia AS (
    SELECT 
        fecha_mes,
        CAST(castigos_periodo AS DECIMAL(18,2)) AS castigos_periodo,
        CAST(recuperaciones_periodo AS DECIMAL(18,2)) AS recuperaciones_periodo
    FROM T2_estado_resultados_RAW
    WHERE TRY_CAST(castigos_periodo AS DECIMAL(18,2)) > 0 -- Solo se evaluan meses con actividad de castigos
)
SELECT 
    fecha_mes,
    castigos_periodo,
    recuperaciones_periodo,
    (recuperaciones_periodo / castigos_periodo) * 100 AS Porcentaje_Recuperacion,
    CASE 
        WHEN (recuperaciones_periodo / castigos_periodo) < 0.08 THEN 'BAJO (Menor al 8%)'
        WHEN (recuperaciones_periodo / castigos_periodo) > 0.35 THEN 'ALTO (Mayor al 35%)'
        ELSE 'DENTRO DE RANGO'
    END AS Estatus_Recuperacion
FROM T2_Castigos_Recup_Limpia;
-- Limpieza: ninguna, todo correcto
