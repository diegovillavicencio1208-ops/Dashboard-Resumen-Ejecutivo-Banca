/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 4. EDA — T4_pagos_cuotas_RAW
   DESCRIPCIÓN   : Análisis Exploratorio de Datos sobre el historial de pagos por cuota.
                   Evalúa la calidad del dato de comportamiento de pago: integridad de
                   campos numéricos y fechas, completitud (nulos en canal_pago y
                   fecha_pago_real), unicidad, negativos en días de retraso y montos,
                   y consistencia de las reglas de negocio por cuota.
   TABLA FUENTE  : T4_pagos_cuotas_RAW
   TABLAS CRUZADAS: T1_creditos_RAW (validación cuota_pactada)
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : Tablas RAW cargadas y scripts anteriores ejecutados.
============================================================================================================================ */

Use RiesgoCrediticioProyecto2;
Go
/*============================================================================================================
    1. EXPLORACIÓN DE INTEGRIDAD TÉCNICA - T4_pagos_cuotas_RAW
    Objetivo: Detectar basura en campos 
============================================================================================================*/

SELECT 
    '1. INTEGRIDAD (BASURA)' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T4_pagos_cuotas_RAW
CROSS APPLY (VALUES 
    -- CUOTA Y MONTOS
    ('numero_cuota', numero_cuota),
    ('cuota_pactada', cuota_pactada),
    ('monto_capital_cuota', monto_capital_cuota),
    ('monto_interes_cuota', monto_interes_cuota),
    ('monto_pagado', monto_pagado),
    ('saldo_pendiente_cuota', saldo_pendiente_cuota),

    -- COMPORTAMIENTO
    ('dias_retraso_pago', dias_retraso_pago),
    ('es_pago_anticipado', es_pago_anticipado)
) AS v(campo, valor_sucio)
WHERE TRY_CAST(v.valor_sucio AS DECIMAL(18,4)) IS NULL 
  AND v.valor_sucio IS NOT NULL 
  AND v.valor_sucio <> ''
GROUP BY v.campo, v.valor_sucio;

-- limpieza, ninguna sin basura en las variables numericas
-----------------------------------------------------------------------------------------------------------------------
-- Verificacion basura fechas
SELECT 
    '1. INTEGRIDAD FECHAS (BASURA)' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T4_pagos_cuotas_RAW
CROSS APPLY (VALUES 
    -- VARIABLES DE FECHA
    ('fecha_vencimiento_cuota', fecha_vencimiento_cuota),
    ('fecha_pago_real',        fecha_pago_real)
) AS v(campo, valor_sucio)
-- Buscamos valores que NO se pueden convertir a fecha
WHERE TRY_CAST(v.valor_sucio AS DATE) IS NULL 
  AND v.valor_sucio IS NOT NULL 
  AND v.valor_sucio <> ''
  AND v.valor_sucio NOT IN ('NULL', 'NaN', 'N/A') -- Excluimos nulos explícitos si los hay
GROUP BY v.campo, v.valor_sucio;

-- Limpieza, ninguna sin basura en las variables fecha

/* ============================================================================================================
    2. COMPLETITUD (Nulos y Vacíos)
============================================================================================================ */
 -- 2.1. Variables numericas
SELECT 
    '2.1 NULOS - Variables Numericas' AS Paso,
    v.campo,
    SUM(v.es_nulo) AS total_nulos,
    COUNT(*) AS registros_totales,
    CAST(SUM(v.es_nulo) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_nulos
FROM T4_pagos_cuotas_RAW
CROSS APPLY (VALUES
    -- Identificadores y Clasificación
    ('id_pago',                CASE WHEN id_pago IS NULL OR id_pago = '' THEN 1 ELSE 0 END),
    ('id_credito',             CASE WHEN id_credito IS NULL OR id_credito = '' THEN 1 ELSE 0 END),
    ('segmento',               CASE WHEN segmento IS NULL OR segmento = '' THEN 1 ELSE 0 END),
    ('zona_geografica',        CASE WHEN zona_geografica IS NULL OR zona_geografica = '' THEN 1 ELSE 0 END),
    
    -- Fechas Críticas (Mora)
    ('fecha_vencimiento_cuota', CASE WHEN fecha_vencimiento_cuota IS NULL OR fecha_vencimiento_cuota = '' THEN 1 ELSE 0 END),
    ('fecha_pago_real',        CASE WHEN fecha_pago_real IS NULL OR fecha_pago_real = '' THEN 1 ELSE 0 END),
    
    -- Montos de la Cuota
    ('cuota_pactada',          CASE WHEN cuota_pactada IS NULL OR cuota_pactada = '' THEN 1 ELSE 0 END),
    ('monto_capital_cuota',    CASE WHEN monto_capital_cuota IS NULL OR monto_capital_cuota = '' THEN 1 ELSE 0 END),
    ('monto_interes_cuota',    CASE WHEN monto_interes_cuota IS NULL OR monto_interes_cuota = '' THEN 1 ELSE 0 END),
    ('monto_pagado',           CASE WHEN monto_pagado IS NULL OR monto_pagado = '' THEN 1 ELSE 0 END),
    ('saldo_pendiente_cuota',  CASE WHEN saldo_pendiente_cuota IS NULL OR saldo_pendiente_cuota = '' THEN 1 ELSE 0 END),
    
    -- Comportamiento y Calidad
    ('estado_pago',            CASE WHEN estado_pago IS NULL OR estado_pago = '' THEN 1 ELSE 0 END),
    ('canal_pago',             CASE WHEN canal_pago IS NULL OR canal_pago = '' THEN 1 ELSE 0 END),
    ('calificacion_al_pago',   CASE WHEN calificacion_al_pago IS NULL OR calificacion_al_pago = '' THEN 1 ELSE 0 END)
) AS v(campo, es_nulo)
GROUP BY v.campo
ORDER BY pct_nulos DESC;
-- Limpieza: VERIFICAR: Nulos en canal_pago - 2288 y fecha_pago_real - 2277

-----------------------------------------------------------------------------------------------------------------------
-- Revision nulos fecha_pago_real - 2277
SELECT 
    id_pago,
    id_credito,
    segmento,
    fecha_vencimiento_cuota,
    fecha_pago_real, 
    estado_pago,
    monto_pagado
FROM T4_pagos_cuotas_RAW
WHERE (fecha_pago_real IS NULL OR fecha_pago_real = '')
ORDER BY fecha_vencimiento_cuota ASC;
-- Limpieza - fecha_pago_real: NINGUNA 
-- NOTA: Se mantienen NULL en fecha_pago_real (7.43% de la T4) por consistencia de negocio.
-- Representan 'No Pago' real con mora de hasta 730 días (Cartera en Pérdida).
-- El nulo evita inflar flujos de caja y permite recalcular la mora técnica exacta.

------------------------------------------------------------------------------------------------------------------------
-- Revision NULOS canal_pago - 2288

SELECT TOP 20
    id_pago,
    segmento,
    fecha_vencimiento_cuota,
    fecha_pago_real,
    monto_pagado,
    estado_pago,
    canal_pago 
FROM T4_pagos_cuotas_RAW
WHERE canal_pago IS NULL OR canal_pago = ''
ORDER BY cast(monto_pagado as decimal (18,2)) DESC; 

-- LIMPIEZA - canal_pago : Estandarizacion de nulos donde monto_pagado > 0 = 'No Identificado' y monto_pagado < 0 = 'No Aplica (Sin Pago)'
-- ESTANDARIZACIÓN DE CANAL: Se clasifica como 'No Identificado' si hay pago sin origen (falla técnica) 
-- y como 'No Aplica (Sin Pago)' si el monto es 0 (mora/evasión). 
-- Esto separa errores de integridad de datos de la ausencia real de transacciones.

/* ============================================================================================================
    3. UNICIDAD (Duplicados) - T4_HISTORIAL_PAGOS_RAW
    Objetivo: Asegurar que cada identificador de pago (PK) sea único.
============================================================================================================ */

SELECT 
    '3. DUPLICADOS' AS Paso,
    id_pago, 
    COUNT(*) AS registros_encontrados,
    -- Concatenamos id_credito y numero_cuota para ver si el error es de duplicidad de cuota
    MAX(id_credito) AS id_credito_reportado,
    MAX(numero_cuota) AS numero_cuota_reportada
FROM T4_pagos_cuotas_RAW
GROUP BY id_pago
HAVING COUNT(*) > 1;
-- Limpieza, ninguna 0 duplicados
/* ============================================================================================================
    4. EXPLORACIÓN DE VALORES NEGATIVOS - T4_HISTORIAL_PAGOS_RAW
    Objetivo: Detectar montos o días menores a cero en todas las variables numéricas de la T4.
============================================================================================================ */
SELECT 
    '4. NEGATIVOS' AS Paso,
    v.campo,
    COUNT(*) AS frecuencia
FROM T4_pagos_cuotas_RAW
CROSS APPLY (VALUES 
    -- MONTOS DE LA CUOTA (Valores Monetarios)
    ('cuota_pactada',          TRY_CAST(cuota_pactada AS DECIMAL(18,4))),
    ('monto_capital_cuota',    TRY_CAST(monto_capital_cuota AS DECIMAL(18,4))),
    ('monto_interes_cuota',    TRY_CAST(monto_interes_cuota AS DECIMAL(18,4))),
    ('monto_pagado',           TRY_CAST(monto_pagado AS DECIMAL(18,4))),
    ('saldo_pendiente_cuota',  TRY_CAST(saldo_pendiente_cuota AS DECIMAL(18,4))),

    -- MÉTRICAS TEMPORALES Y CONTEOS (Enteros)
    ('numero_cuota',           TRY_CAST(numero_cuota AS INT)),
    ('dias_retraso_pago',      TRY_CAST(dias_retraso_pago AS INT)),
    
    -- VARIABLES DE COMPORTAMIENTO (Binarias/Numéricas)
    ('es_pago_anticipado',     TRY_CAST(es_pago_anticipado AS INT))
) AS v(campo, valor_encontrado)
WHERE v.valor_encontrado < 0 
GROUP BY v.campo;

-- Limpieza: dias_retraso_pago 7 negativos, monto_pagado 7 negativos.
-----------------------------------------------------------------------------------------------------------------------
-- Revision de filas con negativos dias_retraso_pago
select
	fecha_vencimiento_cuota,
	fecha_pago_real,
	dias_retraso_pago,
	estado_pago,
	monto_pagado
from T4_pagos_cuotas_RAW
where dias_retraso_pago like '%-%' 


-- Limpieza - dias_retraso_pago: convertir valores negativos a 0

-------------------------------------------------------------------------------------------------------------------------
-- Revision de filas con negativos monto_pagado
select
	id_pago,
	dias_retraso_pago,
	cuota_pactada,
	estado_pago,
	monto_pagado
from T4_pagos_cuotas_RAW
where monto_pagado like '%-%'

-- Limpieza - monto_pagado: Aplicar valor absoluto a monto_pagado
-- Error de sistema, los 7 valores son validos ya que al revisar el resto de parametros se confirma que la informacion es corerente

/* ============================================================================================================
    5. CONSISTENCIA DE VALORES NUMÉRICOS (Reglas de Negocio - T4)
    Objetivo: Validar que el Historial de Pagos por Cuota coincidan con la suma de sus componentes.
============================================================================================================ */

-- Verificacion numero_cuota
-- Regla de negocio, 1 - 36 maximo

with verificar_numero_cuota as (
	select 
		CAST(numero_cuota as INT) as numero_cuota
	from T4_pagos_cuotas_RAW
)
select
	numero_cuota,
	COUNT(*) as frecuencia
from verificar_numero_cuota
group by numero_cuota
order by numero_cuota desc
-- Limpieza - numero_cuota : Niguna, logica correcta

-----------------------------------------------------------------------------------------------------------------

-- Verificacion cuota_pactada
with t4_cuota as(
select
	distinct(id_credito) as id_credito,
	cast(cuota_pactada as decimal(18,2)) as cuota_pactada
from T4_pagos_cuotas_RAW
),
t1_cuota as(
select
	distinct(id_credito) as id_credito,
	cast(cuota_mensual as decimal(18,2)) as cuota_mensual
from T1_creditos_RAW
)
select
	t4.id_credito,
	t1.cuota_mensual,
	t4.cuota_pactada
from t4_cuota as t4
inner join t1_cuota as t1 on t4.id_credito = t1.id_credito
where t1.cuota_mensual <> t4.cuota_pactada

--- limpieza - T4_pagos_cuotas_RAW : imputdar cuotas_pactada erroneas desde t1  

-----------------------------------------------------------------------------------------------------------------
-- Verificacion monto_interes_cuota (cuota_pactada − monto_capital_cuota)
with Ver_monto_interes_cuota as (
	select
		cast(monto_interes_cuota as decimal(18,2)) as monto_interes_cuota_reportado,
		cast(cuota_pactada as decimal(18,2)) as cuota_pactada,
		cast(monto_capital_cuota as decimal(18,2)) as monto_capital_cuota
	from T4_pagos_cuotas_RAW
)
select
	monto_interes_cuota_reportado,
	(cuota_pactada-monto_capital_cuota) as monto_interes_cuota_recalculada,
	monto_interes_cuota_reportado - (cuota_pactada-monto_capital_cuota) as diferencia
from Ver_monto_interes_cuota
where monto_interes_cuota_reportado <> (cuota_pactada-monto_capital_cuota)
;

--Limpieza - monto_interes_cuota : Ninguna, valores rortados correctos
-----------------------------------------------------------------------------------------------------------------
-- monto_pagado - Regla: No puede ser Negativo
with Ver_monto_pagado as (
	select
		cast(monto_pagado as decimal(18,2)) as monto_pagado,
		cast(cuota_pactada as decimal(18,2)) as cuota_pactada,
		estado_pago,
		dias_retraso_pago
	from T4_pagos_cuotas_RAW
)
select
	*
from Ver_monto_pagado
where 
	monto_pagado < 0 AND
	ABS(monto_pagado) <> cuota_pactada;

-- Limpieza -  monto_pagado : Usar valor abs sobre monto_pagado al ser los valores identicos a la cuota pactada 
-- Verificar monto_pagado : mayor a cuota pagada por tema de interses por atraso
	
--------------------------------------------------------------------------------------------------------------------------

-- verificacion saldo_pendiente_cuota ( MAX(cuota_pactada - monto_pagado, 0) )
with Ver_saldo_pendiente_cuota as(
	select
		id_credito,
		cast(saldo_pendiente_cuota as decimal(18,2)) as saldo_pendiente_cuota_reportada,
		case
			when (cast(cuota_pactada as decimal(18,2)) - cast(monto_pagado as decimal(18,2))) < 0 then 0
			else (cast(cuota_pactada as decimal(18,2)) - cast(monto_pagado as decimal(18,2)))
		END as saldo_pendiente_cuota_recalculada,
		cuota_pactada
	from T4_pagos_cuotas_RAW
)
select
	id_credito,
	saldo_pendiente_cuota_reportada,
	cuota_pactada,
	saldo_pendiente_cuota_recalculada,
	saldo_pendiente_cuota_reportada - saldo_pendiente_cuota_recalculada as diferencia 
from Ver_saldo_pendiente_cuota
where 
	saldo_pendiente_cuota_reportada <> saldo_pendiente_cuota_recalculada;

-- Limpieza - saldo_pendiente_cuota: Ninguna
--------------------------------------------------------------------------------------------------------------------------
-- Verificacion - estado_pago: Pago Puntual / Pago Tardío / Pago Parcial / No Pago

select
	estado_pago,
	COUNT(*) as frecuencia
from T4_pagos_cuotas_RAW
group by estado_pago

-- Limpieza - estado_pago: Ninguna 

--------------------------------------------------------------------------------------------------------------------------
-- Verificar es_pago_anticipado : 0 = No · 1 = Sí
select 
	es_pago_anticipado,
	COUNT(*) as frecuencia
from T4_pagos_cuotas_RAW
group by es_pago_anticipado;

-- Limpieza - es_pago_anticipado : los dias negatos presentes en dias_retraso_pago no fueron identificados como pago anticipado
--------------------------------------------------------------------------------------------------------------------------
-- Verificar - calificacion_al_pago : Normal / Potencial / Deficiente / Dudoso / Pérdida
select
	calificacion_al_pago,
	COUNT(*) as frecuencia
from T4_pagos_cuotas_RAW
group by calificacion_al_pago;

-- Limpieza - calificacion_al_pago : Ninguna, catalogo correcto

--------------------------------------------------------------------------------------------------------------------------
-- Verificacion - segmento : Microcrédito / Consumo / Vivienda / Comercial PYME / Comercial Corporativo

select
	segmento,
	count(*)
from T4_pagos_cuotas_RAW
group by segmento;

-- Limpieza - segmento : Ninguno 
--------------------------------------------------------------------------------------------------------------------------
-- Verificacion - oficial_credito : Consistencia a los nombres
select
	oficial_credito,
	count(*)
from T4_pagos_cuotas_RAW
group by oficial_credito
-- Limpieza - oficial_credito : Ninguno 
--------------------------------------------------------------------------------------------------------------------------
-- Verificacion - zona_geografica : Guayaquil / Quito / Cuenca / Ambato / Manta / Portoviejo / Machala / Loja
select
	zona_geografica,
	count(*)
from T4_pagos_cuotas_RAW
group by zona_geografica
-- Limpieza - zona_geografica : Ninguno 
-------------------------------------------------------------------------------------------------------------------------- 

