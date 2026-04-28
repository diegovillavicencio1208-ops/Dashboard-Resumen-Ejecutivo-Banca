/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 5. EDA — T5_clientes_RAW
   DESCRIPCIÓN   : Análisis Exploratorio de Datos sobre el snapshot consolidado de clientes.
                   Valida la calidad del perfil crediticio y financiero de cada cliente:
                   integridad de campos numéricos y fechas, completitud, unicidad,
                   negativos, y consistencia de variables derivadas (tasa_mora, provision,
                   el_total, tasa_nominal_promedio, endeudamiento). Se realizan cruces
                   exhaustivos con T1 para auditar los valores consolidados reportados.
   TABLA FUENTE  : T5_clientes_RAW
   TABLAS CRUZADAS: T1_creditos_RAW (validación de métricas consolidadas por cliente)
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : Tablas RAW cargadas y scripts anteriores ejecutados.
============================================================================================================================ */

Use RiesgoCrediticioProyecto2;
Go

/* ============================================================================================================
	E.  EDA - T5_clientes_RAW 
============================================================================================================
    1. EXPLORACIÓN DE INTEGRIDAD TÉCNICA - T4_pagos_cuotas_RAW
    Objetivo: Detectar basura en campos 
============================================================================================================*/



SELECT 
    '1. INTEGRIDAD (BASURA)' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T5_clientes_RAW
CROSS APPLY (VALUES 
    -- IDENTIFICACIÓN
    ('edad_empresa_anos', edad_empresa_anos),
    ('antiguedad_cliente_meses', antiguedad_cliente_meses),

	-- EXPOSICIÓN FINANCIERA CONSOLIDADA
    ('num_creditos_activos', num_creditos_activos),
    ('num_creditos_sistema', num_creditos_sistema),
    ('num_refinanciaciones', num_refinanciaciones),
	('score_crediticio', score_crediticio),
	('max_dias_atraso', max_dias_atraso),
	('tiene_credito_en_mora', tiene_credito_en_mora),

    -- EXPOSICIÓN FINANCIERA CONSOLIDADA
    ('saldo_total_deuda', saldo_total_deuda),
	('saldo_mora_total', saldo_mora_total),
	('tasa_mora_cliente_pct', tasa_mora_cliente_pct),
	('provision_total_cliente', provision_total_cliente),
	('el_total_cliente', el_total_cliente),
	('tasa_nominal_promedio', tasa_nominal_promedio),
	('ingresos_anuales', ingresos_anuales),
	('tasa_endeudamiento_sistema_pct', tasa_endeudamiento_sistema_pct),
	('es_cliente_preferente', es_cliente_preferente)
) AS v(campo, valor_sucio)
WHERE TRY_CAST(v.valor_sucio AS DECIMAL(18,4)) IS NULL 
  AND v.valor_sucio IS NOT NULL 
  AND v.valor_sucio <> ''
GROUP BY v.campo, v.valor_sucio;

-- Limpieza - Basura (letas en valores numerico) : Ninguna

----------------------------------------------------------------------------------------------------------------------------
-- Basura en fechas
select fecha_primer_credito, fecha_ultimo_corte
from T5_clientes_RAW
where TRY_CASt(fecha_primer_credito as date) is null or
	TRY_CASt(fecha_ultimo_corte as date) is null 
-- Limpieza  - BASURA (Fechas) : Ninguna
/* ============================================================================================================
    2. COMPLETITUD (Nulos y Vacíos) 
============================================================================================================ */
 -- 2.1. Variables numericas
SELECT 
    '2 NULOS' AS Paso,
    v.campo,
    SUM(v.es_nulo) AS total_nulos,
    COUNT(*) AS registros_totales,
    CAST(SUM(v.es_nulo) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_nulos
FROM T5_clientes_RAW  
CROSS APPLY (VALUES
    -- Identificadores y Clasificación
	('fecha_ultimo_corte',			  CASE WHEN fecha_ultimo_corte IS NULL OR fecha_ultimo_corte = '' THEN 1 ELSE 0 END),
	('fecha_primer_credito',          CASE WHEN fecha_primer_credito IS NULL OR fecha_primer_credito = '' THEN 1 ELSE 0 END),
    ('id_cliente',                    CASE WHEN id_cliente IS NULL OR id_cliente = '' THEN 1 ELSE 0 END),
    ('tipo_persona',                  CASE WHEN tipo_persona IS NULL OR tipo_persona = '' THEN 1 ELSE 0 END),
    ('segmento',                      CASE WHEN segmento IS NULL OR segmento = '' THEN 1 ELSE 0 END),
    ('sector_economico',              CASE WHEN sector_economico IS NULL OR sector_economico = '' THEN 1 ELSE 0 END),
    ('zona_geografica',               CASE WHEN zona_geografica IS NULL OR zona_geografica = '' THEN 1 ELSE 0 END),
    ('oficial_credito',               CASE WHEN oficial_credito IS NULL OR oficial_credito = '' THEN 1 ELSE 0 END),
    ('edad_empresa_anos',             CASE WHEN edad_empresa_anos IS NULL OR edad_empresa_anos = '' THEN 1 ELSE 0 END),
    ('antiguedad_cliente_meses',      CASE WHEN antiguedad_cliente_meses IS NULL OR antiguedad_cliente_meses = '' THEN 1 ELSE 0 END),

    -- Exposición Crediticia
    ('num_creditos_activos',          CASE WHEN num_creditos_activos IS NULL OR num_creditos_activos = '' THEN 1 ELSE 0 END),
    ('num_creditos_sistema',          CASE WHEN num_creditos_sistema IS NULL OR num_creditos_sistema = '' THEN 1 ELSE 0 END),
    ('num_refinanciaciones',          CASE WHEN num_refinanciaciones IS NULL OR num_refinanciaciones = '' THEN 1 ELSE 0 END),
    ('calificacion_consolidada',      CASE WHEN calificacion_consolidada IS NULL OR calificacion_consolidada = '' THEN 1 ELSE 0 END),
    ('score_crediticio',              CASE WHEN score_crediticio IS NULL OR score_crediticio = '' THEN 1 ELSE 0 END),
    ('max_dias_atraso',               CASE WHEN max_dias_atraso IS NULL OR max_dias_atraso = '' THEN 1 ELSE 0 END),
    ('tiene_credito_en_mora',         CASE WHEN tiene_credito_en_mora IS NULL OR tiene_credito_en_mora = '' THEN 1 ELSE 0 END),

    -- Exposición Financiera Consolidada
    ('saldo_total_deuda',             CASE WHEN saldo_total_deuda IS NULL OR saldo_total_deuda = '' THEN 1 ELSE 0 END),
    ('saldo_mora_total',              CASE WHEN saldo_mora_total IS NULL OR saldo_mora_total = '' THEN 1 ELSE 0 END),
    ('tasa_mora_cliente_pct',         CASE WHEN tasa_mora_cliente_pct IS NULL OR tasa_mora_cliente_pct = '' THEN 1 ELSE 0 END),
    ('provision_total_cliente',       CASE WHEN provision_total_cliente IS NULL OR provision_total_cliente = '' THEN 1 ELSE 0 END),
    ('el_total_cliente',              CASE WHEN el_total_cliente IS NULL OR el_total_cliente = '' THEN 1 ELSE 0 END),
    ('tasa_nominal_promedio',         CASE WHEN tasa_nominal_promedio IS NULL OR tasa_nominal_promedio = '' THEN 1 ELSE 0 END),
    ('ingresos_anuales',              CASE WHEN ingresos_anuales IS NULL OR ingresos_anuales = '' THEN 1 ELSE 0 END),
    ('tasa_endeudamiento_sistema_pct',CASE WHEN tasa_endeudamiento_sistema_pct IS NULL OR tasa_endeudamiento_sistema_pct = '' THEN 1 ELSE 0 END),
    ('es_cliente_preferente',         CASE WHEN es_cliente_preferente IS NULL OR es_cliente_preferente = '' THEN 1 ELSE 0 END)
) AS v(campo, es_nulo)
GROUP BY v.campo
ORDER BY pct_nulos DESC;

-- Limpieza - NULOS : 
-- calificacion_consolidada - 5 registros, 
-- oficial_credito - 4 registros
-----------------------------------------------------------------------------------------------------------------------
-- Revision nulos calificacion_consolidada 
-- Regla a seguir
-- 0 dias = Normal / 1 a 15 dias = Potencial / 16 a 45 días Deficiente / 46 a 90 días Dudoso / más de 90 días Pérdida

select
	id_cliente,
	calificacion_consolidada,
	max_dias_atraso
from T5_clientes_RAW t5
where calificacion_consolidada is null;

-- Limpieza  - calificacion_consolidada : recategorizar valores nulos de acuerdo a max_dias_atraso correspondiente
-----------------------------------------------------------------------------------------------------------------------
-- Verificacion - oficial_credito


with t1_oficial_credito as(
	select DISTINCT 
		id_cliente as id_cliente_t1,
		oficial_credito,
		segmento
	from T1_creditos_RAW 
)
select
	t1.id_cliente_t1,
	t5.id_cliente,
	t5.oficial_credito,
	t1.oficial_credito,
	t5.segmento,
	t1.segmento
from T5_clientes_RAW as t5
left join t1_oficial_credito as t1 on t5.id_cliente = t1.id_cliente_t1
where t5.oficial_credito is null and t5.segmento = t1.segmento
-- Limpieza - oficial_credito 
-- Rellenar valores nulos de oficial_credito en t5 extrayendolos de t1
-------------------------------------------------------------------------------------------------------------------------
-- 2.2. NULOS fechas
select fecha_primer_credito, fecha_ultimo_corte
from T5_clientes_RAW
where fecha_primer_credito is null or fecha_primer_credito = '' OR
	fecha_ultimo_corte is null or fecha_ultimo_corte = '' 

-- Limpieza - Nulos (fecha) : Ninguno

/* ============================================================================================================
    3. UNICIDAD (Duplicados) - T5_clientes_RAW
    Objetivo: Asegurar que cada identificador de pago (PK) sea único.
============================================================================================================ */

SELECT 
    COUNT(*) AS total_registros, 
    COUNT(DISTINCT id_cliente) AS clientes_unicos,
    COUNT(*) - COUNT(DISTINCT id_cliente) AS diferencia_duplicados
FROM T5_clientes_RAW;

-- Limpieza, ninguna 0 duplicados

/* ============================================================================================================
    4. EXPLORACIÓN DE VALORES NEGATIVOS - T4_HISTORIAL_PAGOS_RAW
    Objetivo: Detectar montos o días menores a cero en todas las variables numéricas de la T4.
============================================================================================================ */
SELECT 
    '4. NEGATIVOS' AS Paso,
    v.campo,
    COUNT(*) AS frecuencia
FROM T5_clientes_RAW
CROSS APPLY (VALUES 
    -- IDENTIFICACIÓN
    ('edad_empresa_anos', TRY_CAST(edad_empresa_anos as decimal(18,2))),
    ('antiguedad_cliente_meses', TRY_CAST(antiguedad_cliente_meses as decimal(18,2))),

	-- EXPOSICIÓN FINANCIERA CONSOLIDADA
    ('num_creditos_activos', TRY_CAST(num_creditos_activos as decimal(18,2))),
    ('num_creditos_sistema', TRY_CAST(num_creditos_sistema as decimal(18,2))),
    ('num_refinanciaciones', TRY_CAST(num_refinanciaciones as decimal(18,2))),
	('score_crediticio', TRY_CAST(score_crediticio as decimal(18,2))),
	('max_dias_atraso', TRY_CAST(max_dias_atraso as decimal(18,2))),
	('tiene_credito_en_mora', TRY_CAST(tiene_credito_en_mora as decimal(18,2))),

    -- EXPOSICIÓN FINANCIERA CONSOLIDADA
    ('saldo_total_deuda', TRY_CAST(saldo_total_deuda as decimal(18,2))),
	('saldo_mora_total', TRY_CAST(saldo_mora_total as decimal(18,2))),
	('tasa_mora_cliente_pct', TRY_CAST(tasa_mora_cliente_pct as decimal(18,2))),
	('provision_total_cliente', TRY_CAST(provision_total_cliente as decimal(18,2))),
	('el_total_cliente', TRY_CAST(el_total_cliente as decimal(18,2))),
	('tasa_nominal_promedio', TRY_CAST(tasa_nominal_promedio as decimal(18,2))),
	('ingresos_anuales', TRY_CAST(ingresos_anuales as decimal(18,2))),
	('tasa_endeudamiento_sistema_pct', TRY_CAST(tasa_endeudamiento_sistema_pct as decimal(18,2))),
	('es_cliente_preferente', TRY_CAST(es_cliente_preferente as decimal(18,2)))
) AS v(campo, valor_encontrado)
WHERE v.valor_encontrado < 0 
GROUP BY v.campo;

-- Limpieza - NEGATIVOS 
-- saldo_total_deuda - 4

---------------------------------------------------------------------------------------------------------
 -- Verifiar NEGATIVO - saldo_total_deuda

select
	id_cliente,
	saldo_total_deuda
from T5_clientes_RAW
where saldo_total_deuda like '%-%';

SELECT 
    id_cliente,
    id_credito, -- Si tienes el ID del crédito en T1
    monto_aprobado,
    saldo_capital,
    ABS(TRY_CAST(saldo_capital as decimal(18,2))) as saldo_absoluto
FROM T1_creditos_RAW
WHERE id_cliente in ('CLI-00462','CLI-00501','CLI-01115','CLI-01455') and 
saldo_capital in ('251958.69','3099540.33','1523350.96','25751.96')

-- Limpieza - saldo_total_deuda : Eliminar filas donde saldo_total_deuda es negativo al no ser un error de signo

/* ============================================================================================================
    5. CONSISTENCIA DE VALORES NUMÉRICOS (Reglas de Negocio - T5_clientes_RAW)
    Objetivo: Validar que el Historial de Pagos por Cuota coincidan con la suma de sus componentes.
============================================================================================================ */

-- Verificar - tipo_persona : Natural / Jurídica
select
	tipo_persona, 
	COUNT(*) as frecuencia
from T5_clientes_RAW
group by tipo_persona;

-- Limpieza - tipo_persona : Ninguna, todo correcto

----------------------------------------------------------------------------------------------------------------
-- Verificar - segmento  : Microcrédito / Consumo / Vivienda / Comercial PYME / Comercial Corporativo

select
	segmento, 
	COUNT(*) as frecuencia
from T5_clientes_RAW
group by segmento;

-- Limpieza - segmento : Ninguna, todo correcto

----------------------------------------------------------------------------------------------------------------
-- Verificar - sector_economico : 
--Comercio / Industria / Servicios / Construcción / Agricultura / Transporte / Turismo / Salud / Exportación / Pesca

select
	sector_economico, 
	COUNT(*) as frecuencia
from T5_clientes_RAW
group by sector_economico;

-- Limpieza - sector_economico : estandarizar catalogo 

----------------------------------------------------------------------------------------------------------------
-- Verificar - zona_geografica :
-- Guayaquil / Quito / Cuenca / Ambato / Manta / Portoviejo / Machala / Loja
select
	zona_geografica, 
	COUNT(*) as frecuencia
from T5_clientes_RAW
group by zona_geografica;

-- Limpieza - zona_geografica : Ninguna, todo correcto
----------------------------------------------------------------------------------------------------------------
-- Verificar - edad_empresa_anos :
-- Personas naturales = 0 años
-- Personas juridicas > 0
with Ver_edad_empresa_anos as(
select
	 cast(edad_empresa_anos as decimal (18,2)) as edad_empresa_anos,
	 CAST(tipo_persona as varchar (50)) as tipo_persona
	 from T5_clientes_RAW
)
SELECT 
    tipo_persona, 
    COUNT(*) AS casos,
    MIN(edad_empresa_anos) AS min_edad,
    MAX(edad_empresa_anos) AS max_edad
FROM Ver_edad_empresa_anos
GROUP BY tipo_persona;

select *
from T5_clientes_RAW
where tipo_persona = 'Natural' and edad_empresa_anos <> '0'
    
-- Limpieza - edad_empresa_anos : Fijar Personas Naturales a 0 años de trabajo en la empresa
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - antiguedad_cliente_meses : ≥ 3 meses
select 
	antiguedad_cliente_meses,
	COUNT(*) as frecuencia
from T5_clientes_RAW
where antiguedad_cliente_meses < 3
group by antiguedad_cliente_meses
-- Limpieza - antiguedad_cliente_meses : Ninguna 
-------------------------------------------------------------------------------------------------------------------
--  POSICIÓN CREDITICIA CONSOLIDADA — Corte diciembre 2025
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - num_creditos_activos : Entero ≥ 1
select 
	num_creditos_activos,
	count(*) as frecuencia
from T5_clientes_RAW
group by num_creditos_activos
order by num_creditos_activos asc
-- Limpieza - num_creditos_activos : Ninguna 
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - num_creditos_sistema : Entero ≥ 1
select 
	num_creditos_sistema,
	count(*) as frecuencia
from T5_clientes_RAW
group by num_creditos_sistema
order by num_creditos_sistema asc
-- Limpieza - num_creditos_sistema : Ninguna
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - num_refinanciaciones : Entero ≥ 0
select 
	num_refinanciaciones,
	count(*) as frecuencia
from T5_clientes_RAW
group by num_refinanciaciones
order by num_refinanciaciones asc
-- Limpieza - num_refinanciaciones : Ninguna
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - calificacion_consolidada
-- 0 dias = Normal / 1 a 15 dias = Potencial / 16 a 45 días Deficiente / 46 a 90 días Dudoso / más de 90 días Pérdida

SELECT 
    id_cliente,
    calificacion_consolidada,
    max_dias_atraso
FROM T5_clientes_RAW
WHERE 
    (calificacion_consolidada = 'Normal' AND max_dias_atraso > 0) OR
    (calificacion_consolidada = 'Potencial' AND (max_dias_atraso < 1 OR max_dias_atraso > 15)) OR 
    (calificacion_consolidada = 'Deficiente' AND (max_dias_atraso < 16 OR max_dias_atraso > 45)) OR
    (calificacion_consolidada = 'Dudoso' AND (max_dias_atraso < 46 OR max_dias_atraso > 90)) OR
    (calificacion_consolidada = 'Pérdida' AND max_dias_atraso <= 90);
-- Limpieza - calificacion_consolidada : Ninguna
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - max_dias_atraso
-- Enero : ≥ 0
select 
	COUNT(*)
from T5_clientes_RAW
where max_dias_atraso < 0
-- Limpieza - max_dias_atraso : Ninguna
-------------------------------------------------------------------------------------------------------------------
-- Verificacion - tiene_credito_en_mora : 0 = No · 1 = Sí
-- Al menos un crédito con dias_atraso > 0.

-- tiene_credito_en_mora recalculo
with ver_tiene_credito_en_mora as(
	select
		case
			when max_dias_atraso > 0 and num_creditos_activos > 0 then '1'
			else '0'
		end as recalculo
	from	T5_clientes_RAW
)
select
	recalculo,
	COUNT(*)
from ver_tiene_credito_en_mora
group by recalculo;
-- tiene_credito_en_mora original 
select
	tiene_credito_en_mora,
	COUNT(*)
from T5_clientes_RAW
group by tiene_credito_en_mora;

-- Limpieza - tiene_credito_en_mora : Ninguna
-------------------------------------------------------------------------------------------------------------------
-- EXPOSICIÓN FINANCIERA CONSOLIDADA
-------------------------------------------------------------------------------------------------------------------
-- Verificar - saldo_mora_total
-- Suma de saldo_mora de todos sus créditos activos.
-- SUM(saldo_mora) T1 último corte por id_cliente
with var_saldo_mora_total as(
	select 
		id_cliente,
		cast(tiene_credito_en_mora as int) as tiene_credito_en_mora,
		CASt(max_dias_atraso as int) as max_dias_atraso,
		cast(saldo_mora_total as decimal(18,2)) as saldo_mora_total
	from T5_clientes_RAW
)
	SELECT 
		id_cliente,
		tiene_credito_en_mora,
		max_dias_atraso,
		saldo_mora_total
	FROM var_saldo_mora_total
	WHERE 
		(saldo_mora_total > 0.00 AND tiene_credito_en_mora = 0) -- Inconsistencia: Debe dinero pero no está "en mora"
		OR 
		(saldo_mora_total = 0.00 AND tiene_credito_en_mora = 1 AND max_dias_atraso > 0); -- Inconsistencia: Está en mora pero debe $0.00
-- Correccion error logico
UPDATE T5_clientes_RAW
SET tiene_credito_en_mora = 0, 
    max_dias_atraso = 0,
    flag_error = 'MORA_SIN_SALDO'
WHERE cast(saldo_mora_total as decimal(18,2)) = 0 AND CAST(max_dias_atraso as int) > 0;

-- Limpieza - saldo_mora_total : Reemplazo de valores por error logico de la IA al generar la base de datos
-----------------------------------------------------------------------------------------------------------------------
-- Verificacion - tasa_mora_cliente_pct
-- saldo_mora_total / saldo_total_deuda × 100
WITH ver_tasa_mora_cliente_pct AS (
    SELECT
        CAST(saldo_mora_total AS DECIMAL(18,2)) AS saldo_mora_total,
        CAST(saldo_total_deuda AS DECIMAL(18,2)) AS saldo_total_deuda,
        CAST(tasa_mora_cliente_pct AS DECIMAL(18,2)) AS tasa_mora_reportada
    FROM T5_clientes_RAW
)
SELECT
    tasa_mora_reportada,
    -- Usamos NULLIF para evitar el error y ISNULL para que el reporte se vea limpio (0.00)
    ISNULL(100 * (saldo_mora_total / NULLIF(saldo_total_deuda, 0)), 0) AS tasa_mora_recalculo,
    tasa_mora_reportada - ISNULL(100 * (saldo_mora_total / NULLIF(saldo_total_deuda, 0)), 0) AS diferencia
FROM ver_tasa_mora_cliente_pct
where tasa_mora_reportada - ISNULL(100 * (saldo_mora_total / NULLIF(saldo_total_deuda, 0)), 0) > 1;
-- Limpieza - tasa_mora_cliente_pct : tasa_mora_cliente_pct > 100%
-----------------------------------------------------------------------------------------------------------------------
-- Verificar - provision_total_cliente 
-- SUM(provision_constituida) T1 último corte por id_cliente
WITH t1_consolidado AS (
    SELECT 
        id_cliente,
        SUM(TRY_CAST(provision_constituida AS DECIMAL(18,2))) AS suma_t1,
        ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY CAST(fecha_corte AS DATE) DESC) as ranking
    FROM T1_creditos_RAW
    GROUP BY id_cliente, CAST(fecha_corte AS DATE) -- Agrupamos por si hay varios créditos en la misma fecha
)
SELECT 
    t5.id_cliente,
    t5.provision_total_cliente AS valor_t5,
    t1.suma_t1 AS valor_t1,
    (t5.provision_total_cliente - t1.suma_t1) AS diferencia
FROM T5_clientes_RAW t5
INNER JOIN t1_consolidado t1 ON t5.id_cliente = t1.id_cliente
WHERE t1.ranking = 1 
  AND ABS(t5.provision_total_cliente - t1.suma_t1) > 0.01; -- Solo donde la diferencia sea mayor a un centavo

  -- Limpieza - provision_total_cliente reemplazar provision_total_cliente con calculos correctos t1
 -----------------------------------------------------------------------------------------------------------------------
 -- Verificacion - el_total_cliente
 -- SUM(el_expected_loss) T1 último corte por id_cliente
 with var_el_expected_loss as (
	select
		id_cliente,
		SUM(CAST(el_expected_loss as decimal(18,2))) as sum_loss,
		ROW_NUMBER() over(partition by id_cliente order by cast(fecha_corte as date) desc) as ranking
	from T1_creditos_RAW
	group by id_cliente, cast(fecha_corte as date)
)
select
	t5.id_cliente,
	t5.el_total_cliente,
	t1_loss.sum_loss,
	(t5.el_total_cliente - t1_loss.sum_loss) AS diferencia
from T5_clientes_RAW as t5
inner join var_el_expected_loss as t1_loss 
on t5.id_cliente = t1_loss.id_cliente
where t1_loss.ranking = 1 and
abs(t5.el_total_cliente - t1_loss.sum_loss) > 0.01;

  -- Limpieza - provision_total_cliente reemplazar el_total_cliente con calculos correctos t1

-----------------------------------------------------------------------------------------------------------------------
-- Verificar -	tasa_nominal_promedio
-- AVERAGE(tasa_nominal_anual_pct) T1 por id_cliente
WITH t1_diciembre AS (
    SELECT 
        id_cliente,
        -- Calculamos la ponderación SOLO con los saldos de Diciembre
        SUM(TRY_CAST(saldo_capital AS DECIMAL(18,2)) * TRY_CAST(tasa_nominal_anual_pct AS DECIMAL(18,4))) 
        / NULLIF(SUM(TRY_CAST(saldo_capital AS DECIMAL(18,2))), 0) AS tasa_ponderada_mensual
    FROM T1_creditos_RAW
    WHERE CAST(fecha_corte AS DATE) = '2025-12-31' -- Filtro de corte mensual
    GROUP BY id_cliente
)
SELECT 
    t5.id_cliente,
    t5.tasa_nominal_promedio AS valor_t5_reportado,
    t1.tasa_ponderada_mensual AS valor_t1_calculado,
    (t5.tasa_nominal_promedio - t1.tasa_ponderada_mensual) AS diferencia
FROM T5_clientes_RAW t5
JOIN t1_diciembre t1 ON t5.id_cliente = t1.id_cliente
WHERE ABS(t5.tasa_nominal_promedio - t1.tasa_ponderada_mensual) > 0.01;

  -- Limpieza - tasa_nominal_promedio reemplazar el_total_cliente con calculos correctos t1

---------------------------------------------------------------------------------------------------------------------
-- Verificacion ingresos_anuales 
-- Del primer registro del cliente en T1
WITH t1_primeros_ingresos AS (
    SELECT 
        id_cliente,
        TRY_CAST(ingresos_anuales AS DECIMAL(18,2)) AS ingreso_t1_inicial,
        ROW_NUMBER() OVER(
            PARTITION BY id_cliente 
            ORDER BY CAST(fecha_corte AS DATE) ASC
        ) as ranking_antiguedad
    FROM T1_creditos_RAW
)
SELECT 
    t5.id_cliente,
    t5.ingresos_anuales AS ingresos_anuales_reportados,
    t1.ingreso_t1_inicial AS ingresos_t1_primera_vez,
    (t5.ingresos_anuales - t1.ingreso_t1_inicial) AS diferencia
FROM T5_clientes_RAW t5
INNER JOIN t1_primeros_ingresos t1 
    ON t5.id_cliente = t1.id_cliente
WHERE t1.ranking_antiguedad = 1 
  AND ABS(t5.ingresos_anuales - t1.ingreso_t1_inicial) > 0.01;

-- Limpieza - tasa_nominal_promedio reemplazar el_total_cliente con calculos correctos t1
---------------------------------------------------------------------------------------------------------------------
-- Verificar - tasa_endeudamiento_sistema_pct
-- saldo_total_deuda / ingresos_anuales × 100
WITH t1_actualidad AS (
    -- Obtenemos el saldo total real (Suma de todos los créditos hoy)
    SELECT id_cliente, 
           SUM(TRY_CAST(saldo_total_exposicion AS DECIMAL(18,2))) AS saldo_real_t1
    FROM T1_creditos_RAW
    WHERE fecha_corte = (SELECT MAX(fecha_corte) FROM T1_creditos_RAW)
    GROUP BY id_cliente
),
t1_ingreso_reciente AS (
    -- Obtenemos el ingreso del mas reciente desc
    SELECT id_cliente, 
           TRY_CAST(ingresos_anuales AS DECIMAL(18,2)) AS ingreso_inicial_t1
    FROM (
        SELECT id_cliente, ingresos_anuales,
               ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY CAST(fecha_corte AS DATE) DESC) as ranking
        FROM T1_creditos_RAW
    ) AS sub WHERE ranking = 1
)
SELECT 
    t5.id_cliente,
    t5.tasa_endeudamiento_sistema_pct AS tasa_t5_reportada,
    -- Recalculamos: (Saldo Actual / Ingreso Inicial) * 100
    (act.saldo_real_t1 / NULLIF(hist.ingreso_inicial_t1, 0)) * 100 AS tasa_t1_calculada,
    -- Diferencia en puntos porcentuales
    (t5.tasa_endeudamiento_sistema_pct - ((act.saldo_real_t1 / NULLIF(hist.ingreso_inicial_t1, 0)) * 100)) AS diferencia_puntos
FROM T5_clientes_RAW t5
INNER JOIN t1_actualidad act ON t5.id_cliente = act.id_cliente
INNER JOIN t1_ingreso_reciente hist ON t5.id_cliente = hist.id_cliente
WHERE 
    ABS(t5.tasa_endeudamiento_sistema_pct - ((act.saldo_real_t1 / NULLIF(hist.ingreso_inicial_t1, 0)) * 100)) > 0.1; -- Diferencia mayor a 0.1%

-- Limpieza : - tasa_endeudamiento_sistema_pct : 
-- Recalcular por errores de concordancia previa saldo_total_exposicion y t1_ingreso_reciente

-------------------------------------------------------------------------------------------------------------------
-- Verificar - es_cliente_preferente

WITH t1_calculo_preferente AS (
    SELECT 
        id_cliente,
        -- Verificamos las 3 condiciones del diccionario
        MAX(CASE WHEN ranking_desc = 1 THEN calificacion_sbs ELSE NULL END) as ultima_calif,
        DATEDIFF(MONTH, MIN(fecha_corte), MAX(fecha_corte)) as meses_antiguedad,
        SUM(CASE WHEN ranking_desc = 1 THEN 1 ELSE 0 END) as creditos_activos
    FROM (
        SELECT 
            id_cliente, 
            calificacion_sbs, 
            fecha_corte,
            ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY CAST(fecha_corte AS DATE) DESC) as ranking_desc
        FROM T1_creditos_RAW
    ) AS sub
    GROUP BY id_cliente
)
SELECT 
    t5.id_cliente,
    t5.es_cliente_preferente AS valor_t5_reportado,
    -- Reconstrucción de la lógica para ver por qué falló
    t1.ultima_calif,
    t1.meses_antiguedad,
    t1.creditos_activos,
    -- Diagnóstico del error de categorización
    CASE 
        WHEN t1.ultima_calif <> 'Normal' THEN 'ERROR: No es calificación Normal'
        WHEN t1.meses_antiguedad <= 24 THEN 'ERROR: Antigüedad <= 24 meses'
        WHEN t1.creditos_activos <= 1 THEN 'ERROR: Solo tiene 1 crédito activo'
    END AS motivo_error_categorizacion
FROM T5_clientes_RAW t5
INNER JOIN t1_calculo_preferente t1 ON t5.id_cliente = t1.id_cliente
WHERE 
    -- Casos donde T5 dice SI (1) pero T1 demuestra que NO cumple alguna regla
    (t5.es_cliente_preferente = 1 AND (t1.ultima_calif <> 'Normal' OR t1.meses_antiguedad <= 24 OR t1.creditos_activos <= 1))
    OR
    -- Casos donde T5 dice NO (0) pero T1 demuestra que SI cumple todas las reglas
    (t5.es_cliente_preferente = 0 AND (t1.ultima_calif = 'Normal' AND t1.meses_antiguedad > 24 AND t1.creditos_activos > 1));

---------------------------------------------------------------------------------------------------------------------
-- Verificacion - sector_economico
-- Comercio / Industria / Servicios / Construcción / Agricultura / Transporte / Turismo / Salud / Exportación / Pesca
select sector_economico, COUNT(*) as frecuencia 
from T5_clientes_RAW
where sector_economico not in('Comercio','Industria','Servicios','Construcción','Agricultura','Transporte', 'Turismo', 'Salud', 'Exportación', 'Pesca')
group by sector_economico

-- Limpieza - sector_economico : Normalizar variable categorica
---------------------------------------------------------------------------------------------------------------------
-- Verificacion - zona_geografica
-- Guayaquil / Quito / Cuenca / Ambato / Manta / Portoviejo / Machala / Loja
select 
	zona_geografica,
	COUNT(*) as frecuencia
from T5_clientes_RAW
where zona_geografica not in('Guayaquil','Quito','Cuenca','Ambato','Manta','Portoviejo', 'Machala', 'Loja')
group by zona_geografica

-- Limpieza - sector_economico : NINGUNA
---------------------------------------------------------------------------------------------------------------------
-- Verificacion - score_crediticio
-- 200-850 válido · -1 si nulo · 9999 o 1200 = error de sistema
select
	score_crediticio,
	COUNT(*)
from T5_clientes_RAW
where TRY_CASt(score_crediticio as decimal(18,2))> 850
group by score_crediticio

select
	id_cliente,
	score_crediticio
from T5_clientes_RAW
where TRY_CASt(score_crediticio as decimal(18,2))> 850

-- Limpieza - id_cliente : eliminar filas con score crediticio que no este entre 200 y 850
----------------------------------------------------------------------------------------------------------------------
-- Verificar - max_dias_atraso
-- Negativos≥ 0

select max_dias_atraso
from T5_clientes_RAW
where max_dias_atraso < 0
-- Limpieza - max_dias_atraso : Ninguna



