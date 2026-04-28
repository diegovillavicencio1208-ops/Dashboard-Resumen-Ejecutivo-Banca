/* ============================================================================================================================
   PROYECTO      : Riesgo Crediticio — Banco Mediano Ecuador
   SCRIPT        : 1. EDA — T1_creditos_RAW
   DESCRIPCIÓN   : Análisis Exploratorio de Datos (EDA) sobre la tabla de cartera de créditos.
                   Evalúa la calidad del dato en 6 dimensiones antes de cualquier transformación:
                   integridad técnica, valores negativos, completitud, unicidad,
                   consistencia de fechas, consistencia de catálogos y reglas de negocio.
   TABLA FUENTE  : T1_creditos_RAW
   BASE DE DATOS : RiesgoCrediticioProyecto2
   AUTOR         : Diego L. Villavicencio Merino
   FECHA         : 20-04-2026
   PRERREQUISITO : T1_creditos_RAW cargada en la base de datos.
============================================================================================================================ */

Use RiesgoCrediticioProyecto2;
Go

/* ============================================================================================================================
   PASO 1: INTEGRIDAD TÉCNICA — Detección de "basura" en campos numéricos
   Objetivo : Identificar valores de texto o caracteres especiales que impidan
              la conversión a tipo numérico (DECIMAL).
   Técnica  : CROSS APPLY + TRY_CAST. Si TRY_CAST devuelve NULL sobre un valor
              no vacío, ese valor es inválido (basura).
   Columnas : Todas las variables numéricas del cuestionario de crédito.
============================================================================================================================ */
SELECT 
    '1. INTEGRIDAD' AS Paso,
    v.campo,
    v.valor_sucio,
    COUNT(*) AS frecuencia
FROM T1_creditos_RAW
CROSS APPLY (VALUES 
    ('plazo_meses', plazo_meses),
    ('monto_aprobado', monto_aprobado),
    ('saldo_capital', saldo_capital),
    ('tasa_nominal_anual_pct', tasa_nominal_anual_pct),
	('score_crediticio',score_crediticio),
	('pd_probabilidad_default',pd_probabilidad_default),
	('lgd_loss_given_default',lgd_loss_given_default),
	('el_expected_loss',el_expected_loss),
	('ingresos_anuales',ingresos_anuales),
	('ratio_endeudamiento',ratio_endeudamiento),
	('ltv_loan_to_value',ltv_loan_to_value),
	('antiguedad_cliente_meses',antiguedad_cliente_meses),
	('numero_creditos_sistema',numero_creditos_sistema),
	('dias_atraso', dias_atraso)
) AS v(campo, valor_sucio)
WHERE TRY_CAST(v.valor_sucio AS DECIMAL(18,4)) IS NULL 
  AND v.valor_sucio IS NOT NULL AND v.valor_sucio <> ''
GROUP BY v.campo, v.valor_sucio;

/* ============================================================================================================================
   PASO 1.1: VALORES NEGATIVOS — Incoherencia financiera
   Objetivo : Detectar montos, tasas o indicadores de riesgo con valores menores a cero.
              Financieramente, variables como saldo_capital, monto_aprobado o tasa no
              pueden ser negativas en una cartera de crédito normal.
   Acción   : Los campos con negativos se gestionan en el script de limpieza (script 6).
============================================================================================================================ */
SELECT 
    '1.1 NEGATIVOS' AS Paso,
    v.campo,
    COUNT(*) AS frecuencia
FROM T1_creditos_RAW
CROSS APPLY (VALUES 
    ('plazo_meses',              TRY_CAST(plazo_meses AS DECIMAL(18,4))),
    ('monto_aprobado',           TRY_CAST(monto_aprobado AS DECIMAL(18,4))),
    ('saldo_capital',            TRY_CAST(saldo_capital AS DECIMAL(18,4))),
    ('tasa_nominal_anual_pct',   TRY_CAST(tasa_nominal_anual_pct AS DECIMAL(18,4))),
    ('score_crediticio',         TRY_CAST(score_crediticio AS DECIMAL(18,4))),
    ('pd_probabilidad_default',  TRY_CAST(pd_probabilidad_default AS DECIMAL(18,4))),
    ('lgd_loss_given_default',   TRY_CAST(lgd_loss_given_default AS DECIMAL(18,4))),
    ('el_expected_loss',         TRY_CAST(el_expected_loss AS DECIMAL(18,4))),
    ('ingresos_anuales',         TRY_CAST(ingresos_anuales AS DECIMAL(18,4))),
    ('ratio_endeudamiento',      TRY_CAST(ratio_endeudamiento AS DECIMAL(18,4))),
    ('ltv_loan_to_value',        TRY_CAST(ltv_loan_to_value AS DECIMAL(18,4))),
    ('antiguedad_cliente_meses', TRY_CAST(antiguedad_cliente_meses AS DECIMAL(18,4))),
    ('numero_creditos_sistema',  TRY_CAST(numero_creditos_sistema AS DECIMAL(18,4))),
	('dias_atraso',				 TRY_CAST(dias_atraso AS DECIMAL(18,4)))
) AS v(campo, valor_encontrado)
WHERE  v.valor_encontrado < 0 -- Filtro estricto para negativos
GROUP BY v.campo;

/* ============================================================================================================================
   PASO 2: COMPLETITUD — Nulos y vacíos en variables clave
   Objetivo : Cuantificar el porcentaje de información faltante en los campos
              más críticos para los indicadores de riesgo.
   Nota     : Se evalúan tanto NULL como cadenas vacías ('') ya que la carga
              CSV puede producir ambos tipos de ausencia.
============================================================================================================================ */
SELECT 
    '2. NULOS' AS Paso,
    v.campo,
    SUM(v.es_nulo) AS total_nulos,
    COUNT(*) AS registros_totales,
    CAST(SUM(v.es_nulo) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_nulos
FROM T1_creditos_RAW
CROSS APPLY (VALUES
    ('id_credito',       CASE WHEN id_credito IS NULL OR id_credito = '' THEN 1 ELSE 0 END),
    ('id_cliente',       CASE WHEN id_cliente IS NULL OR id_cliente = '' THEN 1 ELSE 0 END),
    ('monto_aprobado',   CASE WHEN monto_aprobado IS NULL OR monto_aprobado = '' THEN 1 ELSE 0 END),
    ('score_crediticio', CASE WHEN score_crediticio IS NULL OR score_crediticio = '' THEN 1 ELSE 0 END),
    ('sector_economico', CASE WHEN sector_economico IS NULL OR sector_economico = '' THEN 1 ELSE 0 END)
) AS v(campo, es_nulo)
GROUP BY v.campo
ORDER BY pct_nulos DESC;


/* ============================================================================================================================
   PASO 3: UNICIDAD — Duplicados por crédito y fecha de corte
   Objetivo : Garantizar que no existan dos registros para el mismo crédito
              en el mismo mes. La clave natural de la tabla es (id_credito, fecha_corte).
   Resultado esperado: 0 filas (ningún duplicado).
============================================================================================================================ */
SELECT 
    '3. DUPLICADOS' AS Paso,
    id_credito, 
    fecha_corte, 
    COUNT(*) AS registros_encontrados
FROM T1_creditos_RAW
GROUP BY id_credito, fecha_corte
HAVING COUNT(*) > 1;


/* ============================================================================================================================
   PASO 4: CONSISTENCIA DE FECHAS — Cronología bancaria
   Objetivo : Detectar errores de registro temporal:
              (a) fecha_vencimiento <= fecha_desembolso → crédito ya venció antes de otorgarse.
              (b) fecha_desembolso > hoy → crédito "futuro" no puede existir en el histórico.
============================================================================================================================ */
SELECT 
    '4. FECHAS' AS Paso,
    id_credito, 
    fecha_desembolso, 
    fecha_vencimiento,
    'Vencimiento anterior al desembolso' AS tipo_error
FROM T1_creditos_RAW
WHERE TRY_CAST(fecha_vencimiento AS DATE) <= TRY_CAST(fecha_desembolso AS DATE)
   OR TRY_CAST(fecha_desembolso AS DATE) > CAST(GETDATE() AS DATE);


/* ============================================================================================================================
   PASO 5: CONSISTENCIA DE CATEGORÍAS — Catálogo empresarial
   Objetivo : Verificar que los valores categóricos pertenezcan al catálogo
              oficial (calificacion_sbs, segmento, sector_economico).

   5a. Calificación SBS: se valida la coherencia entre la calificación reportada
       y los días de atraso reales, según la norma de la SBS Ecuador:
         Normal     = 0 días de atraso
         Potencial  = 1–15 días
         Deficiente = 16–45 días
         Dudoso     = 46–90 días
         Pérdida    = > 90 días
============================================================================================================================ */
-- Vista rápida de valores únicos en calificacion_sbs
select calificacion_sbs from T1_creditos_RAW group by calificacion_sbs

-- Validación cruzada: calificación vs. días de atraso reales
SELECT 
    calificacion_sbs,
    COUNT(*) AS Nr_Erronea
FROM T1_creditos_RAW
WHERE 
    -- Caso 1: Dice 'Normal' pero TIENE mora
    (calificacion_sbs = 'Normal' AND TRY_CAST(dias_atraso AS INT) <> 0) OR
    -- Caso 2: Dice 'Potencial' pero NO está en el rango 1-15
    (calificacion_sbs = 'Potencial' AND TRY_CAST(dias_atraso AS INT) NOT BETWEEN 1 AND 15) OR
    -- Caso 3: Dice 'Deficiente' pero NO está en el rango 16-45
    (calificacion_sbs = 'Deficiente' AND TRY_CAST(dias_atraso AS INT) NOT BETWEEN 16 AND 45) OR
    -- Caso 4: Dice 'Dudoso' pero no está en el rango 46-90
	(calificacion_sbs = 'Dudoso' AND TRY_CAST(dias_atraso AS INT) NOT BETWEEN 46 AND 90) OR
	-- Caso 5: Dice 'Pérdida' pero no está en el rango >90
	(calificacion_sbs = 'Pérdida' AND TRY_CAST(dias_atraso AS INT) < 90)
GROUP BY calificacion_sbs;

-- Revisión de catálogo de segmentos
SELECT 
    '5. CATÁLOGO SEGMENTOS' AS Paso,
    segmento,
    COUNT(*) AS frecuencia
FROM T1_creditos_RAW
GROUP BY segmento;

-- Revisión de catálogo de sector_economico
SELECT 
    '5. CATÁLOGO SEGMENTOS' AS Paso,
    sector_economico,
    COUNT(*) AS frecuencia
FROM T1_creditos_RAW
GROUP BY sector_economico;

/* ============================================================================================================================
   PASO 6: REGLAS DE NEGOCIO — Coherencia financiera entre variables
   Objetivo : Validar que el saldo_capital no supere el monto_aprobado.
              Un saldo mayor al monto aprobado es financieramente imposible
              en un crédito de amortización normal.
   Resultado esperado: 0 filas.
============================================================================================================================ */
SELECT 
    '6. REGLAS NEGOCIO' AS Paso,
    id_credito, 
    monto_aprobado, 
    saldo_capital,
    'Saldo mayor al monto aprobado' AS inconsistencia
FROM T1_creditos_RAW
WHERE TRY_CAST(saldo_capital AS DECIMAL(18,2)) > TRY_CAST(monto_aprobado AS DECIMAL(18,2))
  AND TRY_CAST(monto_aprobado AS DECIMAL(18,2)) > 0;
