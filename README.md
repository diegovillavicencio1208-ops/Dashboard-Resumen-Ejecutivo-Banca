# 🏦 Banca OG  Dashboard Ejecutivo de Monitoreo Financiero

Este ecosistema de BI automatiza el monitoreo financiero de la institución, permitiendo al director responder en segundos la pregunta que mueve cada decisión estratégica: ¿cómo está el banco hoy versus el mes anterior?

Su implementación elimina la dependencia de reportes manuales y consolidaciones tardías, poniendo en manos del director una vista ejecutiva del estado real del banco al cierre de cada período  rentabilidad, liquidez, solvencia y calidad de cartera en un solo lugar, con comparación automática contra el mes anterior y alertas visuales de desviación.

---

## 📌 Descripción del proyecto

El dashboard integra cinco fuentes de datos transaccionales de una institución financiera simulada que opera en Ecuador. La vista principal **Resumen Ejecutivo** consolida ocho KPIs estratégicos con comparación mes a mes, visualizaciones de tendencia y un sistema de alertas por color, permitiendo al analista identificar de un vistazo el estado de salud financiera del banco al cierre de cada período.

El análisis responde a tres preguntas clave:

- ¿Está el banco generando retornos sostenibles sobre sus activos y patrimonio?
- ¿La cartera de créditos está deteriorándose y las provisiones son suficientes para cubrirla?
- ¿La institución mantiene niveles adecuados de liquidez y solvencia frente a sus obligaciones?

---

## 🔗 Dashboard interactivo

[Ver dashboard en Power BI](https://app.powerbi.com/view?r=eyJrIjoiMjdiMmZjOGItYjE4MS00M2E2LWEwN2ItYzI4YTA3NDRlNThjIiwidCI6IjU1N2NiNjMxLWI3ZjQtNGM1NC1hNjljLWM4MzQ1NWQxMzEyOCIsImMiOjR9&pageName=5a13ee042a084f76b50a)

---

## 📸 Vista previa del Dashboard

![Resumen Ejecutivo](https://github.com/diegovillavicencio1208-ops/Dashboard-Resumen-Ejecutivo-Banca/blob/ed778fb9197224686aa2946de6d21e4897a40c1e/Resumen%20Ejecutivo%20-%20Dashboard.png)

---

## 💡 Insights principales

### 1. Rentabilidad en alza, pero financiada con liquidez
El banco cerró diciembre 2025 con ROE de **5,07%** (+3,52pp), ROA de **0,59%** (+0,42pp) y NIM de **0,86%** (+0,08pp). Sin embargo, el Índice de Liquidez cayó **-12,59pp** en el mismo período, sugiriendo que la mejora en rentabilidad se está financiando con una reducción agresiva del colchón de disponibilidad. De mantenerse esta tendencia, la institución podría aproximarse a la zona de alerta regulatoria en los primeros meses del año siguiente.

### 2. Mora creciente con cobertura insuficiente
La Tasa de Mora cerró en **11,48%** (+0,32pp) mientras el Ratio de Cobertura cayó a **83,52%** (-9,83pp). Esto indica que la cartera vencida crece más rápido que las provisiones destinadas a cubrirla, generando una brecha de riesgo no provisionado que presiona la estabilidad del balance. La cartera vigente se contrajo de 406 a 363 millones entre junio y diciembre, evidenciando un deterioro sostenido en la calidad crediticia.

### 3. Castigos concentrados en el último trimestre: señal de alerta estructural
El gráfico de castigos revela que los meses de septiembre a diciembre concentraron los valores más altos del año, todos por encima del promedio histórico. Este patrón sugiere que pérdidas acumuladas durante el año fueron reconocidas en bloque al cierre, distorsionando la imagen financiera de los períodos intermedios y evidenciando la necesidad de una política de revisión y castigo mensual más disciplinada.

---

## 🗂️ Modelo de Datos

> Diagrama del modelo relacional implementado en Power BI.

![Modelo de Datos](https://github.com/diegovillavicencio1208-ops/Dashboard-Resumen-Ejecutivo-Banca/blob/ed778fb9197224686aa2946de6d21e4897a40c1e/8.%20Modelo.png)

## 🛠️ Stack tecnológico

| Herramienta | Uso |
|---|---|
| **Power BI Desktop** | Desarrollo del dashboard y visualizaciones |
| **DAX** | Modelado de medidas y KPIs |
| **SQL Server** | Base de datos relacional fuente |
| **Power Query (M)** | Transformación y carga de datos (ETL) |

---

## 📐 Arquitectura del Modelo

El modelo sigue un esquema en **estrella** con una tabla de medidas centralizada y cinco tablas de hechos conectadas a dimensiones compartidas.

| Tabla | Tipo | Descripción |
|---|---|---|
| `FCT_creditos_t1` | Hecho | Créditos individuales con saldos, calificación y métricas de riesgo |
| `FCT_estado_financiero_t2` | Hecho | Estado de resultados mensual (ingresos, márgenes, castigos) |
| `FCT_balance_general_t3` | Hecho | Balance general mensual (activos, pasivos, patrimonio, índices) |
| `FCT_historial_pagos_t4` | Hecho | Historial de pagos por crédito |
| `FCT_clientes_consol_t5` | Hecho | Consolidado de métricas por cliente |
| `DIM_Calendario` | Dimensión | Tabla de fechas con jerarquías temporales |
| `DIM_Calificacion` | Dimensión | Calificación de riesgo crediticio |
| `DIM_cliente` | Dimensión | Segmento, zona y oficial de cuenta |
| `DIM_Producto` | Dimensión | Producto crediticio y tipo de garantía |
| `1_DS_Resumen Ejecutivo` | Medidas | Tabla centralizada con todas las medidas DAX |

---

## 📊 KPIs monitoreados

| KPI | Descripción | Valor dic-2025 |
|---|---|---|
| **ROE** | Retorno sobre Patrimonio | 5,07% ↑ |
| **ROA** | Retorno sobre Activos | 0,59% ↑ |
| **NIM** | Margen de Interés Neto | 0,86% ↑ |
| **Índice de Liquidez** | Disponibilidad frente a obligaciones | 43,47% ↓ |
| **Índice de Solvencia** | Capital frente a activos ponderados | 16,09% ↑ |
| **Tasa de Mora** | Cartera vencida sobre cartera total | 11,48% ↑ |
| **Ratio de Cobertura** | Provisiones sobre cartera en mora | 83,52% ↓ |
| **Costo de Riesgo** | Gasto en provisiones sobre cartera promedio | 8,74% ↓ |

---

## ⚙️ Requisitos previos

- Power BI Desktop (versión mayo 2024 o superior)
- SQL Server con la base de datos `RiesgoCrediticioProyecto2` restaurada
- Credenciales de acceso al servidor local configuradas en Power Query

---

## 📄 Fuente de datos

Datos sintéticos generados para fines académicos y de portafolio.
La estructura del modelo replica estándares reales de información financiera regulatoria en Ecuador (SEPS / SBS).

---

## 👤 Autor

**Diego L. Villavicencio Merino**
Economista | Analista de Datos

[![github](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/diegovillavicencio1208-ops)
[![linkedin](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/diegovillavicenciodl/)

---
