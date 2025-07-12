Hadoop MapReduce WordCount - Simulación

Descripción
Implementación simplificada de un job de Hadoop MapReduce para realizar conteo de palabras (WordCount) sobre documentos de texto y archivos PDF. La simulación incluye todas las fases del proceso MapReduce: Map, Shuffle & Sort, y Reduce.
Características

Procesamiento de PDFs: Extrae texto automáticamente de archivos PDF
Simulación completa de MapReduce: Implementa las tres fases principales
Análisis de rendimiento: Compara tiempos de ejecución con diferentes números de reducers
Interfaz interactiva: Permite elegir entre archivos de ejemplo o subir PDFs propios
Estadísticas detalladas: Muestra palabras más frecuentes y métricas de rendimiento

Requisitos

Python 3.x
Google Colab (recomendado)
Librería PyPDF2 (se instala automáticamente)

Instalación y Uso
Opción 1: Google Colab (Recomendado)

Abre Google Colab: https://colab.research.google.com/
Crea un nuevo notebook
Copia y pega todo el código en una celda
Ejecuta con Shift + Enter

Opción 2: Local
bashpip install PyPDF2
python mapreduce_wordcount.py
Instrucciones de Ejecución

Ejecuta el código completo
Selecciona una opción:

Escribe 1 para usar archivos de ejemplo
Escribe 2 para subir tus propios PDFs


Si eliges opción 2:

Se abrirá un botón "Elegir archivos"
Selecciona uno o varios archivos PDF
Espera a que se procesen


Observa los resultados:

Análisis con 1, 2 y 4 reducers
Top 10 palabras más frecuentes
Comparación de tiempos de ejecución
Estadísticas finales



Arquitectura del Sistema
Componentes Principales
1. Extractor de PDFs
pythondef extract_text_from_pdf(pdf_path):
    # Extrae texto de archivos PDF usando PyPDF2
2. Simulador de HDFS
pythonclass SimpleHDFS:
    # Simula el sistema de archivos distribuido de Hadoop
3. Mapper
pythondef mapper(text):
    # Tokeniza texto y emite pares (palabra, 1)
4. Reducer
pythondef reducer(word_pairs):
    # Agrega conteos por palabra
5. Framework MapReduce
pythonclass MapReduceFramework:
    # Coordina las fases Map, Shuffle & Sort, y Reduce
Flujo de Ejecución

Carga de Archivos: Los documentos se cargan en el HDFS simulado
Fase Map: Cada documento se tokeniza en pares (palabra, 1)
Shuffle & Sort: Los pares se ordenan por palabra
Fase Reduce: Se suman los conteos por palabra
Análisis: Se ejecuta el proceso con diferentes números de reducers

