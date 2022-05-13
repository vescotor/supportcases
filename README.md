# supportcases

Spark scripts:
- Script para el procesamiento y transformación inicial de todo el dataset histórico. El cual crea dos datasets, uno histórico para ser catalogado por el servicio de AWS Glue, mientras que el otro dataset contiene los datos para entrenar el modelo.
- Script de entrenamiento del modelo de clasificación.
- Script para transformar y añadir datos nuevos.

Jupyter Notebook:
- Contiene información relevante del dataset.
- Contiene los modelos para clasificar los casos en Producto y Sub-producto.

Función Lambda:
- Script utilizado para la función Lambda que conecta el endpoint de SageMaker y API Gateway.

