# Import necessary libraries
import csv
import json
from data_processing import Data

# Define file paths
json_path = '/home/nicholas10/pipeline/raw/dados_empresaA.json'
csv_path = '/home/nicholas10/pipeline/raw/dados_empresaB.csv'

# Create Data objects for enterprise A (from JSON) and enterprise B (from CSV)
enterprise_a = Data(json_path, 'json')
enterprise_b = Data(csv_path, 'csv')

# Extract columns from both datasets
print("Columns in enterprise A (JSON):", enterprise_a.get_columns())
print("Columns in enterprise B (CSV):", enterprise_b.get_columns())

# Define key mapping for column transformation
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

# Transform columns in enterprise B using the key mapping
enterprise_b.transform_columns(key_mapping)
print("Transformed columns in enterprise B:", enterprise_b.get_columns())

# Merge data from enterprise A and transformed enterprise B
merge = Data.join_data(enterprise_a, enterprise_b)
print("Merged data:")
print(merge.data)

# Save merged data to a CSV file
merged_data_path = '/home/nicholas10/pipeline/processed_data/combined_data.csv'
merge.save_data(merged_data_path)
print("Merged data saved to:", merged_data_path)
