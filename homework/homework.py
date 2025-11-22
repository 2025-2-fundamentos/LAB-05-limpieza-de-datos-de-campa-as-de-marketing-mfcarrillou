"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
from pathlib import Path
import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_date: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    # Crear directorio de salida si no existe
    output_dir = Path("files/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Leer todos los archivos zip en la carpeta input
    input_dir = Path("files/input")
    zip_files = glob.glob(str(input_dir / "*.zip"))

    # Lista para almacenar todos los dataframes
    all_data = []

    # Procesar cada archivo zip
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            # Obtener la lista de archivos CSV dentro del zip
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            
            for csv_file in csv_files:
                # Leer el CSV directamente desde el zip
                with z.open(csv_file) as f:
                    df = pd.read_csv(f)
                    all_data.append(df)

    # Concatenar todos los dataframes
    data = pd.concat(all_data, ignore_index=True)

    # Crear client_id si no existe
    if 'client_id' not in data.columns:
        data['client_id'] = range(len(data))
    
    # Construir dataframe para client.csv
    client_df = pd.DataFrame()
    client_df['client_id'] = data['client_id']
    client_df['age'] = data['age']
    
    #--Limpiar job
    client_df['job'] = data['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    
    client_df['marital'] = data['marital']
    
    #--Limpiar education
    client_df['education'] = data['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    
    #--Convertir credit_default
    client_df['credit_default'] = (data['credit_default'] == 'yes').astype(int)
    
    #--Convertir mortgage
    client_df['mortgage'] = (data['mortgage'] == 'yes').astype(int)

    # Construir dataframe para campaign.csv
    campaign_df = pd.DataFrame()
    campaign_df['client_id'] = data['client_id']
    campaign_df['number_contacts'] = data['number_contacts']
    campaign_df['contact_duration'] = data['contact_duration']
    campaign_df['previous_campaign_contacts'] = data['previous_campaign_contacts']
    
    #--Convertir previous_outcome
    campaign_df['previous_outcome'] = (data['previous_outcome'] == 'success').astype(int)
    
    #--Convertir campaign_outcome
    campaign_df['campaign_outcome'] = (data['campaign_outcome'] == 'yes').astype(int)
    
    #--Crear last_contact_date
    #----Mapear nombres de meses a números
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    month_num = data['month'].map(month_map)
    day_num = data['day'].astype(str).str.zfill(2)
    campaign_df['last_contact_date'] = '2022-' + month_num + '-' + day_num

    # Construir dataframe para economics.csv
    economics_df = pd.DataFrame()
    economics_df['client_id'] = data['client_id']
    economics_df['cons_price_idx'] = data['cons_price_idx']
    economics_df['euribor_three_months'] = data['euribor_three_months']

    # Guardar los dataframes en archivos CSV
    client_df.to_csv(output_dir/'client.csv', index=False)
    campaign_df.to_csv(output_dir/'campaign.csv', index=False)
    economics_df.to_csv(output_dir/'economics.csv', index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()
