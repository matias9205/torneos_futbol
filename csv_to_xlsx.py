import os
import pandas as pd

BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\torneos_primera_division_arg"
csvs_dir_url = BASE_DIR+"\\CSV"
xlsx_dir_url = BASE_DIR+"\\XLSX"

csvs_dir = os.listdir(csvs_dir_url)
xlsx_dir = "XLSX"

folder_path = os.path.join(BASE_DIR, xlsx_dir)
if not os.path.exists(folder_path):
    os.makedirs(folder_path)  # Crear la carpeta
    print(f"Carpeta '{folder_path}' creada en {folder_path}")
else:
    print(f"Carpeta '{folder_path}' ya existe en {BASE_DIR}")

def convert_csv_to_xlsx(dir_):
    for file_ in dir_:
        print("-------------------------------------------------------------------------")
        data = pd.read_csv(os.path.join(csvs_dir_url, file_))
        xlsx_file = os.path.join(xlsx_dir_url, file_.replace(".csv", ".xlsx"))
        writer = pd.ExcelWriter(xlsx_file, engine='xlsxwriter')
        data.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        (max_row, max_col) = data.shape
        column_settings = []
        for header in data.columns:
            column_settings.append({'header': header})
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        worksheet.set_column(0, max_col - 1, 12)
        writer.close()
        print(f"{xlsx_file} was created successfully")

if __name__ == '__main__':
    convert_csv_to_xlsx(csvs_dir)