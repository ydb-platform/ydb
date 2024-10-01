# NOT USED ANYWHERE, YOU CAN DELETE THIS IF YOU KNOW WHAT ARE YOU DOING
import os
import csv
import urllib.parse

# Функция для декодирования percent-encoded строки
def decode_percent_encoded_string(encoded_string):
    return urllib.parse.unquote(encoded_string)

# Функция для декодирования CSV файла
def decode_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as infile, open(file_path + '.decoded', mode='w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile,escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL, doublequote=True)

        for row in reader:
            decoded_row = [decode_percent_encoded_string(cell) for cell in row]
            writer.writerow(decoded_row)

# Функция для обработки всех CSV файлов в директории
def decode_all_csv_files_in_directory(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            print(f"Processing file: {file_path}")
            decode_csv(file_path)
            os.replace(file_path + '.decoded', file_path)

def main(): 
    directory_path = 'place_your_path_here'
    decode_all_csv_files_in_directory(directory_path)
    
    
if __name__ == "__main__":
    main()
