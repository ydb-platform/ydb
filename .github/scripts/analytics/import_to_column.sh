
#!/bin/bash

# Параметры подключения к YDB
ENDPOINT="grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
DATABASE="/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"
SA_KEY_FILE="/home/kirrysin/fork_2/.github/scripts/my-robot-key.json"
TABLE="test_results/test_runs_column"
DIRECTORY="/home/kirrysin/fork_2/~tmp_backup/test_runs_results"

# Обрабатываем каждый .csv файл в указанной директории
for FILE in "$DIRECTORY"/*.csv; do
    if [[ -f "$FILE" ]]; then
        echo "Импортируем файл: $FILE"
        
        ydb -e "$ENDPOINT" \
            -d "$DATABASE" \
            --sa-key-file "$SA_KEY_FILE" \
            import file csv \
            -p "$TABLE" \
            "$FILE"
        
        if [[ $? -eq 0 ]]; then
            echo "Импорт файла $FILE успешно завершен."
        else
            echo "Ошибка при импорте файла $FILE." >&2
            exit 1
        fi
    fi
done

echo "Импорт всех файлов завершен."