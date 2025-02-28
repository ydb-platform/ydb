PRAGMA ydb.OptShuffleElimination = 'true';
#!/bin/bash

DIRECTORY="./"  # Замените на путь к вашей директории

for FILE in "$DIRECTORY"/*; do
  if [ -f "$FILE" ]; then  # Проверяем, что это файл
    { echo "PRAGMA ydb.OptShuffleElimination = 'true';"; cat "$FILE"; } > "$FILE.tmp" && mv "$FILE.tmp" "$FILE"
  fi
done