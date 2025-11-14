#!/bin/sh

set -xue

# Исправляем относительные инклуды Arrow на полные пути
# Сначала заменяем исключения, чтобы избежать двойных замен
find . -type f | grep -v patches | grep -E '\.(h|cc|cpp|c)$' | while read l; do
    # Сначала заменяем исключения (специальные случаи)
    sed -e 's|#include "arrow/util/config\.h"|#include "contrib/libs/apache/arrow_next/src/arrow/util/config.h"|g' \
        -e 's|#include "arrow/util/config_internal\.h"|#include "contrib/libs/apache/arrow_next/src/arrow/util/config_internal.h"|g' \
        -e 's|#include "parquet/parquet_version\.h"|#include "contrib/libs/apache/arrow_next/src/parquet/parquet_version.h"|g' \
        "${l}" > "${l}.tmp" && mv "${l}.tmp" "${l}"
    
    # Потом заменяем общие случаи
    sed -e 's|#include "arrow/|#include "contrib/libs/apache/arrow_next/cpp/src/arrow/|g' \
        -e 's|#include "parquet/|#include "contrib/libs/apache/arrow_next/cpp/src/parquet/|g' \
        "${l}" > "${l}.tmp" && mv "${l}.tmp" "${l}"
done
