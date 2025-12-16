#!/bin/sh

set -xue

LIB_ROOT="contrib/libs/apache/arrow_next"
TMP_PATTERNS_FILE=".vendored_ignore_patterns.sed"

cat > "$TMP_PATTERNS_FILE" <<EOF
arrow/vendored/xxhash.h
arrow/vendored/xxhash/xxhash.h
arrow/vendored/double-conversion/double-conversion.h
arrow/vendored/uriparser/Uri.h
arrow/vendored/fast_float/fast_float.h
EOF

find . -type f \( -name '*.h' -o -name '*.hpp' -o -name '*.cpp' -o -name '*.cc' -o -name '*.c' \) | while read file; do
    # Replace generated/*.h includes in both forms
    sed -i -E \
        -e "s|^#[[:space:]]*include[[:space:]]+\"generated/([^\"]+)\"|#include \"$LIB_ROOT/cpp/src/generated/\1\"|g" \
        -e "s|^#[[:space:]]*include[[:space:]]+<generated/([^>]+)>|#include <$LIB_ROOT/cpp/src/generated/\1>|g" \
        "$file"

    # Replace specific config and version headers (not in cpp/src)
    sed -i -E \
        -e "s|^#[[:space:]]*include[[:space:]]+\"arrow/util/config\.h\"|#include \"$LIB_ROOT/src/arrow/util/config.h\"|g" \
        -e "s|^#[[:space:]]*include[[:space:]]+\"arrow/util/config_internal\.h\"|#include \"$LIB_ROOT/src/arrow/util/config_internal.h\"|g" \
        -e "s|^#[[:space:]]*include[[:space:]]+\"parquet/parquet_version\.h\"|#include \"$LIB_ROOT/src/parquet/parquet_version.h\"|g" \
        "$file"

    # Quotes: replace arrow/ and parquet/, skipping vendored
    sed -i -E \
        -e "/^#[[:space:]]*include[[:space:]]+\"arrow\/vendored\//! s|^#[[:space:]]*include[[:space:]]+\"arrow/|#include \"$LIB_ROOT/cpp/src/arrow/|g" \
        -e "s|^#[[:space:]]*include[[:space:]]+\"parquet/|#include \"$LIB_ROOT/cpp/src/parquet/|g" \
        "$file"

    # Angle brackets: same
    sed -i -E \
        -e "/^#[[:space:]]*include[[:space:]]+<arrow\/vendored\//! s|^#[[:space:]]*include[[:space:]]+<arrow/|#include <$LIB_ROOT/cpp/src/arrow/|g" \
        -e "s|^#[[:space:]]*include[[:space:]]+<parquet/|#include <$LIB_ROOT/cpp/src/parquet/|g" \
        "$file"

    # Vendored quoted includes
    if grep -qE '^#[[:space:]]*include[[:space:]]+"arrow/vendored/' "$file"; then
        grep -E '^#[[:space:]]*include[[:space:]]+"arrow/vendored/' "$file" | while read -r line; do
            include_path=$(echo "$line" | sed -E 's|^[[:space:]]*#[[:space:]]*include[[:space:]]+"([^"]+)".*|\1|')
            if grep -Fxq "$include_path" "$TMP_PATTERNS_FILE"; then
                continue
            fi
            new_path="$LIB_ROOT/cpp/src/$include_path"
            sed -i "s|\"$include_path\"|\"$new_path\"|" "$file"
        done
    fi

    # Vendored angle brackets
    if grep -qE '^#[[:space:]]*include[[:space:]]+<arrow/vendored/' "$file"; then
        grep -E '^#[[:space:]]*include[[:space:]]+<arrow/vendored/' "$file" | while read -r line; do
            include_path=$(echo "$line" | sed -E 's|^[[:space:]]*#[[:space:]]*include[[:space:]]+<([^>]+)>.*|\1|')
            if grep -Fxq "$include_path" "$TMP_PATTERNS_FILE"; then
                continue
            fi
            new_path="$LIB_ROOT/cpp/src/$include_path"
            sed -i "s|<$include_path>|<$new_path>|" "$file"
        done
    fi
done

rm -f "$TMP_PATTERNS_FILE"
