#!/bin/bash

collect_macros() {
    local macros_file="$1"

    find . -path "*/base/poco/*" -type f \( -name "*.h" -o -name "*.hpp" -o -name "*.cpp" \) -exec grep -Eoh '#\s*define\s+[pP][oO][cC][oO][a-zA-Z0-9_]*' {} + | \
    sed -E 's/#\s*define\s+([pP][oO][cC][oO][a-zA-Z0-9_]*)/\1/' | \
    sort | uniq > "$macros_file"
}

rename_macros_globally() {
    local macros_file="$1"

    while read -r macro; do
        if [ "${macro#CHDB_}" = "$macro" ]; then
            new_macro="CHDB_$macro"
            echo "Renaming $macro to $new_macro..."

            find . -path "*/base/poco/*" -type f \( -name "*.h" -o -name "*.hpp" -o -name "*.cpp" \) -exec sed -i "s/\b$macro\b/$new_macro/g" {} +
        fi
    done < "$macros_file"
}

macros_file=$(mktemp)

collect_macros "$macros_file"

rename_macros_globally "$macros_file"

rm "$macros_file"
