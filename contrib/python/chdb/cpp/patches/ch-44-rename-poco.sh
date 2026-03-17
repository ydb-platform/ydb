# Find header files specifically within */base/poco/*/include/Poco directories and apply the renaming using sed
find . -path "*/base/poco/*/include/Poco/*" -type f \( -name "*.h" -o -name "*.hpp" \) -exec sed -i -E '
    # Match the include guard pattern and prepend CHDB_
    s/\b([A-Za-z0-9_]+_INCLUDED)\b/CHDB_\1/g
' {} +

find . -type f -name "*.*" -exec sed -i \
    -e 's|include <Poco/|include <CHDBPoco/|g' \
    -e 's|include "Poco/|include "CHDBPoco/|g' {} +

find . -type f -name "*.*" -exec sed -i \
    -e 's/\bPoco::/CHDBPoco::/g' \
    -e 's/\bnamespace Poco\b/namespace CHDBPoco/g' {} +

find . -type d -path "*/base/poco/*/include/Poco" -exec bash -c '
    for dir; do
		mv -- "$dir" "${dir%Poco}CHDBPoco"
    done
' bash {} +
