set -xue
sed -e 's|operator"" |operator""|g' include/fmt/format.h -i
sed -e 's|operator"" |operator""|g' include/fmt/xchar.h -i
