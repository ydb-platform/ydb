set -xue
sed -e 's|operator "" |operator ""|g' cpp/src/arrow/vendored/datetime/date.h -i
sed -e 's|operator "" |operator ""|g' cpp/src/arrow/vendored/string_view.hpp -i
