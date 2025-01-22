(
    find . -type f -name '*.cpp'
    find . -type f -name '*.h'
) | while read l; do
    sed -i 's|#include <__threading_support>|#include <__thread/support.h>|g' ${l}
done
