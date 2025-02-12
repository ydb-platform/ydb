sed -i 's/#\( *\)include <__pstl_algorithm>/#error #\1include <__pstl_algorithm>/' include/algorithm
sed -i 's/#\( *\)include <__pstl_execution>/#error #\1include <__pstl_execution>/' include/execution
sed -i 's/#\( *\)include <__external_threading>/#error #\1#include <__external_threading>/' include/__thread/support/external.h
sed -i 's/#\( *\)include <__pstl_memory>/#error #\1include <__pstl_memory>/' include/memory
sed -i 's/#\( *\)include <__pstl_numeric>/#error #\1include <__pstl_numeric>/' include/numeric
sed -i 's|#\( *\)include <__locale_dir/locale_base_api/solaris.h>|#error #\1include <__locale_dir/locale_base_api/solaris.h>|' include/__locale_dir/locale_base_api.h
