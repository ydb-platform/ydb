sed -i 's/#\( *\)include <__external_threading>/#error #\1#include <__external_threading>/' include/__thread/support/external.h
sed -i 's|#\( *\)include <__locale_dir/locale_base_api/solaris.h>|#error #\1include <__locale_dir/locale_base_api/solaris.h>|' include/__locale_dir/locale_base_api.h

sed -i 's|#\( *\)include <ptrauth.h>|#error #\1include <ptrauth.h>|' src/include/overridable_function.h
sed -i 's|#\( *\)include <sys/futex.h>|#error #\1include <sys/futex.h>|' src/atomic.cpp
