find . -type f | while read l; do
    sed -e 's|include <valgrind.h>|include <valgrind/valgrind.h>|' \
        -e 's|include <memcheck.h>|include <valgrind/memcheck.h>|' \
        -i "${l}"
done
