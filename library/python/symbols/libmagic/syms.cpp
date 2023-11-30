#include <contrib/libs/libmagic/src/magic.h>

#include <library/python/symbols/registry/syms.h>

BEGIN_SYMS("magic")
SYM(magic_open)
SYM(magic_close)
SYM(magic_error)
SYM(magic_errno)
SYM(magic_file)
SYM(magic_buffer)
SYM(magic_load)
SYM(magic_setflags)
SYM(magic_check)
SYM(magic_compile)
SYM(magic_descriptor)
SYM(magic_list)
SYM(magic_version)
END_SYMS()
