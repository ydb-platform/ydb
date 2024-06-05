#define QLZ_COMPRESSION_LEVEL COMPRESSION_LEVEL
#define QLZ_STREAMING_BUFFER  STREAMING_MODE
#define QLZ_YVERSION 1_51

#include "prolog.h"
#include "quicklz.c"

#define STR(a) STR2(a)
#define STR2(a) STR3(a)
#define STR3(a) #a

static struct TQuickLZMethods qlz_table = {
    qlz_size_decompressed,
    qlz_size_compressed,
    qlz_decompress,
    qlz_compress,
    qlz_get_setting,
    STR(QLZ_YVERSION) "-" STR(COMPRESSION_LEVEL) "-" STR(STREAMING_MODE)
};

#include "epilog.h"

#undef STR
#undef STR2
#undef STR3
#undef X86X64
#undef MINOFFSET
#undef UNCONDITIONAL_MATCHLEN_COMPRESSOR
#undef UNCONDITIONAL_MATCHLEN_DECOMPRESSOR
#undef UNCOMPRESSED_END
#undef CWORD_LEN
#undef OFFSET_BASE
#undef CAST
#undef OFFSET_BASE
#undef CAST
#undef qlz_likely
#undef qlz_unlikely
#undef qlz_likely
#undef qlz_unlikely
#undef QLZ_HEADER
#undef QLZ_COMPRESSION_LEVEL
#undef QLZ_STREAMING_BUFFER
#undef QLZ_VERSION_MAJOR
#undef QLZ_VERSION_MINOR
#undef QLZ_VERSION_REVISION
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_PTR_64
