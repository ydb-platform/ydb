#include "table.h"

#define STR(a) STR2(a)
#define STR2(a) STR3(a)
#define STR3(a) #a

struct TQuickLZMethods qlz_table = {
    qlz_size_decompressed,
    qlz_size_compressed,
    qlz_decompress,
    qlz_compress,
    qlz_get_setting,
    STR(QLZ_YVERSION) "-" STR(COMPRESSION_LEVEL) "-" STR(STREAMING_MODE)
};

#undef STR
#undef STR2
#undef STR3
#undef QLZ_VERSION_MAJOR
#undef QLZ_VERSION_MINOR
#undef QLZ_VERSION_REVISION
#undef test_rle
#undef speedup_incompressible
#undef __inline
#undef X86X64
#undef HASH_ENTRIES
#undef AND
#undef HASH_SIZE
#undef STREAMING_MODE_VALUE
#undef STREAMING_MODE_ROUNDED
#undef SCRATCH_COMPRESS
#undef SCRATCH_DECOMPRESS
#undef X86X64
#undef MINOFFSET
#undef UNCONDITIONAL_MATCHLEN
#undef UNCOMPRESSED_END
#undef CWORD_LEN
#undef QLZ_HEADER
#undef QLZ_VERSION_MAJOR
#undef QLZ_VERSION_MINOR
#undef QLZ_VERSION_REVISION
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_POINTERS
#undef QLZ_HASH_VALUES
#undef QLZ_ALIGNMENT_PADD
#undef QLZ_BUFFER_COUNTER
#undef QLZ_SCRATCH_COMPRESS
#undef QLZ_SCRATCH_DECOMPRESS
