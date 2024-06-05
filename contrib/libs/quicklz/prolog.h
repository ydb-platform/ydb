#ifndef prolog_h_as78d5f67sdf
#define prolog_h_as78d5f67sdf

#include "table.h"

#define RENAME(a)              DO_RENAME(QLZ_YVERSION, COMPRESSION_LEVEL, STREAMING_MODE, a)
#define DO_RENAME(a, b, c, d)  DO_RENAME2(a, b, c, d)
#define DO_RENAME2(a, b, c, d) yqlz_ ## a ## _ ## b ## _ ## c ## _ ## d

#endif

#define qlz_decompress        RENAME(decompress)
#define qlz_compress          RENAME(compress)
#define qlz_size_decompressed RENAME(size_decompressed)
#define qlz_size_compressed   RENAME(size_compressed)
#define qlz_get_setting       RENAME(get_setting)
#define qlz_table             RENAME(table)
#define qlz_compress_core     RENAME(compress_core)
#define qlz_decompress_core   RENAME(decompress_core)
#define hash_func             RENAME(hash_func)
#define fast_read             RENAME(fast_read)
#define fast_write            RENAME(fast_write)
#define reset_state           RENAME(reset_state)
#define memcpy_up             RENAME(memcpy_up)
#define update_hash           RENAME(update_hash)
#define fast_read_safe        RENAME(fast_read_safe)
#define update_hash_upto      RENAME(update_hash_upto)
#define same                    RENAME(same)
#define reset_table_compress    RENAME(reset_table_compress)
#define reset_table_decompress  RENAME(reset_table_decompress)
#define hashat                  RENAME(hashat)
#define qlz_size_header         RENAME(qlz_size_header)
#define qlz_state_compress      RENAME(qlz_state_compress)
#define qlz_state_decompress    RENAME(qlz_state_decompress)
#define qlz_hash_compress       RENAME(qlz_hash_compress)
#define qlz_hash_decompress     RENAME(qlz_hash_decompress)
