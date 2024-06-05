#define QLZ_COMPRESSION_LEVEL COMPRESSION_LEVEL
#define QLZ_STREAMING_BUFFER  STREAMING_MODE

#include <string.h>

#if defined(__cplusplus)
extern "C" {
#endif

size_t qlz_size_decompressed(const char *source);
size_t qlz_size_compressed(const char *source);
size_t qlz_decompress(const char *source, void *destination, char *scratch_decompress);
size_t qlz_compress(const void *source, char *destination, size_t size, char *scratch_compress);
int qlz_get_setting(int setting);

#if defined(__cplusplus)
}
#endif
