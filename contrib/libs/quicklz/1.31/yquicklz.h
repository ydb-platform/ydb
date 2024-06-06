#include <sys/types.h>

#if defined(__cplusplus)
extern "C" {
#endif

size_t qlz_decompress(const char *source, void *destination, char *scratch);
size_t qlz_compress(const void *source, char *destination, size_t size, char *scratch);
size_t qlz_size_decompressed(const char *source);
size_t qlz_size_compressed(const char *source);
int qlz_get_setting(int setting);

#if defined(__cplusplus)
}
#endif
