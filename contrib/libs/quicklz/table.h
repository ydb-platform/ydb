#ifndef table_h_asd78567asdf
#define table_h_asd78567asdf

#include <string.h>

#if defined(__cplusplus)
extern "C" {
#endif

struct TQuickLZMethods {
    size_t (*SizeDecompressed)(const char* source);
    size_t (*SizeCompressed)(const char* source);
    size_t (*Decompress)(const char* source, void* destination, char* scratch_decompress);
    size_t (*Compress)(const void* source, char* destination, size_t size, char* scratch_compress);
    int    (*Setting)(int setting);

    const char* Name;
};

struct TQuickLZMethods* GetLzqTable(unsigned ver, unsigned level, unsigned buf);

#if defined(__cplusplus)
}
#endif

#endif
