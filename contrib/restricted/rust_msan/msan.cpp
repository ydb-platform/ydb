#include <stdint.h>
#include <cstddef>

typedef size_t uptr;
typedef uint32_t u32;

// defined in clang{XX}-rt/msan
extern "C" void __msan_set_alloca_origin_with_descr(void *a, uptr size, u32 *id_ptr, char *descr);
extern "C" void __msan_unpoison(void *a, uptr size);


extern "C" void __safe_msan_set_alloca_origin_with_descr(void *a, uptr size, u32 *id_ptr, char *descr) {
  __msan_set_alloca_origin_with_descr(a, size, id_ptr, descr);
  __msan_unpoison(a, size);
}


extern "C" void* malloc(size_t size);
extern "C" void *__safe_malloc(size_t size) {
    void* ret = malloc(size);
    __msan_unpoison(ret, size);
    return ret;
}
