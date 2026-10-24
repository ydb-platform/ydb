/* Self-contained malloc family: mmap bump allocator with headers.
   Linked into the exe (exe) or into libm_alloc.so (exe_so). */
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>

#define MAGIC 0xFA57A110CULL
#define REGION (256u << 20)
struct hdr { uint64_t magic; size_t size; uint64_t pad[2]; };
static char *base, *cur;
static volatile int lock;
unsigned long ta_allocs, ta_frees, ta_foreign;

int ta_is_ours(const void *p) {
    return base && (const char *)p >= base + sizeof(struct hdr) && (const char *)p < cur &&
           ((const struct hdr *)p - 1)->magic == MAGIC;
}
static void *alloc_aligned(size_t align, size_t size) {
    if (align < 16) align = 16;
    while (__atomic_exchange_n(&lock, 1, __ATOMIC_ACQUIRE)) {}
    if (!base) {
        base = mmap(0, REGION, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        cur = base;
    }
    uintptr_t p = ((uintptr_t)cur + sizeof(struct hdr) + align - 1) & ~(uintptr_t)(align - 1);
    cur = (char *)p + ((size + 15) & ~(size_t)15);
    ++ta_allocs;
    __atomic_store_n(&lock, 0, __ATOMIC_RELEASE);
    struct hdr *h = (struct hdr *)p - 1;
    h->magic = MAGIC;
    h->size = size;
    return (void *)p;
}
void *malloc(size_t n) { return alloc_aligned(16, n); }
void free(void *p) {
    if (!p) return;
    if (ta_is_ours(p)) __atomic_add_fetch(&ta_frees, 1, __ATOMIC_RELAXED);
    else __atomic_add_fetch(&ta_foreign, 1, __ATOMIC_RELAXED);
}
void *calloc(size_t a, size_t b) {
    void *p = malloc(a * b);
    memset(p, 0, a * b); /* mmap memory is zero, but be explicit */
    return p;
}
size_t malloc_usable_size(void *p) { return ta_is_ours(p) ? ((struct hdr *)p - 1)->size : 0; }
void *realloc(void *p, size_t n) {
    if (!p) return malloc(n);
    void *q = malloc(n);
    if (ta_is_ours(p)) {
        size_t o = ((struct hdr *)p - 1)->size;
        memcpy(q, p, o < n ? o : n);
    } else {
        __atomic_add_fetch(&ta_foreign, 1, __ATOMIC_RELAXED);
    }
    free(p);
    return q;
}
void *memalign(size_t a, size_t n) { return alloc_aligned(a, n); }
void *aligned_alloc(size_t a, size_t n) { return alloc_aligned(a, n); }
int posix_memalign(void **r, size_t a, size_t n) {
    *r = alloc_aligned(a, n);
    return 0;
}
void *valloc(size_t n) { return alloc_aligned(4096, n); }
void *pvalloc(size_t n) { return alloc_aligned(4096, (n + 4095) & ~(size_t)4095); }
