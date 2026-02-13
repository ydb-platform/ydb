#include <string.h>
#include <endian.h>

#include "catomic.h"
#include "logging.h"
#include "memlog.h"
#include "memmap.h"

struct vhd_memory_log {
    unsigned long *base;
    size_t size;
};

struct vhd_memory_log *vhd_memlog_new(size_t size, int fd, off_t offset)
{
    struct vhd_memory_log *log;
    void *base;

    base = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
    if (base == MAP_FAILED) {
        VHD_LOG_ERROR("mmap(%zu, %d, %zu): %s", size, fd, offset,
                      strerror(errno));
        return NULL;
    }

    log = vhd_alloc(sizeof(*log));
    *log = (struct vhd_memory_log) {
        .base = base,
        .size = size,
    };
    return log;
}

void vhd_memlog_free(struct vhd_memory_log *log)
{
    munmap(log->base, log->size);
    vhd_free(log);
}

static void atomic_or_le_ulong(unsigned long *ptr, unsigned long mask)
{
    VHD_STATIC_ASSERT(sizeof(*ptr) == sizeof(uint64_t));
    catomic_or(ptr, htole64(mask));
}

static void bitmap_set_atomic(unsigned long *map, size_t start, size_t end)
{
    static const unsigned bits_per_word = sizeof(*map) * 8;
    size_t start_idx = start / bits_per_word;
    size_t end_idx = end / bits_per_word;
    size_t i;
    unsigned start_in_word = start % bits_per_word;
    unsigned end_in_word = end % bits_per_word;

    /* first partial word */
    if (start_in_word && start_idx < end_idx) {
        atomic_or_le_ulong(&map[start_idx], ~0UL << start_in_word);
        start_in_word = 0;
        start_idx++;
    }

    /* full words: no RMW so relaxed atomic; no endianness */
    for (i = start_idx; i < end_idx; i++) {
        catomic_set(&map[i], ~0UL);
    }

    /* last partial word */
    if (end_in_word) {
        unsigned nr_clear_bits = bits_per_word - (end_in_word - start_in_word);
        atomic_or_le_ulong(&map[end_idx],
                           (~0UL >> nr_clear_bits) << start_in_word);
    } else if (start_idx < end_idx) {
        /*
         * if there were any relaxed catomic_set's not followed by an implicit
         * full memory barrier in catomic_or, do an explicit one
         */
        smp_mb();
    }
}

#define VHOST_LOG_PAGE 0x1000

void vhd_mark_gpa_range_dirty(struct vhd_memory_log *log, uint64_t gpa,
                              size_t len)
{
    size_t start = gpa / VHOST_LOG_PAGE;
    size_t end = (gpa + len - 1) / VHOST_LOG_PAGE + 1;

    /* this is internal function, overflown ranges shouldn't reach here */
    VHD_ASSERT(gpa + len > gpa);

    if (end > log->size * 8) {
        VHD_LOG_ERROR("range 0x%zx-0x%zx beyond log size %zx", gpa,
                      gpa + len - 1, log->size);
        end = log->size * 8;
    }

    bitmap_set_atomic(log->base, start, end);
}

void vhd_mark_range_dirty(struct vhd_memory_log *log,
                          struct vhd_memory_map *mm, void *ptr, size_t len)
{
    uint64_t gpa = ptr_to_gpa(mm, ptr);
    if (gpa != TRANSLATION_FAILED) {
        vhd_mark_gpa_range_dirty(log, gpa, len);
    }
}
