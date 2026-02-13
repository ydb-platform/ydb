#pragma once

#include <stddef.h>

#include "memmap.h"

#ifdef __cplusplus
extern "C" {
#endif

struct vhd_memory_log;

struct vhd_memory_log *vhd_memlog_new(size_t size, int fd, off_t offset);
void vhd_memlog_free(struct vhd_memory_log *log);

void vhd_mark_range_dirty(struct vhd_memory_log *log,
                          struct vhd_memory_map *mm, void *ptr, size_t len);
void vhd_mark_gpa_range_dirty(struct vhd_memory_log *log, uint64_t gpa,
                              size_t len);

#ifdef __cplusplus
}
#endif
