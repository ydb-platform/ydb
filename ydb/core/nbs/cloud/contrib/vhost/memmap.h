#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/mman.h>

#ifdef __cplusplus
extern "C" {
#endif

struct vhd_memory_map;

struct vhd_memory_map *vhd_memmap_new(int (*map_cb)(void *, size_t),
                                      int (*unmap_cb)(void *, size_t));
struct vhd_memory_map *vhd_memmap_dup(struct vhd_memory_map *mm);
struct vhd_memory_map *vhd_memmap_dup_remap(struct vhd_memory_map *mm);

size_t vhd_memmap_max_memslots(void);

int vhd_memmap_add_slot(struct vhd_memory_map *mm, uint64_t gpa, uint64_t uva,
                        size_t size, int fd, off_t offset, bool preserve_fd);
int vhd_memmap_del_slot(struct vhd_memory_map *mm, uint64_t gpa, uint64_t uva,
                        size_t size);

void vhd_memmap_ref(struct vhd_memory_map *mm);
void vhd_memmap_unref(struct vhd_memory_map *mm);

void *gpa_range_to_ptr(struct vhd_memory_map *mm, uint64_t gpa, size_t len);
void *uva_to_ptr(struct vhd_memory_map *mm, uint64_t uva);
#define TRANSLATION_FAILED ((uint64_t)-1)
uint64_t ptr_to_gpa(struct vhd_memory_map *mm, void *ptr);

#ifdef __cplusplus
}
#endif
