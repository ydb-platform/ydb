#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>

#include <cerrno>
#include <cstdint>
#include <climits>
#include <cstdio>

#define SYSCALL_MMAP2_UNIT 4096ULL
#define UNIT SYSCALL_MMAP2_UNIT
#define OFF_MASK ((-0x2000ULL << (8 * sizeof(long) - 1)) | (UNIT - 1))

void* Mmap(void* start, size_t len, int prot, int flags, int fd, off_t off)
{
    void* ret = (void*)-1;
    if (off & OFF_MASK) {
        errno = EINVAL;
        return ret;
    }
    if (len >= PTRDIFF_MAX) {
        errno = ENOMEM;
        return ret;
    }
#ifdef SYS_mmap2
    ret = (void*)syscall(SYS_mmap2, start, len, prot, flags, fd, off / UNIT);
#else
    ret = (void*)syscall(SYS_mmap, start, len, prot, flags, fd, off);
#endif
    /* Fixup incorrect EPERM from kernel. */
    if (ret == (void*)-1 && errno == EPERM && !start && (flags & MAP_ANON) && !(flags & MAP_FIXED)) {
        errno = ENOMEM;
        return (void*)-1;
    }

    return ret;
}

void* mmap(void* start, size_t len, int prot, int flags, int fd, off_t off)
{
    auto res = Mmap(start, len, prot, flags, fd, off);
    if (res == (void*)-1 && errno == ENOMEM) {
        fprintf(stderr, "mmap failed with ENOMEM\n");
        _exit(2);
    }
    return res;
}
