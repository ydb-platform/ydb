#ifdef __linux__
#include <sys/vfs.h>
#include <linux/magic.h>
#include <dlfcn.h>

extern "C" int statfs(const char *path, struct statfs *buf) {
    using real_statfs_t = int (*)(const char *, struct statfs *);
    auto real_statfs = reinterpret_cast<real_statfs_t>(dlsym(RTLD_NEXT, "statfs"));
    if (!real_statfs) {
        return -1;
    }
    int result = real_statfs(path, buf);
    if (result == 0) {
        buf->f_type = NFS_SUPER_MAGIC;
    }
    return result;
}
#endif
