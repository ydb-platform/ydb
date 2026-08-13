#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/resource.h>

static int (*real_setrlimit)(__rlimit_resource_t, const struct rlimit *);

static void force_unlimited_core(void) {
    struct rlimit inf = {RLIM_INFINITY, RLIM_INFINITY};
    if (!real_setrlimit) {
        real_setrlimit =
            (int (*)(__rlimit_resource_t, const struct rlimit *))dlsym(RTLD_NEXT, "setrlimit");
    }
    if (real_setrlimit) {
        real_setrlimit(RLIMIT_CORE, &inf);
    }
}

int setrlimit(__rlimit_resource_t resource, const struct rlimit *rlim) {
    if (!real_setrlimit) {
        real_setrlimit =
            (int (*)(__rlimit_resource_t, const struct rlimit *))dlsym(RTLD_NEXT, "setrlimit");
    }
    if (resource == RLIMIT_CORE) {
        struct rlimit inf = {RLIM_INFINITY, RLIM_INFINITY};
        return real_setrlimit(resource, &inf);
    }
    return real_setrlimit(resource, rlim);
}

__attribute__((constructor)) static void init(void) {
    force_unlimited_core();
}
