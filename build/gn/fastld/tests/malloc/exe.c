#include <dirent.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <malloc.h>
extern unsigned long ta_allocs, ta_frees, ta_foreign;
int ta_is_ours(const void *p);
void *m_lib_malloc(void);
char *m_lib_strdup(void);
void m_lib_free(void *p);

static void check(const char *what, void *p) {
    printf("%-28s %s\n", what, !p ? "NULL" : ta_is_ours(p) ? "ours" : "FOREIGN");
    free(p);
}
static void *thr(void *arg) {
    (void)arg;
    check("strdup in pthread", strdup("t"));
    return 0;
}
int main(void) {
    check("malloc", malloc(10));
    check("strdup", strdup("hello"));
    check("strndup", strndup("hello", 3));
    char *s = 0;
    asprintf(&s, "%d-%s", 42, "x");
    check("asprintf", s);
    char buf[] = "line1\nline2\n";
    FILE *f = fmemopen(buf, sizeof buf - 1, "r");
    char *line = 0;
    size_t cap = 0;
    getline(&line, &cap, f);
    fclose(f);
    check("getline", line);
    char *ms = 0;
    size_t ml = 0;
    FILE *m = open_memstream(&ms, &ml);
    fprintf(m, "%s", "memstream data");
    fclose(m);
    check("open_memstream", ms);
    check("realpath(., NULL)", realpath(".", 0));
    check("get_current_dir_name", get_current_dir_name());
    struct addrinfo hints = {0}, *ai = 0;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo("127.0.0.1", "80", &hints, &ai);
    printf("%-28s %s\n", "getaddrinfo", rc ? "failed" : ta_is_ours(ai) ? "ours" : "FOREIGN");
    if (!rc) freeaddrinfo(ai);
    struct dirent **names = 0;
    int n = scandir("/proc/self", &names, 0, alphasort);
    printf("%-28s %s\n", "scandir array", n <= 0 ? "failed" : ta_is_ours(names) ? "ours" : "FOREIGN");
    for (int i = 0; i < n; ++i) free(names[i]);
    free(names);
    check("lib malloc", m_lib_malloc());
    check("lib strdup", m_lib_strdup());
    m_lib_free(malloc(5));
    void *p = 0;
    posix_memalign(&p, 64, 100);
    printf("posix_memalign 64 aligned: %s\n", ((uintptr_t)p & 63) == 0 ? "yes" : "no");
    check("posix_memalign", p);
    p = aligned_alloc(256, 512);
    printf("aligned_alloc 256 aligned: %s\n", ((uintptr_t)p & 255) == 0 ? "yes" : "no");
    check("aligned_alloc", p);
    p = memalign(4096, 10);
    printf("memalign 4096 aligned: %s\n", ((uintptr_t)p & 4095) == 0 ? "yes" : "no");
    check("memalign", p);
    char *r = strdup("realloc-me");
    r = realloc(r, 1000);
    printf("realloc kept data: %s, usable>=1000: %s\n", r, malloc_usable_size(r) >= 1000 ? "yes" : "no");
    check("realloc", r);
    int *z = calloc(100, sizeof(int));
    int nz = 0;
    for (int i = 0; i < 100; ++i) nz += z[i] != 0;
    printf("calloc zeroed: %s\n", nz ? "no" : "yes");
    check("calloc", z);
    pthread_t t;
    pthread_create(&t, 0, thr, 0);
    pthread_join(t, 0);
    printf("allocator used: %s\n", ta_allocs > 20 ? "yes" : "no");
    printf("foreign frees: %lu\n", ta_foreign);
    return 0;
}
