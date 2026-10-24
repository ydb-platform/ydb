#include <dlfcn.h>
#include <link.h>
#include <stdio.h>
#include <string.h>
typedef const char *(*fn_t)(void);
const char *dup_sym(void) { return "exe"; }
fn_t a_next(void), a_default(void), b_next(void);
const char *b_only(void);
const char *a_marker(void);
void *a_open_plugin2(void);
static const char *yn(int c) { return c ? "yes" : "no"; }
static const char *call(fn_t f) { return f ? f() : "NULL"; }
static const char *base(const char *p) {
    const char *s = strrchr(p, '/');
    return s ? s + 1 : p;
}

int main(void);
static void *probe_addr[4];
static const char *probe_name[4] = {"libdl_a.so", "libdl_b.so", "libdl_plugin.so", 0};
static int phdr_cb(struct dl_phdr_info *info, size_t size, void *data) {
    (void)size;
    int *idx = data;
    const char *b = base(info->dlpi_name);
    int is_exe = (*idx)++ == 0;
    int covers_main = 0, covers_probe = 0, eh = 0;
    void *probe = 0;
    for (int k = 0; probe_name[k]; ++k)
        if (!strcmp(b, probe_name[k])) probe = probe_addr[k];
    for (int i = 0; i < info->dlpi_phnum; ++i) {
        const ElfW(Phdr) *ph = &info->dlpi_phdr[i];
        if (ph->p_type == PT_GNU_EH_FRAME) eh = 1;
        if (ph->p_type != PT_LOAD) continue;
        char *lo = (char *)info->dlpi_addr + ph->p_vaddr, *hi = lo + ph->p_memsz;
        if ((char *)main >= lo && (char *)main < hi) covers_main = 1;
        if (probe && (char *)probe >= lo && (char *)probe < hi) covers_probe = 1;
    }
    if (is_exe) printf("phdr first object is exe: %s, eh_frame_hdr: %s\n", yn(covers_main), yn(eh));
    if (!strncmp(b, "libdl_", 6))
        printf("phdr %s: %s, eh_frame_hdr: %s\n", b, !probe ? "listed" : covers_probe ? "covers its code" : "WRONG ADDR",
               yn(eh));
    return 0;
}
static void iterate(const char *when) {
    int idx = 0;
    printf("dl_iterate_phdr (%s):\n", when);
    dl_iterate_phdr(phdr_cb, &idx);
}

int main(void) {
    printf("RTLD_DEFAULT from exe: %s\n", call((fn_t)dlsym(RTLD_DEFAULT, "dup_sym")));
    printf("RTLD_DEFAULT from dl_a: %s\n", call(a_default()));
    printf("RTLD_NEXT from exe: %s\n", call((fn_t)dlsym(RTLD_NEXT, "dup_sym")));
    printf("RTLD_NEXT from dl_a: %s\n", call(a_next()));
    printf("RTLD_NEXT from dl_b: %s\n", call(b_next()));
    void *self = dlopen(0, RTLD_NOW);
    printf("dlopen(NULL) + dlsym: %s\n", call((fn_t)dlsym(self, "dup_sym")));
    void *hb = dlopen("libdl_b.so", RTLD_NOW | RTLD_NOLOAD);
    printf("dlopen(libdl_b.so, NOLOAD) found: %s\n", yn(hb != 0));
    printf("dlsym(hb, dup_sym): %s\n", call((fn_t)dlsym(hb, "dup_sym")));
    void *hb2 = dlopen("libdl_b.so", RTLD_NOW);
    printf("dlopen(libdl_b.so) same handle: %s\n", yn(hb2 == hb));
    printf("libdl_c.so loaded before plugin: %s\n", yn(dlopen("libdl_c.so", RTLD_NOW | RTLD_NOLOAD) != 0));
    printf("dlsym(RTLD_DEFAULT, missing) is NULL: %s, dlerror set: %s\n",
           yn(dlsym(RTLD_DEFAULT, "no_such_symbol_xyz") == 0), yn(dlerror() != 0));
    printf("dlopen(missing lib) is NULL: %s, dlerror set: %s\n", yn(dlopen("libno_such_lib.so", RTLD_NOW) == 0),
           yn(dlerror() != 0));

    Dl_info di;
    memset(&di, 0, sizeof di);
    int ok = dladdr((void *)b_only, &di);
    printf("dladdr(b_only): ok=%d file=%s abs=%s sym=%s saddr_ok=%s\n", ok != 0, base(di.dli_fname),
           yn(di.dli_fname[0] == '/'), di.dli_sname ? di.dli_sname : "NULL", yn(di.dli_saddr == (void *)b_only));
    printf("dlopen(dli_fname) is the same object: %s\n", yn(dlopen(di.dli_fname, RTLD_NOW | RTLD_NOLOAD) == hb));
    memset(&di, 0, sizeof di);
    ok = dladdr((char *)a_marker + 1, &di);
    printf("dladdr(a_marker+1): ok=%d file=%s sym=%s\n", ok != 0, base(di.dli_fname),
           di.dli_sname ? di.dli_sname : "NULL");
    memset(&di, 0, sizeof di);
    ok = dladdr((void *)dup_sym, &di);
    printf("dladdr(exe dup_sym): ok=%d sym=%s\n", ok != 0, di.dli_sname ? di.dli_sname : "NULL");

    probe_addr[0] = (void *)a_marker;
    probe_addr[1] = (void *)b_only;
    iterate("before plugin");

    void *hp = dlopen("libdl_plugin.so", RTLD_NOW);
    printf("dlopen plugin: %s\n", yn(hp != 0));
    fn_t pf = (fn_t)dlsym(hp, "plugin_fn");
    printf("plugin_fn: %s\n", call(pf));
    printf("plugin_fn via RTLD_DEFAULT (RTLD_LOCAL plugin): %s\n", yn(dlsym(RTLD_DEFAULT, "plugin_fn") != 0));
    printf("c_fn via RTLD_DEFAULT: %s\n", yn(dlsym(RTLD_DEFAULT, "c_fn") != 0));
    printf("c_fn via plugin handle: %s\n", yn(dlsym(hp, "c_fn") != 0));
    memset(&di, 0, sizeof di);
    dladdr((void *)pf, &di);
    printf("dladdr(plugin_fn): file=%s sym=%s\n", base(di.dli_fname), di.dli_sname ? di.dli_sname : "NULL");
    probe_addr[2] = (void *)pf;
    iterate("after plugin");

    printf("dlopen plugin2 from dl_a (GLOBAL): %s\n", yn(a_open_plugin2() != 0));
    printf("late_sym via RTLD_DEFAULT: %s\n", call((fn_t)dlsym(RTLD_DEFAULT, "late_sym")));

    printf("dlclose plugin: %d\n", dlclose(hp));
    printf("libdl_c.so loaded after dlclose: %s\n", yn(dlopen("libdl_c.so", RTLD_NOW | RTLD_NOLOAD) != 0));
    printf("main end\n");
    return 0;
}
