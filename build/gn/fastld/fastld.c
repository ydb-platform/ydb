/* fastld: a minimal loader for gn-built test binaries (prototype).
 *
 *   fastld [--] EXE [ARGS...]
 *
 * EXE and the DT_NEEDED closure found through RUNPATH/RPATH/LD_LIBRARY_PATH
 * are mapped and relocated here. Symbols are resolved through one hash index
 * over the whole global scope: index entry = first scope position that
 * defines the name, then glibc's matching rules from that position on.
 * Libraries not found there (libc, libm, ...) stay with the host ld.so and
 * take part in the scope as "foreign" objects.
 *
 * Environment:
 *   FASTLD_STATS=1         timings and counters on stderr
 *   FASTLD_BINDINGS=FILE   TSV: requester, symbol, version, definer
 *   FASTLD_DISABLE=1       exec EXE with the system loader instead
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <link.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef DT_RELRSZ
#define DT_RELRSZ 35
#define DT_RELR 36
#define DT_RELRENT 37
#endif

#define PAGE 4096UL
#define PAGE_DOWN(x) ((uintptr_t)(x) & ~(PAGE - 1))
#define PAGE_UP(x) (((uintptr_t)(x) + PAGE - 1) & ~(PAGE - 1))
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define EXPORT __attribute__((visibility("default"), used))
#define NOPOS UINT32_MAX
#define OBJ_MAGIC 0x66617374u
#define SCOPE_CAP (1u << 16)
#define EXE_TLS_MODID 1
#define FOREIGN_TLS (1UL << 62)
#define TLS_RESERVE 32768
#define TLS_STATIC (1UL << 63)

/* ---------------------------------------------------------------- utils */

static void vmsg(const char *fmt, va_list ap)
{
    char buf[4096];
    int n = snprintf(buf, sizeof buf, "fastld: ");
    n += vsnprintf(buf + n, sizeof buf - n, fmt, ap);
    if (n > (int)sizeof buf - 2)
        n = sizeof buf - 2;
    buf[n++] = '\n';
    if (write(2, buf, n) < 0) {
    }
}

static void __attribute__((format(printf, 1, 2))) msg(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vmsg(fmt, ap);
    va_end(ap);
}

static void __attribute__((noreturn, format(printf, 1, 2))) die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vmsg(fmt, ap);
    va_end(ap);
    _exit(127);
}

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

/* Bump allocator for loader data. Memory comes zeroed and is never freed. */
static pthread_mutex_t xa_lock = PTHREAD_MUTEX_INITIALIZER;
static char *xa_cur, *xa_end;

static void *map_anon(size_t n)
{
    void *p = mmap(NULL, n, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED)
        die("out of memory (%zu bytes)", n);
    return p;
}

static void *xalloc(size_t n)
{
    const size_t chunk = 64UL << 20;
    n = (n + 15) & ~(size_t)15;
    if (n > chunk / 4)
        return map_anon(PAGE_UP(n));
    pthread_mutex_lock(&xa_lock);
    if ((size_t)(xa_end - xa_cur) < n) {
        xa_cur = map_anon(chunk);
        xa_end = xa_cur + chunk;
    }
    void *r = xa_cur;
    xa_cur += n;
    pthread_mutex_unlock(&xa_lock);
    return r;
}

static char *xstrdup(const char *s)
{
    size_t n = strlen(s) + 1;
    char *p = xalloc(n);
    memcpy(p, s, n);
    return p;
}

static uint32_t gnu_hash(const char *s)
{
    uint32_t h = 5381;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        h = h * 33 + *p;
    return h;
}

static uint32_t elf_hash(const char *s)
{
    uint32_t h = 0, g;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        h = (h << 4) + *p;
        g = h & 0xf0000000;
        if (g)
            h ^= g >> 24;
        h &= ~g;
    }
    return h;
}

/* ------------------------------------------------------------- malloc */
/* Exported so that host libc and ld.so bind to it. Until the scope is
 * relocated it serves from a bump arena; then it forwards to the scope's
 * malloc (tcmalloc in the exe), like glibc's interposition would. */

#define ARENA_SIZE (256UL << 20)
static char *arena_lo, *arena_cur, *arena_hi;
static int malloc_ready;
static void *(*r_malloc)(size_t);
static void (*r_free)(void *);
static void *(*r_calloc)(size_t, size_t);
static void *(*r_realloc)(void *, size_t);
static void *(*r_memalign)(size_t, size_t);
static int (*r_posix_memalign)(void **, size_t, size_t);
static size_t (*r_usable)(void *);

static void *arena_alloc(size_t n, size_t al)
{
    if (!arena_lo) {
        void *p = mmap(NULL, ARENA_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED)
            return NULL;
        arena_lo = arena_cur = p;
        arena_hi = arena_lo + ARENA_SIZE;
    }
    if (al < 16)
        al = 16;
    while (al & (al - 1))
        al += al & -al;
    uintptr_t p = ((uintptr_t)arena_cur + 16 + al - 1) & ~(al - 1);
    if (n > ARENA_SIZE || p + n > (uintptr_t)arena_hi) {
        errno = ENOMEM;
        return NULL;
    }
    ((size_t *)p)[-1] = n;
    arena_cur = (char *)(p + n);
    return (void *)p;
}

static inline int in_arena(const void *p)
{
    return (const char *)p >= arena_lo && (const char *)p < arena_hi;
}

EXPORT void *malloc(size_t n)
{
    if (LIKELY(malloc_ready))
        return r_malloc(n);
    return arena_alloc(n, 16);
}

EXPORT void free(void *p)
{
    if (!p || in_arena(p))
        return;
    if (malloc_ready)
        r_free(p);
}

EXPORT void *calloc(size_t a, size_t b)
{
    size_t n;
    if (__builtin_mul_overflow(a, b, &n)) {
        errno = ENOMEM;
        return NULL;
    }
    if (LIKELY(malloc_ready))
        return r_calloc(a, b);
    return arena_alloc(n, 16);
}

EXPORT void *realloc(void *p, size_t n)
{
    if (!p)
        return malloc(n);
    if (in_arena(p)) {
        size_t old = ((size_t *)p)[-1];
        void *q = malloc(n ? n : 1);
        if (q)
            memcpy(q, p, old < n ? old : n);
        return q;
    }
    if (!malloc_ready)
        return NULL;
    return r_realloc(p, n);
}

EXPORT void *memalign(size_t al, size_t n)
{
    if (LIKELY(malloc_ready)) {
        if (r_memalign)
            return r_memalign(al, n);
        void *p = NULL;
        if (al < sizeof(void *))
            al = sizeof(void *);
        return r_posix_memalign(&p, al, n) ? NULL : p;
    }
    return arena_alloc(n, al);
}

EXPORT int posix_memalign(void **pp, size_t al, size_t n)
{
    if (!al || (al & (al - 1)) || al % sizeof(void *))
        return EINVAL;
    void *p = memalign(al, n);
    if (!p)
        return ENOMEM;
    *pp = p;
    return 0;
}

EXPORT void *aligned_alloc(size_t al, size_t n) { return memalign(al, n); }
EXPORT void *valloc(size_t n) { return memalign(PAGE, n); }
EXPORT void *pvalloc(size_t n) { return memalign(PAGE, PAGE_UP(n ? n : 1)); }

EXPORT size_t malloc_usable_size(void *p)
{
    if (!p)
        return 0;
    if (in_arena(p))
        return ((size_t *)p)[-1];
    return malloc_ready && r_usable ? r_usable(p) : 0;
}

/* ------------------------------------------------------- thread state */
/* The only TLS of the launcher. glibc puts it right below the thread
 * pointer, so its tail hosts the exe's local-exec TLS block. */

struct fl_tls {
    char *area;     /* this thread's block for our TLS modules */
    char **dyn;     /* blocks of modules from dlopen, by module id */
    int armed;      /* the destructor key is set */
    int dtor_calls; /* calls of the key destructor at the thread exit */
    int dlerr_set;
    char dlerr[512];
    char reserve[TLS_RESERVE];
} __attribute__((aligned(64)));

static __thread struct fl_tls T;

static inline char *tp(void)
{
    char *p;
    __asm__("mov %%fs:0, %0" : "=r"(p));
    return p;
}

static void __attribute__((format(printf, 1, 2))) set_dlerror(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(T.dlerr, sizeof T.dlerr, fmt, ap);
    va_end(ap);
    T.dlerr_set = 1;
}

/* ------------------------------------------------------------ objects */

typedef struct ver {
    const char *name;
    uint32_t hash;
    int hidden;
} ver_t;

typedef struct obj {
    /* Public part of struct link_map: our dlopen handles point here. */
    Elf64_Addr l_addr;
    char *l_name;
    Elf64_Dyn *l_ld;
    struct obj *l_next, *l_prev;

    struct obj *self;
    uint32_t magic;
    uint32_t pos;
    int foreign, ready, init_state, fini_done;
    struct link_map *lm; /* foreign only */
    const char *path, *origin, *soname, *runpath, *rpath;
    dev_t dev;
    ino_t ino;
    uintptr_t map_start, map_end;
    const Elf64_Phdr *phdr;
    size_t phnum;
    const char *strtab;
    const Elf64_Sym *symtab;
    uint32_t nsyms;
    uint32_t nbuckets, symoffset, bloom_size, bloom_shift;
    const uint64_t *bloom;
    const uint32_t *buckets, *chain;
    const uint16_t *versym;
    ver_t *versions;
    uint32_t nversions;
    const Elf64_Rela *rela, *jmprel;
    size_t nrela, njmprel;
    const uint64_t *relr;
    size_t nrelr;
    void (*init)(int, char **, char **);
    void (*fini)(void);
    void (**init_array)(int, char **, char **);
    void (**fini_array)(void);
    void (**preinit_array)(int, char **, char **);
    size_t ninit_array, nfini_array, npreinit_array;
    uint32_t *needed;
    size_t nneeded;
    struct obj **deps;
    size_t ndeps;
    uintptr_t relro_start, relro_end;
    size_t tls_modid, tls_tpoff;
    int tls_static, want_static;
    int global, removed;  /* in the global scope; unloaded by dlclose */
    uint32_t dlref;       /* dlopen references, objects from dlopen only */
    struct obj *dl_root;  /* the dlopen that brought it, NULL at startup */
    uintptr_t e_entry;
    const char *tls_image;
    size_t tls_filesz, tls_memsz, tls_align, tls_vaddr;
    uint32_t mark, init_idx;
} obj_t;

static obj_t **g_scope;
static uint32_t g_nscope;
static obj_t *g_exe;
static const char *g_exe_real;
static obj_t **g_own;
static uint32_t g_nown;
static obj_t *g_foreign[256];
static uint32_t g_nforeign;
static obj_t **g_init_order;
static uint32_t g_ninit;
static pthread_mutex_t g_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static const char *g_ld_library_path;

static struct {
    size_t relative, symbolic, lookups, slow, indexed, undef;
} st;
static int g_stats;

static int g_new_global = 1;
static obj_t *g_new_root;

static void scope_append(obj_t *o)
{
    o->global = o->foreign || g_new_global;
    o->dl_root = o->foreign ? NULL : g_new_root;
    if (g_nscope >= SCOPE_CAP)
        die("too many objects");
    o->pos = g_nscope;
    g_scope[g_nscope] = o;
    __atomic_store_n(&g_nscope, g_nscope + 1, __ATOMIC_RELEASE);
}

/* name (DT_NEEDED string or soname) -> object */
typedef struct {
    const char *k;
    obj_t *v;
} nm_ent;
static nm_ent *nm_tab;
static size_t nm_cap, nm_cnt;

static obj_t *nm_get(const char *k)
{
    if (!nm_cap)
        return NULL;
    size_t m = nm_cap - 1;
    for (size_t i = (gnu_hash(k) * 2654435761u) & m; nm_tab[i].k; i = (i + 1) & m)
        if (!strcmp(nm_tab[i].k, k))
            return nm_tab[i].v->removed ? NULL : nm_tab[i].v;
    return NULL;
}

static void nm_put(const char *k, obj_t *v)
{
    if ((nm_cnt + 1) * 2 > nm_cap) {
        size_t ncap = nm_cap ? nm_cap * 2 : 4096;
        nm_ent *old = nm_tab;
        size_t ocap = nm_cap;
        nm_tab = xalloc(ncap * sizeof *nm_tab);
        nm_cap = ncap;
        nm_cnt = 0;
        for (size_t i = 0; i < ocap; i++)
            if (old[i].k)
                nm_put(old[i].k, old[i].v);
    }
    size_t m = nm_cap - 1;
    size_t i = (gnu_hash(k) * 2654435761u) & m;
    for (; nm_tab[i].k; i = (i + 1) & m)
        if (!strcmp(nm_tab[i].k, k)) {
            if (nm_tab[i].v->removed)
                nm_tab[i].v = v;
            return;
        }
    nm_tab[i].k = k;
    nm_tab[i].v = v;
    nm_cnt++;
}

/* ------------------------------------------------------ ELF parsing */

static uintptr_t dptr(const obj_t *o, uintptr_t v)
{
    /* ld.so rewrites d_ptr of loaded objects in place, not always of itself. */
    if (o->foreign)
        return v < o->l_addr ? v + o->l_addr : v;
    return v + o->l_addr;
}

static void parse_dynamic(obj_t *o)
{
    uintptr_t gnuhash = 0, verdef = 0, verneed = 0;
    size_t verdefnum = 0, verneednum = 0, relasz = 0, pltrelsz = 0, relrsz = 0;
    size_t runpath = SIZE_MAX, rpath = SIZE_MAX, soname = SIZE_MAX;
    size_t nneeded = 0;
    const char *what = o->path;

    for (Elf64_Dyn *d = o->l_ld; d->d_tag != DT_NULL; d++) {
        uintptr_t v = d->d_un.d_val;
        switch (d->d_tag) {
        case DT_NEEDED: nneeded++; break;
        case DT_STRTAB: o->strtab = (const char *)dptr(o, v); break;
        case DT_SYMTAB: o->symtab = (const Elf64_Sym *)dptr(o, v); break;
        case DT_GNU_HASH: gnuhash = dptr(o, v); break;
        case DT_VERSYM: o->versym = (const uint16_t *)dptr(o, v); break;
        case DT_VERDEF: verdef = dptr(o, v); break;
        case DT_VERDEFNUM: verdefnum = v; break;
        case DT_VERNEED: verneed = dptr(o, v); break;
        case DT_VERNEEDNUM: verneednum = v; break;
        case DT_RELA: o->rela = (const Elf64_Rela *)dptr(o, v); break;
        case DT_RELASZ: relasz = v; break;
        case DT_RELAENT:
            if (v != sizeof(Elf64_Rela))
                die("%s: bad DT_RELAENT", what);
            break;
        case DT_JMPREL: o->jmprel = (const Elf64_Rela *)dptr(o, v); break;
        case DT_PLTRELSZ: pltrelsz = v; break;
        case DT_PLTREL:
            if (v != DT_RELA)
                die("%s: DT_PLTREL is not DT_RELA", what);
            break;
        case DT_RELR: o->relr = (const uint64_t *)dptr(o, v); break;
        case DT_RELRSZ: relrsz = v; break;
        case DT_INIT: o->init = (void (*)(int, char **, char **))dptr(o, v); break;
        case DT_FINI: o->fini = (void (*)(void))dptr(o, v); break;
        case DT_INIT_ARRAY: o->init_array = (void (**)(int, char **, char **))dptr(o, v); break;
        case DT_INIT_ARRAYSZ: o->ninit_array = v / sizeof(void *); break;
        case DT_FINI_ARRAY: o->fini_array = (void (**)(void))dptr(o, v); break;
        case DT_FINI_ARRAYSZ: o->nfini_array = v / sizeof(void *); break;
        case DT_PREINIT_ARRAY: o->preinit_array = (void (**)(int, char **, char **))dptr(o, v); break;
        case DT_PREINIT_ARRAYSZ: o->npreinit_array = v / sizeof(void *); break;
        case DT_RUNPATH: runpath = v; break;
        case DT_RPATH: rpath = v; break;
        case DT_SONAME: soname = v; break;
        case DT_REL:
        case DT_RELSZ:
            if (!o->foreign)
                die("%s: DT_REL is not supported", what);
            break;
        case DT_TEXTREL:
            if (!o->foreign)
                die("%s: text relocations are not supported", what);
            break;
        case DT_SYMBOLIC:
            if (!o->foreign)
                die("%s: DT_SYMBOLIC is not supported", what);
            break;
        case DT_FLAGS:
            if (!o->foreign && (v & (DF_TEXTREL | DF_SYMBOLIC)))
                die("%s: DF_TEXTREL/DF_SYMBOLIC are not supported", what);
            if (v & DF_STATIC_TLS)
                o->want_static = 1;
            break;
        default: break;
        }
    }
    if (!o->strtab || !o->symtab)
        die("%s: no dynamic symbol table", what);
    o->nrela = relasz / sizeof(Elf64_Rela);
    o->njmprel = pltrelsz / sizeof(Elf64_Rela);
    o->nrelr = relrsz / sizeof(uint64_t);
    if (runpath != SIZE_MAX)
        o->runpath = o->strtab + runpath;
    if (rpath != SIZE_MAX)
        o->rpath = o->strtab + rpath;
    if (soname != SIZE_MAX)
        o->soname = o->strtab + soname;

    o->needed = xalloc((nneeded + 1) * sizeof *o->needed);
    for (Elf64_Dyn *d = o->l_ld; d->d_tag != DT_NULL; d++)
        if (d->d_tag == DT_NEEDED)
            o->needed[o->nneeded++] = d->d_un.d_val;

    if (!gnuhash) {
        if (!o->foreign)
            die("%s: no DT_GNU_HASH", what);
        o->nsyms = 0; /* not searchable: system objects only */
        goto versions;
    }
    const uint32_t *gh = (const uint32_t *)gnuhash;
    o->nbuckets = gh[0];
    o->symoffset = gh[1];
    o->bloom_size = gh[2];
    o->bloom_shift = gh[3];
    o->bloom = (const uint64_t *)(gh + 4);
    o->buckets = (const uint32_t *)(o->bloom + o->bloom_size);
    o->chain = o->buckets + o->nbuckets;
    uint32_t maxi = 0;
    for (uint32_t b = 0; b < o->nbuckets; b++)
        if (o->buckets[b] > maxi)
            maxi = o->buckets[b];
    if (maxi < o->symoffset) {
        o->nsyms = o->symoffset;
    } else {
        while (!(o->chain[maxi - o->symoffset] & 1))
            maxi++;
        o->nsyms = maxi + 1;
    }

versions:;
    /* l_versions as glibc builds it: verneed, then verdef without the base. */
    uint32_t nver = 0;
    const char *p;
    p = (const char *)verneed;
    for (size_t k = 0; p && k < verneednum; k++) {
        const Elf64_Verneed *vn = (const Elf64_Verneed *)p;
        const char *a = p + vn->vn_aux;
        for (size_t j = 0; j < vn->vn_cnt; j++) {
            const Elf64_Vernaux *va = (const Elf64_Vernaux *)a;
            if ((uint32_t)(va->vna_other & 0x7fff) + 1 > nver)
                nver = (va->vna_other & 0x7fff) + 1;
            a += va->vna_next;
        }
        p += vn->vn_next;
    }
    p = (const char *)verdef;
    for (size_t k = 0; p && k < verdefnum; k++) {
        const Elf64_Verdef *vd = (const Elf64_Verdef *)p;
        if ((uint32_t)(vd->vd_ndx & 0x7fff) + 1 > nver)
            nver = (vd->vd_ndx & 0x7fff) + 1;
        p += vd->vd_next;
    }
    if (nver) {
        o->nversions = nver;
        o->versions = xalloc(nver * sizeof(ver_t));
        p = (const char *)verneed;
        for (size_t k = 0; p && k < verneednum; k++) {
            const Elf64_Verneed *vn = (const Elf64_Verneed *)p;
            const char *a = p + vn->vn_aux;
            for (size_t j = 0; j < vn->vn_cnt; j++) {
                const Elf64_Vernaux *va = (const Elf64_Vernaux *)a;
                ver_t *v = &o->versions[va->vna_other & 0x7fff];
                v->name = o->strtab + va->vna_name;
                v->hash = va->vna_hash;
                v->hidden = !!(va->vna_other & 0x8000);
                a += va->vna_next;
            }
            p += vn->vn_next;
        }
        p = (const char *)verdef;
        for (size_t k = 0; p && k < verdefnum; k++) {
            const Elf64_Verdef *vd = (const Elf64_Verdef *)p;
            ver_t *v = &o->versions[vd->vd_ndx & 0x7fff];
            if (!(vd->vd_flags & VER_FLG_BASE) && v->hash == 0) {
                const Elf64_Verdaux *aux = (const Elf64_Verdaux *)(p + vd->vd_aux);
                v->name = o->strtab + aux->vda_name;
                v->hash = vd->vd_hash;
            }
            p += vd->vd_next;
        }
    }
}

static char *dir_of(const char *path)
{
    const char *s = strrchr(path, '/');
    if (!s)
        return xstrdup(".");
    if (s == path)
        return xstrdup("/");
    char *d = xalloc(s - path + 1);
    memcpy(d, path, s - path);
    return d;
}

static obj_t *new_obj(void)
{
    obj_t *o = xalloc(sizeof *o);
    o->self = o;
    o->magic = OBJ_MAGIC;
    o->pos = NOPOS;
    return o;
}

static obj_t *load_file(const char *path, int fd, const struct stat *sb)
{
    Elf64_Ehdr eh;
    if (pread(fd, &eh, sizeof eh, 0) != sizeof eh || memcmp(eh.e_ident, ELFMAG, SELFMAG) ||
        eh.e_ident[EI_CLASS] != ELFCLASS64 || eh.e_machine != EM_X86_64 || eh.e_type != ET_DYN)
        die("%s: not an x86-64 ELF shared object or PIE", path);
    size_t phsz = (size_t)eh.e_phnum * sizeof(Elf64_Phdr);
    Elf64_Phdr *ph = xalloc(phsz);
    if (pread(fd, ph, phsz, eh.e_phoff) != (ssize_t)phsz)
        die("%s: cannot read program headers", path);

    uintptr_t lo = UINTPTR_MAX, hi = 0, align = PAGE;
    for (size_t i = 0; i < eh.e_phnum; i++) {
        if (ph[i].p_type != PT_LOAD)
            continue;
        if (PAGE_DOWN(ph[i].p_vaddr) < lo)
            lo = PAGE_DOWN(ph[i].p_vaddr);
        if (PAGE_UP(ph[i].p_vaddr + ph[i].p_memsz) > hi)
            hi = PAGE_UP(ph[i].p_vaddr + ph[i].p_memsz);
        if (ph[i].p_align > align)
            align = ph[i].p_align;
    }
    if (lo >= hi)
        die("%s: no PT_LOAD", path);
    size_t span = hi - lo;
    char *res = mmap(NULL, span + align - PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (res == MAP_FAILED)
        die("%s: cannot reserve %zu bytes", path, span);
    uintptr_t start = ((uintptr_t)res + align - 1) & ~(align - 1);
    if (start > (uintptr_t)res)
        munmap(res, start - (uintptr_t)res);
    if ((uintptr_t)res + span + align - PAGE > start + span)
        munmap((void *)(start + span), (uintptr_t)res + span + align - PAGE - (start + span));
    uintptr_t base = start - lo;

    obj_t *o = new_obj();
    o->l_addr = base;
    o->map_start = start;
    o->map_end = start + span;
    o->path = xstrdup(path);
    o->l_name = (char *)o->path;
    o->origin = dir_of(path);
    o->dev = sb->st_dev;
    o->ino = sb->st_ino;
    o->phnum = eh.e_phnum;
    o->phdr = ph;
    o->e_entry = eh.e_entry;

    for (size_t i = 0; i < eh.e_phnum; i++) {
        const Elf64_Phdr *p = &ph[i];
        switch (p->p_type) {
        case PT_LOAD: {
            int prot = (p->p_flags & PF_R ? PROT_READ : 0) | (p->p_flags & PF_W ? PROT_WRITE : 0) |
                       (p->p_flags & PF_X ? PROT_EXEC : 0);
            uintptr_t va = base + p->p_vaddr, fend = va + p->p_filesz, mend = va + p->p_memsz;
            uintptr_t mstart = PAGE_DOWN(va), zstart = mstart;
            if (p->p_filesz) {
                if (mmap((void *)mstart, PAGE_UP(fend) - mstart, prot, MAP_PRIVATE | MAP_FIXED, fd,
                         PAGE_DOWN(p->p_offset)) == MAP_FAILED)
                    die("%s: mmap failed: %s", path, strerror(errno));
                zstart = PAGE_UP(fend);
                if (p->p_memsz > p->p_filesz && fend < zstart) {
                    if (!(prot & PROT_WRITE))
                        die("%s: bss in a read-only segment", path);
                    memset((void *)fend, 0, zstart - fend);
                }
            }
            if (p->p_memsz > p->p_filesz && PAGE_UP(mend) > zstart) {
                if (mmap((void *)zstart, PAGE_UP(mend) - zstart, prot, MAP_PRIVATE | MAP_FIXED | MAP_ANONYMOUS, -1,
                         0) == MAP_FAILED)
                    die("%s: mmap failed: %s", path, strerror(errno));
            }
            if (p->p_offset <= eh.e_phoff && eh.e_phoff + phsz <= p->p_offset + p->p_filesz)
                o->phdr = (const Elf64_Phdr *)(base + p->p_vaddr + (eh.e_phoff - p->p_offset));
            break;
        }
        case PT_DYNAMIC: o->l_ld = (Elf64_Dyn *)(base + p->p_vaddr); break;
        case PT_TLS:
            o->tls_image = (const char *)(base + p->p_vaddr);
            o->tls_filesz = p->p_filesz;
            o->tls_memsz = p->p_memsz;
            o->tls_align = p->p_align ? p->p_align : 1;
            o->tls_vaddr = p->p_vaddr;
            break;
        case PT_GNU_RELRO:
            o->relro_start = PAGE_DOWN(base + p->p_vaddr);
            o->relro_end = PAGE_DOWN(base + p->p_vaddr + p->p_memsz);
            break;
        default: break;
        }
    }
    if (!o->l_ld)
        die("%s: no PT_DYNAMIC", path);
    parse_dynamic(o);
    if (g_nown >= SCOPE_CAP)
        die("too many objects");
    g_own[g_nown++] = o;
    return o;
}

static obj_t *foreign_obj(struct link_map *lm)
{
    for (uint32_t i = 0; i < g_nforeign; i++)
        if (g_foreign[i]->lm == lm)
            return g_foreign[i];
    if (g_nforeign >= sizeof g_foreign / sizeof g_foreign[0])
        die("too many system libraries");
    obj_t *o = new_obj();
    o->foreign = 1;
    o->lm = lm;
    o->l_addr = lm->l_addr;
    o->l_ld = lm->l_ld;
    o->path = lm->l_name;
    o->l_name = lm->l_name;
    parse_dynamic(o);
    size_t id = 0;
    if (dlinfo(lm, RTLD_DI_TLS_MODID, &id) == 0 && id)
        o->tls_modid = FOREIGN_TLS | id;
    o->ready = 1;
    g_foreign[g_nforeign++] = o;
    return o;
}

/* Objects the host already has (libc, libm, ld.so, ...) by soname and name. */
static void register_host_objects(void)
{
    struct link_map *lm = dlopen(NULL, RTLD_NOW);
    if (!lm)
        die("dlopen(NULL) failed");
    for (lm = lm->l_next; lm; lm = lm->l_next) {
        if (!lm->l_name || !lm->l_name[0] || !lm->l_ld)
            continue;
        obj_t *o = foreign_obj(lm);
        const char *b = strrchr(o->path, '/');
        nm_put(b ? b + 1 : o->path, o);
        if (o->soname)
            nm_put(o->soname, o);
    }
}

/* ----------------------------------------------------------- search */

static int open_in(const char *list, const char *origin, const char *name, char *out)
{
    while (list && *list) {
        const char *e = strchr(list, ':');
        size_t n = e ? (size_t)(e - list) : strlen(list);
        char dir[PATH_MAX];
        size_t dn = 0;
        for (size_t i = 0; i < n && dn < sizeof dir - 1;) {
            if (list[i] == '$' && !strncmp(list + i, "$ORIGIN", 7) && (i + 7 == n || list[i + 7] == '/')) {
                dn += snprintf(dir + dn, sizeof dir - dn, "%s", origin);
                i += 7;
            } else if (list[i] == '$' && !strncmp(list + i, "${ORIGIN}", 9)) {
                dn += snprintf(dir + dn, sizeof dir - dn, "%s", origin);
                i += 9;
            } else {
                dir[dn++] = list[i++];
            }
        }
        if (dn >= sizeof dir)
            dn = sizeof dir - 1;
        dir[dn] = 0;
        if (!dn)
            strcpy(dir, ".");
        if (snprintf(out, PATH_MAX, "%s/%s", dir, name) < PATH_MAX) {
            int fd = open(out, O_RDONLY | O_CLOEXEC);
            if (fd >= 0)
                return fd;
        }
        list = e ? e + 1 : NULL;
    }
    return -1;
}

static int search_lib(const char *name, const obj_t *req, char *out)
{
    int fd;
    if (strchr(name, '/')) {
        snprintf(out, PATH_MAX, "%s", name);
        return open(name, O_RDONLY | O_CLOEXEC);
    }
    if (!req->runpath) {
        if (req->rpath && (fd = open_in(req->rpath, req->origin, name, out)) >= 0)
            return fd;
        if (req != g_exe && g_exe->rpath && !g_exe->runpath &&
            (fd = open_in(g_exe->rpath, g_exe->origin, name, out)) >= 0)
            return fd;
    }
    if (g_ld_library_path && (fd = open_in(g_ld_library_path, g_exe->origin, name, out)) >= 0)
        return fd;
    if (req->runpath && (fd = open_in(req->runpath, req->origin, name, out)) >= 0)
        return fd;
    return -1;
}

static obj_t *find_by_inode(dev_t dev, ino_t ino)
{
    for (uint32_t i = 0; i < g_nown; i++)
        if (g_own[i]->ino == ino && g_own[i]->dev == dev && !g_own[i]->removed)
            return g_own[i];
    return NULL;
}

/* NULL if not found and !host_fallback. */
static obj_t *find_lib(const char *name, const obj_t *req, int host_fallback)
{
    obj_t *o = nm_get(name);
    if (o)
        return o;
    char path[PATH_MAX];
    int fd = search_lib(name, req, path);
    if (fd >= 0) {
        struct stat sb;
        if (fstat(fd, &sb))
            die("%s: fstat failed", path);
        o = find_by_inode(sb.st_dev, sb.st_ino);
        if (!o)
            o = load_file(path, fd, &sb);
        close(fd);
    } else {
        if (!host_fallback)
            return NULL;
        void *h = dlopen(name, RTLD_NOW | RTLD_GLOBAL);
        if (!h)
            die("cannot load %s needed by %s: %s", name, req->path, dlerror());
        struct link_map *lm;
        if (dlinfo(h, RTLD_DI_LINKMAP, &lm))
            die("dlinfo failed for %s", name);
        o = foreign_obj(lm);
    }
    nm_put(xstrdup(name), o);
    if (o->soname)
        nm_put(o->soname, o);
    return o;
}

/* BFS over DT_NEEDED from scope position `first`. */
static void load_deps_from(uint32_t first)
{
    for (uint32_t i = first; i < g_nscope; i++) {
        obj_t *o = g_scope[i];
        o->deps = xalloc((o->nneeded + 1) * sizeof *o->deps);
        for (size_t k = 0; k < o->nneeded; k++) {
            obj_t *d = find_lib(o->strtab + o->needed[k], o, 1);
            if (d->pos == NOPOS)
                scope_append(d);
            o->deps[o->ndeps++] = d;
        }
    }
}

/* ------------------------------------------------------------ index */

typedef struct {
    uint32_t h, pos, idx;
} ix_ent;
static ix_ent *ix_tab;
static size_t ix_mask, ix_cnt;
static unsigned ix_shift;

static inline size_t ix_slot(uint32_t hk)
{
    return (size_t)(((uint64_t)hk * 0x9E3779B97F4A7C15ull) >> ix_shift);
}

static inline const char *sym_name(const obj_t *o, uint32_t i)
{
    return o->strtab + o->symtab[i].st_name;
}

static void ix_alloc(size_t want)
{
    size_t cap = 1024;
    unsigned bits = 10;
    while (cap < want * 2) {
        cap *= 2;
        bits++;
    }
    ix_ent *old = ix_tab;
    size_t ocap = ix_tab ? ix_mask + 1 : 0;
    ix_tab = map_anon(cap * sizeof(ix_ent));
    memset(ix_tab, 0xff, cap * sizeof(ix_ent));
    ix_mask = cap - 1;
    ix_shift = 64 - bits;
    for (size_t i = 0; i < ocap; i++) {
        if (old[i].pos == NOPOS)
            continue;
        size_t s = ix_slot(old[i].h);
        while (ix_tab[s].pos != NOPOS)
            s = (s + 1) & ix_mask;
        ix_tab[s] = old[i];
    }
    if (old)
        munmap(old, ocap * sizeof(ix_ent));
}

static inline const ix_ent *ix_find(uint32_t h, const char *name)
{
    uint32_t hk = h & ~1u;
    for (size_t s = ix_slot(hk);; s = (s + 1) & ix_mask) {
        const ix_ent *e = &ix_tab[s];
        if (e->pos == NOPOS)
            return NULL;
        if (e->h == hk && !strcmp(sym_name(g_scope[e->pos], e->idx), name))
            return e;
    }
}

static void ix_add_obj(obj_t *o)
{
    if ((ix_cnt + (o->nsyms - o->symoffset)) * 10 > (ix_mask + 1) * 7)
        ix_alloc(ix_cnt + (o->nsyms - o->symoffset));
    for (uint32_t b = 0; b < o->nbuckets; b++) {
        uint32_t i = o->buckets[b];
        if (i < o->symoffset)
            continue;
        for (;; i++) {
            uint32_t ch = o->chain[i - o->symoffset];
            uint32_t hk = ch & ~1u;
            size_t s = ix_slot(hk);
            for (;; s = (s + 1) & ix_mask) {
                ix_ent *e = &ix_tab[s];
                if (e->pos == NOPOS) {
                    e->h = hk;
                    e->pos = o->pos;
                    e->idx = i;
                    ix_cnt++;
                    break;
                }
                if (e->h == hk && !strcmp(sym_name(g_scope[e->pos], e->idx), sym_name(o, i)))
                    break;
            }
            st.indexed++;
            if (ch & 1)
                break;
        }
    }
}

/* ----------------------------------------------------------- lookup */

typedef struct {
    const char *name;
    uint32_t hash;
    const ver_t *ver;
    int newest; /* dlsym: prefer the default version */
} ref_t;

#define ALLOWED_STT                                                                                                    \
    ((1 << STT_NOTYPE) | (1 << STT_OBJECT) | (1 << STT_FUNC) | (1 << STT_COMMON) | (1 << STT_TLS) |                    \
     (1 << STT_GNU_IFUNC))

/* glibc check_match: 1 match, 2 counts as the only-versioned fallback, 0 no. */
static int check_match(const obj_t *o, uint32_t i, const ref_t *r)
{
    const Elf64_Sym *s = &o->symtab[i];
    unsigned stt = ELF64_ST_TYPE(s->st_info);
    if ((s->st_value == 0 && stt != STT_TLS) || s->st_shndx == SHN_UNDEF)
        return 0;
    if (!((1u << stt) & ALLOWED_STT))
        return 0;
    unsigned bind = ELF64_ST_BIND(s->st_info);
    if (bind != STB_GLOBAL && bind != STB_WEAK && bind != STB_GNU_UNIQUE)
        return 0;
    if (strcmp(o->strtab + s->st_name, r->name))
        return 0;
    if (!o->versym)
        return 1;
    uint16_t vs = o->versym[i];
    if (r->ver) {
        uint16_t ndx = vs & 0x7fff;
        const ver_t *dv = ndx < o->nversions ? &o->versions[ndx] : NULL;
        uint32_t dh = dv ? dv->hash : 0;
        int same = dv && dv->name && dh == r->ver->hash && !strcmp(dv->name, r->ver->name);
        if (!same && (r->ver->hidden || dh || (vs & 0x8000)))
            return 0;
        return 1;
    }
    if ((vs & 0x7fff) >= (r->newest ? 2 : 3))
        return (vs & 0x8000) ? 0 : 2;
    return 1;
}

static int obj_lookup(const obj_t *o, const ref_t *r, uint32_t *out)
{
    uint32_t h = r->hash;
    if (!o->nbuckets)
        return 0;
    uint64_t word = o->bloom[(h / 64) % o->bloom_size];
    uint64_t mask = (1ull << (h % 64)) | (1ull << ((h >> o->bloom_shift) % 64));
    if ((word & mask) != mask)
        return 0;
    uint32_t i = o->buckets[h % o->nbuckets];
    if (i < o->symoffset)
        return 0;
    int nver = 0;
    uint32_t vidx = 0;
    for (;; i++) {
        uint32_t ch = o->chain[i - o->symoffset];
        if (((ch ^ h) >> 1) == 0) {
            int m = check_match(o, i, r);
            if (m == 1) {
                *out = i;
                return 1;
            }
            if (m == 2 && nver++ == 0)
                vidx = i;
        }
        if (ch & 1)
            break;
    }
    if (nver == 1) {
        *out = vidx;
        return 1;
    }
    return 0;
}

static int scope_lookup(const ref_t *r, uint32_t from, obj_t **od, uint32_t *oi)
{
    st.lookups++;
    const ix_ent *e = ix_find(r->hash, r->name);
    if (!e)
        return 0;
    uint32_t p = e->pos;
    if (p >= from) {
        if (g_scope[p]->global && !g_scope[p]->removed && check_match(g_scope[p], e->idx, r) == 1) {
            *od = g_scope[p];
            *oi = e->idx;
            return 1;
        }
    } else {
        p = from;
    }
    st.slow++;
    uint32_t n = __atomic_load_n(&g_nscope, __ATOMIC_ACQUIRE);
    for (; p < n; p++) {
        if (!g_scope[p]->global || g_scope[p]->removed)
            continue;
        if (obj_lookup(g_scope[p], r, oi)) {
            *od = g_scope[p];
            return 1;
        }
    }
    return 0;
}

static uintptr_t sym_value(const obj_t *o, const Elf64_Sym *s)
{
    unsigned stt = ELF64_ST_TYPE(s->st_info);
    if (stt == STT_TLS)
        return s->st_value;
    uintptr_t v = o->l_addr + s->st_value;
    if (stt == STT_GNU_IFUNC)
        v = ((uintptr_t(*)(void))v)();
    return v;
}

/* ------------------------------------------------------------- TLS */
/* Module 1 is the exe: its local-exec block sits at the tail of the
 * launcher's static TLS. Modules needing static TLS (TPOFF64, DF_STATIC_TLS)
 * go below it there. Other startup modules live in one lazily mapped
 * per-thread area; modules from dlopen get a block each. */

typedef struct {
    unsigned long mod, off;
} tls_index;

static size_t g_ntls = 2;      /* next module id; 0 none, 1 the exe */
static size_t g_ntls_area = 2; /* ids below it are startup modules */
static size_t *g_tls_off;      /* area offset, TLS_STATIC if static */
static obj_t **g_tls_obj;
static size_t g_tls_size;
static pthread_key_t g_tls_key;
static size_t g_exe_tls_off, g_static_used;
static int g_need_tramp;
static obj_t *g_static_objs[64];
static uint32_t g_nstatic;
static void *(*host_tls_get_addr)(tls_index *);

static void tls_arm(void)
{
    if (!T.armed) {
        T.armed = 1;
        pthread_setspecific(g_tls_key, (void *)1);
    }
}

static char *tls_alloc_area(void)
{
    char *a = mmap(NULL, g_tls_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (a == MAP_FAILED)
        die("cannot allocate TLS (%zu bytes)", g_tls_size);
    for (size_t m = 2; m < g_ntls_area; m++)
        if (!(g_tls_off[m] & TLS_STATIC))
            memcpy(a + g_tls_off[m], g_tls_obj[m]->tls_image, g_tls_obj[m]->tls_filesz);
    T.area = a;
    tls_arm();
    return a;
}

static char *tls_alloc_dyn(size_t m)
{
    if (!T.dyn) {
        T.dyn = mmap(NULL, SCOPE_CAP * sizeof(char *), PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (T.dyn == MAP_FAILED)
            die("cannot allocate TLS");
        tls_arm();
    }
    const obj_t *o = g_tls_obj[m];
    size_t fb = o->tls_vaddr & (o->tls_align - 1);
    char *b = mmap(NULL, PAGE_UP(fb + o->tls_memsz), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (b == MAP_FAILED)
        die("cannot allocate TLS");
    memcpy(b + fb, o->tls_image, o->tls_filesz);
    T.dyn[m] = b + fb;
    return b + fb;
}

/* The key destructors of other libraries may run after ours in the same
 * round and still use their TLS (util/system/tls.cpp -> pg_wrapper): like
 * glibc, which frees TLS after all of them, free it only in the last round. */
static void tls_dtor(void *unused)
{
    (void)unused;
    if (++T.dtor_calls < PTHREAD_DESTRUCTOR_ITERATIONS) {
        pthread_setspecific(g_tls_key, (void *)1);
        return;
    }
    T.armed = 0;
    if (T.area) {
        munmap(T.area, g_tls_size);
        T.area = NULL;
    }
    if (T.dyn) {
        size_t n = __atomic_load_n(&g_ntls, __ATOMIC_ACQUIRE);
        for (size_t m = g_ntls_area; m < n; m++)
            if (T.dyn[m]) {
                const obj_t *o = g_tls_obj[m];
                size_t fb = o->tls_vaddr & (o->tls_align - 1);
                munmap(T.dyn[m] - fb, PAGE_UP(fb + o->tls_memsz));
            }
        munmap(T.dyn, SCOPE_CAP * sizeof(char *));
        T.dyn = NULL;
    }
}

static void *__attribute__((noinline)) tls_slow(tls_index *ti)
{
    unsigned long m = ti->mod;
    if (m == EXE_TLS_MODID && g_exe->tls_modid == EXE_TLS_MODID)
        return tp() - g_exe_tls_off + ti->off;
    if (m & FOREIGN_TLS) {
        tls_index h = {m & ~FOREIGN_TLS, ti->off};
        return host_tls_get_addr(&h);
    }
    if (m >= 2 && m < g_ntls_area) {
        if (g_tls_off[m] & TLS_STATIC)
            return tp() - g_tls_obj[m]->tls_tpoff + ti->off;
        char *a = T.area ? T.area : tls_alloc_area();
        return a + g_tls_off[m] + ti->off;
    }
    if (m >= g_ntls_area && m < __atomic_load_n(&g_ntls, __ATOMIC_ACQUIRE)) {
        char *b = T.dyn && T.dyn[m] ? T.dyn[m] : tls_alloc_dyn(m);
        return b + ti->off;
    }
    die("__tls_get_addr: bad module id %lu", m);
}

static __attribute__((force_align_arg_pointer)) void *fl_tls_get_addr(tls_index *ti)
{
    char *a = T.area;
    unsigned long m = ti->mod;
    if (LIKELY(a != NULL && m - 2 < g_ntls_area - 2)) {
        size_t off = g_tls_off[m];
        if (LIKELY(!(off & TLS_STATIC)))
            return a + off + ti->off;
    }
    return tls_slow(ti);
}

static void *tls_addr_of(const obj_t *o, size_t off)
{
    tls_index ti = {o->tls_modid, off};
    return fl_tls_get_addr(&ti);
}

/* glibc's variant II placement below what is already taken. */
static void make_static(obj_t *o)
{
    if (o == g_exe || o->tls_static)
        return;
    if (o->foreign || !o->tls_modid || o->tls_modid >= g_ntls_area)
        die("%s: static TLS is only supported for startup libraries", o->path);
    if (o->tls_align > 64 || g_nstatic >= sizeof g_static_objs / sizeof g_static_objs[0])
        die("%s: unsupported static TLS", o->path);
    size_t fb = (-o->tls_vaddr) & (o->tls_align - 1);
    size_t off = ((g_static_used + o->tls_memsz - fb + o->tls_align - 1) & ~(o->tls_align - 1)) + fb;
    if (off > TLS_RESERVE)
        die("%s: static TLS reserve exhausted (%zu > %d)", o->path, off, TLS_RESERVE);
    g_static_used = off;
    o->tls_tpoff = off;
    o->tls_static = 1;
    g_tls_off[o->tls_modid] |= TLS_STATIC;
    g_static_objs[g_nstatic++] = o;
    for (size_t k = 0; k < o->tls_filesz; k++)
        if (o->tls_image[k])
            g_need_tramp = 1;
}

static void tls_layout(void)
{
    size_t off = 0;
    g_tls_off = xalloc((SCOPE_CAP + 2) * sizeof *g_tls_off);
    g_tls_obj = xalloc((SCOPE_CAP + 2) * sizeof *g_tls_obj);
    for (uint32_t i = 0; i < g_nscope; i++) {
        obj_t *o = g_scope[i];
        if (o->foreign || !o->tls_memsz)
            continue;
        if (o->tls_align > PAGE || (o->tls_align & (o->tls_align - 1)))
            die("%s: unsupported TLS alignment %zu", o->path, o->tls_align);
        if (o == g_exe) {
            size_t fb = o->tls_vaddr & (o->tls_align - 1);
            if (fb || o->tls_align > 64)
                die("%s: unsupported exe TLS layout", o->path);
            g_exe_tls_off = (o->tls_memsz + o->tls_align - 1) & ~(o->tls_align - 1);
            if (g_exe_tls_off > TLS_RESERVE)
                die("%s: exe TLS too big (%zu > %d)", o->path, g_exe_tls_off, TLS_RESERVE);
            o->tls_modid = EXE_TLS_MODID;
            for (size_t k = 0; k < o->tls_filesz; k++)
                if (o->tls_image[k])
                    g_need_tramp = 1;
            continue;
        }
        size_t fb = o->tls_vaddr & (o->tls_align - 1);
        off += (fb - off) & (o->tls_align - 1);
        o->tls_modid = g_ntls;
        g_tls_off[g_ntls] = off;
        g_tls_obj[g_ntls] = o;
        g_ntls++;
        off += o->tls_memsz;
    }
    g_ntls_area = g_ntls;
    g_tls_size = PAGE_UP(off ? off : 1);
    g_static_used = g_exe_tls_off;
    for (uint32_t i = 0; i < g_nscope; i++)
        if (g_scope[i]->want_static && g_scope[i]->tls_memsz)
            make_static(g_scope[i]);
}

/* Static TLS images for the calling thread. */
static void static_tls_init(void)
{
    if (g_exe->tls_modid == EXE_TLS_MODID)
        memcpy(tp() - g_exe_tls_off, g_exe->tls_image, g_exe->tls_filesz);
    for (uint32_t i = 0; i < g_nstatic; i++)
        memcpy(tp() - g_static_objs[i]->tls_tpoff, g_static_objs[i]->tls_image, g_static_objs[i]->tls_filesz);
}

/* ------------------------------------------------------------ shims */

static obj_t **g_sorted;
static uint32_t g_nsorted;

static int cmp_obj_start(const void *a, const void *b)
{
    uintptr_t x = (*(obj_t *const *)a)->map_start, y = (*(obj_t *const *)b)->map_start;
    return x < y ? -1 : x > y;
}

static void rebuild_sorted(void)
{
    obj_t **s = xalloc(g_nown * sizeof *s);
    uint32_t n = 0;
    for (uint32_t i = 0; i < g_nown; i++)
        if (!g_own[i]->removed)
            s[n++] = g_own[i];
    qsort(s, n, sizeof *s, cmp_obj_start);
    __atomic_store_n(&g_nsorted, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&g_sorted, s, __ATOMIC_RELEASE);
    __atomic_store_n(&g_nsorted, n, __ATOMIC_RELEASE);
}

static obj_t *find_own(uintptr_t a)
{
    obj_t **s = __atomic_load_n(&g_sorted, __ATOMIC_ACQUIRE);
    uint32_t lo = 0, hi = __atomic_load_n(&g_nsorted, __ATOMIC_ACQUIRE);
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (s[mid]->map_start <= a)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo && a < s[lo - 1]->map_end && s[lo - 1]->ready)
        return s[lo - 1];
    return NULL;
}

static int is_own(const void *h)
{
    const obj_t *o = h;
    return h && h != RTLD_NEXT && o->magic == OBJ_MAGIC && o->self == o && !o->foreign;
}

static void *tls_data_of(const obj_t *o)
{
    size_t m = o->tls_modid;
    if (m == EXE_TLS_MODID)
        return tp() - g_exe_tls_off;
    if (o->tls_static)
        return tp() - o->tls_tpoff;
    if (m >= 2 && m < g_ntls_area && T.area)
        return T.area + g_tls_off[m];
    if (m >= g_ntls_area && m < g_ntls && T.dyn)
        return T.dyn[m];
    return NULL;
}

struct iter_ctx {
    int (*cb)(struct dl_phdr_info *, size_t, void *);
    void *data;
    unsigned long long adds, subs;
};

static int host_counts_cb(struct dl_phdr_info *i, size_t sz, void *p)
{
    (void)sz;
    struct iter_ctx *c = p;
    c->adds = i->dlpi_adds + g_nown;
    c->subs = i->dlpi_subs;
    return 1;
}

static int host_iter_cb(struct dl_phdr_info *i, size_t sz, void *p)
{
    struct iter_ctx *c = p;
    struct dl_phdr_info copy = *i;
    copy.dlpi_adds = c->adds;
    copy.dlpi_subs = c->subs;
    return c->cb(&copy, sz, c->data);
}

static int fl_dl_iterate_phdr(int (*cb)(struct dl_phdr_info *, size_t, void *), void *data)
{
    struct iter_ctx c = {cb, data, 0, 0};
    dl_iterate_phdr(host_counts_cb, &c);
    uint32_t n = __atomic_load_n(&g_nscope, __ATOMIC_ACQUIRE);
    for (uint32_t i = 0; i < n; i++) {
        obj_t *o = g_scope[i];
        if (o->foreign || !o->ready || o->removed)
            continue;
        struct dl_phdr_info info;
        memset(&info, 0, sizeof info);
        info.dlpi_addr = o->l_addr;
        info.dlpi_name = o->path;
        info.dlpi_phdr = o->phdr;
        info.dlpi_phnum = o->phnum;
        info.dlpi_adds = c.adds;
        info.dlpi_subs = c.subs;
        info.dlpi_tls_modid = o->tls_modid;
        info.dlpi_tls_data = tls_data_of(o);
        int r = cb(&info, sizeof info, data);
        if (r)
            return r;
    }
    return dl_iterate_phdr(host_iter_cb, &c);
}

static const Elf64_Sym *nearest_sym(const obj_t *o, uintptr_t a)
{
    const Elf64_Sym *best = NULL;
    uintptr_t bv = 0;
    for (uint32_t i = 1; i < o->nsyms; i++) {
        const Elf64_Sym *s = &o->symtab[i];
        if (s->st_shndx == SHN_UNDEF || ELF64_ST_TYPE(s->st_info) == STT_TLS)
            continue;
        uintptr_t v = o->l_addr + s->st_value;
        if (v <= a && (a < v + s->st_size || a == v) && (!best || v > bv)) {
            best = s;
            bv = v;
        }
    }
    return best;
}

static int fl_dladdr1(const void *addr, Dl_info *info, void **extra, int flags)
{
    obj_t *o = find_own((uintptr_t)addr);
    if (!o)
        return dladdr1(addr, info, extra, flags);
    const Elf64_Sym *s = nearest_sym(o, (uintptr_t)addr);
    info->dli_fname = o->path;
    info->dli_fbase = (void *)o->map_start;
    info->dli_sname = s ? o->strtab + s->st_name : NULL;
    info->dli_saddr = s ? (void *)(o->l_addr + s->st_value) : NULL;
    if (flags == RTLD_DL_SYMENT)
        *extra = (void *)s;
    else if (flags == RTLD_DL_LINKMAP)
        *extra = o;
    return 1;
}

static int fl_dladdr(const void *addr, Dl_info *info)
{
    if (!find_own((uintptr_t)addr))
        return dladdr(addr, info);
    return fl_dladdr1(addr, info, NULL, 0);
}

static void *override_of(const char *name, uint32_t h);

static void *found_value(obj_t *d, uint32_t i)
{
    const Elf64_Sym *s = &d->symtab[i];
    if (ELF64_ST_TYPE(s->st_info) == STT_TLS)
        return tls_addr_of(d, s->st_value);
    return (void *)sym_value(d, s);
}

/* Local scope of a handle: the object and its dependencies, BFS. */
/* BFS closure over DT_NEEDED; the buffer is reused, call under g_lock. */
static obj_t **closure(obj_t *root, uint32_t *n)
{
    static uint32_t epoch;
    static obj_t **q;
    if (!q)
        q = xalloc(SCOPE_CAP * sizeof *q);
    uint32_t qn = 0, ep = ++epoch;
    q[qn++] = root;
    root->mark = ep;
    for (uint32_t k = 0; k < qn; k++) {
        obj_t *o = q[k];
        for (size_t j = 0; j < o->ndeps; j++)
            if (o->deps[j]->mark != ep) {
                o->deps[j]->mark = ep;
                q[qn++] = o->deps[j];
            }
    }
    *n = qn;
    return q;
}

static int local_lookup(obj_t *root, const ref_t *r, obj_t **od, uint32_t *oi)
{
    uint32_t n;
    obj_t **q = closure(root, &n);
    for (uint32_t k = 0; k < n; k++)
        if (!q[k]->removed && obj_lookup(q[k], r, oi)) {
            *od = q[k];
            return 1;
        }
    return 0;
}

static void *dlsym_impl(void *h, const char *name, const char *ver, void *caller)
{
    ver_t v;
    ref_t r = {name, gnu_hash(name), NULL, 1};
    if (ver) {
        v.name = ver;
        v.hash = elf_hash(ver);
        v.hidden = 1;
        r.ver = &v;
        r.newest = 0;
    }
    obj_t *d = NULL;
    uint32_t i = 0;
    int found = 0;
    if (h == RTLD_NEXT) {
        obj_t *c = find_own((uintptr_t)caller);
        if (!c)
            return ver ? dlvsym(RTLD_NEXT, name, ver) : dlsym(RTLD_NEXT, name);
        pthread_mutex_lock(&g_lock);
        found = scope_lookup(&r, c->pos + 1, &d, &i);
        pthread_mutex_unlock(&g_lock);
    } else if (h == RTLD_DEFAULT || h == g_exe) {
        void *ov = override_of(name, r.hash);
        if (ov)
            return ov;
        pthread_mutex_lock(&g_lock);
        found = scope_lookup(&r, 0, &d, &i);
        pthread_mutex_unlock(&g_lock);
        if (!found)
            return ver ? dlvsym(RTLD_DEFAULT, name, ver) : dlsym(RTLD_DEFAULT, name);
    } else if (is_own(h)) {
        pthread_mutex_lock(&g_lock);
        found = local_lookup(h, &r, &d, &i);
        pthread_mutex_unlock(&g_lock);
    } else {
        return ver ? dlvsym(h, name, ver) : dlsym(h, name);
    }
    if (!found) {
        set_dlerror("fastld: undefined symbol: %s%s%s", name, ver ? "@" : "", ver ? ver : "");
        return NULL;
    }
    return found_value(d, i);
}

static void *fl_dlsym(void *h, const char *name)
{
    return dlsym_impl(h, name, NULL, __builtin_return_address(0));
}

static void *fl_dlvsym(void *h, const char *name, const char *ver)
{
    return dlsym_impl(h, name, ver, __builtin_return_address(0));
}

static char *fl_dlerror(void)
{
    if (T.dlerr_set) {
        T.dlerr_set = 0;
        return T.dlerr;
    }
    return dlerror();
}

static void run_fini(obj_t *o);

static int cmp_init_idx_desc(const void *a, const void *b)
{
    uint32_t x = (*(obj_t *const *)a)->init_idx, y = (*(obj_t *const *)b)->init_idx;
    return x > y ? -1 : x < y;
}

/* Objects from dlopen whose references drop to zero run fini and leave the
 * scope. Their mappings stay. */
static int fl_dlclose(void *h)
{
    if (!is_own(h))
        return dlclose(h);
    obj_t *root = h;
    if (!root->dl_root)
        return 0;
    pthread_mutex_lock(&g_lock);
    uint32_t n, nd = 0;
    obj_t **q = closure(root, &n);
    obj_t **dead = xalloc((n + 1) * sizeof *dead);
    for (uint32_t k = 0; k < n; k++)
        if (q[k]->dl_root && !q[k]->removed && q[k]->dlref && --q[k]->dlref == 0)
            dead[nd++] = q[k];
    qsort(dead, nd, sizeof *dead, cmp_init_idx_desc);
    for (uint32_t k = 0; k < nd; k++)
        run_fini(dead[k]);
    for (uint32_t k = 0; k < nd; k++)
        dead[k]->removed = 1;
    if (nd)
        rebuild_sorted();
    pthread_mutex_unlock(&g_lock);
    return 0;
}

static void dlopen_ref(obj_t *root, int global)
{
    uint32_t n;
    obj_t **q = closure(root, &n);
    for (uint32_t k = 0; k < n; k++)
        if (q[k]->dl_root) {
            q[k]->dlref++;
            if (global)
                q[k]->global = 1;
        }
}

static int fl_dlinfo(void *h, int req, void *arg)
{
    if (!is_own(h))
        return dlinfo(h, req, arg);
    obj_t *o = h;
    switch (req) {
    case RTLD_DI_LINKMAP: *(void **)arg = o; return 0;
    case RTLD_DI_ORIGIN: strcpy(arg, o->origin); return 0;
    case RTLD_DI_TLS_MODID: *(size_t *)arg = o->tls_modid; return 0;
    case RTLD_DI_TLS_DATA: *(void **)arg = tls_data_of(o); return 0;
    default: set_dlerror("fastld: dlinfo request %d is not supported", req); return -1;
    }
}

static int load_more(obj_t *root);

static void *fl_dlopen(const char *file, int mode)
{
    if (!file)
        return g_exe;
    void *caller = __builtin_return_address(0);
    pthread_mutex_lock(&g_lock);
    obj_t *o = nm_get(file);
    if (!o && strchr(file, '/')) {
        struct stat sb;
        if (stat(file, &sb) == 0)
            o = find_by_inode(sb.st_dev, sb.st_ino);
    }
    if (!o && !(mode & RTLD_NOLOAD)) {
        obj_t *req = find_own((uintptr_t)caller);
        o = find_lib(file, req ? req : g_exe, 0);
        if (o && !o->foreign && o->pos == NOPOS) {
            g_new_global = !!(mode & RTLD_GLOBAL);
            g_new_root = o;
            scope_append(o);
            int rc = load_more(o);
            g_new_global = 1;
            g_new_root = NULL;
            if (rc) {
                pthread_mutex_unlock(&g_lock);
                return NULL;
            }
        }
    }
    if (o && !o->foreign)
        dlopen_ref(o, !!(mode & RTLD_GLOBAL));
    pthread_mutex_unlock(&g_lock);
    if (o && !o->foreign)
        return o;
    return dlopen(file, mode);
}

/* Threads get the exe's .tdata image in the launcher-hosted block. */
struct tramp {
    void *(*fn)(void *);
    void *arg;
};

static void *tramp_start(void *p)
{
    static_tls_init();
    struct tramp t = *(struct tramp *)p;
    free(p);
    return t.fn(t.arg);
}

static int fl_pthread_create(pthread_t *th, const pthread_attr_t *attr, void *(*fn)(void *), void *arg)
{
    if (!g_need_tramp)
        return pthread_create(th, attr, fn, arg);
    struct tramp *t = malloc(sizeof *t);
    if (!t)
        return EAGAIN;
    t->fn = fn;
    t->arg = arg;
    int rc = pthread_create(th, attr, tramp_start, t);
    if (rc)
        free(t);
    return rc;
}

static ssize_t exe_link(char *buf, size_t n)
{
    size_t l = strlen(g_exe_real);
    if (l > n)
        l = n;
    memcpy(buf, g_exe_real, l);
    return l;
}

static ssize_t fl_readlink(const char *p, char *buf, size_t n)
{
    if (p && !strcmp(p, "/proc/self/exe"))
        return exe_link(buf, n);
    return readlink(p, buf, n);
}

static ssize_t fl_readlinkat(int fd, const char *p, char *buf, size_t n)
{
    if (p && !strcmp(p, "/proc/self/exe"))
        return exe_link(buf, n);
    return readlinkat(fd, p, buf, n);
}

static struct {
    const char *name;
    void *fn;
    uint32_t hash;
} g_over[] = {
    {"__tls_get_addr", (void *)fl_tls_get_addr, 0},
    {"dl_iterate_phdr", (void *)fl_dl_iterate_phdr, 0},
    {"dladdr", (void *)fl_dladdr, 0},
    {"dladdr1", (void *)fl_dladdr1, 0},
    {"dlsym", (void *)fl_dlsym, 0},
    {"dlvsym", (void *)fl_dlvsym, 0},
    {"dlopen", (void *)fl_dlopen, 0},
    {"dlclose", (void *)fl_dlclose, 0},
    {"dlerror", (void *)fl_dlerror, 0},
    {"dlinfo", (void *)fl_dlinfo, 0},
    {"pthread_create", (void *)fl_pthread_create, 0},
    {"readlink", (void *)fl_readlink, 0},
    {"readlinkat", (void *)fl_readlinkat, 0},
};

static void *override_of(const char *name, uint32_t h)
{
    for (size_t i = 0; i < sizeof g_over / sizeof g_over[0]; i++)
        if (g_over[i].hash == h && !strcmp(g_over[i].name, name))
            return g_over[i].fn;
    return NULL;
}

/* ------------------------------------------------------ relocation */

static int g_bind_fd = -1;
static char *g_bind_buf;
static size_t g_bind_len;
#define BIND_BUF (1UL << 20)

static void bind_flush(void)
{
    size_t off = 0;
    while (off < g_bind_len) {
        ssize_t w = write(g_bind_fd, g_bind_buf + off, g_bind_len - off);
        if (w <= 0)
            die("cannot write FASTLD_BINDINGS");
        off += w;
    }
    g_bind_len = 0;
}

static void bind_put(const char *s)
{
    size_t n = strlen(s);
    if (g_bind_len + n > BIND_BUF)
        bind_flush();
    if (n > BIND_BUF)
        n = BIND_BUF;
    memcpy(g_bind_buf + g_bind_len, s, n);
    g_bind_len += n;
}

static void bind_dump(const obj_t *req, const ref_t *r, const obj_t *def)
{
    bind_put(req->path);
    bind_put("\t");
    bind_put(r->name);
    bind_put("\t");
    bind_put(r->ver ? r->ver->name : "");
    bind_put("\t");
    bind_put(def ? def->path : "");
    bind_put("\n");
}

typedef struct {
    uintptr_t val;
    obj_t *def;
    const Elf64_Sym *sym;
    int state; /* 0 not yet, 1 resolved, 2 undefined weak, 3 undefined */
} rc_ent;

static int g_reloc_errors;

static rc_ent *resolve(obj_t *o, uint32_t si, rc_ent *cache)
{
    rc_ent *c = &cache[si];
    if (c->state)
        return c;
    const Elf64_Sym *ref = &o->symtab[si];
    if (ELF64_ST_BIND(ref->st_info) == STB_LOCAL ||
        (ref->st_shndx != SHN_UNDEF && ELF64_ST_VISIBILITY(ref->st_other) != STV_DEFAULT)) {
        c->def = o;
        c->sym = ref;
        c->val = sym_value(o, ref);
        c->state = 1;
        return c;
    }
    ref_t r = {o->strtab + ref->st_name, 0, NULL, 0};
    r.hash = gnu_hash(r.name);
    if (o->versym) {
        uint16_t nd = o->versym[si] & 0x7fff;
        if (nd < o->nversions && o->versions[nd].hash)
            r.ver = &o->versions[nd];
    }
    obj_t *d = NULL;
    uint32_t di = 0;
    int found = scope_lookup(&r, 0, &d, &di);
    if (!found && !o->global && o->dl_root)
        found = local_lookup(o->dl_root, &r, &d, &di);
    if (g_bind_fd >= 0)
        bind_dump(o, &r, found ? d : NULL);
    void *ov = override_of(r.name, r.hash);
    if (ov) {
        c->def = d;
        c->sym = found ? &d->symtab[di] : NULL;
        c->val = (uintptr_t)ov;
        c->state = 1;
        return c;
    }
    if (!found) {
        if (ELF64_ST_BIND(ref->st_info) == STB_WEAK) {
            c->state = 2;
        } else {
            c->state = 3;
            st.undef++;
            if (g_reloc_errors++ < 20)
                msg("symbol lookup error: %s: undefined symbol: %s%s%s", o->path, r.name,
                    r.ver ? ", version " : "", r.ver ? r.ver->name : "");
        }
        return c;
    }
    c->def = d;
    c->sym = &d->symtab[di];
    c->val = sym_value(d, c->sym);
    c->state = 1;
    return c;
}

static void do_rela(obj_t *o, const Elf64_Rela *r, size_t n, rc_ent *cache)
{
    uintptr_t base = o->l_addr;
    for (size_t k = 0; k < n; k++, r++) {
        uint64_t *w = (uint64_t *)(base + r->r_offset);
        uint32_t t = ELF64_R_TYPE(r->r_info);
        if (LIKELY(t == R_X86_64_RELATIVE)) {
            *w = base + r->r_addend;
            st.relative++;
            continue;
        }
        uint32_t si = ELF64_R_SYM(r->r_info);
        rc_ent *c;
        st.symbolic++;
        switch (t) {
        case R_X86_64_NONE: break;
        case R_X86_64_64:
        case R_X86_64_GLOB_DAT:
        case R_X86_64_JUMP_SLOT:
            c = resolve(o, si, cache);
            *w = (c->state == 1 ? c->val : 0) + r->r_addend;
            break;
        case R_X86_64_DTPMOD64:
            if (!si) {
                *w = o->tls_modid;
            } else {
                c = resolve(o, si, cache);
                *w = c->state == 1 && c->def ? c->def->tls_modid : 0;
            }
            break;
        case R_X86_64_TPOFF64: {
            obj_t *d = o;
            size_t v = 0;
            if (si) {
                c = resolve(o, si, cache);
                if (c->state != 1 || !c->def || !c->sym)
                    die("%s: TPOFF64 against an undefined symbol", o->path);
                d = c->def;
                v = c->sym->st_value;
            }
            if (d->foreign || !d->tls_modid)
                die("%s: TPOFF64 into %s is not supported", o->path, d->path);
            make_static(d);
            *w = v + r->r_addend - (d == g_exe ? g_exe_tls_off : d->tls_tpoff);
            break;
        }
        case R_X86_64_DTPOFF64:
            if (!si) {
                *w = r->r_addend;
            } else {
                c = resolve(o, si, cache);
                *w = (c->state == 1 && c->sym ? c->sym->st_value : 0) + r->r_addend;
            }
            break;
        default: die("%s: unsupported relocation type %u", o->path, t);
        }
    }
}

static void relocate(obj_t *o, rc_ent *cache)
{
    memset(cache, 0, o->nsyms * sizeof *cache);
    uintptr_t base = o->l_addr;
    uint64_t *where = NULL;
    for (size_t k = 0; k < o->nrelr; k++) {
        uint64_t e = o->relr[k];
        if (!(e & 1)) {
            where = (uint64_t *)(base + e);
            *where++ += base;
        } else {
            uint64_t *w = where;
            for (e >>= 1; e; e >>= 1, w++)
                if (e & 1)
                    *w += base;
            where += 63;
        }
    }
    do_rela(o, o->rela, o->nrela, cache);
    do_rela(o, o->jmprel, o->njmprel, cache);
    if (o->relro_end > o->relro_start && mprotect((void *)o->relro_start, o->relro_end - o->relro_start, PROT_READ))
        die("%s: mprotect(RELRO) failed", o->path);
}

static int relocate_range(uint32_t first)
{
    uint32_t maxsyms = 1;
    for (uint32_t i = first; i < g_nscope; i++)
        if (!g_scope[i]->foreign && g_scope[i]->nsyms > maxsyms)
            maxsyms = g_scope[i]->nsyms;
    size_t csz = PAGE_UP(maxsyms * sizeof(rc_ent));
    rc_ent *cache = map_anon(csz);
    for (uint32_t i = g_nscope; i-- > first;)
        if (!g_scope[i]->foreign)
            relocate(g_scope[i], cache);
    munmap(cache, csz);
    return g_reloc_errors;
}

/* ------------------------------------------------------ init / fini */

static int g_argc;
static char **g_argv;

static void call_init(obj_t *o)
{
    if (o->init)
        o->init(g_argc, g_argv, environ);
    for (size_t k = 0; k < o->ninit_array; k++) {
        void (*f)(int, char **, char **) = o->init_array[k];
        if (f && f != (void *)-1)
            f(g_argc, g_argv, environ);
    }
}

/* Dependencies first: DFS post-order over DT_NEEDED. */
static void run_inits(obj_t *root)
{
    if (root->foreign || root->init_state)
        return;
    obj_t **stk = xalloc((g_nscope + 1) * sizeof *stk);
    size_t *it = xalloc((g_nscope + 1) * sizeof *it);
    size_t sp = 0;
    root->init_state = 1;
    stk[sp] = root;
    it[sp++] = 0;
    while (sp) {
        obj_t *o = stk[sp - 1];
        if (it[sp - 1] < o->ndeps) {
            obj_t *d = o->deps[it[sp - 1]++];
            if (!d->foreign && !d->init_state) {
                d->init_state = 1;
                stk[sp] = d;
                it[sp++] = 0;
            }
            continue;
        }
        sp--;
        o->init_idx = g_ninit;
        g_init_order[g_ninit++] = o;
        call_init(o);
        o->init_state = 2;
    }
}

/* glibc 2.31 _dl_sort_maps over l_initfini[1..]: move each object behind the
 * last object that depends on it. Same moves, but "last dependent" comes
 * from reverse edges instead of scanning every later object. */
static void sort_maps(obj_t **maps, uint32_t n)
{
    if (n <= 1)
        return;
    uint32_t *at = xalloc(SCOPE_CAP * sizeof *at); /* scope pos -> index in maps */
    uint32_t *rcnt = xalloc((SCOPE_CAP + 1) * sizeof *rcnt);
    for (uint32_t i = 0; i < SCOPE_CAP; i++)
        at[i] = NOPOS;
    for (uint32_t i = 0; i < n; i++)
        at[maps[i]->pos] = i;
    size_t total = 0;
    for (uint32_t i = 0; i < n; i++)
        for (size_t j = 0; j < maps[i]->ndeps; j++) {
            rcnt[maps[i]->deps[j]->pos + 1]++;
            total++;
        }
    for (uint32_t i = 0; i < SCOPE_CAP; i++)
        rcnt[i + 1] += rcnt[i];
    obj_t **rdep = xalloc((total + 1) * sizeof *rdep);
    uint32_t *fill = xalloc(SCOPE_CAP * sizeof *fill);
    for (uint32_t i = 0; i < n; i++)
        for (size_t j = 0; j < maps[i]->ndeps; j++) {
            uint32_t d = maps[i]->deps[j]->pos;
            rdep[rcnt[d] + fill[d]++] = maps[i];
        }
    uint16_t *seen = xalloc(n * sizeof *seen);
    uint32_t i = 0;
    for (;;) {
        ++seen[i];
        obj_t *thisp = maps[i];
        uint32_t k = 0;
        for (uint32_t r = rcnt[thisp->pos]; r < rcnt[thisp->pos + 1]; r++) {
            uint32_t pk = at[rdep[r]->pos];
            if (pk != NOPOS && pk > i && pk > k)
                k = pk;
        }
        if (k) {
            memmove(&maps[i], &maps[i + 1], (k - i) * sizeof maps[0]);
            maps[k] = thisp;
            for (uint32_t q = i; q <= k; q++)
                at[maps[q]->pos] = q;
            if (seen[i + 1] > n - i) {
                ++i;
                memset(&seen[i], 0, (n - i) * sizeof seen[0]);
                continue;
            }
            uint16_t ts = seen[i];
            memmove(&seen[i], &seen[i + 1], (k - i) * sizeof seen[0]);
            seen[k] = ts;
            continue;
        }
        if (++i == n)
            break;
        memset(&seen[i], 0, (n - i) * sizeof seen[0]);
    }
}

static void run_fini(obj_t *o)
{
    if (o->fini_done || o->init_state != 2)
        return;
    o->fini_done = 1;
    for (size_t k = o->nfini_array; k-- > 0;) {
        void (*f)(void) = o->fini_array[k];
        if (f && f != (void *)-1)
            f();
    }
    if (o->fini)
        o->fini();
}

static void run_finis(void)
{
    for (uint32_t i = g_ninit; i-- > 0;)
        run_fini(g_init_order[i]);
}

/* dlopen of a new library from the build dir: approximate RTLD_GLOBAL. */
static int load_more(obj_t *root)
{
    uint32_t first = root->pos;
    load_deps_from(first);
    for (uint32_t i = first; i < g_nscope; i++) {
        obj_t *o = g_scope[i];
        if (!o->foreign && o->tls_memsz) {
            if (o->want_static) {
                set_dlerror("fastld: %s: static TLS in a dlopen'ed library is not supported", o->path);
                return -1;
            }
            o->tls_modid = g_ntls;
            g_tls_obj[g_ntls] = o;
            __atomic_store_n(&g_ntls, g_ntls + 1, __ATOMIC_RELEASE);
        }
        ix_add_obj(o);
    }
    g_reloc_errors = 0;
    if (relocate_range(first)) {
        set_dlerror("fastld: %s: undefined symbols", root->path);
        return -1;
    }
    for (uint32_t i = first; i < g_nscope; i++)
        g_scope[i]->ready = 1;
    rebuild_sorted();
    run_inits(root);
    return 0;
}

/* ------------------------------------------------------------- main */

static char *find_exe(const char *name)
{
    if (strchr(name, '/'))
        return xstrdup(name);
    const char *path = getenv("PATH");
    char buf[PATH_MAX];
    while (path && *path) {
        const char *e = strchr(path, ':');
        size_t n = e ? (size_t)(e - path) : strlen(path);
        if (snprintf(buf, sizeof buf, "%.*s/%s", (int)n, n ? path : ".", name) < (int)sizeof buf &&
            access(buf, X_OK) == 0)
            return xstrdup(buf);
        path = e ? e + 1 : NULL;
    }
    die("%s: not found in PATH", name);
}

/* What ld.so in explicit mode does: auxv describes the program. */
static void patch_auxv(const char *exe_path)
{
    char **e = environ;
    while (*e)
        e++;
    for (Elf64_auxv_t *av = (Elf64_auxv_t *)(e + 1); av->a_type != AT_NULL; av++) {
        switch (av->a_type) {
        case AT_PHDR: av->a_un.a_val = (uintptr_t)g_exe->phdr; break;
        case AT_PHNUM: av->a_un.a_val = g_exe->phnum; break;
        case AT_ENTRY: av->a_un.a_val = g_exe->l_addr + g_exe->e_entry; break;
        case AT_EXECFN: av->a_un.a_val = (uintptr_t)exe_path; break;
        default: break;
        }
    }
}

static void *scope_fn(const char *name)
{
    ref_t r = {name, gnu_hash(name), NULL, 0};
    obj_t *d;
    uint32_t i;
    if (!scope_lookup(&r, 0, &d, &i))
        return NULL;
    return (void *)sym_value(d, &d->symtab[i]);
}

static void switch_malloc(void)
{
    r_malloc = scope_fn("malloc");
    r_free = scope_fn("free");
    r_calloc = scope_fn("calloc");
    r_realloc = scope_fn("realloc");
    r_memalign = scope_fn("memalign");
    r_posix_memalign = scope_fn("posix_memalign");
    r_usable = scope_fn("malloc_usable_size");
    if (!r_malloc || !r_free || !r_calloc || !r_realloc || (!r_memalign && !r_posix_memalign))
        die("no malloc family in the scope");
    __atomic_store_n(&malloc_ready, 1, __ATOMIC_RELEASE);
}

int main(int argc, char **argv)
{
    double t0 = now_ms();
    if ((char *)&T + sizeof T != tp())
        die("unexpected TLS layout of the launcher");
    int ai = 1;
    if (ai < argc && !strcmp(argv[ai], "--"))
        ai++;
    if (ai >= argc) {
        msg("usage: fastld [--] EXE [ARGS...]");
        return 2;
    }
    const char *dis = getenv("FASTLD_DISABLE");
    if (dis && *dis && strcmp(dis, "0")) {
        execv(argv[ai], argv + ai);
        die("exec %s: %s", argv[ai], strerror(errno));
    }
    const char *s = getenv("FASTLD_STATS");
    g_stats = s && *s && strcmp(s, "0");
    const char *bf = getenv("FASTLD_BINDINGS");
    if (bf && *bf) {
        g_bind_fd = open(bf, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (g_bind_fd < 0)
            die("%s: %s", bf, strerror(errno));
        g_bind_buf = xalloc(BIND_BUF);
    }
    g_ld_library_path = getenv("LD_LIBRARY_PATH");
    for (size_t i = 0; i < sizeof g_over / sizeof g_over[0]; i++)
        g_over[i].hash = gnu_hash(g_over[i].name);
    host_tls_get_addr = (void *(*)(tls_index *))dlsym(RTLD_DEFAULT, "__tls_get_addr");
    if (pthread_key_create(&g_tls_key, tls_dtor))
        die("pthread_key_create failed");

    g_scope = xalloc(SCOPE_CAP * sizeof *g_scope);
    g_own = xalloc(SCOPE_CAP * sizeof *g_own);
    g_init_order = xalloc(SCOPE_CAP * sizeof *g_init_order);
    register_host_objects();

    char *exe_path = find_exe(argv[ai]);
    char *real = realpath(exe_path, NULL);
    if (!real)
        die("%s: %s", exe_path, strerror(errno));
    g_exe_real = xstrdup(real);
    int fd = open(exe_path, O_RDONLY | O_CLOEXEC);
    struct stat sb;
    if (fd < 0 || fstat(fd, &sb))
        die("%s: %s", exe_path, strerror(errno));
    g_exe = load_file(exe_path, fd, &sb);
    close(fd);
    g_exe->origin = dir_of(g_exe_real);
    g_exe->l_name = "";
    scope_append(g_exe);
    load_deps_from(0);
    for (uint32_t i = 1; i < g_nscope; i++) {
        g_scope[i - 1]->l_next = g_scope[i];
        g_scope[i]->l_prev = g_scope[i - 1];
    }
    double t1 = now_ms();

    size_t total = 0;
    for (uint32_t i = 0; i < g_nscope; i++)
        total += g_scope[i]->nsyms - g_scope[i]->symoffset;
    ix_alloc(total);
    for (uint32_t i = 0; i < g_nscope; i++)
        ix_add_obj(g_scope[i]);
    double t2 = now_ms();

    tls_layout();
    if (relocate_range(0))
        die("%d undefined symbols", g_reloc_errors);
    if (g_bind_fd >= 0) {
        bind_flush();
        close(g_bind_fd);
    }
    double t3 = now_ms();

    if (g_ntls_area > 2)
        tls_alloc_area();
    static_tls_init();
    switch_malloc();
    for (uint32_t i = 0; i < g_nscope; i++)
        g_scope[i]->ready = 1;
    rebuild_sorted();

    g_argc = argc - ai;
    g_argv = argv + ai;
    program_invocation_name = g_argv[0];
    const char *sl = strrchr(g_argv[0], '/');
    program_invocation_short_name = (char *)(sl ? sl + 1 : g_argv[0]);
    int (*mainf)(int, char **, char **) = (int (*)(int, char **, char **))scope_fn("main");
    if (!mainf)
        die("%s: no main", exe_path);
    patch_auxv(exe_path);

    /* As glibc: preinit, libraries in sorted order, fini registration, exe. */
    obj_t **order = xalloc(g_nscope * sizeof *order);
    memcpy(order, g_scope, g_nscope * sizeof *order);
    sort_maps(order + 1, g_nscope - 1);
    double t3s = now_ms();
    for (size_t k = 0; k < g_exe->npreinit_array; k++)
        g_exe->preinit_array[k](g_argc, g_argv, environ);
    for (uint32_t i = g_nscope; i-- > 1;) {
        obj_t *o = order[i];
        if (o->foreign)
            continue;
        o->init_state = 2;
        o->init_idx = g_ninit;
        g_init_order[g_ninit++] = o;
        call_init(o);
    }
    atexit(run_finis);
    g_exe->init_state = 2;
    g_exe->init_idx = g_ninit;
    g_init_order[g_ninit++] = g_exe;
    call_init(g_exe);
    double t4 = now_ms();

    if (g_stats) {
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        msg("objects %u (own %u, system %u), tls modules %zu", g_nscope, g_nown, g_nscope - g_nown, g_ntls - 2);
        msg("symbols indexed %zu, distinct %zu", st.indexed, ix_cnt);
        msg("relocs relative %zu, symbolic %zu, lookups %zu, slow %zu", st.relative, st.symbolic, st.lookups,
            st.slow);
        msg("map %.1f ms, index %.1f ms, reloc %.1f ms, sort %.1f ms, init %.1f ms, total %.1f ms", t1 - t0,
            t2 - t1, t3 - t2, t3s - t3, t4 - t3s, t4 - t0);
        msg("cpu user %.2f s, sys %.2f s, maxrss %ld MB", ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6,
            ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6, ru.ru_maxrss / 1024);
    }
    exit(mainf(g_argc, g_argv, environ));
}
