#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
void *memcpy_old(void *, const void *, size_t);
__asm__(".symver memcpy_old, memcpy@GLIBC_2.2.5");
int pcw_old(pthread_cond_t *, pthread_mutex_t *);
__asm__(".symver pcw_old, pthread_cond_wait@GLIBC_2.2.5");
void *lib_memcpy_old(void), *lib_memcpy_new(void), *lib_pcw_old(void), *lib_pcw_new(void);
void *lib_strlen(void);
long lib_random(void);
int lib_rand(void);
/* Exported (-rdynamic) unversioned definition: satisfies rand@GLIBC_2.2.5 in v_lib. */
int rand(void) { return 31337; }
static const char *yn(int c) { return c ? "yes" : "no"; }
int main(void) {
    void *libc = dlopen("libc.so.6", RTLD_NOW | RTLD_NOLOAD);
    void *lpt = dlopen("libpthread.so.0", RTLD_NOW | RTLD_NOLOAD);
    void *mc225 = dlvsym(libc, "memcpy", "GLIBC_2.2.5");
    void *mc214 = dlvsym(libc, "memcpy", "GLIBC_2.14");
    /* libc.so.6 also defines pthread_cond_wait (forwarders, both versions) and precedes
       libpthread.so.0 in the scope, so references bind to libc's copies. */
    void *pc225 = dlvsym(libc, "pthread_cond_wait", "GLIBC_2.2.5");
    void *pc232 = dlvsym(libc, "pthread_cond_wait", "GLIBC_2.3.2");
    printf("libc's pthread_cond_wait differs from libpthread's: %s\n",
           yn(pc232 != dlvsym(lpt, "pthread_cond_wait", "GLIBC_2.3.2")));
    printf("dlvsym found all four: %s\n", yn(mc225 && mc214 && pc225 && pc232));
    printf("memcpy@2.2.5 differs from memcpy@@2.14: %s\n", yn(mc225 != mc214));
    printf("exe memcpy@2.2.5 bound right: %s\n", yn((void *)memcpy_old == mc225));
    printf("exe memcpy default bound right: %s\n", yn((void *)memcpy == mc214));
    printf("lib memcpy@2.2.5 bound right: %s\n", yn(lib_memcpy_old() == mc225));
    printf("lib memcpy default bound right: %s\n", yn(lib_memcpy_new() == mc214));
    printf("exe pthread_cond_wait@2.2.5 bound right: %s\n", yn((void *)pcw_old == pc225));
    printf("exe pthread_cond_wait default bound right: %s\n", yn((void *)pthread_cond_wait == pc232));
    printf("lib pthread_cond_wait@2.2.5 bound right: %s\n", yn(lib_pcw_old() == pc225));
    printf("lib pthread_cond_wait default bound right: %s\n", yn(lib_pcw_new() == pc232));
    printf("lib strlen (ifunc) == dlsym: %s\n", yn(lib_strlen() == dlsym(libc, "strlen")));
    printf("exe strlen (ifunc) == dlsym: %s\n", yn((void *)strlen == dlsym(libc, "strlen")));
    char buf[] = "abcdefghij";
    memcpy_old(buf + 1, buf, 8); /* 2.2.5 memcpy is memmove */
    printf("memcpy@2.2.5 overlap result: %s\n", buf);
    printf("lib random() -> v_def unversioned def: %ld\n", lib_random());
    printf("lib rand() -> exe unversioned def: %d\n", lib_rand());
    return 0;
}
