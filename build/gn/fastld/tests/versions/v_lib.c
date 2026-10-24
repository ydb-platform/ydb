#include <pthread.h>
#include <stdlib.h>
#include <string.h>
void *memcpy_old(void *, const void *, size_t);
__asm__(".symver memcpy_old, memcpy@GLIBC_2.2.5");
int pcw_old(pthread_cond_t *, pthread_mutex_t *);
__asm__(".symver pcw_old, pthread_cond_wait@GLIBC_2.2.5");
void *lib_memcpy_old(void) { return (void *)memcpy_old; }
void *lib_memcpy_new(void) { return (void *)memcpy; }
void *lib_pcw_old(void) { return (void *)pcw_old; }
void *lib_pcw_new(void) { return (void *)pthread_cond_wait; }
void *lib_strlen(void) { return (void *)strlen; } /* IFUNC in libc */
long lib_random(void) { return random(); }
int lib_rand(void) { return rand(); }
