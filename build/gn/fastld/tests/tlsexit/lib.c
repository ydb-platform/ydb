/* Dynamic (general-dynamic) TLS read by a pthread key destructor at the
 * thread exit, as util/system/tls.cpp does with the TLS of pg_wrapper. */
#include <pthread.h>
#include <stdio.h>

__thread int tls_value = 42;
__thread char tls_name[16] = "initial";

static pthread_key_t key;

static void dtor(void *p)
{
    int round = (int)(long)p;
    printf("dtor round %d: value=%d name=%s\n", round, tls_value, tls_name);
    fflush(stdout);
    if (round < 2)
        pthread_setspecific(key, (void *)(long)(round + 1));
}

__attribute__((constructor)) static void init(void)
{
    pthread_key_create(&key, dtor);
}

void lib_touch(int v, const char *name)
{
    tls_value = v;
    snprintf(tls_name, sizeof(tls_name), "%s", name);
    pthread_setspecific(key, (void *)1L);
}
