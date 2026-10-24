#include <pthread.h>
#include <stdio.h>

void lib_touch(int v, const char *name);

static void *thread(void *arg)
{
    (void)arg;
    lib_touch(7, "thread");
    return NULL;
}

int main(void)
{
    for (int i = 0; i < 2; i++) {
        pthread_t t;
        pthread_create(&t, NULL, thread, NULL);
        pthread_join(t, NULL);
    }
    printf("main done\n");
    return 0;
}
