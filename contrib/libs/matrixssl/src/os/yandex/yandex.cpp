#include <util/random/entropy.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>
#include <util/stream/input.h>
#include <util/generic/yexception.h>

#include "../osLayer.h"

int32 sslCreateMutex(sslMutex_t *mutex) {
    *mutex = new TMutex();
    return 0;
}

int32 sslLockMutex(sslMutex_t *mutex) {
    Y_ASSERT(*mutex);
    ((TMutex *)(*mutex))->Acquire();
    return 0;
}

int32 sslUnlockMutex(sslMutex_t *mutex) {
    Y_ASSERT(*mutex);
    ((TMutex *)(*mutex))->Release();
    return 0;
}

void sslDestroyMutex(sslMutex_t *mutex) {
    delete (TMutex*)(*mutex);
    *mutex = NULL;
}

int32 sslInitMsecs(sslTime_t *timePtr) {
    *timePtr = millisec();
    return (int32)(*timePtr);
}

long sslDiffMsecs(sslTime_t then, sslTime_t now) {
    return (long)(now - then);
}

int32 sslDiffSecs(sslTime_t then, sslTime_t now) {
    return (int32)((now - then) / 1000);
}

int32 sslCompareTime(sslTime_t a, sslTime_t b) {
    return (a < b) ? 1 : 0;
}

int32 sslOpenOsdep(void) {
    return 0;
}

int32 sslCloseOsdep(void) {
    return 0;
}

int32 sslGetEntropy(unsigned char *bytes, int32 size) {
    size_t read = EntropyPool().Load(bytes, size);
    return (read ? read : -1);
}

#ifdef MSSLDEBUG
void psBreak(void)
{
    abort();
}
#endif

/******************************************************************************/
