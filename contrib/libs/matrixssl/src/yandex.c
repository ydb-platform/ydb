/* Made in Yandex */

#include "matrixInternal.h"

void matrixSslClearByteCount(ssl_t *ctx) {
#ifdef USE_ARC4
    ctx->sec.decryptCtx.arc4.byteCount = 0;
#endif
}
