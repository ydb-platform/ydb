#include "rw.h"

TRwSignature* RwSignatureNew(void) {
    TRwSignature* sig = NULL;
    sig = malloc(sizeof(TRwSignature));
    if (!sig)
        return NULL;
    sig->S = NULL;
    return sig;
}

void RwSignatureFree(TRwSignature* sig) {
    if (sig) {
        if (sig->S)
            BN_free(sig->S);
        free(sig);
    }
}

int RwNoPaddingSign(int flen, const unsigned char* from, unsigned char* to, TRwKey* rw) {
    int i = 0, r = 0, num = -1;
    TRwSignature* sig = NULL;

    if (!rw || !rw->N || !rw->Meth || !rw->Meth->RwSign || !from || !to)
        goto err;

    if ((sig = rw->Meth->RwSign(from, flen, rw)) == NULL)
        goto err;
    num = BN_num_bytes(rw->N);

    r = BN_bn2bin(sig->S, to);
    if (r < 0)
        goto err;

    /* put zeroes to the rest of the 'to' buffer */
    for (i = r; i < num; i++) {
        to[i] = 0x00;
    }

err:
    if (sig != NULL) {
        RwSignatureFree(sig);
    }

    return r;
}
