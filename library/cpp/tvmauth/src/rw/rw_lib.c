#include "rw.h"

#include <contrib/libs/openssl/include/openssl/asn1.h>

#include <stdio.h>

TRwKey* RwNew(void) {
    TRwKey* ret = NULL;

    ret = (TRwKey*)malloc(sizeof(TRwKey));
    if (ret == NULL) {
        return (NULL);
    }
    ret->Meth = RwDefaultMethods();

    ret->P = NULL;
    ret->Q = NULL;
    ret->N = NULL;
    ret->Iqmp = NULL;
    ret->Twomq = NULL;
    ret->Twomp = NULL;
    ret->Dp = NULL;
    ret->Dq = NULL;

    return ret;
}

void RwFree(TRwKey* r) {
    if (r == NULL)
        return;

    if (r->P != NULL)
        BN_clear_free(r->P);
    if (r->Q != NULL)
        BN_clear_free(r->Q);
    if (r->N != NULL)
        BN_clear_free(r->N);
    if (r->Iqmp != NULL)
        BN_clear_free(r->Iqmp);
    if (r->Dp != NULL)
        BN_clear_free(r->Dp);
    if (r->Dq != NULL)
        BN_clear_free(r->Dq);
    if (r->Twomp != NULL)
        BN_clear_free(r->Twomp);
    if (r->Twomq != NULL)
        BN_clear_free(r->Twomq);

    free(r);
}

int RwSize(const TRwKey* r) {
    int ret = 0, i = 0;
    ASN1_INTEGER bs;
    unsigned char buf[4]; /* 4 bytes looks really small.
                             However, i2d_ASN1_INTEGER() will not look
                             beyond the first byte, as long as the second
                             parameter is NULL. */

    i = BN_num_bits(r->N);
    bs.length = (i + 7) / 8;
    bs.data = buf;
    bs.type = V_ASN1_INTEGER;
    /* If the top bit is set the asn1 encoding is 1 larger. */
    buf[0] = 0xff;

    i = i2d_ASN1_INTEGER(&bs, NULL);

    ret = ASN1_object_size(1, i, V_ASN1_SEQUENCE);
    return ret;
}

int RwModSize(const TRwKey* rw) {
    if (rw == NULL || rw->N == NULL)
        return 0;
    return BN_num_bytes(rw->N);
}
