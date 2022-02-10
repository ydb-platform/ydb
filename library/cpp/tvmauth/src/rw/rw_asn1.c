#include "rw.h"

#include <contrib/libs/openssl/include/openssl/asn1.h>
#include <contrib/libs/openssl/include/openssl/asn1t.h>
#include <contrib/libs/openssl/include/openssl/rand.h>

#include <stdio.h>

/* Override the default new methods */
/* This callback is used by OpenSSL's ASN.1 parser */
static int SignatureCallback(int operation, ASN1_VALUE** pval, const ASN1_ITEM* it, void* exarg) { 
    (void)it;
    (void)exarg;

    if (operation == ASN1_OP_NEW_PRE) {
        TRwSignature* sig; 
        sig = OPENSSL_malloc(sizeof(TRwSignature)); 
        if (!sig)
            return 0;
        sig->S = NULL; 
        *pval = (ASN1_VALUE*)sig;
        return 2;
    }
    return 1;
}

/* ASN.1 structure representing RW signature value */
ASN1_SEQUENCE_cb(TRwSignature, SignatureCallback) = { 
    ASN1_SIMPLE(TRwSignature, S, BIGNUM), 
} ASN1_SEQUENCE_END_cb(TRwSignature, TRwSignature) 

    /* i2d_ and d2i functions implementation for RW */ 
    IMPLEMENT_ASN1_ENCODE_FUNCTIONS_const_fname(TRwSignature, TRwSignature, TRwSignature) 

    /* Override the default free and new methods */
    static int RwCallback(int operation, ASN1_VALUE** pval, const ASN1_ITEM* it, void* exarg) { 
    (void)it;
    (void)exarg;

    if (operation == ASN1_OP_NEW_PRE) {
        *pval = (ASN1_VALUE*)RwNew(); 
        if (*pval)
            return 2;
        return 0;
    } else if (operation == ASN1_OP_FREE_PRE) {
        RwFree((TRwKey*)*pval); 
        *pval = NULL;
        return 2;
    }
    return 1;
}

/* ASN.1 representation of RW's private key */
ASN1_SEQUENCE_cb(RWPrivateKey, RwCallback) = { 
    ASN1_SIMPLE(TRwKey, N, BIGNUM), 
    ASN1_SIMPLE(TRwKey, P, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Q, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Iqmp, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Dq, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Dp, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Twomp, CBIGNUM), 
    ASN1_SIMPLE(TRwKey, Twomq, CBIGNUM)} ASN1_SEQUENCE_END_cb(TRwKey, RWPrivateKey); 

/* i2d_ and d2i_ functions for RW's private key */ 
IMPLEMENT_ASN1_ENCODE_FUNCTIONS_const_fname(TRwKey, RWPrivateKey, RWPrivateKey); 

/* ASN.1 representation of RW public key */ 
ASN1_SEQUENCE_cb(RWPublicKey, RwCallback) = { 
    ASN1_SIMPLE(TRwKey, N, BIGNUM), 
} ASN1_SEQUENCE_END_cb(TRwKey, RWPublicKey); 

/* i2d_ and d2i functions for RW public key */ 
IMPLEMENT_ASN1_ENCODE_FUNCTIONS_const_fname(TRwKey, RWPublicKey, RWPublicKey); 

TRwKey* RwPublicKeyDup(TRwKey* rw) { 
    return ASN1_item_dup(ASN1_ITEM_rptr(RWPublicKey), rw);
}

TRwKey* RwPrivateKeyDup(TRwKey* rw) { 
    return ASN1_item_dup(ASN1_ITEM_rptr(RWPrivateKey), rw);
}
