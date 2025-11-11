
#include <stdlib.h> /* for malloc, calloc, free */
#include "header.h"

extern struct SN_env * SN_create_env(int S_size, int I_size)
{
    static const struct SN_env default_SN_env = {};
    struct SN_env * z = (struct SN_env *) malloc(sizeof(struct SN_env));
    if (z == NULL) return NULL;
    *z = default_SN_env;
    z->p = create_s();
    if (z->p == NULL) goto error;
    if (S_size)
    {
        int i;
        z->S = (symbol * *) malloc(S_size * sizeof(symbol *));
        if (z->S == NULL) goto error;

        for (i = 0; i < S_size; i++)
        {
            z->S[i] = create_s();
            if (z->S[i] == NULL) {
                S_size = i;
                goto error;
            }
        }
    }

    if (I_size)
    {
        z->I = (int *) calloc(I_size, sizeof(int));
        if (z->I == NULL) goto error;
    }

    return z;
error:
    SN_close_env(z, S_size);
    return NULL;
}

extern void SN_close_env(struct SN_env * z, int S_size)
{
    if (z == NULL) return;
    if (z->S)
    {
        int i;
        for (i = 0; i < S_size; i++)
        {
            lose_s(z->S[i]);
        }
        free(z->S);
    }
    free(z->I);
    if (z->p) lose_s(z->p);
    free(z);
}

extern int SN_set_current(struct SN_env * z, int size, const symbol * s)
{
    int err = replace_s(z, 0, z->l, size, s, NULL);
    z->c = 0;
    return err;
}
