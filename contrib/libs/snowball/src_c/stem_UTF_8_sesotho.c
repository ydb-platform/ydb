/* Generated from sesotho.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_sesotho.h"

#include <stddef.h>

#include "../runtime/snowball_runtime.h"

struct SN_local {
    struct SN_env z;
    int i_pV;
};

typedef struct SN_local SN_local;

#ifdef __cplusplus
extern "C" {
#endif
extern int sesotho_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_remove_nominal_suffixes(struct SN_env * z);
static int r_remove_verb_suffixes(struct SN_env * z);
static int r_remove_noun_prefixes(struct SN_env * z);
static int r_mark_regions(struct SN_env * z);


static const symbol s_0_0[2] = { 'b', 'a' };
static const symbol s_0_1[3] = { 'b', 'o', 'i' };
static const symbol s_0_2[2] = { 'l', 'e' };
static const symbol s_0_3[2] = { 'l', 'i' };
static const symbol s_0_4[2] = { 'm', 'a' };
static const symbol s_0_5[2] = { 'm', 'e' };
static const symbol s_0_6[2] = { 'm', 'o' };
static const symbol s_0_7[2] = { 's', 'e' };
static const struct among a_0[8] = {
{ 2, s_0_0, 0, -1, 0},
{ 3, s_0_1, 0, -1, 0},
{ 2, s_0_2, 0, -1, 0},
{ 2, s_0_3, 0, -1, 0},
{ 2, s_0_4, 0, -1, 0},
{ 2, s_0_5, 0, -1, 0},
{ 2, s_0_6, 0, -1, 0},
{ 2, s_0_7, 0, -1, 0}
};

static const symbol s_1_0[1] = { 'a' };
static const symbol s_1_1[3] = { 'e', 'l', 'a' };
static const symbol s_1_2[3] = { 'i', 's', 'a' };
static const symbol s_1_3[2] = { 'w', 'a' };
static const symbol s_1_4[3] = { 'i', 'l', 'e' };
static const symbol s_1_5[4] = { 'e', 't', 's', 'e' };
static const symbol s_1_6[3] = { 'a', 'n', 'g' };
static const symbol s_1_7[3] = { 'e', 'n', 'g' };
static const symbol s_1_8[3] = { 'o', 'n', 'g' };
static const struct among a_1[9] = {
{ 1, s_1_0, 0, 1, 0},
{ 3, s_1_1, -1, 1, 0},
{ 3, s_1_2, -2, 1, 0},
{ 2, s_1_3, -3, 1, 0},
{ 3, s_1_4, 0, 1, 0},
{ 4, s_1_5, 0, 1, 0},
{ 3, s_1_6, 0, 1, 0},
{ 3, s_1_7, 0, 1, 0},
{ 3, s_1_8, 0, 1, 0}
};

static const symbol s_2_0[3] = { 'a', 'n', 'a' };
static const symbol s_2_1[5] = { 'n', 'y', 'a', 'n', 'a' };
static const symbol s_2_2[2] = { 'o', 'a' };
static const symbol s_2_3[1] = { 'i' };
static const symbol s_2_4[3] = { 'a', 'n', 'o' };
static const struct among a_2[5] = {
{ 3, s_2_0, 0, 1, 0},
{ 5, s_2_1, -1, 1, 0},
{ 2, s_2_2, 0, 1, 0},
{ 1, s_2_3, 0, 1, 0},
{ 3, s_2_4, 0, 1, 0}
};

static const unsigned char g_v[] = { 17, 65, 16 };

static int r_mark_regions(struct SN_env * z) {
    {
        int v_1 = z->c;
        {
            int ret = out_grouping_U(z, g_v, 97, 117, 1);
            if (ret < 0) return 0;
            z->c += ret;
        }
        ((SN_local *)z)->i_pV = z->c;
        z->c = v_1;
    }
    {
        int v_2 = z->c;
        {
            int ret = skip_utf8(z->p, z->c, z->l, 2);
            if (ret < 0) return 0;
            z->c = ret;
        }
        if (z->c <= ((SN_local *)z)->i_pV) goto lab0;
        ((SN_local *)z)->i_pV = z->c;
    lab0:
        z->c = v_2;
    }
    return 1;
}

static int r_remove_noun_prefixes(struct SN_env * z) {
    z->bra = z->c;
    if (z->c + 1 >= z->l || z->p[z->c + 1] >> 5 != 3 || !((33314 >> (z->p[z->c + 1] & 0x1f)) & 1)) return 0;
    if (!find_among(z, a_0, 8, 0)) return 0;
    z->ket = z->c;
    {
        int v_1 = z->c;
        {
            int ret = skip_utf8(z->p, z->c, z->l, 1);
            if (ret < 0) return 0;
            z->c = ret;
        }
        if (z->c >= z->l) return 0;
        z->c = v_1;
    }
    {
        int ret = out_grouping_U(z, g_v, 97, 117, 1);
        if (ret < 0) return 0;
        z->c += ret;
    }
    {
        int ret = slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_remove_verb_suffixes(struct SN_env * z) {
    {
        int v_1;
        if (z->c < ((SN_local *)z)->i_pV) return 0;
        v_1 = z->lb; z->lb = ((SN_local *)z)->i_pV;
        z->ket = z->c;
        if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((162 >> (z->p[z->c - 1] & 0x1f)) & 1)) { z->lb = v_1; return 0; }
        if (!find_among_b(z, a_1, 9, 0)) { z->lb = v_1; return 0; }
        z->bra = z->c;
        {
            int ret = slice_del(z);
            if (ret < 0) return ret;
        }
        z->lb = v_1;
    }
    return 1;
}

static int r_remove_nominal_suffixes(struct SN_env * z) {
    {
        int v_1;
        if (z->c < ((SN_local *)z)->i_pV) return 0;
        v_1 = z->lb; z->lb = ((SN_local *)z)->i_pV;
        z->ket = z->c;
        if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((33282 >> (z->p[z->c - 1] & 0x1f)) & 1)) { z->lb = v_1; return 0; }
        if (!find_among_b(z, a_2, 5, 0)) { z->lb = v_1; return 0; }
        z->bra = z->c;
        {
            int ret = slice_del(z);
            if (ret < 0) return ret;
        }
        z->lb = v_1;
    }
    return 1;
}

extern int sesotho_UTF_8_stem(struct SN_env * z) {
    {
        int ret = r_mark_regions(z);
        if (ret <= 0) return ret;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_1 = z->l - z->c;
        {
            int ret = r_remove_nominal_suffixes(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_1;
    }
    {
        int v_2 = z->l - z->c;
        {
            int ret = r_remove_verb_suffixes(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_2;
    }
    z->c = z->lb;
    {
        int v_3 = z->c;
        {
            int ret = r_remove_noun_prefixes(z);
            if (ret < 0) return ret;
        }
        z->c = v_3;
    }
    return 1;
}

extern struct SN_env * sesotho_UTF_8_create_env(void) {
    struct SN_env * z = SN_new_env(sizeof(SN_local));
    if (z) {
        ((SN_local *)z)->i_pV = 0;
    }
    return z;
}

extern void sesotho_UTF_8_close_env(struct SN_env * z) {
    SN_delete_env(z);
}

