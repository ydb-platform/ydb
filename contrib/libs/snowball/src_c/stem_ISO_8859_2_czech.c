/* Generated from czech.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_ISO_8859_2_czech.h"

#include <stddef.h>

#include "../runtime/snowball_runtime.h"

struct SN_local {
    struct SN_env z;
    int i_p1;
};

typedef struct SN_local SN_local;

#ifdef __cplusplus
extern "C" {
#endif
extern int czech_ISO_8859_2_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_case_suffix(struct SN_env * z);
static int r_possessive_suffix(struct SN_env * z);
static int r_mark_regions(struct SN_env * z);
static int r_palatalise_i(struct SN_env * z);
static int r_palatalise_e(struct SN_env * z);

static const symbol s_0[] = { 'k' };
static const symbol s_1[] = { 0xED, 'n', 'k' };
static const symbol s_2[] = { 'k' };
static const symbol s_3[] = { 0xED, 'n', 'k' };
static const symbol s_4[] = { 'c', 'k' };
static const symbol s_5[] = { 's', 'k' };
static const symbol s_6[] = { 'e', 't' };
static const symbol s_7[] = { 't', 0xF8 };
static const symbol s_8[] = { 'b' };
static const symbol s_9[] = { 'c' };
static const symbol s_10[] = { 'k' };
static const symbol s_11[] = { 0xF2, 'k' };
static const symbol s_12[] = { 'n' };
static const symbol s_13[] = { 't' };
static const symbol s_14[] = { 'v' };
static const symbol s_15[] = { 't' };

static const symbol s_0_0[1] = { 'c' };
static const symbol s_0_1[2] = { 'n', 'c' };
static const symbol s_0_2[3] = { 0xED, 'n', 'c' };
static const symbol s_0_3[3] = { 'a', 'v', 'c' };
static const symbol s_0_4[3] = { 'o', 'v', 'c' };
static const struct among a_0[5] = {
{ 1, s_0_0, 0, 1, 0},
{ 2, s_0_1, -1, -1, 0},
{ 3, s_0_2, -1, 2, 0},
{ 3, s_0_3, -3, -1, 0},
{ 3, s_0_4, -4, -1, 0}
};

static const symbol s_1_0[1] = { 'c' };
static const symbol s_1_1[2] = { 'n', 'c' };
static const symbol s_1_2[3] = { 0xED, 'n', 'c' };
static const symbol s_1_3[3] = { 'a', 'v', 'c' };
static const symbol s_1_4[3] = { 'o', 'v', 'c' };
static const symbol s_1_5[2] = { 0xB9, 't' };
static const symbol s_1_6[4] = { 'd', 'e', 0xB9, 't' };
static const symbol s_1_7[4] = { 'l', 'e', 0xB9, 't' };
static const symbol s_1_8[3] = { 'i', 0xB9, 't' };
static const symbol s_1_9[5] = { 'p', 'o', 'u', 0xB9, 't' };
static const symbol s_1_10[3] = { 0xE1, 0xB9, 't' };
static const symbol s_1_11[3] = { 0xED, 0xB9, 't' };
static const symbol s_1_12[2] = { 0xE8, 't' };
static const struct among a_1[13] = {
{ 1, s_1_0, 0, 1, 0},
{ 2, s_1_1, -1, -1, 0},
{ 3, s_1_2, -1, 2, 0},
{ 3, s_1_3, -3, -1, 0},
{ 3, s_1_4, -4, -1, 0},
{ 2, s_1_5, 0, 4, 0},
{ 4, s_1_6, -1, -1, 0},
{ 4, s_1_7, -2, -1, 0},
{ 3, s_1_8, -3, -1, 0},
{ 5, s_1_9, -4, -1, 0},
{ 3, s_1_10, -5, -1, 0},
{ 3, s_1_11, -6, -1, 0},
{ 2, s_1_12, 0, 3, 0}
};

static const symbol s_2_0[2] = { 'i', 'n' };
static const symbol s_2_1[2] = { 'o', 'v' };
static const symbol s_2_2[2] = { 0xF9, 'v' };
static const struct among a_2[3] = {
{ 2, s_2_0, 0, 2, 0},
{ 2, s_2_1, 0, 1, 0},
{ 2, s_2_2, 0, 1, 0}
};

static const symbol s_3_1[1] = { 'l' };
static const symbol s_3_2[2] = { 't', 'l' };
static const symbol s_3_3[1] = { 's' };
static const symbol s_3_4[2] = { 'e', 's' };
static const symbol s_3_5[1] = { 0xBE };
static const symbol s_3_6[1] = { 0xE8 };
static const symbol s_3_7[2] = { 'e', 0xE8 };
static const symbol s_3_8[1] = { 0xF8 };
static const struct among a_3[9] = {
{ 0, 0, 0, 2, 0},
{ 1, s_3_1, -1, 1, 0},
{ 2, s_3_2, -1, 2, 0},
{ 1, s_3_3, -3, 1, 0},
{ 2, s_3_4, -1, 2, 0},
{ 1, s_3_5, -5, 1, 0},
{ 1, s_3_6, -6, 1, 0},
{ 2, s_3_7, -1, 2, 0},
{ 1, s_3_8, -8, 1, 0}
};

static const symbol s_4_0[3] = { 'o', 'b', 'l' };
static const symbol s_4_1[2] = { 's', 'n' };
static const symbol s_4_2[3] = { 'd', 'o', 't' };
static const struct among a_4[3] = {
{ 3, s_4_0, 0, -1, 0},
{ 2, s_4_1, 0, -1, 0},
{ 3, s_4_2, 0, -1, 0}
};

static const symbol s_5_0[2] = { 'u', 'c' };
static const symbol s_5_1[1] = { 'h' };
static const symbol s_5_2[2] = { 'o', 'k' };
static const symbol s_5_3[3] = { 'k', 'a', 'r' };
static const symbol s_5_4[1] = { 0xE8 };
static const struct among a_5[5] = {
{ 2, s_5_0, 0, -1, 0},
{ 1, s_5_1, 0, -1, 0},
{ 2, s_5_2, 0, -1, 0},
{ 3, s_5_3, 0, -1, 0},
{ 1, s_5_4, 0, -1, 0}
};

static const symbol s_6_0[1] = { 'a' };
static const symbol s_6_1[3] = { 'a', 'm', 'a' };
static const symbol s_6_2[3] = { 'a', 't', 'a' };
static const symbol s_6_3[2] = { 'e', 'b' };
static const symbol s_6_4[2] = { 'e', 'c' };
static const symbol s_6_5[1] = { 'e' };
static const symbol s_6_6[3] = { 'e', 't', 'e' };
static const symbol s_6_7[3] = { 0xEC, 't', 'e' };
static const symbol s_6_8[3] = { 'e', 'c', 'h' };
static const symbol s_6_9[5] = { 'a', 't', 'e', 'c', 'h' };
static const symbol s_6_10[3] = { 0xE1, 'c', 'h' };
static const symbol s_6_11[3] = { 0xED, 'c', 'h' };
static const symbol s_6_12[3] = { 0xFD, 'c', 'h' };
static const symbol s_6_13[1] = { 'i' };
static const symbol s_6_14[2] = { 'm', 'i' };
static const symbol s_6_15[3] = { 'a', 'm', 'i' };
static const symbol s_6_16[3] = { 'e', 'm', 'i' };
static const symbol s_6_17[3] = { 0xBB, 'm', 'i' };
static const symbol s_6_18[3] = { 0xEC, 'm', 'i' };
static const symbol s_6_19[3] = { 0xED, 'm', 'i' };
static const symbol s_6_20[3] = { 0xFD, 'm', 'i' };
static const symbol s_6_21[3] = { 'e', 't', 'i' };
static const symbol s_6_22[3] = { 0xEC, 't', 'i' };
static const symbol s_6_23[3] = { 'o', 'v', 'i' };
static const symbol s_6_24[2] = { 'e', 'k' };
static const symbol s_6_25[2] = { 0xEC, 'k' };
static const symbol s_6_26[2] = { 'e', 'm' };
static const symbol s_6_27[4] = { 'e', 't', 'e', 'm' };
static const symbol s_6_28[4] = { 0xEC, 't', 'e', 'm' };
static const symbol s_6_29[2] = { 0xE1, 'm' };
static const symbol s_6_30[2] = { 0xE9, 'm' };
static const symbol s_6_31[2] = { 0xEC, 'm' };
static const symbol s_6_32[2] = { 0xED, 'm' };
static const symbol s_6_33[2] = { 0xF9, 'm' };
static const symbol s_6_34[4] = { 'a', 't', 0xF9, 'm' };
static const symbol s_6_35[2] = { 0xFD, 'm' };
static const symbol s_6_36[1] = { 'o' };
static const symbol s_6_37[3] = { 0xE9, 'h', 'o' };
static const symbol s_6_38[3] = { 0xED, 'h', 'o' };
static const symbol s_6_39[2] = { 'u', 's' };
static const symbol s_6_40[2] = { 'a', 't' };
static const symbol s_6_41[2] = { 'e', 't' };
static const symbol s_6_42[1] = { 'u' };
static const symbol s_6_43[3] = { 0xE9, 'm', 'u' };
static const symbol s_6_44[3] = { 0xED, 'm', 'u' };
static const symbol s_6_45[2] = { 'o', 'u' };
static const symbol s_6_46[2] = { 'e', 'v' };
static const symbol s_6_47[1] = { 'y' };
static const symbol s_6_48[3] = { 'a', 't', 'y' };
static const symbol s_6_49[1] = { 0xBB };
static const symbol s_6_50[1] = { 0xE1 };
static const symbol s_6_51[1] = { 0xE9 };
static const symbol s_6_52[3] = { 'o', 'v', 0xE9 };
static const symbol s_6_53[1] = { 0xEC };
static const symbol s_6_54[1] = { 0xED };
static const symbol s_6_55[2] = { 'e', 0xF2 };
static const symbol s_6_56[1] = { 0xF9 };
static const symbol s_6_57[1] = { 0xFD };
static const struct among a_6[58] = {
{ 1, s_6_0, 0, 1, 0},
{ 3, s_6_1, -1, 1, 0},
{ 3, s_6_2, -2, 1, 0},
{ 2, s_6_3, 0, 4, 0},
{ 2, s_6_4, 0, 5, 0},
{ 1, s_6_5, 0, 2, 0},
{ 3, s_6_6, -1, 3, 0},
{ 3, s_6_7, -2, 1, 0},
{ 3, s_6_8, 0, 2, 0},
{ 5, s_6_9, -1, 1, 0},
{ 3, s_6_10, 0, 1, 0},
{ 3, s_6_11, 0, 12, 0},
{ 3, s_6_12, 0, 1, 0},
{ 1, s_6_13, 0, 12, 0},
{ 2, s_6_14, -1, 1, 0},
{ 3, s_6_15, -1, 1, 0},
{ 3, s_6_16, -2, 2, 0},
{ 3, s_6_17, -3, 11, 0},
{ 3, s_6_18, -4, 1, 0},
{ 3, s_6_19, -5, 12, 0},
{ 3, s_6_20, -6, 1, 0},
{ 3, s_6_21, -8, 3, 0},
{ 3, s_6_22, -9, 1, 0},
{ 3, s_6_23, -10, 1, 0},
{ 2, s_6_24, 0, 6, 0},
{ 2, s_6_25, 0, 7, 0},
{ 2, s_6_26, 0, 2, 0},
{ 4, s_6_27, -1, 3, 0},
{ 4, s_6_28, -2, 1, 0},
{ 2, s_6_29, 0, 1, 0},
{ 2, s_6_30, 0, 1, 0},
{ 2, s_6_31, 0, 1, 0},
{ 2, s_6_32, 0, 12, 0},
{ 2, s_6_33, 0, 1, 0},
{ 4, s_6_34, -1, 1, 0},
{ 2, s_6_35, 0, 1, 0},
{ 1, s_6_36, 0, 1, 0},
{ 3, s_6_37, -1, 1, 0},
{ 3, s_6_38, -2, 12, 0},
{ 2, s_6_39, 0, 1, 0},
{ 2, s_6_40, 0, 1, 0},
{ 2, s_6_41, 0, 9, 0},
{ 1, s_6_42, 0, 1, 0},
{ 3, s_6_43, -1, 1, 0},
{ 3, s_6_44, -2, 12, 0},
{ 2, s_6_45, -3, 1, 0},
{ 2, s_6_46, 0, 10, 0},
{ 1, s_6_47, 0, 1, 0},
{ 3, s_6_48, -1, 1, 0},
{ 1, s_6_49, 0, 11, 0},
{ 1, s_6_50, 0, 1, 0},
{ 1, s_6_51, 0, 1, 0},
{ 3, s_6_52, -1, 1, 0},
{ 1, s_6_53, 0, 1, 0},
{ 1, s_6_54, 0, 12, 0},
{ 2, s_6_55, 0, 8, 0},
{ 1, s_6_56, 0, 1, 0},
{ 1, s_6_57, 0, 1, 0}
};

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 25, 4, 19 };

static const unsigned char g_v_or_syllabic_c[] = { 17, 73, 18, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 25, 4, 19 };

static const unsigned char g_ev_ending[] = { 73, 20, 4 };

static const unsigned char g_env_ending[] = { 71, 66, 23, 1, 0, 0, 0, 0, 0, 0, 128, 16, 0, 0, 0, 0, 64 };

static int r_mark_regions(struct SN_env * z) {
    int i_x;
    {
        int v_1 = z->c;
        if (z->c + 3 > z->l) return 0;
        z->c += 3;
        i_x = z->c;
        z->c = v_1;
    }
    ((SN_local *)z)->i_p1 = z->l;
    {
        int v_2 = z->c;
        do {
            if (in_grouping(z, g_v, 97, 253, 0)) goto lab1;
            break;
        lab1:
            if (z->c >= z->l) goto lab0;
            z->c++;
            {
                int ret = out_grouping(z, g_v_or_syllabic_c, 97, 253, 1);
                if (ret < 0) goto lab0;
                z->c += ret;
            }
        } while (0);
        {
            int ret = in_grouping(z, g_v, 97, 253, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        ((SN_local *)z)->i_p1 = z->c;
        if (((SN_local *)z)->i_p1 >= i_x) goto lab2;
        ((SN_local *)z)->i_p1 = i_x;
    lab2:
    lab0:
        z->c = v_2;
    }
    return 1;
}

static int r_palatalise_e(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c <= z->lb || z->p[z->c - 1] != 99) return 0;
    among_var = find_among_b(z, a_0, 5, 0);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 1, s_0);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 3, s_1);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_palatalise_i(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c <= z->lb || (z->p[z->c - 1] != 99 && z->p[z->c - 1] != 116)) return 0;
    among_var = find_among_b(z, a_1, 13, 0);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 1, s_2);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 3, s_3);
                if (ret < 0) return ret;
            }
            break;
        case 3:
            {
                int ret = slice_from_s(z, 2, s_4);
                if (ret < 0) return ret;
            }
            break;
        case 4:
            {
                int ret = slice_from_s(z, 2, s_5);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_possessive_suffix(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c - 1 <= z->lb || (z->p[z->c - 1] != 110 && z->p[z->c - 1] != 118)) return 0;
    among_var = find_among_b(z, a_2, 3, 0);
    if (!among_var) return 0;
    z->bra = z->c;
    if (((SN_local *)z)->i_p1 > z->c) return 0;
    switch (among_var) {
        case 1:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_1 = z->l - z->c;
                {
                    int ret = r_palatalise_i(z);
                    if (ret == 0) { z->c = z->l - v_1; goto lab0; }
                    if (ret < 0) return ret;
                }
            lab0:
                ;
            }
            break;
    }
    return 1;
}

static int r_case_suffix(struct SN_env * z) {
    int among_var;
    {
        int v_1;
        if (z->c < ((SN_local *)z)->i_p1) return 0;
        v_1 = z->lb; z->lb = ((SN_local *)z)->i_p1;
        z->ket = z->c;
        among_var = find_among_b(z, a_6, 58, 0);
        if (!among_var) { z->lb = v_1; return 0; }
        z->bra = z->c;
        z->lb = v_1;
    }
    switch (among_var) {
        case 1:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_2 = z->l - z->c;
                {
                    int ret = r_palatalise_e(z);
                    if (ret == 0) { z->c = z->l - v_2; goto lab0; }
                    if (ret < 0) return ret;
                }
            lab0:
                ;
            }
            break;
        case 3:
            among_var = find_among_b(z, a_3, 9, 0);
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 2, s_6);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        case 4:
            {
                int v_3 = z->l - z->c;
                if (out_grouping_b(z, g_v, 97, 253, 0)) return 0;
                z->c = z->l - v_3;
            }
            if (!(eq_s_b(z, 2, s_7))) goto lab1;
            return 0;
        lab1:
            {
                int ret = slice_from_s(z, 1, s_8);
                if (ret < 0) return ret;
            }
            break;
        case 5:
            {
                int v_4 = z->l - z->c;
                if (out_grouping_b(z, g_v, 97, 253, 0)) return 0;
                z->c = z->l - v_4;
            }
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int ret = insert_s(z, z->c, z->c, 1, s_9);
                if (ret < 0) return ret;
            }
            {
                int v_5 = z->l - z->c;
                {
                    int ret = r_palatalise_e(z);
                    if (ret == 0) { z->c = z->l - v_5; goto lab2; }
                    if (ret < 0) return ret;
                }
            lab2:
                ;
            }
            break;
        case 6:
            {
                int v_6 = z->l - z->c;
                if (out_grouping_b(z, g_v, 97, 253, 0)) return 0;
                z->c = z->l - v_6;
            }
            {
                int v_7 = z->l - z->c;
                if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((1069056 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab3;
                if (!find_among_b(z, a_4, 3, 0)) goto lab3;
                return 0;
            lab3:
                z->c = z->l - v_7;
            }
            {
                int ret = slice_from_s(z, 1, s_10);
                if (ret < 0) return ret;
            }
            break;
        case 7:
            if (z->c <= z->lb || z->p[z->c - 1] != 'n') return 0;
            z->c--;
            z->bra = z->c;
            {
                int ret = slice_from_s(z, 2, s_11);
                if (ret < 0) return ret;
            }
            break;
        case 8:
            {
                int v_8 = z->l - z->c;
                if (in_grouping_b(z, g_env_ending, 98, 232, 0)) return 0;
                z->c = z->l - v_8;
            }
            {
                int ret = slice_from_s(z, 1, s_12);
                if (ret < 0) return ret;
            }
            break;
        case 9:
            if (!find_among_b(z, a_5, 5, 0)) return 0;
            {
                int ret = slice_from_s(z, 1, s_13);
                if (ret < 0) return ret;
            }
            break;
        case 10:
            if (in_grouping_b(z, g_ev_ending, 104, 122, 0)) return 0;
            {
                int ret = slice_from_s(z, 1, s_14);
                if (ret < 0) return ret;
            }
            break;
        case 11:
            {
                int ret = slice_from_s(z, 1, s_15);
                if (ret < 0) return ret;
            }
            break;
        case 12:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_9 = z->l - z->c;
                {
                    int ret = r_palatalise_i(z);
                    if (ret == 0) { z->c = z->l - v_9; goto lab4; }
                    if (ret < 0) return ret;
                }
            lab4:
                ;
            }
            break;
    }
    return 1;
}

extern int czech_ISO_8859_2_stem(struct SN_env * z) {
    {
        int ret = r_mark_regions(z);
        if (ret <= 0) return ret;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_1 = z->l - z->c;
        {
            int ret = r_case_suffix(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_1;
    }
    {
        int v_2 = z->l - z->c;
        {
            int ret = r_possessive_suffix(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_2;
    }
    z->c = z->lb;
    return 1;
}

extern struct SN_env * czech_ISO_8859_2_create_env(void) {
    struct SN_env * z = SN_new_env(sizeof(SN_local));
    if (z) {
        ((SN_local *)z)->i_p1 = 0;
    }
    return z;
}

extern void czech_ISO_8859_2_close_env(struct SN_env * z) {
    SN_delete_env(z);
}

