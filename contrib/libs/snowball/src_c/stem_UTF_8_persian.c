/* Generated from persian.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_persian.h"

#include <stddef.h>

#include "../runtime/snowball_runtime.h"

struct SN_local {
    struct SN_env z;
    int i_p1;
    unsigned char b_remove_verb_person_endings;
    unsigned char b_saw_present_prefix;
};

typedef struct SN_local SN_local;

#ifdef __cplusplus
extern "C" {
#endif
extern int persian_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_Stem_Verb(struct SN_env * z);
static int r_Stem_Noun_or_Adjective(struct SN_env * z);
static int r_Irregular_Noun(struct SN_env * z);
static int r_AN_Exception(struct SN_env * z);
static int r_R1(struct SN_env * z);
static int r_Protect_Lexical_AN(struct SN_env * z);
static int r_Delete_ZWNJ(struct SN_env * z);
static int r_Prefixes(struct SN_env * z);
static int r_Normalize_Characters(struct SN_env * z);

static const symbol s_0[] = { 0xDA, 0xA9 };
static const symbol s_1[] = { 0xDB, 0x8C };
static const symbol s_2[] = { 0xD9, 0x87 };
static const symbol s_3[] = { 0xD8, 0xA7 };
static const symbol s_4[] = { 0xD9, 0x88 };
static const symbol s_5[] = { 0xE2, 0x80, 0x8C };
static const symbol s_6[] = { 0xD8, 0xAE, 0xD8, 0xA8, 0xD8, 0xB1 };
static const symbol s_7[] = { 0xD8, 0xA7, 0xD8, 0xB3, 0xD8, 0xAA, 0xD8, 0xA7, 0xD8, 0xAF };
static const symbol s_8[] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA };
static const symbol s_9[] = { 0xD8, 0xAF };
static const symbol s_10[] = { 0xD8, 0xAA };

static const symbol s_0_1[1] = { ' ' };
static const symbol s_0_2[2] = { 0xD8, 0xA3 };
static const symbol s_0_3[2] = { 0xD8, 0xA4 };
static const symbol s_0_4[2] = { 0xD8, 0xA5 };
static const symbol s_0_5[2] = { 0xD8, 0xA6 };
static const symbol s_0_6[2] = { 0xD8, 0xA9 };
static const symbol s_0_7[2] = { 0xD9, 0x83 };
static const symbol s_0_8[2] = { 0xD9, 0x8A };
static const symbol s_0_9[2] = { 0xDB, 0x81 };
static const symbol s_0_10[3] = { 0xE2, 0x80, 0x8D };
static const struct among a_0[11] = {
{ 0, 0, 0, 7, 0},
{ 1, s_0_1, -1, 6, 0},
{ 2, s_0_2, -2, 4, 0},
{ 2, s_0_3, -3, 5, 0},
{ 2, s_0_4, -4, 4, 0},
{ 2, s_0_5, -5, 2, 0},
{ 2, s_0_6, -6, 3, 0},
{ 2, s_0_7, -7, 1, 0},
{ 2, s_0_8, -8, 2, 0},
{ 2, s_0_9, -9, 3, 0},
{ 3, s_0_10, -10, 6, 0}
};

static const symbol s_1_0[7] = { 0xD9, 0x85, 0xDB, 0x8C, 0xE2, 0x80, 0x8C };
static const symbol s_1_1[9] = { 0xD9, 0x86, 0xD9, 0x85, 0xDB, 0x8C, 0xE2, 0x80, 0x8C };
static const struct among a_1[2] = {
{ 7, s_1_0, 0, 2, 0},
{ 9, s_1_1, 0, 1, 0}
};

static const symbol s_2_0[6] = { 0xD9, 0x88, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_2_1[8] = { 0xD8, 0xB3, 0xD8, 0xAA, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_2_2[6] = { 0xD8, 0xB1, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_2_3[6] = { 0xD8, 0xB3, 0xD8, 0xA7, 0xD9, 0x86 };
static const struct among a_2[4] = {
{ 6, s_2_0, 0, -1, 0},
{ 8, s_2_1, 0, -1, 0},
{ 6, s_2_2, 0, -1, 0},
{ 6, s_2_3, 0, -1, 0}
};

static const symbol s_3_0[10] = { 0xDA, 0xAF, 0xDB, 0x8C, 0xD9, 0x84, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_1[10] = { 0xD8, 0xA2, 0xD9, 0x84, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_2[12] = { 0xD9, 0x85, 0xD8, 0xB3, 0xD9, 0x84, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_3[12] = { 0xD8, 0xB3, 0xD9, 0x84, 0xDB, 0x8C, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_4[10] = { 0xD8, 0xA7, 0xDB, 0x8C, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_5[10] = { 0xD9, 0xBE, 0xDB, 0x8C, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_6[14] = { 0xD8, 0xB3, 0xD8, 0xA7, 0xD8, 0xAE, 0xD8, 0xAA, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_7[8] = { 0xD8, 0xB1, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_8[12] = { 0xD9, 0x82, 0xD9, 0x87, 0xD8, 0xB1, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_9[10] = { 0xDA, 0xA9, 0xD8, 0xB1, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_10[10] = { 0xD8, 0xAF, 0xD8, 0xB1, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_11[12] = { 0xD9, 0x87, 0xD9, 0x85, 0xD8, 0xB2, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_12[12] = { 0xD8, 0xB3, 0xD8, 0xA7, 0xD8, 0xB2, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_13[10] = { 0xD8, 0xA2, 0xD8, 0xB3, 0xD9, 0x85, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_14[10] = { 0xDB, 0x8C, 0xD9, 0x88, 0xD9, 0x86, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_15[10] = { 0xD9, 0x84, 0xD8, 0xA8, 0xD9, 0x86, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_16[12] = { 0xD8, 0xA7, 0xD8, 0xB5, 0xD9, 0x81, 0xD9, 0x87, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_17[10] = { 0xD9, 0xBE, 0xD8, 0xA7, 0xDB, 0x8C, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_18[8] = { 0xD8, 0xA8, 0xDB, 0x8C, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_19[10] = { 0xD8, 0xAC, 0xD8, 0xB1, 0xDB, 0x8C, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_20[10] = { 0xD8, 0xA7, 0xD9, 0x85, 0xDA, 0xA9, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_21[18] = { 0xD8, 0xA2, 0xD8, 0xB0, 0xD8, 0xB1, 0xD8, 0xA8, 0xD8, 0xA7, 0xDB, 0x8C, 0xD8, 0xAC, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_22[10] = { 0xD9, 0x87, 0xD9, 0x85, 0xD8, 0xAF, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_23[12] = { 0xD8, 0xAE, 0xD8, 0xA7, 0xD9, 0x86, 0xD8, 0xAF, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_24[10] = { 0xD8, 0xB2, 0xD9, 0x86, 0xD8, 0xAF, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_25[10] = { 0xD9, 0x85, 0xDB, 0x8C, 0xD8, 0xB2, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_26[14] = { 0xD8, 0xA2, 0xD8, 0xAA, 0xD8, 0xB4, 0xD9, 0x81, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_27[8] = { 0xD9, 0x86, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_28[10] = { 0xD8, 0xA7, 0xDB, 0x8C, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_29[12] = { 0xD9, 0xBE, 0xD8, 0xB1, 0xDB, 0x8C, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_30[12] = { 0xDA, 0xA9, 0xD9, 0x87, 0xDA, 0xA9, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_31[12] = { 0xD8, 0xAF, 0xD8, 0xB1, 0xD8, 0xAE, 0xD8, 0xB4, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_3_32[10] = { 0xD8, 0xB3, 0xD9, 0x84, 0xD8, 0xB7, 0xD8, 0xA7, 0xD9, 0x86 };
static const struct among a_3[33] = {
{ 10, s_3_0, 0, 1, 0},
{ 10, s_3_1, 0, 1, 0},
{ 12, s_3_2, 0, 1, 0},
{ 12, s_3_3, 0, 1, 0},
{ 10, s_3_4, 0, 1, 0},
{ 10, s_3_5, 0, 1, 0},
{ 14, s_3_6, 0, 1, 0},
{ 8, s_3_7, 0, 1, 0},
{ 12, s_3_8, -1, 1, 0},
{ 10, s_3_9, -2, 1, 0},
{ 10, s_3_10, -3, 1, 0},
{ 12, s_3_11, 0, 1, 0},
{ 12, s_3_12, 0, 1, 0},
{ 10, s_3_13, 0, 1, 0},
{ 10, s_3_14, 0, 1, 0},
{ 10, s_3_15, 0, 1, 0},
{ 12, s_3_16, 0, 1, 0},
{ 10, s_3_17, 0, 1, 0},
{ 8, s_3_18, 0, 1, 0},
{ 10, s_3_19, 0, 1, 0},
{ 10, s_3_20, 0, 1, 0},
{ 18, s_3_21, 0, 1, 0},
{ 10, s_3_22, 0, 1, 0},
{ 12, s_3_23, 0, 1, 0},
{ 10, s_3_24, 0, 1, 0},
{ 10, s_3_25, 0, 1, 0},
{ 14, s_3_26, 0, 1, 0},
{ 8, s_3_27, 0, 1, 0},
{ 10, s_3_28, 0, 1, 0},
{ 12, s_3_29, 0, 1, 0},
{ 12, s_3_30, 0, 1, 0},
{ 12, s_3_31, 0, 1, 0},
{ 10, s_3_32, 0, 1, 0}
};

static const symbol s_4_0[12] = { 0xD8, 0xA7, 0xD8, 0xB3, 0xD8, 0xA7, 0xD8, 0xAA, 0xDB, 0x8C, 0xD8, 0xAF };
static const symbol s_4_1[10] = { 0xD8, 0xA7, 0xD8, 0xAE, 0xD8, 0xA8, 0xD8, 0xA7, 0xD8, 0xB1 };
static const struct among a_4[2] = {
{ 12, s_4_0, 0, 2, 0},
{ 10, s_4_1, 0, 1, 0}
};

static const symbol s_5_0[4] = { 0xD8, 0xA7, 0xD9, 0x85 };
static const symbol s_5_1[4] = { 0xDB, 0x8C, 0xD9, 0x86 };
static const symbol s_5_2[8] = { 0xD8, 0xAA, 0xD8, 0xB1, 0xDB, 0x8C, 0xD9, 0x86 };
static const symbol s_5_3[4] = { 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_5_4[6] = { 0xDB, 0x8C, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_5_5[6] = { 0xD8, 0xA8, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_5_6[6] = { 0xDA, 0xAF, 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_5_7[6] = { 0xD8, 0xA7, 0xD9, 0x86, 0xD9, 0x87 };
static const symbol s_5_8[6] = { 0xDA, 0xAF, 0xD8, 0xA7, 0xD9, 0x87 };
static const symbol s_5_9[6] = { 0xD8, 0xA7, 0xD9, 0x86, 0xDB, 0x8C };
static const symbol s_5_10[4] = { 0xDB, 0x8C, 0xDB, 0x8C };
static const symbol s_5_11[6] = { 0xD9, 0x87, 0xD8, 0xA7, 0xDB, 0x8C };
static const symbol s_5_12[4] = { 0xDA, 0xAF, 0xDB, 0x8C };
static const symbol s_5_13[4] = { 0xD9, 0x87, 0xD8, 0xA7 };
static const symbol s_5_14[6] = { 0xD9, 0x86, 0xD8, 0xA7, 0xDA, 0xA9 };
static const symbol s_5_15[4] = { 0xDB, 0x8C, 0xD8, 0xAA };
static const symbol s_5_16[4] = { 0xD8, 0xA7, 0xD8, 0xAA };
static const symbol s_5_17[6] = { 0xD9, 0x85, 0xD9, 0x86, 0xD8, 0xAF };
static const symbol s_5_18[6] = { 0xD9, 0x88, 0xD8, 0xA7, 0xD8, 0xB1 };
static const symbol s_5_19[6] = { 0xDA, 0xAF, 0xD8, 0xA7, 0xD8, 0xB1 };
static const symbol s_5_20[4] = { 0xD8, 0xAA, 0xD8, 0xB1 };
static const symbol s_5_21[4] = { 0xD8, 0xA7, 0xD8, 0xB4 };
static const struct among a_5[22] = {
{ 4, s_5_0, 0, 1, 0},
{ 4, s_5_1, 0, 1, 0},
{ 8, s_5_2, -1, 1, 0},
{ 4, s_5_3, 0, 1, 0},
{ 6, s_5_4, -1, 1, 0},
{ 6, s_5_5, -2, 1, 0},
{ 6, s_5_6, -3, 1, 0},
{ 6, s_5_7, 0, 1, 0},
{ 6, s_5_8, 0, 1, 0},
{ 6, s_5_9, 0, 1, 0},
{ 4, s_5_10, 0, 1, 0},
{ 6, s_5_11, 0, 1, 0},
{ 4, s_5_12, 0, 1, 0},
{ 4, s_5_13, 0, 1, 0},
{ 6, s_5_14, 0, 1, 0},
{ 4, s_5_15, 0, 1, 0},
{ 4, s_5_16, 0, 1, 0},
{ 6, s_5_17, 0, 1, 0},
{ 6, s_5_18, 0, 1, 0},
{ 6, s_5_19, 0, 1, 0},
{ 4, s_5_20, 0, 2, 0},
{ 4, s_5_21, 0, 1, 0}
};

static const symbol s_6_0[4] = { 0xDB, 0x8C, 0xD9, 0x85 };
static const symbol s_6_1[6] = { 0xD8, 0xA7, 0xDB, 0x8C, 0xD9, 0x85 };
static const symbol s_6_2[4] = { 0xD8, 0xA7, 0xDB, 0x8C };
static const symbol s_6_3[6] = { 0xD8, 0xA7, 0xD8, 0xB3, 0xD8, 0xAA };
static const symbol s_6_4[6] = { 0xD8, 0xA7, 0xD9, 0x86, 0xD8, 0xAF };
static const symbol s_6_5[4] = { 0xDB, 0x8C, 0xD8, 0xAF };
static const symbol s_6_6[6] = { 0xD8, 0xA7, 0xDB, 0x8C, 0xD8, 0xAF };
static const symbol s_6_7[4] = { 0xD8, 0xA7, 0xD8, 0xB3 };
static const struct among a_6[8] = {
{ 4, s_6_0, 0, 1, 0},
{ 6, s_6_1, -1, 1, 0},
{ 4, s_6_2, 0, 1, 0},
{ 6, s_6_3, 0, 1, 0},
{ 6, s_6_4, 0, 1, 0},
{ 4, s_6_5, 0, 1, 0},
{ 6, s_6_6, -1, 1, 0},
{ 4, s_6_7, 0, 1, 0}
};

static const symbol s_7_0[2] = { 0xD9, 0x85 };
static const symbol s_7_1[4] = { 0xDB, 0x8C, 0xD9, 0x85 };
static const symbol s_7_2[10] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA, 0xDB, 0x8C, 0xD9, 0x85 };
static const symbol s_7_3[4] = { 0xD8, 0xA7, 0xD9, 0x85 };
static const symbol s_7_4[8] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA, 0xD9, 0x85 };
static const symbol s_7_5[4] = { 0xD8, 0xA7, 0xD9, 0x86 };
static const symbol s_7_6[4] = { 0xD8, 0xAA, 0xD9, 0x87 };
static const symbol s_7_7[4] = { 0xD8, 0xAF, 0xD9, 0x87 };
static const symbol s_7_8[6] = { 0xD9, 0x86, 0xD8, 0xAF, 0xD9, 0x87 };
static const symbol s_7_9[8] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA, 0xDB, 0x8C };
static const symbol s_7_10[2] = { 0xD8, 0xAF };
static const symbol s_7_11[6] = { 0xD8, 0xA7, 0xD9, 0x86, 0xD8, 0xAF };
static const symbol s_7_12[12] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA, 0xD8, 0xA7, 0xD9, 0x86, 0xD8, 0xAF };
static const symbol s_7_13[4] = { 0xDB, 0x8C, 0xD8, 0xAF };
static const symbol s_7_14[10] = { 0xD8, 0xB1, 0xD9, 0x81, 0xD8, 0xAA, 0xDB, 0x8C, 0xD8, 0xAF };
static const struct among a_7[15] = {
{ 2, s_7_0, 0, 1, 0},
{ 4, s_7_1, -1, 1, 0},
{ 10, s_7_2, -1, 2, 0},
{ 4, s_7_3, -3, 1, 0},
{ 8, s_7_4, -4, 2, 0},
{ 4, s_7_5, 0, 3, 0},
{ 4, s_7_6, 0, 5, 0},
{ 4, s_7_7, 0, 4, 0},
{ 6, s_7_8, -1, 3, 0},
{ 8, s_7_9, 0, 2, 0},
{ 2, s_7_10, 0, 1, 0},
{ 6, s_7_11, -1, 1, 0},
{ 12, s_7_12, -1, 2, 0},
{ 4, s_7_13, -3, 1, 0},
{ 10, s_7_14, -1, 2, 0}
};

static int r_Normalize_Characters(struct SN_env * z) {
    int among_var;
    while (1) {
        int v_1 = z->c;
        z->bra = z->c;
        among_var = find_among(z, a_0, 11, 0);
        z->ket = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 2, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 2, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 2, s_2);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 2, s_3);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = slice_from_s(z, 2, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 7:
                {
                    int ret = skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab0;
                    z->c = ret;
                }
                break;
        }
        continue;
    lab0:
        z->c = v_1;
        break;
    }
    return 1;
}

static int r_Prefixes(struct SN_env * z) {
    int among_var;
    z->bra = z->c;
    if (z->c + 6 >= z->l || (z->p[z->c + 6] != 140 && z->p[z->c + 6] != 226)) return 0;
    among_var = find_among(z, a_1, 2, 0);
    if (!among_var) return 0;
    z->ket = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = skip_utf8(z->p, z->c, z->l, 2);
                if (ret < 0) return 0;
                z->c = ret;
            }
            ((SN_local *)z)->b_saw_present_prefix = 1;
            break;
        case 2:
            {
                int ret = skip_utf8(z->p, z->c, z->l, 2);
                if (ret < 0) return 0;
                z->c = ret;
            }
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            ((SN_local *)z)->b_saw_present_prefix = 1;
            break;
    }
    return 1;
}

static int r_Delete_ZWNJ(struct SN_env * z) {
    while (1) {
        int v_1 = z->c;
        while (1) {
            int v_2 = z->c;
            z->bra = z->c;
            if (!(eq_s(z, 3, s_5))) goto lab1;
            z->ket = z->c;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            z->c = v_2;
            break;
        lab1:
            z->c = v_2;
            {
                int ret = skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) goto lab0;
                z->c = ret;
            }
        }
        continue;
    lab0:
        z->c = v_1;
        break;
    }
    return 1;
}

static int r_R1(struct SN_env * z) {
    return ((SN_local *)z)->i_p1 <= z->c;
}

static int r_Protect_Lexical_AN(struct SN_env * z) {
    {
        int v_1 = z->l - z->c;
        {
            int ret = r_AN_Exception(z);
            if (ret == 0) goto lab0;
            if (ret < 0) return ret;
        }
        return 0;
    lab0:
        z->c = z->l - v_1;
    }
    {
        int v_2 = z->l - z->c;
        if (z->c - 5 <= z->lb || z->p[z->c - 1] != 134) goto lab1;
        if (!find_among_b(z, a_2, 4, 0)) goto lab1;
        return 0;
    lab1:
        z->c = z->l - v_2;
    }
    return 1;
}

static int r_AN_Exception(struct SN_env * z) {
    if (z->c - 7 <= z->lb || z->p[z->c - 1] != 134) return 0;
    if (!find_among_b(z, a_3, 33, 0)) return 0;
    if (z->c > z->lb) return 0;
    return 1;
}

static int r_Irregular_Noun(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c - 9 <= z->lb || (z->p[z->c - 1] != 175 && z->p[z->c - 1] != 177)) return 0;
    among_var = find_among_b(z, a_4, 2, 0);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 6, s_6);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 10, s_7);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_Stem_Noun_or_Adjective(struct SN_env * z) {
    int among_var;
    do {
        int v_1 = z->l - z->c;
        {
            int ret = r_Irregular_Noun(z);
            if (ret == 0) goto lab0;
            if (ret < 0) return ret;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        {
            int v_2;
            if (z->c < ((SN_local *)z)->i_p1) return 0;
            v_2 = z->lb; z->lb = ((SN_local *)z)->i_p1;
            z->ket = z->c;
            among_var = find_among_b(z, a_5, 22, 0);
            if (!among_var) { z->lb = v_2; return 0; }
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (z->c <= z->lb) { z->lb = v_2; return 0; }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            z->lb = v_2;
        }
    } while (0);
    return 1;
}

static int r_Stem_Verb(struct SN_env * z) {
    int among_var;
    do {
        int v_1 = z->l - z->c;
        z->ket = z->c;
        if (!find_among_b(z, a_6, 8, 0)) goto lab0;
        z->bra = z->c;
        {
            int ret = r_R1(z);
            if (ret == 0) goto lab0;
            if (ret < 0) return ret;
        }
        {
            int ret = slice_del(z);
            if (ret < 0) return ret;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        z->ket = z->c;
        among_var = find_among_b(z, a_7, 15, 0);
        if (!among_var) return 0;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                if (!((SN_local *)z)->b_remove_verb_person_endings) return 0;
                {
                    int ret = r_R1(z);
                    if (ret <= 0) return ret;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 6, s_8);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = r_R1(z);
                    if (ret <= 0) return ret;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                ((SN_local *)z)->b_remove_verb_person_endings = 1;
                break;
            case 4:
                if (z->c <= z->lb) return 0;
                {
                    int ret = slice_from_s(z, 2, s_9);
                    if (ret < 0) return ret;
                }
                ((SN_local *)z)->b_remove_verb_person_endings = 1;
                break;
            case 5:
                if (z->c <= z->lb) return 0;
                {
                    int ret = slice_from_s(z, 2, s_10);
                    if (ret < 0) return ret;
                }
                ((SN_local *)z)->b_remove_verb_person_endings = 1;
                break;
        }
    } while (0);
    return 1;
}

extern int persian_UTF_8_stem(struct SN_env * z) {
    ((SN_local *)z)->b_saw_present_prefix = 0;
    {
        int v_1 = z->c;
        {
            int ret = r_Normalize_Characters(z);
            if (ret < 0) return ret;
        }
        z->c = v_1;
    }
    {
        int v_2 = z->c;
        {
            int ret = r_Prefixes(z);
            if (ret < 0) return ret;
        }
        z->c = v_2;
    }
    {
        int v_3 = z->c;
        {
            int ret = r_Delete_ZWNJ(z);
            if (ret < 0) return ret;
        }
        z->c = v_3;
    }
    ((SN_local *)z)->i_p1 = z->l;
    {
        int v_4 = z->c;
        {
            int ret = skip_utf8(z->p, z->c, z->l, 3);
            if (ret < 0) goto lab0;
            z->c = ret;
        }
        ((SN_local *)z)->i_p1 = z->c;
    lab0:
        z->c = v_4;
    }
    z->lb = z->c; z->c = z->l;
    while (1) {
        int v_5 = z->l - z->c;
        {
            int v_6 = z->l - z->c;
            ((SN_local *)z)->b_remove_verb_person_endings = 0;
            if (!((SN_local *)z)->b_saw_present_prefix) goto lab2;
            ((SN_local *)z)->b_remove_verb_person_endings = 1;
        lab2:
            {
                int ret = r_Protect_Lexical_AN(z);
                if (ret == 0) goto lab1;
                if (ret < 0) return ret;
            }
            do {
                int v_7 = z->l - z->c;
                {
                    int ret = r_Stem_Noun_or_Adjective(z);
                    if (ret == 0) goto lab3;
                    if (ret < 0) return ret;
                }
                break;
            lab3:
                z->c = z->l - v_7;
                {
                    int ret = r_Stem_Verb(z);
                    if (ret == 0) goto lab1;
                    if (ret < 0) return ret;
                }
            } while (0);
            z->c = z->l - v_6;
        }
        continue;
    lab1:
        z->c = z->l - v_5;
        break;
    }
    z->c = z->lb;
    return 1;
}

extern struct SN_env * persian_UTF_8_create_env(void) {
    struct SN_env * z = SN_new_env(sizeof(SN_local));
    if (z) {
        ((SN_local *)z)->i_p1 = 0;
        ((SN_local *)z)->b_remove_verb_person_endings = 0;
        ((SN_local *)z)->b_saw_present_prefix = 0;
    }
    return z;
}

extern void persian_UTF_8_close_env(struct SN_env * z) {
    SN_delete_env(z);
}

