#include "erasure.h"
#include <util/string/printf.h>

#include <bit>

namespace NKikimr {

#define RESTORE_BLOCK(RestoreMask) /* all other parts are assumed available */ \
    if (RestoreMask == 5) { \
        const ui64 z0 = a1[0] ^ a1[1] ^ a1[2] ^ a1[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a4[0] ^ a4[2] ^ a5[2]; \
        const ui64 z1 = a1[0] ^ a3[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[2] ^ a5[3]; \
        const ui64 z2 = a1[2] ^ a1[3] ^ a3[1] ^ a3[2] ^ a4[0] ^ a4[1] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z3 = a1[0] ^ a1[1] ^ a1[2] ^ a3[0] ^ a3[1] ^ a3[3] ^ a4[1] ^ a5[1] ^ a5[3]; \
        const ui64 z4 = a1[1] ^ a1[2] ^ a1[3] ^ a3[1] ^ a3[2] ^ a4[2] ^ a5[2]; \
        const ui64 z5 = a1[0] ^ a1[1] ^ a3[1] ^ a3[3] ^ a4[0] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[2] ^ a5[3]; \
        const ui64 z6 = a1[3] ^ a3[1] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z7 = a1[0] ^ a1[1] ^ a1[2] ^ a1[3] ^ a3[0] ^ a3[1] ^ a4[1] ^ a4[3] ^ a5[1] ^ a5[3]; \
        out0[0] = z0; \
        out0[1] = z1; \
        out0[2] = z2; \
        out0[3] = z3; \
        out2[0] = z4; \
        out2[1] = z5; \
        out2[2] = z6; \
        out2[3] = z7; \
    } \
    if (RestoreMask == 1 || RestoreMask == 33) { \
        const ui64 z0 = a1[0] ^ a2[0] ^ a3[0] ^ a4[0]; \
        const ui64 z1 = a1[1] ^ a2[1] ^ a3[1] ^ a4[1]; \
        const ui64 z2 = a1[2] ^ a2[2] ^ a3[2] ^ a4[2]; \
        const ui64 z3 = a1[3] ^ a2[3] ^ a3[3] ^ a4[3]; \
        out0[0] = z0; \
        out0[1] = z1; \
        out0[2] = z2; \
        out0[3] = z3; \
    } \
    if (RestoreMask == 9) { \
        const ui64 z0 = a1[1] ^ a1[2] ^ a2[0] ^ a2[2] ^ a2[3] ^ a4[2] ^ a5[0] ^ a5[2]; \
        const ui64 z1 = a1[1] ^ a1[3] ^ a2[1] ^ a2[2] ^ a4[0] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z2 = a1[1] ^ a2[0] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z3 = a1[0] ^ a1[1] ^ a2[1] ^ a2[2] ^ a2[3] ^ a4[1] ^ a4[3] ^ a5[1]; \
        const ui64 z4 = a1[0] ^ a1[1] ^ a1[2] ^ a2[2] ^ a2[3] ^ a4[0] ^ a4[2] ^ a5[0] ^ a5[2]; \
        const ui64 z5 = a1[3] ^ a2[2] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z6 = a1[1] ^ a1[2] ^ a2[0] ^ a2[2] ^ a4[0] ^ a4[1] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z7 = a1[0] ^ a1[1] ^ a1[3] ^ a2[1] ^ a2[2] ^ a4[1] ^ a5[1]; \
        out0[0] = z0; \
        out0[1] = z1; \
        out0[2] = z2; \
        out0[3] = z3; \
        out3[0] = z4; \
        out3[1] = z5; \
        out3[2] = z6; \
        out3[3] = z7; \
    } \
    if (RestoreMask == 3) { \
        const ui64 z0 = a2[3] ^ a3[2] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z1 = a2[0] ^ a2[3] ^ a3[0] ^ a3[2] ^ a3[3] ^ a4[0] ^ a5[0] ^ a5[1]; \
        const ui64 z2 = a2[1] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[2] ^ a4[3] ^ a5[3]; \
        const ui64 z3 = a2[2] ^ a2[3] ^ a3[1] ^ a3[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z4 = a2[0] ^ a2[3] ^ a3[0] ^ a3[2] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z5 = a2[0] ^ a2[1] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[0] ^ a4[1] ^ a5[0] ^ a5[1]; \
        const ui64 z6 = a2[1] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[3] ^ a4[3] ^ a5[3]; \
        const ui64 z7 = a2[2] ^ a3[1] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[2] ^ a5[3]; \
        out0[0] = z0; \
        out0[1] = z1; \
        out0[2] = z2; \
        out0[3] = z3; \
        out1[0] = z4; \
        out1[1] = z5; \
        out1[2] = z6; \
        out1[3] = z7; \
    } \
    if (RestoreMask == 17) { \
        const ui64 z0 = a3[2] ^ a2[3] ^ a3[1] ^ a2[2] ^ a1[3] ^ a5[0]; \
        const ui64 z1 = a1[0] ^ a3[3] ^ a3[1] ^ a2[2] ^ a1[3] ^ a5[1]; \
        const ui64 z2 = a2[0] ^ a1[1] ^ a3[1] ^ a2[2] ^ a1[3] ^ a5[2]; \
        const ui64 z3 = a3[0] ^ a2[1] ^ a1[2] ^ a3[1] ^ a2[2] ^ a1[3] ^ a5[3]; \
        const ui64 z4 = a1[0] ^ a1[3] ^ a2[0] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a5[0]; \
        const ui64 z5 = a1[0] ^ a1[1] ^ a1[3] ^ a2[1] ^ a2[2] ^ a3[3] ^ a5[1]; \
        const ui64 z6 = a1[1] ^ a1[2] ^ a1[3] ^ a2[0] ^ a3[1] ^ a3[2] ^ a5[2]; \
        const ui64 z7 = a1[2] ^ a2[1] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[3] ^ a5[3]; \
        out0[0] = z0; \
        out0[1] = z1; \
        out0[2] = z2; \
        out0[3] = z3; \
        out4[0] = z4; \
        out4[1] = z5; \
        out4[2] = z6; \
        out4[3] = z7; \
    } \
    if (RestoreMask == 18) { \
        const ui64 z0 = a0[0] ^ a0[1] ^ a2[3] ^ a3[2] ^ a3[3] ^ a5[0] ^ a5[1]; \
        const ui64 z1 = a0[0] ^ a0[2] ^ a2[0] ^ a2[3] ^ a3[2] ^ a5[0] ^ a5[2]; \
        const ui64 z2 = a0[0] ^ a0[3] ^ a2[1] ^ a2[3] ^ a3[0] ^ a3[2] ^ a5[0] ^ a5[3]; \
        const ui64 z3 = a0[0] ^ a3[2] ^ a2[3] ^ a3[1] ^ a2[2] ^ a5[0]; \
        const ui64 z4 = a0[1] ^ a2[0] ^ a2[3] ^ a3[0] ^ a3[2] ^ a3[3] ^ a5[0] ^ a5[1]; \
        const ui64 z5 = a0[0] ^ a0[1] ^ a0[2] ^ a2[0] ^ a2[1] ^ a2[3] ^ a3[1] ^ a3[2] ^ a5[0] ^ a5[2]; \
        const ui64 z6 = a0[0] ^ a0[2] ^ a0[3] ^ a2[1] ^ a2[2] ^ a2[3] ^ a3[0] ^ a5[0] ^ a5[3]; \
        const ui64 z7 = a0[0] ^ a0[3] ^ a2[2] ^ a3[1] ^ a3[2] ^ a3[3] ^ a5[0]; \
        out1[0] = z0; \
        out1[1] = z1; \
        out1[2] = z2; \
        out1[3] = z3; \
        out4[0] = z4; \
        out4[1] = z5; \
        out4[2] = z6; \
        out4[3] = z7; \
    } \
    if (RestoreMask == 2 || RestoreMask == 34) { \
        const ui64 z0 = a0[0] ^ a2[0] ^ a3[0] ^ a4[0]; \
        const ui64 z1 = a0[1] ^ a2[1] ^ a3[1] ^ a4[1]; \
        const ui64 z2 = a0[2] ^ a2[2] ^ a3[2] ^ a4[2]; \
        const ui64 z3 = a0[3] ^ a2[3] ^ a3[3] ^ a4[3]; \
        out1[0] = z0; \
        out1[1] = z1; \
        out1[2] = z2; \
        out1[3] = z3; \
    } \
    if (RestoreMask == 6) { \
        const ui64 z0 = a0[1] ^ a3[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[2] ^ a5[3]; \
        const ui64 z1 = a0[0] ^ a0[1] ^ a0[2] ^ a3[0] ^ a3[3] ^ a4[0] ^ a5[1] ^ a5[2]; \
        const ui64 z2 = a0[0] ^ a0[2] ^ a0[3] ^ a3[1] ^ a3[3] ^ a4[2] ^ a4[3] ^ a5[0]; \
        const ui64 z3 = a0[0] ^ a0[3] ^ a3[2] ^ a3[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z4 = a0[0] ^ a0[1] ^ a3[0] ^ a3[3] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[2] ^ a5[3]; \
        const ui64 z5 = a0[0] ^ a0[2] ^ a3[0] ^ a3[1] ^ a3[3] ^ a4[0] ^ a4[1] ^ a5[1] ^ a5[2]; \
        const ui64 z6 = a0[0] ^ a0[3] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[3] ^ a5[0]; \
        const ui64 z7 = a0[0] ^ a3[2] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[1] ^ a5[2] ^ a5[3]; \
        out1[0] = z0; \
        out1[1] = z1; \
        out1[2] = z2; \
        out1[3] = z3; \
        out2[0] = z4; \
        out2[1] = z5; \
        out2[2] = z6; \
        out2[3] = z7; \
    } \
    if (RestoreMask == 10) { \
        const ui64 z0 = a0[2] ^ a0[3] ^ a2[0] ^ a2[1] ^ a2[2] ^ a2[3] ^ a4[0] ^ a4[2] ^ a5[0] ^ a5[3]; \
        const ui64 z1 = a0[2] ^ a2[0] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z2 = a0[0] ^ a0[2] ^ a2[2] ^ a2[3] ^ a4[0] ^ a4[1] ^ a4[3] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z3 = a0[1] ^ a0[2] ^ a2[0] ^ a2[1] ^ a2[2] ^ a4[1] ^ a5[2]; \
        const ui64 z4 = a0[0] ^ a0[2] ^ a0[3] ^ a2[1] ^ a2[2] ^ a2[3] ^ a4[2] ^ a5[0] ^ a5[3]; \
        const ui64 z5 = a0[1] ^ a0[2] ^ a2[0] ^ a2[1] ^ a4[0] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z6 = a0[0] ^ a2[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[1] ^ a5[2] ^ a5[3]; \
        const ui64 z7 = a0[1] ^ a0[2] ^ a0[3] ^ a2[0] ^ a2[1] ^ a2[2] ^ a2[3] ^ a4[1] ^ a4[3] ^ a5[2]; \
        out1[0] = z0; \
        out1[1] = z1; \
        out1[2] = z2; \
        out1[3] = z3; \
        out3[0] = z4; \
        out3[1] = z5; \
        out3[2] = z6; \
        out3[3] = z7; \
    } \
    if (RestoreMask == 4 || RestoreMask == 36) { \
        const ui64 z0 = a0[0] ^ a1[0] ^ a3[0] ^ a4[0]; \
        const ui64 z1 = a0[1] ^ a1[1] ^ a3[1] ^ a4[1]; \
        const ui64 z2 = a0[2] ^ a1[2] ^ a3[2] ^ a4[2]; \
        const ui64 z3 = a0[3] ^ a1[3] ^ a3[3] ^ a4[3]; \
        out2[0] = z0; \
        out2[1] = z1; \
        out2[2] = z2; \
        out2[3] = z3; \
    } \
    if (RestoreMask == 20) { \
        const ui64 z0 = a0[1] ^ a0[2] ^ a1[0] ^ a1[1] ^ a3[3] ^ a5[1] ^ a5[2]; \
        const ui64 z1 = a0[1] ^ a0[3] ^ a1[0] ^ a1[2] ^ a3[0] ^ a3[3] ^ a5[1] ^ a5[3]; \
        const ui64 z2 = a1[0] ^ a0[1] ^ a3[3] ^ a3[1] ^ a1[3] ^ a5[1]; \
        const ui64 z3 = a0[0] ^ a0[1] ^ a1[0] ^ a3[2] ^ a3[3] ^ a5[0] ^ a5[1]; \
        const ui64 z4 = a0[0] ^ a0[1] ^ a0[2] ^ a1[1] ^ a3[0] ^ a3[3] ^ a5[1] ^ a5[2]; \
        const ui64 z5 = a0[3] ^ a1[0] ^ a1[1] ^ a1[2] ^ a3[0] ^ a3[1] ^ a3[3] ^ a5[1] ^ a5[3]; \
        const ui64 z6 = a0[1] ^ a0[2] ^ a1[0] ^ a1[2] ^ a1[3] ^ a3[1] ^ a3[2] ^ a3[3] ^ a5[1]; \
        const ui64 z7 = a0[0] ^ a0[1] ^ a0[3] ^ a1[0] ^ a1[3] ^ a3[2] ^ a5[0] ^ a5[1]; \
        out2[0] = z0; \
        out2[1] = z1; \
        out2[2] = z2; \
        out2[3] = z3; \
        out4[0] = z4; \
        out4[1] = z5; \
        out4[2] = z6; \
        out4[3] = z7; \
    } \
    if (RestoreMask == 12) { \
        const ui64 z0 = a0[2] ^ a1[1] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z1 = a0[0] ^ a0[2] ^ a0[3] ^ a1[0] ^ a1[1] ^ a1[2] ^ a4[0] ^ a5[2] ^ a5[3]; \
        const ui64 z2 = a0[0] ^ a0[1] ^ a0[2] ^ a0[3] ^ a1[0] ^ a1[2] ^ a1[3] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1]; \
        const ui64 z3 = a0[1] ^ a0[3] ^ a1[0] ^ a1[3] ^ a4[0] ^ a4[1] ^ a4[2] ^ a5[0] ^ a5[2] ^ a5[3]; \
        const ui64 z4 = a0[0] ^ a0[2] ^ a1[0] ^ a1[1] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[1] ^ a5[3]; \
        const ui64 z5 = a0[0] ^ a0[1] ^ a0[2] ^ a0[3] ^ a1[0] ^ a1[2] ^ a4[0] ^ a4[1] ^ a5[2] ^ a5[3]; \
        const ui64 z6 = a0[0] ^ a0[1] ^ a0[3] ^ a1[0] ^ a1[3] ^ a4[3] ^ a5[0] ^ a5[1]; \
        const ui64 z7 = a0[1] ^ a1[0] ^ a4[0] ^ a4[1] ^ a4[2] ^ a4[3] ^ a5[0] ^ a5[2] ^ a5[3]; \
        out2[0] = z0; \
        out2[1] = z1; \
        out2[2] = z2; \
        out2[3] = z3; \
        out3[0] = z4; \
        out3[1] = z5; \
        out3[2] = z6; \
        out3[3] = z7; \
    } \
    if (RestoreMask == 8 || RestoreMask == 40) { \
        const ui64 z0 = a0[0] ^ a1[0] ^ a2[0] ^ a4[0]; \
        const ui64 z1 = a0[1] ^ a1[1] ^ a2[1] ^ a4[1]; \
        const ui64 z2 = a0[2] ^ a1[2] ^ a2[2] ^ a4[2]; \
        const ui64 z3 = a0[3] ^ a1[3] ^ a2[3] ^ a4[3]; \
        out3[0] = z0; \
        out3[1] = z1; \
        out3[2] = z2; \
        out3[3] = z3; \
    } \
    if (RestoreMask == 24) { \
        const ui64 z0 = a0[2] ^ a0[3] ^ a1[1] ^ a1[2] ^ a2[0] ^ a2[1] ^ a5[2] ^ a5[3]; \
        const ui64 z1 = a2[0] ^ a1[1] ^ a0[2] ^ a2[2] ^ a1[3] ^ a5[2]; \
        const ui64 z2 = a0[0] ^ a0[2] ^ a1[1] ^ a2[0] ^ a2[3] ^ a5[0] ^ a5[2]; \
        const ui64 z3 = a0[1] ^ a0[2] ^ a1[0] ^ a1[1] ^ a2[0] ^ a5[1] ^ a5[2]; \
        const ui64 z4 = a0[0] ^ a0[2] ^ a0[3] ^ a1[0] ^ a1[1] ^ a1[2] ^ a2[1] ^ a5[2] ^ a5[3]; \
        const ui64 z5 = a0[1] ^ a0[2] ^ a1[3] ^ a2[0] ^ a2[1] ^ a2[2] ^ a5[2]; \
        const ui64 z6 = a0[0] ^ a1[1] ^ a1[2] ^ a2[0] ^ a2[2] ^ a2[3] ^ a5[0] ^ a5[2]; \
        const ui64 z7 = a0[1] ^ a0[2] ^ a0[3] ^ a1[0] ^ a1[1] ^ a1[3] ^ a2[0] ^ a2[3] ^ a5[1] ^ a5[2]; \
        out3[0] = z0; \
        out3[1] = z1; \
        out3[2] = z2; \
        out3[3] = z3; \
        out4[0] = z4; \
        out4[1] = z5; \
        out4[2] = z6; \
        out4[3] = z7; \
    } \
    if (RestoreMask == 16 || RestoreMask == 48) { \
        const ui64 z0 = a0[0] ^ a1[0] ^ a2[0] ^ a3[0]; \
        const ui64 z1 = a0[1] ^ a1[1] ^ a2[1] ^ a3[1]; \
        const ui64 z2 = a0[2] ^ a1[2] ^ a2[2] ^ a3[2]; \
        const ui64 z3 = a0[3] ^ a1[3] ^ a2[3] ^ a3[3]; \
        out4[0] = z0; \
        out4[1] = z1; \
        out4[2] = z2; \
        out4[3] = z3; \
    } \
    if (RestoreMask == 40) { \
        const ui64 z0 = a0[0] ^ a0[1] ^ a0[2] ^ a1[1] ^ a1[2] ^ a1[3] ^ a2[1] ^ a2[3] ^ a4[1] ^ a4[2]; \
        const ui64 z1 = a0[3] ^ a1[0] ^ a1[1] ^ a2[1] ^ a2[2] ^ a2[3] ^ a4[1] ^ a4[3]; \
        const ui64 z2 = a0[1] ^ a0[2] ^ a1[3] ^ a2[0] ^ a2[1] ^ a2[2] ^ a4[1]; \
        const ui64 z3 = a0[0] ^ a0[1] ^ a0[3] ^ a1[0] ^ a1[1] ^ a1[2] ^ a1[3] ^ a2[0] ^ a2[2] ^ a4[0] ^ a4[1]; \
        out5[0] = z0; \
        out5[1] = z1; \
        out5[2] = z2; \
        out5[3] = z3; \
    } \
    if (RestoreMask == 36) { \
        const ui64 z0 = a0[0] ^ a0[2] ^ a0[3] ^ a1[2] ^ a3[1] ^ a3[3] ^ a4[2] ^ a4[3]; \
        const ui64 z1 = a0[1] ^ a0[2] ^ a1[0] ^ a1[2] ^ a1[3] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[2]; \
        const ui64 z2 = a0[0] ^ a1[0] ^ a1[1] ^ a1[2] ^ a1[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a4[0] ^ a4[2]; \
        const ui64 z3 = a0[1] ^ a0[2] ^ a0[3] ^ a1[1] ^ a1[3] ^ a3[0] ^ a3[2] ^ a4[1] ^ a4[2]; \
        out5[0] = z0; \
        out5[1] = z1; \
        out5[2] = z2; \
        out5[3] = z3; \
    } \
    if (RestoreMask == 34) { \
        const ui64 z0 = a0[0] ^ a0[3] ^ a2[2] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[3]; \
        const ui64 z1 = a0[0] ^ a0[1] ^ a0[3] ^ a2[0] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a4[0] ^ a4[3]; \
        const ui64 z2 = a0[1] ^ a0[2] ^ a0[3] ^ a2[0] ^ a2[1] ^ a2[2] ^ a2[3] ^ a3[3] ^ a4[1] ^ a4[3]; \
        const ui64 z3 = a0[2] ^ a2[1] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a3[3] ^ a4[2] ^ a4[3]; \
        out5[0] = z0; \
        out5[1] = z1; \
        out5[2] = z2; \
        out5[3] = z3; \
    } \
    if (RestoreMask == 32 || RestoreMask == 48) { \
        const ui64 z0 = a0[0] ^ a3[2] ^ a2[3] ^ a3[1] ^ a2[2] ^ a1[3]; \
        const ui64 z1 = a1[0] ^ a0[1] ^ a3[3] ^ a3[1] ^ a2[2] ^ a1[3]; \
        const ui64 z2 = a2[0] ^ a1[1] ^ a0[2] ^ a3[1] ^ a2[2] ^ a1[3]; \
        const ui64 z3 = a3[0] ^ a2[1] ^ a1[2] ^ a0[3] ^ a3[1] ^ a2[2] ^ a1[3]; \
        out5[0] = z0; \
        out5[1] = z1; \
        out5[2] = z2; \
        out5[3] = z3; \
    } \
    if (RestoreMask == 33) { \
        const ui64 z0 = a1[0] ^ a1[3] ^ a2[0] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[2] ^ a4[0]; \
        const ui64 z1 = a1[0] ^ a1[1] ^ a1[3] ^ a2[1] ^ a2[2] ^ a3[3] ^ a4[1]; \
        const ui64 z2 = a1[1] ^ a1[2] ^ a1[3] ^ a2[0] ^ a3[1] ^ a3[2] ^ a4[2]; \
        const ui64 z3 = a1[2] ^ a2[1] ^ a2[2] ^ a2[3] ^ a3[0] ^ a3[1] ^ a3[3] ^ a4[3]; \
        out5[0] = z0; \
        out5[1] = z1; \
        out5[2] = z2; \
        out5[3] = z3; \
    }

    template<ui32 RestoreMask>
    void ErasureRestoreBlock42Do(std::span<TRope> parts) {
        const ui32 blockSize = 32;

        auto iter0 = parts[0].Begin();
        auto iter1 = parts[1].Begin();
        auto iter2 = parts[2].Begin();
        auto iter3 = parts[3].Begin();
        auto iter4 = parts[4].Begin();
        auto iter5 = parts[5].Begin();

        alignas(32) ui64 temp[4 * 6]; // temporary storage to fetch data into
        alignas(32) ui64 restore[4]; // temporary holder for unused output block

        const ui64 *a0 = nullptr;
        const ui64 *a1 = nullptr;
        const ui64 *a2 = nullptr;
        const ui64 *a3 = nullptr;
        const ui64 *a4 = nullptr;
        const ui64 *a5 = nullptr;

        ui64 *out0 = nullptr;
        ui64 *out1 = nullptr;
        ui64 *out2 = nullptr;
        ui64 *out3 = nullptr;
        ui64 *out4 = nullptr;
        ui64 *out5 = nullptr;

        ui32 offset = 0;

        for (;;) {
            size_t common = Max<size_t>();
#define COMMON_SIZE(I) \
            const size_t size##I = iter##I.ContiguousSize(); \
            if constexpr (~RestoreMask >> I & 1) { \
                common = Min(common, size##I); \
            }
            COMMON_SIZE(0)
            COMMON_SIZE(1)
            COMMON_SIZE(2)
            COMMON_SIZE(3)
            COMMON_SIZE(4)
            COMMON_SIZE(5)
            if (!common) {
                break;
            }

            size_t numBlocks = Min<size_t>(10, Max<size_t>(1, common / blockSize));
            const size_t numBytes = numBlocks * blockSize;

#define FETCH_BLOCK(I) \
            if constexpr (~RestoreMask >> I & 1) { /* input */ \
                if (size##I < blockSize) { \
                    a##I = temp + I * 4; \
                    iter##I.ExtractPlainDataAndAdvance(temp + I * 4, blockSize); \
                    Y_DEBUG_ABORT_UNLESS(numBytes == blockSize && numBlocks == 1); \
                } else { \
                    a##I = reinterpret_cast<const ui64*>(iter##I.ContiguousData()); \
                    iter##I += numBytes; \
                } \
            } else if (parts[I]) { /* expected output */ \
                Y_ABORT_UNLESS(parts[I].IsContiguous()); \
                auto span = parts[I].GetContiguousSpanMut(); \
                out##I = reinterpret_cast<ui64*>(span.data() + offset); \
                Y_DEBUG_ABORT_UNLESS(offset < span.size()); \
                Y_DEBUG_ABORT_UNLESS(numBytes <= span.size() - offset); \
            } else { \
                out##I = restore; /* temporary storage */ \
            }

            FETCH_BLOCK(0)
            FETCH_BLOCK(1)
            FETCH_BLOCK(2)
            FETCH_BLOCK(3)
            FETCH_BLOCK(4)
            FETCH_BLOCK(5)

            offset += numBytes;

            while (numBlocks--) {
                // restore all but available parts; there can be one part that goes to "restore" local storage and
                // further discarded
                RESTORE_BLOCK(RestoreMask)

#define NEXT(I) \
                if constexpr (~RestoreMask >> I & 1) { \
                    a##I += 4; \
                } else if (parts[I]) { \
                    out##I += 4; \
                }
                NEXT(0)
                NEXT(1)
                NEXT(2)
                NEXT(3)
                NEXT(4)
                NEXT(5)
            }
        }
    }

    void ErasureRestoreBlock42(ui32 fullSize, TRope *whole, std::span<TRope> parts, ui32 restoreMask) {
        const ui32 blockSize = 32;
        const ui32 fullBlockSize = 4 * blockSize;
        const ui32 fullBlocks = fullSize / fullBlockSize;

        Y_ABORT_UNLESS(parts.size() == 6);

        ui32 availableMask = 0;
        ui32 size = 0; // actual size of a part

        for (ui32 i = 0; i < parts.size(); ++i) {
            if (parts[i]) {
                availableMask |= 1 << i;
                if (!size) {
                    size = parts[i].size();
                } else {
                    Y_ABORT_UNLESS(size == parts[i].size());
                }
            }
        }

        Y_ABORT_UNLESS(std::popcount(availableMask) >= 4);
        Y_ABORT_UNLESS(size % 32 == 0);

        if (whole) {
            restoreMask |= (1 << 4) - 1; // we have to restore all the data parts
        }

        restoreMask &= ~availableMask;

        Y_ABORT_UNLESS(std::popcount(restoreMask) <= 2);

        for (ui32 part = 0; part < 6; ++part) {
            if (restoreMask >> part & 1) {
                Y_ABORT_UNLESS(!parts[part]);
                parts[part] = TRcBuf::Uninitialized(size);
            }
        }

        switch (restoreMask) {
            case 0: break;

#define SINGLE(I) \
            case 1 << I: \
                switch (63 & ~availableMask &~ restoreMask) { /* we may have 4 parts available and need only one to restore */ \
                    case 0     : ErasureRestoreBlock42Do<1 << I         >(parts); break; \
                    case 1 << 0: ErasureRestoreBlock42Do<1 << I | 1 << 0>(parts); break; \
                    case 1 << 1: ErasureRestoreBlock42Do<1 << I | 1 << 1>(parts); break; \
                    case 1 << 2: ErasureRestoreBlock42Do<1 << I | 1 << 2>(parts); break; \
                    case 1 << 3: ErasureRestoreBlock42Do<1 << I | 1 << 3>(parts); break; \
                    case 1 << 4: ErasureRestoreBlock42Do<1 << I | 1 << 4>(parts); break; \
                    case 1 << 5: ErasureRestoreBlock42Do<1 << I | 1 << 5>(parts); break; \
                } \
                break;

            SINGLE(0)
            SINGLE(1)
            SINGLE(2)
            SINGLE(3)
            SINGLE(4)
            SINGLE(5)

#define DOUBLE(I, J) \
            case 1 << I | 1 << J: \
                ErasureRestoreBlock42Do<1 << I | 1 << J>(parts); \
                break;

            DOUBLE(0, 1)
            DOUBLE(0, 2)
            DOUBLE(0, 3)
            DOUBLE(0, 4)
            DOUBLE(0, 5)
            DOUBLE(1, 2)
            DOUBLE(1, 3)
            DOUBLE(1, 4)
            DOUBLE(1, 5)
            DOUBLE(2, 3)
            DOUBLE(2, 4)
            DOUBLE(2, 5)
            DOUBLE(3, 4)
            DOUBLE(3, 5)
            DOUBLE(4, 5)

            default:
                Y_ABORT();
        }

        if (whole) {
            *whole = {};

            ui32 remains = fullSize % fullBlockSize;

            for (ui32 part = 0; part < 4; ++part) {
                ui32 partLen = fullBlocks * blockSize;
                if (remains >= blockSize || part == 3) {
                    const ui32 len = Min(remains, blockSize);
                    partLen += len;
                    remains -= len;
                }

                auto& p = parts[part];
                Y_ABORT_UNLESS(partLen <= p.size());
                whole->Insert(whole->End(), TRope(p.Begin(), p.Position(partLen)));
            }
        }
    }

    void ErasureRestoreFallback(TErasureType::ECrcMode crcMode, TErasureType erasure, ui32 fullSize, TRope *whole,
            std::span<TRope> parts, ui32 restoreMask, ui32 offset, bool isFragment) {
        Y_ABORT_UNLESS(parts.size() == erasure.TotalPartCount());
        Y_ABORT_UNLESS(!isFragment || !whole);

        const ui32 partSize = erasure.PartSize(crcMode, fullSize);

        // ensure all the set parts have the same size
        ui32 commonSize = 0;
        for (const TRope& part : parts) {
            if (part) {
                Y_ABORT_UNLESS(!commonSize || part.size() == commonSize);
                commonSize = part.size();
            }
        }
        Y_ABORT_UNLESS(commonSize);
        Y_ABORT_UNLESS(isFragment ? commonSize <= partSize - offset : commonSize == partSize);

        ui32 inputMask = 0;

        TDataPartSet p;
        p.FullDataSize = fullSize;
        p.IsFragment = isFragment;
        Y_ABORT_UNLESS(isFragment || !offset);
        for (ui32 i = 0; i < parts.size(); ++i) {
            TPartFragment fragment;
            if (parts[i]) {
                inputMask |= 1 << i;
                parts[i].Compact(); // this is the price of using TErasureType::RestoreData
                if (isFragment) {
                    fragment.ReferenceTo(parts[i], offset, parts[i].size(), partSize);
                } else {
                    fragment.ReferenceTo(parts[i]);
                }
            } else {
                fragment.ReferenceTo(TRcBuf::Uninitialized(commonSize), offset, commonSize, partSize);
            }
            p.Parts.push_back(std::move(fragment));
        }
        p.PartsMask = inputMask;

        const bool restoreParts = restoreMask & ((1 << erasure.DataParts()) - 1);
        const bool restoreParityParts = restoreMask >> erasure.DataParts();
        if (whole) {
            whole->Compact();
            erasure.RestoreData(crcMode, p, *whole, restoreParts, true, restoreParityParts);
        } else {
            erasure.RestoreData(crcMode, p, restoreParts, false, restoreParityParts);
        }

        for (ui32 i = 0; i < parts.size(); ++i) {
            if (restoreMask >> i & 1) {
                parts[i] = std::move(p.Parts[i].OwnedString);
            }
        }
    }

    void ErasureRestore(TErasureType::ECrcMode crcMode, TErasureType erasure, ui32 fullSize, TRope *whole,
            std::span<TRope> parts, ui32 restoreMask, ui32 offset, bool isFragment) {
        if (crcMode == TErasureType::CrcModeNone && erasure.GetErasure() == TErasureType::Erasure4Plus2Block) {
            Y_ABORT_UNLESS(parts.size() == erasure.TotalPartCount());
            Y_ABORT_UNLESS(!isFragment || !whole);
            Y_ABORT_UNLESS(offset % 32 == 0);
            ErasureRestoreBlock42(fullSize, whole, parts, restoreMask);
        } else {
            ErasureRestoreFallback(crcMode, erasure, fullSize, whole, parts, restoreMask, offset, isFragment);
        }
    }

} // NKikimr
