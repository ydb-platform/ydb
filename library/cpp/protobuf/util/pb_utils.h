#pragma once

#define UPDATE_PB_FIELD_MAX(PBMESS, FIELD, VAL) \
    if ((VAL) > (PBMESS).Get##FIELD()) {        \
        (PBMESS).Set##FIELD(VAL);               \
    }

#define UPDATE_OPT_PB_FIELD_MAX(PBMESS, FIELD, VAL)                  \
    if (!(PBMESS).Has##FIELD() || ((VAL) > (PBMESS).Get##FIELD())) { \
        (PBMESS).Set##FIELD(VAL);                                    \
    }
