#ifndef __RE2MACROS_H
#define __RE2MACROS_H

#include <stdio.h>
#include "re2/stringpiece.h"

static inline re2::StringPiece * new_StringPiece_array(int n)
{
    re2::StringPiece * sp = new re2::StringPiece[n];
    return sp;
}

#endif
