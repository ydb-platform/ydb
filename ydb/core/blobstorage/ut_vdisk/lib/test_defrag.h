#pragma once

#include "defs.h"
#include "prepare.h"


#define SIMPLE_CLASS_DEF_NO_PARAMS(name)    \
struct name {                               \
    void operator ()(TConfiguration *conf); \
};

SIMPLE_CLASS_DEF_NO_PARAMS(TDefragEmptyDB)
SIMPLE_CLASS_DEF_NO_PARAMS(TDefrag50PercentGarbage)

#undef SIMPLE_CLASS_DEF_NO_PARAMS
