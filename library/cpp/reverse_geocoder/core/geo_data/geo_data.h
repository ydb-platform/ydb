#pragma once

#include "def.h"

namespace NReverseGeocoder {
    class IGeoData {
#define GEO_BASE_DEF_VAR(TVar, Var) \
    virtual const TVar& Var() const = 0;

#define GEO_BASE_DEF_ARR(TArr, Arr)      \
    virtual const TArr* Arr() const = 0; \
    virtual TNumber Arr##Number() const = 0;

    public:
        GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

        virtual ~IGeoData() {
        }
    };

}
