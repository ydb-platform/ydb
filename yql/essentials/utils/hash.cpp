#include "hash.h"
#include <util/system/getpid.h>
#include <util/system/env.h>

namespace NYql {

#ifndef NDEBUG
size_t VaryingHash(size_t src) {
    struct TPid {
        size_t Value;

        TPid()
            : Value(GetEnv("YQL_MUTATE_HASHCODE") ? IntHash(GetPID()) : 0)
        {}
    };

    return Singleton<TPid>()->Value ^ src;
}
#endif

}
