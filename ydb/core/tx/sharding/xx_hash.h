#pragma once
#include "hash.h"

#ifndef XXH_STATIC_LINKING_ONLY
#  define XXH_STATIC_LINKING_ONLY    /* XXH64_state_t */
#endif
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NSharding {

class TStreamStringHashCalcer: public IHashCalcer {
private:
    ui64 Seed;
    XXH64_state_t HashState;
public:
    TStreamStringHashCalcer(const ui64 seed)
        : Seed(seed) {
    }

    virtual void Start() override;
    virtual void Update(const ui8* data, const ui32 size) override;
    virtual ui64 Finish() override;
};

}
