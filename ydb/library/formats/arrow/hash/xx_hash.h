#pragma once
#ifndef XXH_STATIC_LINKING_ONLY
#  define XXH_STATIC_LINKING_ONLY    /* XXH64_state_t */
#endif
#include <contrib/libs/xxhash/xxhash.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NHash::NXX64 {

class TStreamStringHashCalcer {
private:
    const ui64 Seed;
    XXH64_state_t HashState;
public:
    TStreamStringHashCalcer(const ui64 seed)
        : Seed(seed) {
    }

    void Start();
    void Update(const ui8* data, const ui32 size);
    ui64 Finish();
};

class TStreamStringHashCalcer_H3 {
private:
    const ui64 Seed;
    XXH3_state_t HashState;
public:
    TStreamStringHashCalcer_H3(const ui64 seed)
        : Seed(seed) {
    }

    void Start();
    void Update(const ui8* data, const ui32 size);
    ui64 Finish();
};

}
