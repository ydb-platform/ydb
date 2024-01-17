#pragma once
#include <util/system/types.h>

namespace NKikimr::NArrow::NHash {

class IHashCalcer {
public:
    virtual void Update(const ui8* data, const ui32 size) = 0;
    virtual ui64 Finish() = 0;
    virtual void Start() = 0;
};

}
