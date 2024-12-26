#include "xx_hash.h"

namespace NKikimr::NArrow::NHash::NXX64 {

void TStreamStringHashCalcer::Start() {
    XXH3_64bits_reset_withSeed(&HashState, Seed);
}

void TStreamStringHashCalcer::Update(const ui8* data, const ui32 size) {
    XXH3_64bits_update(&HashState, data, size);
}

ui64 TStreamStringHashCalcer::Finish() {
    return XXH3_64bits_digest(&HashState);
}

}
