#include "xx_hash.h"

namespace NKikimr::NSharding {

void TStreamStringHashCalcer::Start() {
    XXH64_reset(&HashState, Seed);
}

void TStreamStringHashCalcer::Update(const ui8* data, const ui32 size) {
    XXH64_update(&HashState, data, size);
}

ui64 TStreamStringHashCalcer::Finish() {
    return XXH64_digest(&HashState);
}

}
