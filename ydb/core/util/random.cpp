#include <util/random/entropy.h>
#include <util/stream/input.h>

#include "random.h"

namespace NKikimr {

void SafeEntropyPoolRead(void* buf, size_t len) {
    auto& pool = EntropyPool();
    size_t read = 0;
    while (read < len) {
        read += pool.Read((char*)buf + read, len - read);
    }
}

TString GenRandomBuffer(size_t len) {
    TString res = TString::Uninitialized(len);
    SafeEntropyPoolRead(res.Detach(), res.size());
    return res;
}

} // namespace NKikimr
