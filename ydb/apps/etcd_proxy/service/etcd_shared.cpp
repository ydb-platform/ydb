#include "etcd_shared.h"

namespace NEtcd {

namespace {
const TSharedStuff::TPtr Shared = std::make_shared<TSharedStuff>();
}

TSharedStuff::TPtr TSharedStuff::Get()  { return Shared; }

TString DecrementKey(TString key) {
    for (auto i = key.size(); i > 0u;) {
        if (const auto k = key[--i]) {
            key[i] = k - '\x01';
            return key;
        } else {
            key[i] = '\xFF';
        }
    }
    return TString();
}

}
