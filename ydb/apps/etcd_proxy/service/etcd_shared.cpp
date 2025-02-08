#include "etcd_shared.h"

namespace NEtcd {

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
