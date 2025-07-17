#include "etcd_shared.h"

namespace NEtcd {

void TSharedStuff::UpdateRevision(i64 revision) {
    auto stored = Revision.load();
    while (revision > stored && !Revision.compare_exchange_weak(stored, revision))
        continue;
}

std::string IncrementKey(std::string key) {
    for (auto i = key.size(); i > 0u;) {
        if (const auto k = key[--i]; ~k) {
            key[i] = k + '\x01';
            return key;
        } else if (!i) {
            return key;
        } else {
            key[i] = '\x00';
        }
    }
    return std::string();
}

std::string DecrementKey(std::string key) {
    for (auto i = key.size(); i > 0u;) {
        if (const auto k = key[--i]) {
            key[i] = k - '\x01';
            return key;
        } else if (!i) {
            return key;
        } else {
            key[i] = '\xFF';
        }
    }
    return std::string();
}

std::ostream& DumpKeyRange(std::ostream& out, std::string_view key, std::string_view end) {
    if (end.empty())
        out << '=' << key;
    else if (Endless == end)
        out << '>' << '=' << key;
    else if (end == key)
        out << '^' << key;
    else
        out << '[' << key << ',' << end << ']';
    return out;
}

}
