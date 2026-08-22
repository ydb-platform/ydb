#include "config.h"
#include "ini.h"

#include <util/stream/mem.h>

namespace NIniConfig {

TConfig TConfig::ReadIni(TStringBuf in) {
    TMemoryInput mi(in.data(), in.size());

    return ParseIni(mi);
}

bool TConfig::Has(const TStringBuf& key) const {
    return !operator[](key).IsNull();
}

const TConfig& TConfig::operator[](const TStringBuf& key) const {
    return Get<TDict>().Find(key);
}

const TConfig& TConfig::At(const TStringBuf& key) const {
    return Get<TDict>().At(key);
}

const TConfig& TConfig::operator[](size_t index) const {
    return Get<TArray>().Index(index);
}

size_t TConfig::GetArraySize() const {
    return Get<TArray>().size();
}

const TConfig& TDict::Find(const TStringBuf& key) const {
    const_iterator it = find(key);

    if (it == end()) {
        return Default<TConfig>();
    }

    return it->second;
}

const TConfig& TDict::At(const TStringBuf& key) const {
    const_iterator it = find(key);

    Y_ENSURE_BT(it != end(), "missing key '" << key << "'");

    return it->second;
}

const TConfig& TArray::Index(size_t index) const {
    if (index < size()) {
        return (*this)[index];
    }

    return Default<TConfig>();
}

const TConfig& TArray::At(size_t index) const {
    Y_ENSURE_BT(index < size(), "index " << index << " is out of bounds");

    return (*this)[index];
}

}
