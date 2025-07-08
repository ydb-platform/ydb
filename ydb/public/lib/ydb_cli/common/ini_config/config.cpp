#include "config.h"

namespace NIniConfig {

    TConfig::TConfig()
        : V_(Null())
    {}

    TConfig::TConfig(IValue* v)
        : V_(v)
    {}

    bool TConfig::IsNull() const {
        return V_.Get() == Null();
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

    const TConfig& TConfig::Or(const TConfig& r) const {
        return IsNull() ? r : *this;
    }

    const TConfig& TDict::Find(const TStringBuf& key) const {
        auto it = find(TString(key));
        if (it == end()) {
            static TConfig nullConfig;
            return nullConfig;
        }
        return it->second;
    }

    const TConfig& TDict::At(const TStringBuf& key) const {
        auto it = find(TString(key));
        if (it == end()) {
            throw TConfigParseError() << "Missing key: " << key;
        }
        return it->second;
    }

}
