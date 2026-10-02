#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <initializer_list>
#include <variant>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TLogPrefix
{
public:
    using TValue = std::variant<TString, int, ui64, double, bool>;

private:
    TVector<std::pair<TString, TValue>> Tags;
    TString CachedPrefix;

public:
    TLogPrefix(std::initializer_list<std::pair<TString, TValue>> tags);

    void AddTag(const TString& key, TValue value);

    void AddTags(std::initializer_list<std::pair<TString, TValue>> tags);

    TString Get() const;

private:
    void Rebuild();
    TString ValueToString(const TValue& val) const;
};

inline IOutputStream& operator<<(IOutputStream& out, const TLogPrefix& prefix)
{
    return out << prefix.Get();
}

}   // namespace NYdb::NBS
