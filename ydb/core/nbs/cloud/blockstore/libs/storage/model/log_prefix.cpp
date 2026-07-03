#include "log_prefix.h"

#include <util/string/builder.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TLogPrefix::TLogPrefix(
    std::initializer_list<std::pair<TString, TLogPrefix::TValue>> tags)
{
    for (const auto& [k, v]: tags) {
        Tags.emplace_back(k, v);
    }
    Rebuild();
}

void TLogPrefix::AddTag(const TString& key, TValue value)
{
    for (auto& [k, v]: Tags) {
        if (k == key) {
            v = std::move(value);
            Rebuild();
            return;
        }
    }
    Tags.emplace_back(key, std::move(value));
    Rebuild();
}

void TLogPrefix::AddTags(
    std::initializer_list<std::pair<TString, TLogPrefix::TValue>> tags)
{
    for (const auto& [k, v]: tags) {
        bool updated = false;
        for (auto& [ek, ev]: Tags) {
            if (ek == k) {
                ev = v;
                updated = true;
                break;
            }
        }
        if (!updated) {
            Tags.emplace_back(k, v);
        }
    }
    Rebuild();
}

TString TLogPrefix::Get() const
{
    return CachedPrefix;
}

TString TLogPrefix::ValueToString(const TValue& val) const
{
    return std::visit(
        [](auto&& arg) -> TString
        {
            TStringBuilder sb;
            sb << arg;
            return sb;
        },
        val);
}

void TLogPrefix::Rebuild()
{
    TStringBuilder sb;
    sb << "[";

    bool first = true;
    for (const auto& [k, v]: Tags) {
        if (!first) {
            sb << " ";
        }
        sb << k << ":" << ValueToString(v);
        first = false;
    }

    sb << "]";
    CachedPrefix = sb;
}

}   // namespace NYdb::NBS
