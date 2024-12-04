#include "error_attributes.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TErrorAttributes::TErrorAttributes(void* attributes)
    : Attributes_(attributes)
{ }

void TErrorAttributes::Clear()
{
    for (const auto& key : ListKeys()) {
        Remove(key);
    }
}

TErrorAttributes::TValue TErrorAttributes::GetYsonAndRemove(const TKey& key)
{
    auto result = GetYson(key);
    Remove(key);
    return result;
}

bool TErrorAttributes::Contains(TStringBuf key) const
{
    return FindYson(key).operator bool();
}

bool operator == (const TErrorAttributes& lhs, const TErrorAttributes& rhs)
{
    auto lhsPairs = lhs.ListPairs();
    auto rhsPairs = rhs.ListPairs();
    if (lhsPairs.size() != rhsPairs.size()) {
        return false;
    }

    std::sort(lhsPairs.begin(), lhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    std::sort(rhsPairs.begin(), rhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    for (auto index = 0; index < std::ssize(lhsPairs); ++index) {
        if (lhsPairs[index].first != rhsPairs[index].first) {
            return false;
        }
    }

    for (auto index = 0; index < std::ssize(lhsPairs); ++index) {
        if (lhsPairs[index].second != rhsPairs[index].second) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
