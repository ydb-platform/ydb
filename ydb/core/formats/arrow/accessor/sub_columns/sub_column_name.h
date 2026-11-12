#pragma once

#include <util/digest/fnv.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

#include <utility>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Canonical internal names, may be compared for == without further normalization.
// Default construction stands for a full column request; Parse("") selects an empty-name subcolumn.
// An invalid non-parseable input will create a canonical name that is invalid JsonPath - see `Parse` comment.
class TCanonicalSubColumnName {
private:
    TString Value;

    explicit TCanonicalSubColumnName(TString value)
        : Value(std::move(value)) {
    }

public:
    TCanonicalSubColumnName() = default;

    // Parse is non-failing, error results in original name returned.
    // This is preexisting behavior, consistent with other code paths.
    static TCanonicalSubColumnName Parse(TStringBuf path);

    const TString& GetValue() const {
        return Value;
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TCanonicalSubColumnName& name) {
        return out << name.Value;
    }

    explicit operator bool() const {
        return !!Value;
    }

    bool operator==(const TCanonicalSubColumnName& item) const {
        return Value == item.Value;
    }

    bool operator<(const TCanonicalSubColumnName& item) const {
        return Value < item.Value;
    }

    size_t GetHash() const {
        return FnvHash<ui64>(Value.data(), Value.size());
    }

    explicit operator size_t() const {
        return GetHash();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
