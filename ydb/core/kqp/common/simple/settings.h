#pragma once
#include <tuple>
#include <util/str_stl.h>

namespace NKikimr::NKqp {

struct TKqpQuerySettings {
    bool DocumentApiRestricted = true;
    bool IsInternalCall = false;

    bool operator==(const TKqpQuerySettings& other) const {
        return
            DocumentApiRestricted == other.DocumentApiRestricted &&
            IsInternalCall == other.IsInternalCall;
    }

    bool operator!=(const TKqpQuerySettings& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQuerySettings&) = delete;
    bool operator>(const TKqpQuerySettings&) = delete;
    bool operator<=(const TKqpQuerySettings&) = delete;
    bool operator>=(const TKqpQuerySettings&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(DocumentApiRestricted, IsInternalCall);
        return THash<decltype(tuple)>()(tuple);
    }
};

} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQuerySettings> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQuerySettings& settings) const {
        return settings.GetHash();
    }
};
