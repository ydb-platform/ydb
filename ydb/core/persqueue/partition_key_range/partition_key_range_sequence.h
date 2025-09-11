#pragma once

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <expected>
#include <span>

namespace NKikimr::NPQ {

    struct TPartitionKeyRangeView {
        ui32 PartitionId;
        TMaybe<TStringBuf> FromBound;
        TMaybe<TStringBuf> ToBound;
    };

    // checks if set of intetervals is disjoined and covers all (-inf, +inf) range of keys
    std::expected<void, std::string> ValidateKeyRangeSequence(const std::span<const TPartitionKeyRangeView> bounds);

} // namespace NKikimr::NPQ
