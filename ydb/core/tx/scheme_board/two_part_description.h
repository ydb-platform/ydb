#pragma once

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/string.h>

namespace NKikimr {
namespace NSchemeBoard {

struct TTwoPartDescription {
    TString PreSerialized;
    NKikimrScheme::TEvDescribeSchemeResult Record;

    TTwoPartDescription() = default;

    explicit TTwoPartDescription(TString preSerialized, NKikimrScheme::TEvDescribeSchemeResult record)
        : PreSerialized(std::move(preSerialized))
        , Record(std::move(record))
    {
    }

    bool Empty() const;
    operator bool() const;
};

} // NSchemeBoard
} // NKikimr
