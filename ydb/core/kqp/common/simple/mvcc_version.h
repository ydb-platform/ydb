#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {

// Internal calls only. The caller owns the snapshot and its retention.
inline constexpr char YDB_MVCC_VERSION_HEADER[] = "x-ydb-mvcc-version";

inline TString FormatMvccVersion(const TRowVersion& version) {
    return TStringBuilder() << version.Step << ':' << version.TxId;
}

inline TMaybe<TRowVersion> ParseMvccVersion(TStringBuf value) {
    TStringBuf step, txId;
    if (!value.TrySplit(':', step, txId)) {
        return Nothing();
    }

    const auto isDecimal = [](TStringBuf part) {
        if (part.empty()) {
            return false;
        }
        for (char c : part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    };

    TRowVersion version;
    if (!isDecimal(step) || !isDecimal(txId) || !TryFromString(step, version.Step) || !TryFromString(txId, version.TxId) || version.IsMin() ||
        version.Step == Max<ui64>())
    {
        return Nothing();
    }
    return version;
}

}   // namespace NKikimr::NKqp
