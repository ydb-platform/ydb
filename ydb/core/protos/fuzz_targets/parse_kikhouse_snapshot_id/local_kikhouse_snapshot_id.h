#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NGRpcService {

struct TKikhouseSnapshotId {
    ui64 Step = 0;
    ui64 TxId = 0;

    TKikhouseSnapshotId() = default;
    TKikhouseSnapshotId(ui64 step, ui64 txId)
        : Step(step)
        , TxId(txId)
    {
    }

    explicit operator bool() const {
        return Step != 0 || TxId != 0;
    }

    TString ToUri() const;
    bool Parse(const TString& uri);
};

} // namespace NKikimr::NGRpcService
