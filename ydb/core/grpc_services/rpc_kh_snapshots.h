#pragma once
#include "defs.h"

namespace NKikimr {
namespace NGRpcService {

struct TKikhouseSnapshotId {
    ui64 Step;
    ui64 TxId;

    TKikhouseSnapshotId()
        : Step(0)
        , TxId(0)
    { }

    TKikhouseSnapshotId(ui64 step, ui64 txId)
        : Step(step)
        , TxId(txId)
    { }

    explicit operator bool() const {
        return Step != 0 || TxId != 0;
    }

    TString ToUri() const;

    bool Parse(const TString& uri);
};

} // namespace NKikimr
} // namespace NGRpcService
