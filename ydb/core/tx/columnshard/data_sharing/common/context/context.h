#pragma once
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/sessions.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NOlap::NDataSharing {

class TTransferContext {
private:
    YDB_READONLY(TTabletId, DestinationTabletId, (TTabletId)0);
    YDB_READONLY_DEF(THashSet<TTabletId>, SourceTabletIds);
    YDB_READONLY(bool, Moving, false);
    TSnapshot SnapshotBarrier = TSnapshot::Zero();
    YDB_READONLY_DEF(std::optional<ui64>, TxId);
public:
    TTransferContext() = default;
    bool IsEqualTo(const TTransferContext& context) const;
    TString DebugString() const;

    const TSnapshot& GetSnapshotBarrierVerified() const;

    TTransferContext(const TTabletId destination, const THashSet<TTabletId>& sources, const TSnapshot& snapshotBarrier, const bool moving, const std::optional<ui64> txId = {});
    NKikimrColumnShardDataSharingProto::TTransferContext SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TTransferContext& proto);
};

}