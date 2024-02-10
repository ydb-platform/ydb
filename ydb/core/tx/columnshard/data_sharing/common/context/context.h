#pragma once
#include <ydb/core/tx/columnshard/common/tablet_id.h>
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
public:
    NKikimrColumnShardDataSharingProto::TTransferContext SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TTransferContext& proto);
};

}