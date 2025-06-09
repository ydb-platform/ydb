#pragma once

#include "defs.h"

#include <ydb/core/control/lib/defs.h>

#include <ydb/core/protos/tablet.pb.h>

namespace NKikimr::NUtil {

inline void ShrinkToFit(TString& input) {
    // capacity / size > 1.5
    if (input.capacity() * 2 > input.size() * 3) {
        input = input.copy();
    }
}

#define TABLET_TYPES_LIST(X)\
    X(Unknown)\
    X(OldSchemeShard)\
    X(OldDataShard)\
    X(OldHive)\
    X(OldCoordinator)\
    X(Mediator)\
    X(OldTxProxy)\
    X(OldBSController)\
    X(Dummy)\
    X(RTMRPartition)\
    X(OldKeyValue)\
    X(KeyValue)\
    X(Coordinator)\
    X(Hive)\
    X(BSController)\
    X(SchemeShard)\
    X(TxProxy)\
    X(DataShard)\
    X(PersQueue)\
    X(Cms)\
    X(NodeBroker)\
    X(TxAllocator)\
    X(PersQueueReadBalancer)\
    X(BlockStoreVolume)\
    X(BlockStorePartition)\
    X(TenantSlotBroker)\
    X(Console)\
    X(Kesus)\
    X(BlockStorePartition2)\
    X(BlockStoreDiskRegistry)\
    X(SysViewProcessor)\
    X(FileStore)\
    X(ColumnShard)\
    X(TestShard)\
    X(SequenceShard)\
    X(ReplicationController)\
    X(BlobDepot)\
    X(StatisticsAggregator)\
    X(GraphShard)\
    X(BackupController)


inline TMaybe<EStaticControlType> GetLogFlushDelayOverrideUsecTabletTypeControl(NKikimrTabletBase::TTabletTypes::EType type) {
#define CASE_TYPE_CONTROL(tablet)\
        case TTabletTypes::tablet: \
            return EStaticControlType::tablet ## LogFlushDelayOverrideUsec;

    switch (type) {
        TABLET_TYPES_LIST(CASE_TYPE_CONTROL)
    case NKikimrTabletBase::TTabletTypes_EType_Reserved43:
        return Nothing();
    case NKikimrTabletBase::TTabletTypes_EType_Reserved44:
        return Nothing();
    case NKikimrTabletBase::TTabletTypes_EType_Reserved45:
        return Nothing();
    case NKikimrTabletBase::TTabletTypes_EType_Reserved46:
        return Nothing();
    case NKikimrTabletBase::TTabletTypes_EType_TypeInvalid:
        return Nothing();
    case NKikimrTabletBase::TTabletTypes_EType_UserTypeStart:
        return Nothing();
    };
    return Nothing();
}

#undef CASE_TYPE_CONTROL
#undef TABLET_TYPES_LIST
}
