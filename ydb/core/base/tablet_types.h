#pragma once

#include "defs.h"
#include "tabletid.h"
#include <ydb/core/protos/tablet.pb.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
/// The TTabletTypes class
////////////////////////////////////////////
struct TTabletTypes : NKikimrTabletBase::TTabletTypes {
public:
    static constexpr EType USER_TYPE_START = UserTypeStart;
    static constexpr EType TYPE_INVALID = TypeInvalid;
    static constexpr EType FLAT_SCHEMESHARD = SchemeShard;
    static constexpr EType FLAT_DATASHARD = DataShard;
    static constexpr EType COLUMNSHARD = ColumnShard;
    static constexpr EType KEYVALUEFLAT = KeyValue;
    static constexpr EType PERSQUEUE = PersQueue;
    static constexpr EType PERSQUEUE_READ_BALANCER = PersQueueReadBalancer;
    static constexpr EType TX_DUMMY = Dummy;
    static constexpr EType FLAT_TX_COORDINATOR = Coordinator;
    static constexpr EType TX_MEDIATOR = Mediator;
    static constexpr EType FLAT_HIVE = Hive;
    static constexpr EType FLAT_BS_CONTROLLER = BSController;
    static constexpr EType TX_ALLOCATOR = TxAllocator;
    static constexpr EType NODE_BROKER = NodeBroker;
    static constexpr EType CMS = Cms;
    static constexpr EType RTMR_PARTITION = RTMRPartition;
    static constexpr EType BLOCKSTORE_VOLUME = BlockStoreVolume; 
    static constexpr EType BLOCKSTORE_PARTITION = BlockStorePartition; 
    static constexpr EType BLOCKSTORE_PARTITION2 = BlockStorePartition2;
    static constexpr EType BLOCKSTORE_DISK_REGISTRY = BlockStoreDiskRegistry; 
    static constexpr EType TENANT_SLOT_BROKER = TenantSlotBroker;
    static constexpr EType CONSOLE = Console;
    static constexpr EType KESUS = Kesus;
    static constexpr EType SYS_VIEW_PROCESSOR = SysViewProcessor;
    static constexpr EType FILESTORE = FileStore; 
    static constexpr EType TESTSHARD = TestShard;
    static constexpr EType SEQUENCESHARD = SequenceShard;
    static constexpr EType REPLICATION_CONTROLLER = ReplicationController;

    static const char* TypeToStr(EType t) {
        return EType_Name(t).c_str();
    }

    static EType StrToType(const TString& t) {
        EType type;
        if (EType_Parse(t, &type)) {
            return type;
        } else {
            return TypeInvalid;
        }
    }
};

} // end of NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimrTabletBase::TTabletTypes::EType, os, type) {
    os << NKikimrTabletBase::TTabletTypes::EType_Name(type);
}
