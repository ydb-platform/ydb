#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>
#include <ydb/core/protos/tenant_pool.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EPutHandleClass, stream, value) {
    stream << NKikimrBlobStorage::EPutHandleClass_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EGetHandleClass, stream, value) {
    stream << NKikimrBlobStorage::EGetHandleClass_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TVDiskKind::EVDiskKind, stream, value) {
    stream << NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus, stream, value) {
    stream << NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TSyncGuidInfo::EState, stream, value) {
    stream << NKikimrBlobStorage::TSyncGuidInfo::EState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TLocalGuidInfo::EState, stream, value) {
    stream << NKikimrBlobStorage::TLocalGuidInfo::EState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TSyncerStatus::EPhase, stream, value) {
    stream << NKikimrBlobStorage::TSyncerStatus::EPhase_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EVDiskQueueId, stream, value) {
    stream << NKikimrBlobStorage::EVDiskQueueId_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EVDiskInternalQueueId, stream, value) {
    stream << NKikimrBlobStorage::EVDiskInternalQueueId_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::ESyncFullStage, stream, value) {
    stream << NKikimrBlobStorage::ESyncFullStage_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrProto::EReplyStatus, stream, value) {
    stream << NKikimrProto::EReplyStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TStatus::ECode, stream, value) {
    stream << NKikimrCms::TStatus::ECode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::EState, stream, value) {
    stream << NKikimrCms::EState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EDriveStatus, stream, value) {
    stream << NKikimrBlobStorage::EDriveStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EDecommitStatus, stream, value) {
    stream << NKikimrBlobStorage::EDecommitStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TGroupStatus::E, stream, value) {
    stream << NKikimrBlobStorage::TGroupStatus::E_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TDriveLifeStage::E, stream, value) {
    stream << NKikimrBlobStorage::TDriveLifeStage::E_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TSerialManagementStage::E, stream, value) {
    stream << NKikimrBlobStorage::TSerialManagementStage::E_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrResourceBroker::EResourceType, stream, value) {
    stream << NKikimrResourceBroker::EResourceType_Name(value);
}
/* FIXME
Y_DECLARE_OUT_SPEC(, Ydb::Cms::GetDatabaseStatusResult::State, stream, value) {
    stream << Ydb::Cms::GetDatabaseStatusResult::State_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::StatusIds::StatusCode, stream, value) {
    stream << Ydb::StatusIds::StatusCode_Name(value);
}
*/
Y_DECLARE_OUT_SPEC(, NKikimrConsole::TConfigItem::EKind, stream, value) {
    stream << NKikimrConsole::TConfigItem::EKind_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTenantPool::EStatus, stream, value) {
    stream << NKikimrTenantPool::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EPDiskType, stream, value) {
    stream << NKikimrBlobStorage::EPDiskType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::EVDiskStatus, stream, value) {
    stream << NKikimrBlobStorage::EVDiskStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrNodeBroker::TStatus::ECode, stream, value) {
    stream << NKikimrNodeBroker::TStatus::ECode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::EMarker, stream, value) {
    stream << NKikimrCms::EMarker_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::ETransactionKind, stream, value) {
    stream << NKikimrTxDataShard::ETransactionKind_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::ETextFormat, stream, value) {
    stream << NKikimrCms::ETextFormat_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TLogRecordData::EType, stream, value) {
    stream << NKikimrCms::TLogRecordData::EType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrConfig::TBootstrap::ETabletType, stream, value) {
    stream << NKikimrConfig::TBootstrap::ETabletType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvProposeTransactionResult::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvProposeTransactionResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::EDatashardState, stream, value) {
    stream << NKikimrTxDataShard::EDatashardState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TError::EKind, stream, value) {
    stream << NKikimrTxDataShard::TError::EKind_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TPDiskState::E, stream, value) {
    stream << NKikimrBlobStorage::TPDiskState::E_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrBlobStorage::TPDiskSpaceColor::E, stream, value) {
    stream << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrWhiteboard::EFlag, stream, value) {
    stream << NKikimrWhiteboard::EFlag_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvCompactTableResult::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvCompactTableResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrKqp::EQueryAction, stream, value) {
    stream << NKikimrKqp::EQueryAction_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrKqp::EQueryType, stream, value) {
    stream << NKikimrKqp::EQueryType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::TTTLSettings::EUnit, stream, value) {
    stream << NKikimrSchemeOp::TTTLSettings::EUnit_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrScheme::EStatus, stream, value) {
    stream << NKikimrScheme::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::EPathType, stream, value) {
    stream << NKikimrSchemeOp::EPathType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::EPathState, stream, value) {
    stream << NKikimrSchemeOp::EPathState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::EFreezeState, stream, value) {
    stream << NKikimrSchemeOp::EFreezeState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::EIndexType, stream, value) {
    stream << NKikimrSchemeOp::EIndexType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::EIndexState, stream, value) {
    stream << NKikimrSchemeOp::EIndexState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::ECdcStreamMode, stream, value) {
    stream << NKikimrSchemeOp::ECdcStreamMode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::ECdcStreamFormat, stream, value) {
    stream << NKikimrSchemeOp::ECdcStreamFormat_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrSchemeOp::ECdcStreamState, stream, value) {
    stream << NKikimrSchemeOp::ECdcStreamState_Name(value);
}
