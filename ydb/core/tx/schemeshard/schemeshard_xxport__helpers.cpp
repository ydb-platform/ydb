#include "schemeshard_xxport__helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    const auto& labels = operationParams.labels();
    auto it = labels.find("uid");
    if (it != labels.end()) {
        return it->second;
    }
    return {};
}

template <class TInfo>
THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransactionImpl(TSchemeShard* ss, TTxId txId, const TInfo& xxportInfo) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;
    record.SetPeerName(xxportInfo.PeerName);
    record.SetSanitizedToken(xxportInfo.SanitizedToken);
    if (xxportInfo.UserSID) {
        record.SetOwner(*xxportInfo.UserSID);
        record.SetUserSID(*xxportInfo.UserSID);
    }
    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TExportInfo& exportInfo) {
    return MakeModifySchemeTransactionImpl(ss, txId, exportInfo);
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TImportInfo& importInfo) {
    return MakeModifySchemeTransactionImpl(ss, txId, importInfo);
}

const TVector<XxportProperties>& GetXxportProperties() {
    static TVector<XxportProperties> properties = {
        {NYdb::NDump::NFiles::TableScheme().FileName, NBackup::EBackupFileType::TableSchema, NKikimrSchemeOp::EPathType::EPathTypeTable},
        {NYdb::NDump::NFiles::CreateView().FileName, NBackup::EBackupFileType::ViewCreate, NKikimrSchemeOp::EPathType::EPathTypeView},
        {NYdb::NDump::NFiles::CreateTopic().FileName, NBackup::EBackupFileType::TopicCreate, NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup},
        {NYdb::NDump::NFiles::CreateAsyncReplication().FileName, NBackup::EBackupFileType::AsyncReplicationCreate, NKikimrSchemeOp::EPathType::EPathTypeReplication},
        {NYdb::NDump::NFiles::CreateTransfer().FileName, NBackup::EBackupFileType::TransferCreate, NKikimrSchemeOp::EPathType::EPathTypeTransfer},
        {NYdb::NDump::NFiles::CreateExternalDataSource().FileName, NBackup::EBackupFileType::ExternalDataSourceCreate, NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource},
        {NYdb::NDump::NFiles::CreateExternalTable().FileName, NBackup::EBackupFileType::ExternalTableCreate, NKikimrSchemeOp::EPathType::EPathTypeExternalTable},
        {NYdb::NDump::NFiles::CreateCoordinationNode().FileName, NBackup::EBackupFileType::CoordinationNodeCreate, NKikimrSchemeOp::EPathType::EPathTypeKesus},
    };

    return properties;
}

TMaybe<XxportProperties> PathTypeToXxportProperties(NKikimrSchemeOp::EPathType pathType) {
    auto it = std::ranges::find_if(GetXxportProperties(), [&](const XxportProperties& prop) {
        return prop.PathType == pathType;
    });

    if (it == GetXxportProperties().end()) {
        return Nothing();
    }

    return *it;
}

}  // NKikimr::NSchemeShard
