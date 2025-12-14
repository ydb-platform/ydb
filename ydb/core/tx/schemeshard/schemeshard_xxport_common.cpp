#include "schemeshard_xxport_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

namespace {

TMaybe<TString> PathTypeToFileName(NKikimrSchemeOp::EPathType pathType) {
    switch (pathType) {
        case NKikimrSchemeOp::EPathType::EPathTypeTable:
            return NYdb::NDump::NFiles::TableScheme().FileName;
        case NKikimrSchemeOp::EPathType::EPathTypeView:
            return NYdb::NDump::NFiles::CreateView().FileName;
        case NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup:
            return NYdb::NDump::NFiles::CreateTopic().FileName;
        case NKikimrSchemeOp::EPathType::EPathTypeReplication:
            return NYdb::NDump::NFiles::CreateAsyncReplication().FileName;
        default:
            return Nothing();
    }
}

} // anonymous namespace

const TVector<XxportProperties>& GetXxportProperties() {
    static TVector<XxportProperties> properties = {
        {NYdb::NDump::NFiles::TableScheme().FileName, NBackup::EBackupFileType::TableSchema},
        {NYdb::NDump::NFiles::CreateView().FileName, NBackup::EBackupFileType::ViewCreate},
        {NYdb::NDump::NFiles::CreateTopic().FileName, NBackup::EBackupFileType::TopicCreate},
        {NYdb::NDump::NFiles::CreateAsyncReplication().FileName, NBackup::EBackupFileType::AsyncReplicationCreate},
    };

    return properties;
}

TMaybe<XxportProperties> PathTypeToXxportProperties(NKikimrSchemeOp::EPathType pathType) {
    auto fileName = PathTypeToFileName(pathType);
    if (!fileName) {
        return Nothing();
    }

    auto it = std::ranges::find_if(GetXxportProperties(), [&](const XxportProperties& properties) {
        return properties.FileName == *fileName;
    });

    if (it == GetXxportProperties().end()) {
        return Nothing();
    }

    return *it;
}

}
