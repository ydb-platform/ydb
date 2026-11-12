#include "schemeshard_export_helpers.h"
#include "schemeshard_info_types.h"

#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

namespace NKikimr::NSchemeShard {

THolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot> MakeExportChecksumSnapshotRequest(
    TExportInfo& info, TRowVersion version, const TString& database) {
    if (!info.EnableBackupTableChecksum || info.Kind == TExportInfo::EKind::YT || !info.IsInProgress() ||
        info.ChecksumSnapshots.contains(version)) {
        return {};
    }
    TVector<TTableId> tables;
    for (const auto& item : info.Items) {
        if (item.SourcePathType == NKikimrSchemeOp::EPathTypeTable && item.ParentIdx == Max<ui32>()) {
            tables.emplace_back(item.SourcePathId);
        }
    }
    if (tables.empty()) {
        return {};
    }
    info.ChecksumSnapshots.try_emplace(version);
    return MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>(database, std::move(tables), version);
}

NKikimrSchemeOp::EPathType GetPathType(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return describeResult.GetPathDescription().GetSelf().GetPathType();
}

TString GetExportCompletionPrefix(const Ydb::Export::ExportToS3Settings& settings) {
    const auto normalize = [](const TString& prefix) {
        return NBackup::NormalizeItemPrefix(NBackup::NormalizeItemPath(prefix));
    };
    const auto contains = [](const TString& prefix, const TString& destination) {
        return destination == prefix || destination.StartsWith(prefix + '/');
    };

    if (settings.destination_prefix()) {
        return normalize(settings.destination_prefix());
    }
    if (settings.items().empty()) {
        return {};
    }

    TString prefix = normalize(settings.items(0).destination_prefix());
    for (const auto& item : settings.items()) {
        const TString destination = normalize(item.destination_prefix());
        while (prefix && !contains(prefix, destination)) {
            const size_t slash = prefix.rfind('/');
            prefix = slash == TString::npos ? TString() : prefix.substr(0, slash);
        }
    }

    return prefix;
}

} // namespace NKikimr::NSchemeShard
