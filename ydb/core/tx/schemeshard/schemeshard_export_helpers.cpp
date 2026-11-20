#include "schemeshard_export_helpers.h"

#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

namespace NKikimr::NSchemeShard {

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
