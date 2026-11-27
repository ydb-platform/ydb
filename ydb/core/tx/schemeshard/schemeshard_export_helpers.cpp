#include "schemeshard_export_helpers.h"

#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

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

bool ResolveDirectoryExportPrefixes(TVector<TExportObjectPrefix>& items, bool hasMapping, TString& error) {
    TSet<TString> topLevelNames;
    for (const auto& item : items) {
        topLevelNames.emplace(TStringBuf(item.Prefix).Before('/'));
    }
    TString fallback = ".ydb-objects";
    while (topLevelNames.contains(fallback)) {
        fallback += '_';
    }

    TMap<TString, TString> occupied;
    if (hasMapping) {
        for (const char* name : {"SchemaMapping", "metadata.json", "metadata.json.sha256", "export_complete.json"}) {
            occupied.emplace(name, "export metadata");
        }
    }

    const auto conflict = [&](const TString& path) -> const TString* {
        for (TStringBuf parent = path; parent; parent.RNextTok('/')) {
            if (const auto* owner = occupied.FindPtr(TString(parent))) {
                return owner;
            }
        }
        const TString childPrefix = path + '/';
        const auto it = occupied.lower_bound(childPrefix);
        return it != occupied.end() && it->first.StartsWith(childPrefix) ? &it->second : nullptr;
    };
    const auto files = [](const TExportObjectPrefix& item) {
        TVector<TString> paths;
        if (item.Directory) {
            for (const char* name : {"metadata.json", "permissions.pb"}) {
                for (const char* suffix : {"", ".enc", ".sha256"}) {
                    paths.emplace_back(TStringBuilder() << item.Prefix << '/' << name << suffix);
                }
            }
            paths.emplace_back(TStringBuilder() << item.Prefix << '/' << ExportDataCompleteFileName);
        } else {
            paths.push_back(item.Prefix);
        }
        return paths;
    };

    // Reserve user-supplied destinations first. Only automatic names can move.
    for (bool explicitPrefix : {true, false}) {
        for (size_t i = 0; i < items.size(); ++i) {
            auto& item = items[i];
            if (item.Explicit != explicitPrefix) {
                continue;
            }
            const TString* owner = nullptr;
            auto paths = files(item);
            for (const auto& path : paths) {
                if ((owner = conflict(path))) {
                    break;
                }
            }
            if (owner || item.Prefix.empty()) {
                if (explicitPrefix || !hasMapping) {
                    error = TStringBuilder() << "Export destination for " << item.SourcePath
                        << " conflicts with " << (owner ? *owner : TString("the backup root"));
                    return false;
                }
                item.Prefix = TStringBuilder() << fallback << '/' << i + 1;
                paths = files(item);
            }
            for (auto& path : paths) {
                occupied.emplace(std::move(path), item.SourcePath);
            }
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
