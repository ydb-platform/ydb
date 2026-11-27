#include "schemeshard_export_helpers.h"

#include <ydb/core/backup/common/checksum.h>
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

bool ResolveDirectoryExportPrefixes(TVector<TExportObjectPrefix>& items, const TDirectoryExportFiles& files, TString& error) {
    TSet<TString> topLevelNames;
    for (const auto& item : items) {
        topLevelNames.emplace(TStringBuf(item.Prefix).Before('/'));
    }
    TString fallback = ".ydb-objects";
    while (topLevelNames.contains(fallback)) {
        fallback += '_';
    }

    TMap<TString, TString> occupied;
    if (files.HasMapping) {
        occupied.emplace("SchemaMapping", "export metadata");
        occupied.emplace("metadata.json", "export metadata");
        if (files.Checksums) {
            occupied.emplace(NBackup::ChecksumKey("metadata.json"), "export metadata");
        }
        if (files.CompletionMarker) {
            occupied.emplace(ExportCompleteFileName, "export metadata");
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
    const auto objectPaths = [&](const TExportObjectPrefix& item) {
        TVector<TString> paths;
        if (item.Directory) {
            const auto add = [&](TStringBuf name) {
                const TString path = TStringBuilder() << item.Prefix << '/' << name;
                paths.push_back(files.Encrypted ? path + ".enc" : path);
                if (files.Checksums) {
                    paths.push_back(NBackup::ChecksumKey(path));
                }
            };
            add("metadata.json");
            if (files.Permissions) {
                add("permissions.pb");
            }
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
            auto paths = objectPaths(item);
            for (const auto& path : paths) {
                if ((owner = conflict(path))) {
                    break;
                }
            }
            if (owner || item.Prefix.empty()) {
                if (explicitPrefix || !files.HasMapping) {
                    error = TStringBuilder() << "Export destination for " << item.SourcePath
                        << " conflicts with " << (owner ? *owner : TString("the backup root"));
                    return false;
                }
                item.Prefix = TStringBuilder() << fallback << '/' << i + 1;
                paths = objectPaths(item);
            }
            for (auto& path : paths) {
                occupied.emplace(std::move(path), item.SourcePath);
            }
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
