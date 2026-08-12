#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/algorithm.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_DESCRIBER

#define LOG_PREFIX NActors::TlsActivationContext->AsActorContext().SelfID

namespace NKikimr::NPQ::NDescriber {

namespace {

using namespace NSchemeCache;

bool HasAccess(const TDescribeSettings& settings, TIntrusivePtr<TSecurityObject> securityObject) {
    if (!settings.UserToken) {
        return true;
    }
    if (securityObject->CheckAccess(settings.AccessRights.Access, *settings.UserToken)) {
        return true;
    }
    if (settings.AccessRights.AccessOr) {
        return securityObject->CheckAccess(*settings.AccessRights.AccessOr, *settings.UserToken);
    }
    return false;
}

TString MakeFederationTopicPath(const TString& federationRoot, const TString& topicPath) {
    auto parts = NKikimr::SplitPath(federationRoot);
    for (const auto& part : NKikimr::SplitPath(topicPath)) {
        parts.push_back(part);
    }
    return CanonizePath(NKikimr::JoinPath(parts));
}

// First path component is federation account; requires account/topic shape.
std::optional<TString> ExtractFederationAccount(const TString& topicPath) {
    auto parts = NKikimr::SplitPath(topicPath);
    if (parts.size() < 2 || parts[0].empty()) {
        return std::nullopt;
    }
    return parts[0];
}

TString MakeFederationAccountDatabase(const TString& federationRoot, const TString& account) {
    return CanonizePath(NKikimr::JoinPath({federationRoot, account}));
}

// Federation retries append account/topic under FederationRoot. Build that relative
// suffix from the navigated absolute path vs the request database — never from a
// database-prefixed originalPath (would become FederationRoot/Root/account/...).
std::optional<TString> MakeFederationRelativeTopicPath(const TString& databasePath, const TString& originalPath, const TString& realPath) {
    if (!databasePath.empty()) {
        const auto dbParts = NKikimr::SplitPath(databasePath);
        const auto realParts = NKikimr::SplitPath(realPath);
        if (realParts.size() <= dbParts.size()) {
            return std::nullopt;
        }
        for (size_t i = 0; i < dbParts.size(); ++i) {
            if (dbParts[i] != realParts[i]) {
                return std::nullopt;
            }
        }
        return NKikimr::JoinPath(TVector<TString>(realParts.begin() + dbParts.size(), realParts.end()));
    }

    // No database in the request: originalPath must already be account/topic-shaped.
    const auto parts = NKikimr::SplitPath(originalPath);
    if (parts.size() < 2 || parts[0].empty()) {
        return std::nullopt;
    }
    return NKikimr::JoinPath(parts);
}

class TDescribeActor : public TActorBootstrapped<TDescribeActor> {
public:
    TDescribeActor(const NActors::TActorId& parent, const TString& databasePath, absl::flat_hash_set<TString>&& topicPaths, const TDescribeSettings& settings)
        : Parent(parent)
        , DatabasePath(databasePath)
        , TopicPaths(std::move(topicPaths))
        , Settings(settings)
    {
    }

    void Bootstrap() {
        Become(&TDescribeActor::StateWork);
        if (!AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            FederationRoot = AppData()->PQConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot();
        }
        RetryWithSyncVersion = Settings.ForceSyncVersion;
        UsedSyncVersion = Settings.ForceSyncVersion;
        RequestDatabaseName = DatabasePath;
        DoRequest(TopicPaths);
    }

    void DoRequest(const absl::flat_hash_set<TString>& topicPath) {
        YDB_LOG_DEBUG("Create request with",
            {"logPrefix", LOG_PREFIX},
            {"topicPaths", JoinRange(", ", topicPath.begin(), topicPath.end())},
            {"syncVersion", RetryWithSyncVersion},
            {"databaseName", RequestDatabaseName});

        auto schemeRequest = std::make_unique<TSchemeCacheNavigate>(1);
        schemeRequest->DatabaseName = RequestDatabaseName;

        auto addEntry = [&](const TString& topic) {
            auto split = NKikimr::SplitPath(topic);

            schemeRequest->ResultSet.emplace_back();
            auto& entry = schemeRequest->ResultSet.back();
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            entry.Operation = TSchemeCacheNavigate::OpList;
            entry.SyncVersion = RetryWithSyncVersion;
            entry.ShowPrivatePath = true;
        };

        for (const auto& topic : topicPath) {
            auto normalizedPath = NKikimr::NormalizePath(RequestDatabaseName, CanonizePath(topic));
            // Keep the originally requested path across retries (sync / Federation / CDC).
            PathToOriginalPath.try_emplace(normalizedPath, topic);
            addEntry(normalizedPath);
        }

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"logPrefix", LOG_PREFIX});
        auto& result = ev->Get()->Request;

        absl::flat_hash_set<TString> unknownPaths;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            const auto& entry = result->ResultSet[i];
            auto realPath = CanonizePath(NKikimr::JoinPath(entry.Path));
            Y_ASSERT(PathToOriginalPath.contains(realPath));
            auto originalPath = PathToOriginalPath[realPath];

            bool isCDCStream = false;
            TString cdcStreamName;

            auto it = CDCPaths.find(realPath);
            if (it != CDCPaths.end()) {
                originalPath = it->second.OriginalPath;
                isCDCStream = true;
                cdcStreamName = it->second.CdcStreamName;
            } else if (auto federationIt = FederationPaths.find(realPath); federationIt != FederationPaths.end()) {
                originalPath = federationIt->second.OriginalPath;
            }

            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    [[fallthrough]];
                case TSchemeCacheNavigate::EStatus::RootUnknown: {
                    if (RetryWithSyncVersion) {
                        if (entry.SecurityObject && !HasAccess(Settings, entry.SecurityObject)) {
                            YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});

                            SetErrorResult(originalPath, EStatus::UNAUTHORIZED);
                        } else if (TryScheduleFederationRetry(originalPath, realPath)) {
                            YDB_LOG_DEBUG("Path not found, will try FederationRoot",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath},
                                {"originalPath", originalPath},
                                {"federationRoot", FederationRoot});
                        } else {
                            YDB_LOG_DEBUG("Path not found",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});

                            SetErrorResult(originalPath, EStatus::NOT_FOUND);
                        }
                    } else {
                        unknownPaths.insert(realPath);
                    }
                    break;
                }
                case TSchemeCacheNavigate::EStatus::AccessDenied: {
                    YDB_LOG_DEBUG("Path ACCESS DENIED",
                        {"logPrefix", LOG_PREFIX},
                        {"realPath", realPath});
                    SetErrorResult(originalPath, EStatus::UNAUTHORIZED);
                    break;
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        YDB_LOG_DEBUG("Path is CDC",
                            {"logPrefix", LOG_PREFIX},
                            {"realPath", realPath});

                        CDCPaths[TStringBuilder() << realPath << "/streamImpl"] = {
                            .OriginalPath = originalPath,
                            .CdcStreamName = entry.Self->Info.GetName(),
                            .AccountDatabase = RequestDatabaseName
                        };
                        break;
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        if (!entry.PQGroupInfo || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                            if (RetryWithSyncVersion) {
                                if (TryScheduleFederationRetry(originalPath, realPath)) {
                                    YDB_LOG_DEBUG("Path not found, will try FederationRoot",
                                        {"logPrefix", LOG_PREFIX},
                                        {"realPath", realPath},
                                        {"originalPath", originalPath},
                                        {"federationRoot", FederationRoot});
                                } else {
                                    YDB_LOG_DEBUG("Path not found",
                                        {"logPrefix", LOG_PREFIX},
                                        {"realPath", realPath});
                                    SetErrorResult(originalPath, EStatus::NOT_FOUND);
                                }
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (!HasAccess(Settings, entry.SecurityObject)) {
                                YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});

                                Result[originalPath] = TTopicInfo{
                                    .Status = entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)
                                            ? EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS : EStatus::UNAUTHORIZED
                                };
                            } else {
                                YDB_LOG_DEBUG("Path SUCCESS",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::SUCCESS,
                                    .RealPath = realPath,
                                    .CdcStream = isCDCStream,
                                    .CdcStreamName = cdcStreamName,
                                    .CreateStep = entry.CreateStep,
                                    .Info = entry.PQGroupInfo,
                                    .Self = entry.Self,
                                    .SecurityObject = entry.SecurityObject
                                };
                            }
                        }
                    } else {
                        YDB_LOG_DEBUG("Path is not a",
                            {"logPrefix", LOG_PREFIX},
                            {"realPath", realPath},
                            {"topic", entry.Kind});
                        if (Settings.UserToken && !entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)) {
                            YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});
                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::UNAUTHORIZED
                            };
                        } else {
                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::NOT_TOPIC,
                                .RealPath = realPath
                            };
                        }
                    }
                    break;
                }
                default: {
                    YDB_LOG_DEBUG("Path unknown error",
                        {"logPrefix", LOG_PREFIX},
                        {"realPath", realPath});
                    Result[originalPath] = TTopicInfo{
                        .Status = EStatus::UNKNOWN_ERROR,
                        .RealPath = realPath
                    };
                    break;
                }
            }
        }

        if (!unknownPaths.empty()) {
            RetryWithSyncVersion = true;
            UsedSyncVersion = true;
            return DoRequest(unknownPaths);
        }

        if (TryStartNextFederationDatabaseRequest()) {
            return;
        }

        if (TryStartNextCdcDatabaseRequest()) {
            return;
        }

        Send(Parent, new TEvDescribeTopicsResponse(std::move(Result), UsedSyncVersion));
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    bool TryScheduleFederationRetry(const TString& originalPath, const TString& realPath) {
        if (FederationRoot.empty()) {
            return false;
        }
        if (CDCPaths.contains(realPath)) {
            // streamImpl miss must not spawn FederationRoot/<...>/streamImpl retries.
            return false;
        }
        if (FederationPaths.contains(realPath)) {
            // This response is already for a FederationRoot path.
            return false;
        }
        for (const auto& [_, info] : FederationPaths) {
            if (info.OriginalPath == originalPath) {
                // FederationRoot path is already scheduled.
                return true;
            }
        }
        if (RetryWithFederation) {
            return false;
        }

        const auto relativePath = MakeFederationRelativeTopicPath(DatabasePath, originalPath, realPath);
        if (!relativePath) {
            return false;
        }

        const auto account = ExtractFederationAccount(*relativePath);
        if (!account) {
            return false;
        }

        const auto accountDatabase = MakeFederationAccountDatabase(FederationRoot, *account);
        const auto federationPath = MakeFederationTopicPath(FederationRoot, *relativePath);
        // Same path string can still need a retry with DatabaseName = account DB
        // (e.g. DatabasePath == FederationRoot: /Root/account/topic under /Root).
        if (federationPath == realPath && RequestDatabaseName == accountDatabase) {
            return false;
        }

        FederationPaths[federationPath] = TFederationTopicInfo{
            .OriginalPath = originalPath,
            .AccountDatabase = accountDatabase
        };
        return true;
    }

    // One SchemeCache request per account database (DatabaseName = FederationRoot/account).
    // Empty AccountDatabase is valid (fetch/API callers may pass Database="").
    bool TryStartNextFederationDatabaseRequest() {
        std::optional<TString> nextDatabase;
        for (const auto& [_, info] : FederationPaths) {
            if (!RequestedFederationDatabases.contains(info.AccountDatabase)) {
                nextDatabase = info.AccountDatabase;
                break;
            }
        }
        if (!nextDatabase) {
            return false;
        }

        RetryWithFederation = true;
        RetryWithSyncVersion = false;
        RequestDatabaseName = *nextDatabase;
        RequestedFederationDatabases.insert(*nextDatabase);

        absl::flat_hash_set<TString> newPath;
        for (const auto& [path, info] : FederationPaths) {
            if (info.AccountDatabase == *nextDatabase) {
                newPath.insert(path);
            }
        }

        DoRequest(newPath);
        return true;
    }

    // One SchemeCache request per account database for CDC streamImpl paths.
    // Empty AccountDatabase is valid (fetch/API callers may pass Database="").
    bool TryStartNextCdcDatabaseRequest() {
        std::optional<TString> nextDatabase;
        for (const auto& [_, info] : CDCPaths) {
            if (!RequestedCdcDatabases.contains(info.AccountDatabase)) {
                nextDatabase = info.AccountDatabase;
                break;
            }
        }
        if (!nextDatabase) {
            return false;
        }

        RetryWithCDC = true;
        RetryWithSyncVersion = false;
        RequestDatabaseName = *nextDatabase;
        RequestedCdcDatabases.insert(*nextDatabase);

        absl::flat_hash_set<TString> newPath;
        for (const auto& [path, info] : CDCPaths) {
            if (info.AccountDatabase == *nextDatabase) {
                newPath.insert(path);
            }
        }

        DoRequest(newPath);
        return true;
    }

    void SetErrorResult(const TString& originalPath, EStatus status, const TString& realPath = {}) {
        auto it = Result.find(originalPath);
        if (it != Result.end() && it->second.Status == EStatus::SUCCESS) {
            return;
        }
        Result[originalPath] = TTopicInfo{
            .Status = status,
            .RealPath = realPath
        };
    }

private:
    const NActors::TActorId Parent;
    const TString DatabasePath;
    const absl::flat_hash_set<TString> TopicPaths;
    const TDescribeSettings Settings;
    // normalized path -> original path
    absl::flat_hash_map<TString, TString> PathToOriginalPath;

    bool RetryWithSyncVersion = false;
    bool UsedSyncVersion = false;
    bool RetryWithCDC = false;
    bool RetryWithFederation = false;
    TString FederationRoot;
    // DatabaseName for the current SchemeCache request (account DB on Federation retry).
    TString RequestDatabaseName;
    absl::flat_hash_set<TString> RequestedFederationDatabases;
    absl::flat_hash_set<TString> RequestedCdcDatabases;
    // CDC streamImpl path -> original changefeed path
    struct TCDCTopicInfo {
        TString OriginalPath;
        TString CdcStreamName;
        TString AccountDatabase;
    };
    absl::flat_hash_map<TString, TCDCTopicInfo> CDCPaths;
    // FederationRoot-prefixed path -> original topic path
    struct TFederationTopicInfo {
        TString OriginalPath;
        TString AccountDatabase;
    };
    absl::flat_hash_map<TString, TFederationTopicInfo> FederationPaths;
    absl::flat_hash_map<TString, TTopicInfo> Result;
};

} // namespace

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent, const TString& databasePath, absl::flat_hash_set<TString>&& topicPaths, const TDescribeSettings& settings) {
    return new TDescribeActor(parent, databasePath, std::move(topicPaths), settings);
}


Ydb::StatusIds::StatusCode Convert(const EStatus status) {
    switch (status) {
        case EStatus::SUCCESS:
            return Ydb::StatusIds::SUCCESS;
        case EStatus::NOT_FOUND:
        case EStatus::NOT_TOPIC:
            return Ydb::StatusIds::NOT_FOUND;
        case EStatus::UNAUTHORIZED:
        case EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            return Ydb::StatusIds::UNAUTHORIZED;
        case EStatus::UNKNOWN_ERROR:
            return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

TString Description(const TString& topicPath, const EStatus status) {
    switch (status) {
        case EStatus::SUCCESS:
            return TStringBuilder() << "The topic '" << topicPath << "' has been successfully described";
        case EStatus::NOT_FOUND:
        case EStatus::UNAUTHORIZED:
            return TStringBuilder() << "You do not have access permissions or the '" << topicPath << "' does not exist";
        case EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            return TStringBuilder() << "You do not have access permissions to the '" << topicPath << "' topic";
        case EStatus::NOT_TOPIC:
            return TStringBuilder() << "The '" << topicPath << "' path is not a topic";
        case EStatus::UNKNOWN_ERROR:
            return TStringBuilder() << "Error describing the path '" << topicPath << "'";
    }
}

}
