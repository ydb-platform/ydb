#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/algorithm.h>

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

TString MakeLbUserTopicPath(const TString& lbUserDatabaseRoot, const TString& topicPath) {
    auto parts = NKikimr::SplitPath(lbUserDatabaseRoot);
    for (const auto& part : NKikimr::SplitPath(topicPath)) {
        parts.push_back(part);
    }
    return CanonizePath(NKikimr::JoinPath(parts));
}

// First path component is federation account; requires account/topic shape.
TMaybe<TString> ExtractFederationAccount(const TString& topicPath) {
    auto parts = NKikimr::SplitPath(topicPath);
    if (parts.size() < 2 || parts[0].empty()) {
        return Nothing();
    }
    return parts[0];
}

TString MakeLbUserAccountDatabase(const TString& lbUserDatabaseRoot, const TString& account) {
    return CanonizePath(NKikimr::JoinPath({lbUserDatabaseRoot, account}));
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
            LbUserDatabaseRoot = AppData()->PQConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot();
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
            // Keep the originally requested path across retries (sync / LbRoot / CDC).
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
            } else if (auto lbIt = LbRootPaths.find(realPath); lbIt != LbRootPaths.end()) {
                originalPath = lbIt->second.OriginalPath;
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
                        } else if (TryScheduleLbRootRetry(originalPath, realPath)) {
                            YDB_LOG_DEBUG("Path not found, will try LbUserDatabaseRoot",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath},
                                {"originalPath", originalPath},
                                {"lbUserDatabaseRoot", LbUserDatabaseRoot});
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
                    Result[originalPath] = TTopicInfo{
                        .Status = EStatus::UNAUTHORIZED
                    };
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
                                if (TryScheduleLbRootRetry(originalPath, realPath)) {
                                    YDB_LOG_DEBUG("Path not found, will try LbUserDatabaseRoot",
                                        {"logPrefix", LOG_PREFIX},
                                        {"realPath", realPath},
                                        {"originalPath", originalPath},
                                        {"lbUserDatabaseRoot", LbUserDatabaseRoot});
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
                                .Status = EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS
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

        if (TryStartNextLbRootDatabaseRequest()) {
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
    bool TryScheduleLbRootRetry(const TString& originalPath, const TString& realPath) {
        if (LbUserDatabaseRoot.empty()) {
            return false;
        }
        if (LbRootPaths.contains(realPath)) {
            // This response is already for an LbUserDatabaseRoot path.
            return false;
        }
        for (const auto& [_, info] : LbRootPaths) {
            if (info.OriginalPath == originalPath) {
                // LbUserDatabaseRoot path is already scheduled.
                return true;
            }
        }
        if (RetryWithLbRoot) {
            return false;
        }

        const auto account = ExtractFederationAccount(originalPath);
        if (!account.Defined()) {
            return false;
        }

        const auto accountDatabase = MakeLbUserAccountDatabase(LbUserDatabaseRoot, *account);
        const auto lbPath = MakeLbUserTopicPath(LbUserDatabaseRoot, originalPath);
        // Same path string can still need a retry with DatabaseName = account DB
        // (e.g. DatabasePath == LbUserDatabaseRoot: /Root/account/topic under /Root).
        if (lbPath == realPath && RequestDatabaseName == accountDatabase) {
            return false;
        }

        LbRootPaths[lbPath] = TLbRootTopicInfo{
            .OriginalPath = originalPath,
            .AccountDatabase = accountDatabase
        };
        return true;
    }

    // One SchemeCache request per account database (DatabaseName = LbRoot/account).
    bool TryStartNextLbRootDatabaseRequest() {
        TString nextDatabase;
        for (const auto& [_, info] : LbRootPaths) {
            if (!RequestedLbRootDatabases.contains(info.AccountDatabase)) {
                nextDatabase = info.AccountDatabase;
                break;
            }
        }
        if (nextDatabase.empty()) {
            return false;
        }

        RetryWithLbRoot = true;
        RetryWithSyncVersion = false;
        RequestDatabaseName = nextDatabase;
        RequestedLbRootDatabases.insert(nextDatabase);

        absl::flat_hash_set<TString> newPath;
        for (const auto& [path, info] : LbRootPaths) {
            if (info.AccountDatabase == nextDatabase) {
                newPath.insert(path);
            }
        }

        DoRequest(newPath);
        return true;
    }

    // One SchemeCache request per account database for CDC streamImpl paths.
    bool TryStartNextCdcDatabaseRequest() {
        TString nextDatabase;
        for (const auto& [_, info] : CDCPaths) {
            if (!RequestedCdcDatabases.contains(info.AccountDatabase)) {
                nextDatabase = info.AccountDatabase;
                break;
            }
        }
        if (nextDatabase.empty()) {
            return false;
        }

        RetryWithCDC = true;
        RetryWithSyncVersion = false;
        RequestDatabaseName = nextDatabase;
        RequestedCdcDatabases.insert(nextDatabase);

        absl::flat_hash_set<TString> newPath;
        for (const auto& [path, info] : CDCPaths) {
            if (info.AccountDatabase == nextDatabase) {
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
    bool RetryWithLbRoot = false;
    TString LbUserDatabaseRoot;
    // DatabaseName for the current SchemeCache request (account DB on LbRoot retry).
    TString RequestDatabaseName;
    absl::flat_hash_set<TString> RequestedLbRootDatabases;
    absl::flat_hash_set<TString> RequestedCdcDatabases;
    // CDC streamImpl path -> original changefeed path
    struct TCDCTopicInfo {
        TString OriginalPath;
        TString CdcStreamName;
        TString AccountDatabase;
    };
    absl::flat_hash_map<TString, TCDCTopicInfo> CDCPaths;
    // LbUserDatabaseRoot-prefixed path -> original topic path
    struct TLbRootTopicInfo {
        TString OriginalPath;
        TString AccountDatabase;
    };
    absl::flat_hash_map<TString, TLbRootTopicInfo> LbRootPaths;
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
