#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/ptr.h>
#include <util/string/join.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_DESCRIBER

#define LOG_PREFIX NActors::TlsActivationContext->AsActorContext().SelfID

namespace NKikimr::NPQ::NDescriber {

namespace {

using namespace NSchemeCache;

TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> EnsureTopicPath(
    TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> info,
    const TString& realPath)
{
    // Same as PQ metacache CheckEntrySetHasTopicPath: tablet config from scheme cache
    // often has an empty TopicPath. UpgradeToFullConverter needs it.
    if (!info || !info->Description.HasPQTabletConfig()) {
        return info;
    }
    if (!info->Description.GetPQTabletConfig().GetTopicPath().empty() || realPath.empty()) {
        return info;
    }
    auto copy = MakeIntrusive<TSchemeCacheNavigate::TPQGroupInfo>(*info);
    copy->Description.MutablePQTabletConfig()->SetTopicPath(realPath);
    return copy;
}

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
        RetryWithSyncVersion = Settings.ForceSyncVersion;
        UsedSyncVersion = Settings.ForceSyncVersion;

        for (const auto& topic : TopicPaths) {
            auto resolved = NNameResolver::ResolveName(DatabasePath, topic);
            if (!resolved) {
                YDB_LOG_DEBUG("Name resolve failed",
                    {"logPrefix", LOG_PREFIX},
                    {"topic", topic},
                    {"reason", resolved.error()});
                SetErrorResult(topic, EStatus::BAD_REQUEST);
                continue;
            }
            YDB_LOG_DEBUG("Name resolved",
                {"logPrefix", LOG_PREFIX},
                {"topic", topic},
                {"resolvedPath", resolved->Path},
                {"navigateDatabase", resolved->NavigateDatabase});
            PathToOriginalPaths[resolved->Path].push_back(topic);
            PendingByDatabase[resolved->NavigateDatabase].insert(resolved->Path);
        }

        if (PendingByDatabase.empty()) {
            Send(Parent, new TEvDescribeTopicsResponse(std::move(Result), UsedSyncVersion));
            PassAway();
            return;
        }
        StartNextDatabaseRequest();
    }

    void DoRequest(const absl::flat_hash_set<TString>& topicPath) {
        YDB_LOG_DEBUG("Create request with",
            {"logPrefix", LOG_PREFIX},
            {"topicPaths", JoinRange(", ", topicPath.begin(), topicPath.end())},
            {"syncVersion", RetryWithSyncVersion},
            {"databaseName", RequestDatabaseName});

        auto schemeRequest = std::make_unique<TSchemeCacheNavigate>(1);
        schemeRequest->DatabaseName = RequestDatabaseName;

        for (const auto& topic : topicPath) {
            auto split = NKikimr::SplitPath(topic);
            schemeRequest->ResultSet.emplace_back();
            auto& entry = schemeRequest->ResultSet.back();
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            entry.Operation = TSchemeCacheNavigate::OpList;
            entry.SyncVersion = RetryWithSyncVersion;
            entry.ShowPrivatePath = true;
        }

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()),
             0, 0, Settings.TraceId.Clone());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"logPrefix", LOG_PREFIX});
        auto& result = ev->Get()->Request;

        absl::flat_hash_set<TString> unknownPaths;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            const auto& entry = result->ResultSet[i];
            auto realPath = CanonizePath(NKikimr::JoinPath(entry.Path));
            const auto& originals = OriginalsFor(realPath);
            Y_ASSERT(!originals.empty());

            bool isCDCStream = false;
            TString cdcStreamName;

            if (auto it = CDCPaths.find(realPath); it != CDCPaths.end()) {
                isCDCStream = true;
                cdcStreamName = it->second.CdcStreamName;
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

                            SetErrorResults(originals, EStatus::UNAUTHORIZED);
                        } else {
                            YDB_LOG_DEBUG("Path not found",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});

                            SetErrorResults(originals, EStatus::NOT_FOUND);
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
                    SetErrorResults(originals, EStatus::UNAUTHORIZED);
                    break;
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        YDB_LOG_DEBUG("Path is CDC",
                            {"logPrefix", LOG_PREFIX},
                            {"realPath", realPath});

                        // Copy before mutating PathToOriginalPaths (rehash must not invalidate originals).
                        TVector<TString> originalsCopy = originals;
                        const TString streamImplPath = TStringBuilder() << realPath << "/streamImpl";
                        PathToOriginalPaths[streamImplPath] = std::move(originalsCopy);
                        CDCPaths[streamImplPath] = {
                            .CdcStreamName = entry.Self->Info.GetName(),
                            .AccountDatabase = RequestDatabaseName
                        };
                        break;
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        if (!entry.PQGroupInfo || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                            if (RetryWithSyncVersion) {
                                YDB_LOG_DEBUG("Path not found",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});
                                SetErrorResults(originals, EStatus::NOT_FOUND);
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (!HasAccess(Settings, entry.SecurityObject)) {
                                YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});

                                SetTopicResults(originals, TTopicInfo{
                                    .Status = entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)
                                            ? EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS : EStatus::UNAUTHORIZED
                                });
                            } else {
                                YDB_LOG_DEBUG("Path SUCCESS",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});
                                SetTopicResults(originals, TTopicInfo{
                                    .Status = EStatus::SUCCESS,
                                    .RealPath = realPath,
                                    .CdcStream = isCDCStream,
                                    .CdcStreamName = cdcStreamName,
                                    .CreateStep = entry.CreateStep,
                                    .Info = EnsureTopicPath(entry.PQGroupInfo, realPath),
                                    .Self = entry.Self,
                                    .SecurityObject = entry.SecurityObject,
                                    .IsServerless = entry.DomainInfo && entry.DomainInfo->IsServerless(),
                                });
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
                            SetTopicResults(originals, TTopicInfo{
                                .Status = EStatus::UNAUTHORIZED
                            });
                        } else {
                            SetTopicResults(originals, TTopicInfo{
                                .Status = EStatus::NOT_TOPIC,
                                .RealPath = realPath
                            });
                        }
                    }
                    break;
                }
                default: {
                    YDB_LOG_DEBUG("Path unknown error",
                        {"logPrefix", LOG_PREFIX},
                        {"realPath", realPath});
                    SetTopicResults(originals, TTopicInfo{
                        .Status = EStatus::UNKNOWN_ERROR,
                        .RealPath = realPath
                    });
                    break;
                }
            }
        }

        if (!unknownPaths.empty()) {
            RetryWithSyncVersion = true;
            UsedSyncVersion = true;
            return DoRequest(unknownPaths);
        }

        if (StartNextDatabaseRequest()) {
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
    const TVector<TString>& OriginalsFor(const TString& realPath) const {
        auto it = PathToOriginalPaths.find(realPath);
        AFL_ENSURE(it != PathToOriginalPaths.end())("realPath", realPath);
        return it->second;
    }

    // One SchemeCache request per NavigateDatabase from ResolveName.
    bool StartNextDatabaseRequest() {
        for (auto& [database, paths] : PendingByDatabase) {
            if (RequestedDatabases.contains(database)) {
                continue;
            }
            // PendingByDatabase only stores non-empty path sets.
            RetryWithSyncVersion = Settings.ForceSyncVersion;
            RequestDatabaseName = database;
            RequestedDatabases.insert(database);
            DoRequest(paths);
            return true;
        }
        return false;
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
        Result[originalPath] = TTopicInfo{
            .Status = status,
            .RealPath = realPath
        };
    }

    void SetErrorResults(const TVector<TString>& originals, EStatus status, const TString& realPath = {}) {
        for (const auto& originalPath : originals) {
            SetErrorResult(originalPath, status, realPath);
        }
    }

    void SetTopicResults(const TVector<TString>& originals, const TTopicInfo& info) {
        for (const auto& originalPath : originals) {
            Result[originalPath] = info;
        }
    }

private:
    const NActors::TActorId Parent;
    const TString DatabasePath;
    const absl::flat_hash_set<TString> TopicPaths;
    const TDescribeSettings Settings;
    // navigate path -> originally requested client path(s)
    absl::flat_hash_map<TString, TVector<TString>> PathToOriginalPaths;
    // SchemeCache DatabaseName -> resolved paths (from ResolveName.NavigateDatabase)
    absl::flat_hash_map<TString, absl::flat_hash_set<TString>> PendingByDatabase;

    bool RetryWithSyncVersion = false;
    bool UsedSyncVersion = false;
    TString RequestDatabaseName;
    absl::flat_hash_set<TString> RequestedDatabases;
    absl::flat_hash_set<TString> RequestedCdcDatabases;
    // CDC streamImpl path metadata (originals live in PathToOriginalPaths)
    struct TCDCTopicInfo {
        TString CdcStreamName;
        TString AccountDatabase;
    };
    absl::flat_hash_map<TString, TCDCTopicInfo> CDCPaths;
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
        case EStatus::BAD_REQUEST:
            return Ydb::StatusIds::BAD_REQUEST;
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
        case EStatus::BAD_REQUEST:
            return TStringBuilder() << "Invalid topic name '" << topicPath << "'";
        case EStatus::UNKNOWN_ERROR:
            return TStringBuilder() << "Error describing the path '" << topicPath << "'";
    }
}

}
