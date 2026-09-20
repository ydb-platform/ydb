#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/string/join.h>

#include <optional>

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

class TDescribeActor : public TBaseActor<TDescribeActor>
                      , public TConstantLogPrefix {
public:
    TDescribeActor(const NActors::TActorId& parent, const TString& databasePath, absl::flat_hash_set<TString>&& topicPaths, const TDescribeSettings& settings)
        : TBaseActor(NKikimrServices::PQ_DESCRIBER)
        , Parent(parent)
        , DatabasePath(databasePath)
        , TopicPaths(std::move(topicPaths))
        , Settings(settings)
    {
    }

    TStructuredMessage BuildLogPrefix() const override {
        return {};
    }

    void Bootstrap() {
        Become(&TDescribeActor::StateWork);
        RetryWithSyncVersion = Settings.ForceSyncVersion;
        UsedSyncVersion = Settings.ForceSyncVersion;

        for (const auto& topic : TopicPaths) {
            auto resolved = NNameResolver::ResolveName(DatabasePath, topic);
            if (!resolved) {
                LOG_D("Name resolve failed",
                    {"topic", topic},
                    {"reason", resolved.error()});
                SetErrorResult(topic, EStatus::BadRequest);
                continue;
            }
            LOG_D("Name resolved",
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
        LOG_D("Create request with",
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

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult");
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
                            LOG_D("Path UNAUTHORIZED",
                                {"realPath", realPath});

                            SetErrorResults(originals, EStatus::Unauthorized);
                        } else {
                            LOG_D("Path not found",
                                {"realPath", realPath});

                            SetErrorResults(originals, EStatus::NotFound);
                        }
                    } else {
                        unknownPaths.insert(realPath);
                    }
                    break;
                }
                case TSchemeCacheNavigate::EStatus::AccessDenied: {
                    LOG_D("Path ACCESS DENIED",
                        {"realPath", realPath});
                    SetErrorResults(originals, EStatus::Unauthorized);
                    break;
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        LOG_D("Path is CDC",
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
                                LOG_D("Path not found",
                                    {"realPath", realPath});
                                SetErrorResults(originals, EStatus::NotFound);
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (!HasAccess(Settings, entry.SecurityObject)) {
                                LOG_D("Path UNAUTHORIZED",
                                    {"realPath", realPath});

                                SetTopicResults(originals, TTopicInfo{
                                    .Status = entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)
                                            ? EStatus::UnauthorizedWithDescribeAccess : EStatus::Unauthorized
                                });
                            } else {
                                LOG_D("Path SUCCESS",
                                    {"realPath", realPath});
                                SetTopicResults(originals, TTopicInfo{
                                    .Status = EStatus::Success,
                                    .RealPath = realPath,
                                    .CdcStream = isCDCStream,
                                    .CdcStreamName = cdcStreamName,
                                    .CreateStep = entry.CreateStep,
                                    .Info = entry.PQGroupInfo,
                                    .Self = entry.Self,
                                    .SecurityObject = entry.SecurityObject
                                });
                            }
                        }
                    } else {
                        LOG_D("Path is not a",
                            {"realPath", realPath},
                            {"topic", entry.Kind});
                        if (Settings.UserToken && !entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)) {
                            LOG_D("Path UNAUTHORIZED",
                                {"realPath", realPath});
                            SetTopicResults(originals, TTopicInfo{
                                .Status = EStatus::Unauthorized
                            });
                        } else {
                            SetTopicResults(originals, TTopicInfo{
                                .Status = EStatus::NotTopic,
                                .RealPath = realPath
                            });
                        }
                    }
                    break;
                }
                default: {
                    LOG_D("Path unknown error",
                        {"realPath", realPath});
                    SetTopicResults(originals, TTopicInfo{
                        .Status = EStatus::UnknownError,
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
        case EStatus::Success:
            return Ydb::StatusIds::SUCCESS;
        case EStatus::NotFound:
        case EStatus::NotTopic:
            return Ydb::StatusIds::NOT_FOUND;
        case EStatus::Unauthorized:
        case EStatus::UnauthorizedWithDescribeAccess:
            return Ydb::StatusIds::UNAUTHORIZED;
        case EStatus::BadRequest:
            return Ydb::StatusIds::BAD_REQUEST;
        case EStatus::UnknownError:
            return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

TString Description(const TString& topicPath, const EStatus status) {
    switch (status) {
        case EStatus::Success:
            return TStringBuilder() << "The topic '" << topicPath << "' has been successfully described";
        case EStatus::NotFound:
        case EStatus::Unauthorized:
            return TStringBuilder() << "You do not have access permissions or the '" << topicPath << "' does not exist";
        case EStatus::UnauthorizedWithDescribeAccess:
            return TStringBuilder() << "You do not have access permissions to the '" << topicPath << "' topic";
        case EStatus::NotTopic:
            return TStringBuilder() << "The '" << topicPath << "' path is not a topic";
        case EStatus::BadRequest:
            return TStringBuilder() << "Invalid topic name '" << topicPath << "'";
        case EStatus::UnknownError:
            return TStringBuilder() << "Error describing the path '" << topicPath << "'";
    }
}

}
