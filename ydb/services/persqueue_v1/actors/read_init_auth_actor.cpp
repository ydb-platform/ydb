#include "read_init_auth_actor.h"

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/containers/absl/flat_hash_set.h>

#include <fmt/format.h>

#include <string_view>


namespace NKikimr::NGRpcProxy::V1 {

namespace {

TProcessingResult DescribeFailure(const TString& path, NPQ::NDescriber::EStatus status) {
    const std::string_view topic = path;
    switch (status) {
        case NPQ::NDescriber::EStatus::NotFound:
            return {
                Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC,
                TString(fmt::format("no path '{}', Marker# PQ15", topic)),
                true
            };
        case NPQ::NDescriber::EStatus::NotTopic:
            return {
                Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC,
                TString(fmt::format("item '{}' is not a topic, Marker# PQ13", topic)),
                true
            };
        case NPQ::NDescriber::EStatus::Unauthorized:
        case NPQ::NDescriber::EStatus::UnauthorizedWithDescribeAccess:
            return {
                Ydb::PersQueue::ErrorCode::ERROR,
                TString(fmt::format("topic '{}' describe error, Status# AccessDenied, Marker# PQ1", topic)),
                true
            };
        default:
            return {
                Ydb::PersQueue::ErrorCode::ERROR,
                TString(fmt::format("topic '{}' describe error, Status# Unknown, Marker# PQ1", topic)),
                true
            };
    }
}

} // namespace


TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const TString& database, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
        const NPersQueue::TTopicsToConverter& topics, const TString& localCluster, bool skipReadRuleCheck
)
    : TBase(NKikimrServices::PQ_READ_PROXY)
    , ParentId(parentId)
    , Cookie(cookie)
    , Session(session)
    , Database(database)
    , NewSchemeCache(newSchemeCache)
    , ClientId(clientId)
    , ClientPath(NPersQueue::ConvertOldConsumerName(ClientId, ctx))
    , SkipReadRuleCheck(skipReadRuleCheck)
    , Token(token)
    , Counters(counters)
    , LocalCluster(localCluster)
{
    for (const auto& [path, converter] : topics.Topics) {
        Topics[path].DiscoveryConverter = converter;
    }
}


TReadInitAndAuthActor::~TReadInitAndAuthActor() = default;


void TReadInitAndAuthActor::Bootstrap(const TActorContext &ctx) {
    LOG_D("Auth",
        {"clientId", ClientId});
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext&) {
    absl::flat_hash_set<TString> topics;
    for (const auto& topic : Topics) {
        AFL_ENSURE(topic.second.DiscoveryConverter->IsValid());
        topics.insert(topic.second.DiscoveryConverter->GetPrimaryPath());
    }
    DescribePaths(std::move(topics));
}

void TReadInitAndAuthActor::DescribePaths(absl::flat_hash_set<TString> topics) {
    // Paths come from the discovery converter (primary, then modern secondary).
    // ResolveName would rewrite a legacy primary path under LbUserDatabaseRoot
    // and drop the dc baked into -mirrored-from-.
    NPQ::NDescriber::TDescribeSettings settings;
    settings.ResolvedPaths = true;
    DescriberId = RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(
        SelfId(), Database, std::move(topics), settings));
}

void TReadInitAndAuthActor::Die(const TActorContext& ctx) {
    if (DescriberId) {
        ctx.Send(DescriberId, new TEvents::TEvPoison());
        DescriberId = {};
    }
    for (auto& [_, holder] : Topics) {
        if (holder.PipeClient)
            NTabletPipe::CloseClient(ctx, holder.PipeClient);
    }

    LOG_D("Auth is DEAD");

    TBase::Die(ctx);
}

bool TReadInitAndAuthActor::OnUnhandledException(const std::exception& exc) {
    auto ctx = *NActors::TlsActivationContext;
    LOG_C("Unhandled exception",
        {"typeName", TypeName(exc)},
        {"exception", exc.what()},
        {"backTrace", TBackTrace::FromCurrentException().PrintToString()});

    CloseSession("Internal error", PersQueue::ErrorCode::ErrorCode::ERROR, ctx.AsActorContext());

    return true;
}

void TReadInitAndAuthActor::CloseSession(const TString& errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode code,
                                         const TActorContext& ctx)
{
    ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(errorReason, code));
    Die(ctx);
}

void TReadInitAndAuthActor::SendCacheNavigateRequest(const TActorContext& ctx, const TString& path) {
    auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(path);
    entry.SyncVersion = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    schemeCacheRequest->ResultSet.emplace_back(entry);
    schemeCacheRequest->DatabaseName = AppData(ctx)->PQConfig.GetDatabase();
    LOG_D("Send client acl request");
    ctx.Send(NewSchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
}


bool TReadInitAndAuthActor::ProcessTopicSchemeCacheResponse(
        const NPQ::NDescriber::TTopicInfo& topicInfo,
        THashMap<TString, TTopicHolder>::iterator topicsIter,
        const TActorContext& ctx
) {
    AFL_ENSURE(topicInfo.Info);
    // Scheme board describe leaves TopicPath empty. Metacache filled it from the
    // navigate path before UpgradeToFullConverter; do the same here.
    auto pqInfo = topicInfo.Info;
    if (pqInfo->Description.GetPQTabletConfig().GetTopicPath().empty()) {
        auto copy = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>(*pqInfo);
        copy->Description.MutablePQTabletConfig()->SetTopicPath(
            topicInfo.RealPath.empty() ? ("/" + topicsIter->first) : topicInfo.RealPath);
        pqInfo = copy;
    }
    auto& pqDescr = pqInfo->Description;
    topicsIter->second.TabletID = pqDescr.GetBalancerTabletID();
    topicsIter->second.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    topicsIter->second.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    topicsIter->second.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    topicsIter->second.MeteringMode = pqDescr.GetPQTabletConfig().GetMeteringMode();
    topicsIter->second.DbPath = pqDescr.GetPQTabletConfig().GetYdbDatabasePath();
    topicsIter->second.IsServerless = topicInfo.IsServerless;
    topicsIter->second.SetPartitionGraph(pqInfo->PartitionGraph);
    if (topicInfo.CdcStream) {
        topicsIter->second.CdcStreamPath = CanonizePath(topicsIter->first);
        topicsIter->second.IsAsyncReplication = topicInfo.IsAsyncReplication;
    }
    const auto* consumer = NPQ::GetConsumer(pqDescr.GetPQTabletConfig(), ClientId);
    topicsIter->second.IsReplicationConsumer = consumer
        && consumer->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION;

    for (const auto& partitionDescription : pqDescr.GetPartitions()) {
        topicsIter->second.Partitions[partitionDescription.GetPartitionId()] =
            TPartitionInfo{ partitionDescription.GetTabletId() };
    }

    if (!topicsIter->second.DiscoveryConverter->IsValid()) {
        const TString printable = topicsIter->second.DiscoveryConverter->GetPrintableString();
        TString errorReason(fmt::format("Internal server error with topic '{}', Marker# PQ503", std::string_view(printable)));
        CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        return false;
    }
    topicsIter->second.FullConverter = topicsIter->second.DiscoveryConverter->UpgradeToFullConverter(
        pqDescr.GetPQTabletConfig(),
        AppData(ctx)->PQConfig.GetTestDatabaseRoot(),
        topicsIter->second.CdcStreamPath
    );
    AFL_ENSURE(topicsIter->second.FullConverter->IsValid());
    return CheckTopicACL(topicInfo, topicsIter->first, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Handle describe topics response");
    DescriberId = {};
    const bool secondaryPass = SecondaryDescribe;
    SecondaryDescribe = false;

    absl::flat_hash_set<TString> secondaryPaths;
    const TString lbRoot = AppData(ctx)->PQConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot();

    for (const auto& [path, topicInfo] : ev->Get()->Topics) {
        TVector<TString> originals{path};
        if (secondaryPass) {
            auto mapped = SecondaryToOriginal.find(path);
            AFL_ENSURE(mapped != SecondaryToOriginal.end())("path", path);
            originals = mapped->second;
        }

        for (const auto& original : originals) {
            auto it = Topics.find(original);
            AFL_ENSURE(it != Topics.end())("path", original);

            if (topicInfo.Status == NPQ::NDescriber::EStatus::Success) {
                if (!ProcessTopicSchemeCacheResponse(topicInfo, it, ctx)) {
                    return;
                }
                continue;
            }

            // Metacache looks up the legacy primary path first and, on a miss,
            // retries the modern path under LbUserDatabaseRoot/account.
            if (!secondaryPass && topicInfo.Status == NPQ::NDescriber::EStatus::NotFound) {
                // The input converter is shared with the caller and must not be mutated.
                auto converter = std::make_shared<NPersQueue::TDiscoveryConverter>(*it->second.DiscoveryConverter);
                if (auto secondary = converter->TrySecondaryDiscoveryPath(lbRoot); secondary && *secondary != original) {
                    it->second.DiscoveryConverter = std::move(converter);
                    SecondaryToOriginal[*secondary].push_back(original);
                    secondaryPaths.insert(*secondary);
                    continue;
                }
            }

            auto processResult = DescribeFailure(path, topicInfo.Status);
            Topics.erase(it);
            if (Topics.empty() && secondaryPaths.empty()) {
                TStringBuilder reason;
                reason << "Discovery for all topics failed. The last error was: " << processResult.Reason;
                return CloseSession(reason, processResult.ErrorCode, ctx);
            }
        }
    }

    if (!secondaryPaths.empty()) {
        SecondaryDescribe = true;
        DescribePaths(std::move(secondaryPaths));
        return;
    }
    SecondaryToOriginal.clear();

    if (Topics.empty()) {
        CloseSession("no topics found", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    // ToDo[migration] - separate option - ?
    bool doCheckClientAcl = DoCheckACL && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() && !SkipReadRuleCheck;
    if (doCheckClientAcl) {
        CheckClientACL(ctx);
    } else {
        FinishInitialization(ctx);
    }
}


bool TReadInitAndAuthActor::CheckTopicACL(
        const NPQ::NDescriber::TTopicInfo& topicInfo, const TString& topic, const TActorContext& ctx
) {
    auto& pqDescr = topicInfo.Info->Description;
    //ToDo[migration] - proper auth setup
    if (Token && !CheckACLPermissionsForNavigate(
            topicInfo.SecurityObject, topic, NACLib::EAccessRights::SelectRow,
            "No ReadTopic permissions", ctx
    )) {
        return false;
    }
    if (!SkipReadRuleCheck && (Token || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen())) {
        //TODO : add here checking of client-service-type password. Provide it via API-call.
        const auto* consumer = NPQ::GetConsumer(pqDescr.GetPQTabletConfig(), ClientId);
        if (!consumer || consumer->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            CloseSession(
                    TStringBuilder() << "no read rule provided for consumer '" << ClientPath << "' in topic '" << topic << "' in current cluster '" << LocalCluster << "'",
                    PersQueue::ErrorCode::UNKNOWN_READ_RULE, ctx
            );
            return false;
        }
    }
    return true;
}


void TReadInitAndAuthActor::CheckClientACL(const TActorContext& ctx) {
    // ToDo[migration] - Through converter/metacache - ?
    SendCacheNavigateRequest(ctx, AppData(ctx)->PQConfig.GetRoot() + "/" + ClientPath);
}


void TReadInitAndAuthActor::HandleClientSchemeCacheResponse(
        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx
) {
    TEvTxProxySchemeCache::TEvNavigateKeySetResult* msg = ev->Get();
    const NSchemeCache::TSchemeCacheNavigate* navigate = msg->Request.Get();

    AFL_ENSURE(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();
    auto path = "/" + JoinPath(entry.Path); // ToDo [migration] - through converter ?
    if (navigate->ErrorCount > 0) {
        const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;
        PersQueue::ErrorCode::ErrorCode errorCode = ConvertNavigateStatus(status);

        CloseSession(TStringBuilder() << "Failed to read ACL for '" << path << "' Scheme cache error : " << status,  errorCode, ctx);
        return;
    }

    // in future use right UseConsumer
    auto selectRowRights = NACLib::EAccessRights::SelectRow;
    auto accessAttributesRights = NACLib::EAccessRights::ReadAttributes | NACLib::EAccessRights::WriteAttributes;
    if (DoCheckACL && !(entry.SecurityObject->CheckAccess(selectRowRights, *Token) || entry.SecurityObject->CheckAccess(accessAttributesRights, *Token))) {
        CloseSession(TStringBuilder() << "No ReadAsConsumer permissions" << " for '" << path
                    << "' for subject '" << Token->GetUserSID() << "'",
                    PersQueue::ErrorCode::ACCESS_DENIED, ctx);
        return;
    }
    FinishInitialization(ctx);
}


bool TReadInitAndAuthActor::CheckACLPermissionsForNavigate(
        const TIntrusivePtr<TSecurityObject>& secObject, const TString& path,
        NACLib::EAccessRights rights, const TString& errorTextWhenAccessDenied, const TActorContext& ctx
) {
    // TODO: SCHEME_ERROR если нет топика/консумера
    // TODO: если AccessDenied на корень, то надо ACCESS_DENIED, а не SCHEME_ERROR

    if (DoCheckACL && !secObject->CheckAccess(rights, *Token)) {
        CloseSession(
                TStringBuilder() << errorTextWhenAccessDenied << " for '" << path
                                 << "' for subject '" << Token->GetUserSID() << "'",
                PersQueue::ErrorCode::ACCESS_DENIED, ctx
        );
        return false;
    }
    return true;
}


void TReadInitAndAuthActor::FinishInitialization(const TActorContext& ctx) {
    TTopicInitInfoMap res;
    for (auto& [name, holder] : Topics) {
        res.insert(std::make_pair(name, TTopicInitInfo{
            holder.FullConverter,
            holder.TabletID,
            holder.CloudId,
            holder.DbId,
            holder.DbPath,
            holder.IsServerless,
            holder.FolderId,
            holder.MeteringMode,
            holder.Partitions,
            holder.GetPartitionGraph(),
            holder.IsAsyncReplication,
            holder.IsReplicationConsumer
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

}
