#include "read_init_auth_actor.h"

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/utils.h>


namespace NKikimr::NGRpcProxy::V1 {


TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const NActors::TActorId& metaCache, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
        const NPersQueue::TTopicsToConverter& topics, const TString& localCluster, bool skipReadRuleCheck
)
    : ParentId(parentId)
    , Cookie(cookie)
    , Session(session)
    , MetaCacheId(metaCache)
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
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth for : " << ClientId);
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx, true);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate) {
    TVector<NPersQueue::TDiscoveryConverterPtr> topics;
    for (const auto& topic : Topics) {
        topics.push_back(topic.second.DiscoveryConverter);
        Y_ABORT_UNLESS(topic.second.DiscoveryConverter->IsValid());
    }

    //LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " describe topics: " << JoinSeq(", ", topicNames));
    ctx.Send(MetaCacheId, new TEvDescribeTopicsRequest(topics, true, showPrivate));
}

void TReadInitAndAuthActor::Die(const TActorContext& ctx) {
    for (auto& [_, holder] : Topics) {
        if (holder.PipeClient)
            NTabletPipe::CloseClient(ctx, holder.PipeClient);

        // In case of cdc, primary path (actual cdc stream path) was overwritten, so restore previous value
        if (holder.CdcStreamPath)
            holder.DiscoveryConverter->RestorePrimaryPath();
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth is DEAD");

    TActorBootstrapped<TReadInitAndAuthActor>::Die(ctx);
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
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Send client acl request");
    ctx.Send(NewSchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
}


bool TReadInitAndAuthActor::ProcessTopicSchemeCacheResponse(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry,
        THashMap<TString, TTopicHolder>::iterator topicsIter,
        const TActorContext& ctx
) {
    Y_ABORT_UNLESS(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    auto& pqDescr = entry.PQGroupInfo->Description;
    topicsIter->second.TabletID = pqDescr.GetBalancerTabletID();
    topicsIter->second.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    topicsIter->second.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    topicsIter->second.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    topicsIter->second.MeteringMode = pqDescr.GetPQTabletConfig().GetMeteringMode();
    topicsIter->second.DbPath = pqDescr.GetPQTabletConfig().GetYdbDatabasePath();
    topicsIter->second.IsServerless = entry.DomainInfo->IsServerless();

    NPQ::TPartitionGraph graph = NPQ::MakePartitionGraph(pqDescr);

    for (const auto& partitionDescription : pqDescr.GetPartitions()) {
        topicsIter->second.Partitions[partitionDescription.GetPartitionId()] =
            TPartitionInfo{ partitionDescription.GetTabletId() };
    }

    if (!topicsIter->second.DiscoveryConverter->IsValid()) {
        TString errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503",
                                      topicsIter->second.DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        return false;
    }
    topicsIter->second.FullConverter = topicsIter->second.DiscoveryConverter->UpgradeToFullConverter(
        pqDescr.GetPQTabletConfig(),
        AppData(ctx)->PQConfig.GetTestDatabaseRoot(),
        topicsIter->second.CdcStreamPath
    );
    Y_ABORT_UNLESS(topicsIter->second.FullConverter->IsValid());
    return CheckTopicACL(entry, topicsIter->first, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Handle describe topics response");

    bool reDescribe = false;
    auto i = 0u;
    auto& topicsRequested = ev->Get()->TopicsRequested;
    for (const auto& entry : ev->Get()->Result->ResultSet) {
        const auto& path = topicsRequested[i++]->GetOriginalPath();
        auto it = Topics.find(path);
        Y_ABORT_UNLESS(it != Topics.end());

        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
            Y_ABORT_UNLESS(entry.ListNodeEntry->Children.size() == 1);
            const auto& topic = entry.ListNodeEntry->Children.at(0);

            // primary path used to re-describe
            it->second.DiscoveryConverter->SetPrimaryPath(JoinPath(ChildPath(entry.Path, topic.Name)));
            it->second.CdcStreamPath = CanonizePath(entry.Path);

//            Topics[it->second.DiscoveryConverter->GetInternalName()] = it->second;
//            Topics.erase(it);

            reDescribe = true;
            continue;
        }

        auto processResult = ProcessMetaCacheTopicResponse(entry);
        if (processResult.IsFatal) {
            Topics.erase(it);
            if (Topics.empty()) {
                TStringBuilder reason;
                reason << "Discovery for all topics failed. The last error was: " << processResult.Reason;
                return CloseSession(reason, processResult.ErrorCode, ctx);
            } else {
                continue;
            }
        }

        if (!ProcessTopicSchemeCacheResponse(entry, it, ctx)) {
            return;
        }
    }

    if (Topics.empty()) {
        CloseSession("no topics found", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (reDescribe) {
        return DescribeTopics(ctx, true);
    }

    // ToDo[migration] - separate option - ?
    bool doCheckClientAcl = DoCheckACL && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen();
    if (doCheckClientAcl) {
        CheckClientACL(ctx);
    } else {
        FinishInitialization(ctx);
    }
}


bool TReadInitAndAuthActor::CheckTopicACL(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& topic, const TActorContext& ctx
) {
    auto& pqDescr = entry.PQGroupInfo->Description;
    //ToDo[migration] - proper auth setup
    if (Token && !CheckACLPermissionsForNavigate(
            entry.SecurityObject, topic, NACLib::EAccessRights::SelectRow,
            "No ReadTopic permissions", ctx
    )) {
        return false;
    }
    if (!SkipReadRuleCheck && (Token || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen())) {
        //TODO : add here checking of client-service-type password. Provide it via API-call.
        if (!NPQ::HasConsumer(pqDescr.GetPQTabletConfig(), ClientId)) {
            CloseSession(
                    TStringBuilder() << "no read rule provided for consumer '" << ClientPath << "' in topic '" << topic << "' in current cluster '" << LocalCluster << "'",
                    PersQueue::ErrorCode::BAD_REQUEST, ctx
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

    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();
    auto path = "/" + JoinPath(entry.Path); // ToDo [migration] - through converter ?
    if (navigate->ErrorCount > 0) {
        const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;

        PersQueue::ErrorCode::ErrorCode errorCode;
        switch(status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                errorCode = PersQueue::ErrorCode::ERROR;
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                errorCode = PersQueue::ErrorCode::UNKNOWN_TOPIC;
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                errorCode = PersQueue::ErrorCode::ACCESS_DENIED;
                break;
        }

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
            holder.FullConverter, holder.TabletID, holder.CloudId, holder.DbId, holder.DbPath, holder.IsServerless, holder.FolderId, holder.MeteringMode, holder.Partitions
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

}
