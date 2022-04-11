#include "read_init_auth_actor.h"

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>


namespace NKikimr::NGRpcProxy::V1 {

#define PQ_LOG_PREFIX "session cookie " << Cookie << " consumer " << ClientPath << " session " << Session


TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const NActors::TActorId& metaCache, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NACLib::TUserToken> token,
        const NPersQueue::TTopicsToConverter& topics, const TString& localCluster
)
    : ParentId(parentId)
    , Cookie(cookie)
    , Session(session)
    , MetaCacheId(metaCache)
    , NewSchemeCache(newSchemeCache)
    , ClientId(clientId)
    , ClientPath(NPersQueue::ConvertOldConsumerName(ClientId, ctx))
    , Token(token)
    , Counters(counters)
    , LocalCluster(localCluster)
{
    for (const auto& [path, converter] : topics) {
        Topics[path].TopicNameConverter = converter;
    }
}


TReadInitAndAuthActor::~TReadInitAndAuthActor() = default;


void TReadInitAndAuthActor::Bootstrap(const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth for : " << ClientId);
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate) {
    TVector<TString> topicNames;
    for (const auto& [_, holder] : Topics) {
        topicNames.emplace_back(holder.TopicNameConverter->GetPrimaryPath());
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " describe topics: " << JoinSeq(", ", topicNames));
    ctx.Send(MetaCacheId, new TEvDescribeTopicsRequest(topicNames, true, showPrivate));
}

void TReadInitAndAuthActor::Die(const TActorContext& ctx) {
    for (auto& [_, holder] : Topics) {
        if (holder.PipeClient)
            NTabletPipe::CloseClient(ctx, holder.PipeClient);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth is DEAD");

    TActorBootstrapped<TReadInitAndAuthActor>::Die(ctx);
}

void TReadInitAndAuthActor::CloseSession(const TString& errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode code, const TActorContext& ctx)
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
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, THashMap<TString, TTopicHolder>::iterator topicsIter,
        const TActorContext& ctx
) {
    Y_VERIFY(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    auto& pqDescr = entry.PQGroupInfo->Description;
    topicsIter->second.TabletID = pqDescr.GetBalancerTabletID();
    topicsIter->second.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    topicsIter->second.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    topicsIter->second.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    return CheckTopicACL(entry, topicsIter->first, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Handle describe topics response");

    bool reDescribe = false;
    for (const auto& entry : ev->Get()->Result->ResultSet) {
        auto path = JoinPath(entry.Path);
        auto it = Topics.find(path);
        Y_VERIFY(it != Topics.end());

        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
            Y_VERIFY(entry.ListNodeEntry->Children.size() == 1);
            const auto& topic = entry.ListNodeEntry->Children.at(0);

            it->second.TopicNameConverter->SetPrimaryPath(JoinPath(ChildPath(entry.Path, topic.Name)));
            Topics[it->second.TopicNameConverter->GetPrimaryPath()] = it->second;
            Topics.erase(it);

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
    if (Token || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        bool found = false;
        for (auto& cons : pqDescr.GetPQTabletConfig().GetReadRules() ) {
            if (cons == ClientId) {
                found = true;
                break;
            }
        }
        if (!found) {
            CloseSession(
                    TStringBuilder() << "no read rule provided for consumer '" << ClientPath << "' in topic '" << topic << "' in current cluster '" << LocalCluster,
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

    Y_VERIFY(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();
    auto path = "/" + JoinPath(entry.Path); // ToDo [migration] - through converter ?
    if (navigate->ErrorCount > 0) {
        const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;
        CloseSession(TStringBuilder() << "Failed to read ACL for '" << path << "' Scheme cache error : " << status, PersQueue::ErrorCode::ERROR, ctx);
        return;
    }

    NACLib::EAccessRights rights = (NACLib::EAccessRights)(NACLib::EAccessRights::ReadAttributes + NACLib::EAccessRights::WriteAttributes);
    if (
            !CheckACLPermissionsForNavigate(entry.SecurityObject, path, rights, "No ReadAsConsumer permissions", ctx)
    ) {
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
    TTopicTabletsPairs res;
    for (auto& [_, holder] : Topics) {
        res.emplace_back(decltype(res)::value_type({
            holder.TopicNameConverter, holder.TabletID, holder.CloudId, holder.DbId, holder.FolderId
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

}
