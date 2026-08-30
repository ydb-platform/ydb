#include "read_init_auth_actor.h"

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/public/utils.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY


namespace NKikimr::NGRpcProxy::V1 {

namespace {

Ydb::PersQueue::ErrorCode::ErrorCode ConvertDescriberStatus(NPQ::NDescriber::EStatus status) {
    using EStatus = NPQ::NDescriber::EStatus;
    switch (status) {
        case EStatus::SUCCESS:
            return PersQueue::ErrorCode::OK;
        case EStatus::NOT_FOUND:
        case EStatus::NOT_TOPIC:
            return PersQueue::ErrorCode::UNKNOWN_TOPIC;
        case EStatus::UNAUTHORIZED:
        case EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            return PersQueue::ErrorCode::ACCESS_DENIED;
        case EStatus::BAD_REQUEST:
            return PersQueue::ErrorCode::BAD_REQUEST;
        case EStatus::UNKNOWN_ERROR:
            return PersQueue::ErrorCode::ERROR;
    }
}

} // namespace


TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const NActors::TActorId& /*schemeCache*/, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
        const THashSet<TString>& topicPaths, const TString& database, const TString& localCluster, bool skipReadRuleCheck
)
    : ParentId(parentId)
    , Cookie(cookie)
    , Session(session)
    , NewSchemeCache(newSchemeCache)
    , Database(database)
    , ClientId(clientId)
    , ClientPath(NPersQueue::ConvertOldConsumerName(ClientId, ctx))
    , SkipReadRuleCheck(skipReadRuleCheck)
    , Token(token)
    , TopicPaths(topicPaths)
    , Counters(counters)
    , LocalCluster(localCluster)
{
}


TReadInitAndAuthActor::~TReadInitAndAuthActor() = default;


void TReadInitAndAuthActor::Bootstrap(const TActorContext &ctx) {
    YDB_LOG_DEBUG_CTX(ctx, "Auth",
        {"PQLOGPREFIX", PQ_LOG_PREFIX},
        {"clientId", ClientId});
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext& ctx) {
    absl::flat_hash_set<TString> paths;
    for (const auto& path : TopicPaths) {
        paths.insert(path);
    }
    NPQ::NDescriber::TDescribeSettings settings;
    settings.UserToken = Token;
    settings.AccessRights = NPQ::NDescriber::TAccessRights(NACLib::EAccessRights::SelectRow);
    settings.LocalDc = LocalCluster;
    ctx.Register(NPQ::NDescriber::CreateDescriberActor(SelfId(), Database, std::move(paths), settings));
}

void TReadInitAndAuthActor::Die(const TActorContext& ctx) {
    for (auto& [_, holder] : Topics) {
        if (holder.PipeClient)
            NTabletPipe::CloseClient(ctx, holder.PipeClient);
    }

    YDB_LOG_DEBUG_CTX(ctx, "Auth is DEAD",
        {"PQLOGPREFIX", PQ_LOG_PREFIX});

    TActorBootstrapped<TReadInitAndAuthActor>::Die(ctx);
}

bool TReadInitAndAuthActor::OnUnhandledException(const std::exception& exc) {
    auto ctx = *NActors::TlsActivationContext;
    YDB_LOG_CRIT_CTX(ctx, "Unhandled exception",
        {"PQLOGPREFIX", PQ_LOG_PREFIX},
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
    YDB_LOG_DEBUG_CTX(ctx, "Send client acl request",
        {"PQLOGPREFIX", PQ_LOG_PREFIX});
    ctx.Send(NewSchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
}


bool TReadInitAndAuthActor::ProcessTopicInfo(
        const TString& path,
        const NPQ::NDescriber::TTopicInfo& info,
        const TActorContext& ctx
) {
    AFL_ENSURE(info.Info);
    auto& pqDescr = info.Info->Description;
    auto& holder = Topics[path];
    holder.TabletID = pqDescr.GetBalancerTabletID();
    holder.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    holder.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    holder.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    holder.MeteringMode = pqDescr.GetPQTabletConfig().GetMeteringMode();
    holder.DbPath = pqDescr.GetPQTabletConfig().GetYdbDatabasePath();
    holder.IsServerless = info.IsServerless();
    holder.SetPartitionGraph(info.Info->PartitionGraph);
    holder.FullConverter = info.Names;

    for (const auto& partitionDescription : pqDescr.GetPartitions()) {
        holder.Partitions[partitionDescription.GetPartitionId()] =
            TPartitionInfo{ partitionDescription.GetTabletId() };
    }

    if (!holder.FullConverter || !holder.FullConverter->IsValid()) {
        TString errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503",
                                      path.c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        return false;
    }
    return CheckTopicACL(info, path, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    const auto ctx = ActorContext();
    YDB_LOG_DEBUG_CTX(ctx, "Handle describe topics response",
        {"PQLOGPREFIX", PQ_LOG_PREFIX});

    Ydb::PersQueue::ErrorCode::ErrorCode lastError = PersQueue::ErrorCode::BAD_REQUEST;
    TString lastReason = "no topics found";
    for (const auto& [path, info] : ev->Get()->Topics) {
        if (info.Status != NPQ::NDescriber::EStatus::SUCCESS) {
            YDB_LOG_DEBUG_CTX(ctx, "Describe topic failed",
                {"PQLOGPREFIX", PQ_LOG_PREFIX},
                {"path", path},
                {"status", ToString(info.Status)});
            lastError = ConvertDescriberStatus(info.Status);
            lastReason = NPQ::NDescriber::Description(path, info.Status);
            continue;
        }
        if (!ProcessTopicInfo(path, info, ctx)) {
            return;
        }
    }

    if (Topics.empty()) {
        CloseSession(lastReason, lastError, ctx);
        return;
    }

    bool doCheckClientAcl = DoCheckACL && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() && !SkipReadRuleCheck;
    if (doCheckClientAcl) {
        CheckClientACL(ctx);
    } else {
        FinishInitialization(ctx);
    }
}


bool TReadInitAndAuthActor::CheckTopicACL(
        const NPQ::NDescriber::TTopicInfo& info, const TString& topic, const TActorContext& ctx
) {
    auto& pqDescr = info.Info->Description;
    if (Token && !CheckACLPermissionsForNavigate(
            info.SecurityObject, topic, NACLib::EAccessRights::SelectRow,
            "No ReadTopic permissions", ctx
    )) {
        return false;
    }
    if (!SkipReadRuleCheck && (Token || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen())) {
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
    SendCacheNavigateRequest(ctx, AppData(ctx)->PQConfig.GetRoot() + "/" + ClientPath);
}


void TReadInitAndAuthActor::HandleClientSchemeCacheResponse(
        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx
) {
    TEvTxProxySchemeCache::TEvNavigateKeySetResult* msg = ev->Get();
    const NSchemeCache::TSchemeCacheNavigate* navigate = msg->Request.Get();

    AFL_ENSURE(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();
    auto path = "/" + JoinPath(entry.Path);
    if (navigate->ErrorCount > 0) {
        const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;
        PersQueue::ErrorCode::ErrorCode errorCode = ConvertNavigateStatus(status);

        CloseSession(TStringBuilder() << "Failed to read ACL for '" << path << "' Scheme cache error : " << status,  errorCode, ctx);
        return;
    }

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
            holder.GetPartitionGraph()
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

}
