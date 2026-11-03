#include "read_init_auth_actor.h"

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/public/utils.h>


namespace NKikimr::NGRpcProxy::V1 {


TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const NActors::TActorId& metaCache, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
        const NPersQueue::TTopicsToConverter& topics, const TString& localCluster, bool skipReadRuleCheck
)
    : TBase(NKikimrServices::PQ_READ_PROXY)
    , ParentId(parentId)
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
    LOG_D("Auth",
        {"clientId", ClientId});
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx, true);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate) {
    TVector<NPersQueue::TDiscoveryConverterPtr> topics;
    for (const auto& topic : Topics) {
        topics.push_back(topic.second.DiscoveryConverter);
        AFL_ENSURE(topic.second.DiscoveryConverter->IsValid());
    }

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
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry,
        THashMap<TString, TTopicHolder>::iterator topicsIter,
        const TActorContext& ctx
) {
    AFL_ENSURE(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    auto& pqDescr = entry.PQGroupInfo->Description;
    topicsIter->second.TabletID = pqDescr.GetBalancerTabletID();
    topicsIter->second.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    topicsIter->second.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    topicsIter->second.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    topicsIter->second.MeteringMode = pqDescr.GetPQTabletConfig().GetMeteringMode();
    topicsIter->second.DbPath = pqDescr.GetPQTabletConfig().GetYdbDatabasePath();
    topicsIter->second.IsServerless = entry.DomainInfo->IsServerless();
    topicsIter->second.SetPartitionGraph(entry.PQGroupInfo->PartitionGraph);

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
    AFL_ENSURE(topicsIter->second.FullConverter->IsValid());
    return CheckTopicACL(entry, topicsIter->first, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Handle describe topics response");

    const auto* response = ev->Get();
    if (!response->Result) {
        LOG_C("Describe topics response has no scheme cache result");
        return CloseSession("Internal error describing topics", PersQueue::ErrorCode::ERROR, ctx);
    }

    const auto& topicsRequested = response->TopicsRequested;
    const auto& resultSet = response->Result->ResultSet;
    if (topicsRequested.size() != resultSet.size()) {
        LOG_C("Describe topics response size mismatch",
            {"topics", topicsRequested.size()},
            {"results", resultSet.size()});
        return CloseSession("Internal error describing topics", PersQueue::ErrorCode::ERROR, ctx);
    }

    bool reDescribe = false;
    for (size_t i = 0; i < resultSet.size(); ++i) {
        const auto& entry = resultSet[i];
        const auto& converter = topicsRequested[i];
        if (!converter) {
            LOG_C("Describe topics response has an empty converter", {"index", i});
            return CloseSession("Internal error describing topics", PersQueue::ErrorCode::ERROR, ctx);
        }

        // GetOriginalPath dereferences the converter. A missing path used to abort the process
        // (YDBBUGS-1107); close this read session instead.
        const TString path = converter->GetOriginalPath();
        if (path.empty()) {
            LOG_C("Describe topics response has an empty topic path",
                {"index", i},
                {"topic", converter->GetOriginalTopic()},
                {"valid", converter->IsValid()},
                {"reason", converter->GetReason()});
            return CloseSession("Internal error describing topics", PersQueue::ErrorCode::ERROR, ctx);
        }

        auto it = Topics.find(path);
        if (it == Topics.end()) {
            LOG_C("Describe topics response path is not in the read session",
                {"path", path},
                {"topic", converter->GetOriginalTopic()});
            return CloseSession(TStringBuilder() << "Internal error describing topic '" << path << "'",
                PersQueue::ErrorCode::ERROR, ctx);
        }

        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
            if (!entry.ListNodeEntry || entry.ListNodeEntry->Children.size() != 1) {
                LOG_C("CDC stream describe entry has no single child topic", {"path", path});
                return CloseSession(TStringBuilder() << "Internal error describing CDC stream '" << path << "'",
                    PersQueue::ErrorCode::ERROR, ctx);
            }
            const auto& topic = entry.ListNodeEntry->Children.at(0);
            const TString childPath = JoinPath(ChildPath(entry.Path, topic.Name));
            if (childPath.empty()) {
                LOG_C("CDC stream child topic path is empty", {"path", path});
                return CloseSession(TStringBuilder() << "Internal error describing CDC stream '" << path << "'",
                    PersQueue::ErrorCode::ERROR, ctx);
            }

            // primary path used to re-describe; GetOriginalPath keeps the path this session was keyed by
            it->second.DiscoveryConverter->SetPrimaryPath(childPath);
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
    bool doCheckClientAcl = DoCheckACL && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() && !SkipReadRuleCheck;
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
            holder.GetPartitionGraph()
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

}
