#include "msgbus_tabletreq.h"

#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_metacache.h"
#include "msgbus_server_pq_metarequest.h"
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/generic/is_in.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NSchemeCache;
using namespace NPqMetaCacheV2;

const TDuration TPersQueueBaseRequestProcessor::TIMEOUT = TDuration::MilliSeconds(90000);

namespace {
    const ui32 DefaultTimeout = 90000;
    const TDuration CHECK_INFLY_SEMAPHORE_DURATION = TDuration::Seconds(1);
    const ui32 MAX_INFLY = 100000;
}

static TAtomic Infly = 0;

const TString& TopicPrefix(const TActorContext& ctx) {
    static const TString topicPrefix = AppData(ctx)->PQConfig.GetRoot() + "/";
    return topicPrefix;
}

TProcessingResult ProcessMetaCacheAllTopicsResponse(TEvPqNewMetaCache::TEvDescribeAllTopicsResponse::TPtr& ev) {
    auto& res = ev->Get()->Result;
    const TString& path = ev->Get()->Path;
    TProcessingResult result;
    if (!ev->Get()->Success) {
        return TProcessingResult{
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("path '%s' has invalid/unknown root prefix, Marker# PQ14", path.c_str()),
                true
        };
    }
    if (!res) {
        return TProcessingResult{
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::ERROR,
                Sprintf("path '%s' describe error, Status# no status, reason: no reason, Marker# PQ1", path.c_str()),
                true
        };
    }
    return {};
}

TProcessingResult ProcessMetaCacheSingleTopicsResponse(
        const TSchemeCacheNavigate::TEntry& entry
) {
    auto fullPath = JoinPath(entry.Path);
    switch (entry.Status) {
        case TSchemeCacheNavigate::EStatus::RootUnknown : {
            return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("path '%s' has unknown/invalid root prefix '%s', Marker# PQ14",
                        fullPath.c_str(), entry.Path.empty() ? "" :entry.Path[0].c_str()),
                true
            };
        }
        case TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
            return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("no path '%s', Marker# PQ150", fullPath.c_str()),
                true
            };
        }
        case TSchemeCacheNavigate::EStatus::Ok:
            break;
        default: {
            return TProcessingResult {
                    MSTATUS_ERROR,
                    NPersQueue::NErrorCode::ERROR,
                    Sprintf("topic '%s' describe error, Status# %s, Marker# PQ1",
                            fullPath.c_str(), ToString(entry.Status).c_str()),
                    true
            };
        }
    }

    if (entry.Kind != TSchemeCacheNavigate::KindTopic) {
        return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("item '%s' is not a topic, Marker# PQ13", fullPath.c_str()),
                true
        };
    }
    if (!entry.PQGroupInfo) {
        return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("topic '%s' describe error, reason: could not retrieve topic description, Marker# PQ2",
                        fullPath.c_str()),
                true
        };
    }
    auto topicName = entry.PQGroupInfo->Description.GetName();
    if (topicName.empty()) {
        topicName = entry.Path.back();
    }
    if (!entry.PQGroupInfo->Description.HasBalancerTabletID()) {
        return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("topic '%s' has no balancer, Marker# PQ193", topicName.c_str()),
                true
        };
    }
    if (entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
        return TProcessingResult {
                MSTATUS_ERROR,
                NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                Sprintf("topic '%s' is not created, Marker# PQ94", topicName.c_str()),
                true
        };
    }
    return {};
}

NKikimrClient::TResponse CreateErrorReply(EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
    NKikimrClient::TResponse rec;
    rec.SetStatus(status);
    rec.SetErrorCode(code);
    rec.SetErrorReason(errorReason);
    return rec;
}

struct TTopicInfo {
    TVector<ui64> Tablets;
    THashMap<ui32, ui64> PartitionToTablet;
    ui64 BalancerTabletId = 0;

    THolder<NKikimrPQ::TReadSessionsInfoResponse> ReadSessionsInfo;

    NKikimrPQ::TPQTabletConfig Config;
    TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> PQInfo;
    NPersQueue::TDiscoveryConverterPtr Converter;
    ui32 NumParts = 0;
    THashSet<ui32> PartitionsToRequest;

    //fetchRequest part
    THashMap<ui32, TAutoPtr<TEvPersQueue::TEvHasDataInfo>> FetchInfo;
};

struct TTabletInfo {
    ui32 NodeId = 0;
    TString Topic;
    TActorId PipeClient;
    bool BrokenPipe = false;
    bool IsBalancer = false;
    TVector<NKikimrPQ::TOffsetsResponse::TPartResult> OffsetResponses;
    TVector<NKikimrPQ::TStatusResponse::TPartResult> StatusResponses;
};

TPersQueueBaseRequestProcessor::TPersQueueBaseRequestProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& pqMetaCacheId, bool listNodes)
    : RequestProto(new NKikimrClient::TPersQueueRequest(request))
    , RequestId(RequestProto->HasRequestId() ? RequestProto->GetRequestId() : "<none>")
    , PqMetaCache(pqMetaCacheId)
    , ListNodes(listNodes)
{
}

void TPersQueueBaseRequestProcessor::SendErrorReplyAndDie(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
    SendReplyAndDie(CreateErrorReply(status, code, errorReason), ctx);
}

bool TPersQueueBaseRequestProcessor::ReadyForAnswer(const TActorContext& ) {
    return ReadyToCreateChildren() && NeedChildrenCreation == false && ChildrenAnswered == Children.size();
}

void TPersQueueBaseRequestProcessor::AnswerAndDie(const TActorContext& ctx) {
    try {
        SendReplyAndDie(MergeSubactorReplies(), ctx);
    } catch (const std::exception& ex) {
        SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::ERROR, ex.what());
    }
}

void TPersQueueBaseRequestProcessor::Bootstrap(const TActorContext& ctx) {
    StartTimestamp = ctx.Now();

    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "Send to PqMetaCache TEvDescribeAllTopicsRequest");
    bool ret = ctx.Send(PqMetaCache, new NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsRequest());
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "Send to PqMetaCache TEvDescribeAllTopicsRequest Result:" << ret);

    if (ListNodes) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
    }

    Become(&TPersQueueBaseRequestProcessor::StateFunc, ctx, CHECK_INFLY_SEMAPHORE_DURATION, new TEvents::TEvWakeup());
}

void TPersQueueBaseRequestProcessor::Die(const TActorContext& ctx) {
    // Clear children
    for (const auto& child : Children) {
        ctx.Send(child.first, new TEvents::TEvPoisonPill());
    }
    TActorBootstrapped::Die(ctx);
}

STFUNC(TPersQueueBaseRequestProcessor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvInterconnect::TEvNodesInfo, Handle);
        HFunc(NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse, Handle);
        HFunc(NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsResponse, Handle);
        HFunc(NPqMetaCacheV2::TEvPqNewMetaCache::TEvGetNodesMappingResponse, Handle);
        HFunc(TEvPersQueue::TEvResponse, Handle);
        CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    }
}

void TPersQueueBaseRequestProcessor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Record.GetStatus() != MSTATUS_OK) {
        return SendReplyAndDie(std::move(ev->Get()->Record), ctx);
    }
    auto answeredChild = Children.find(ev->Sender);
    Y_ABORT_UNLESS(answeredChild != Children.end());
    Y_ABORT_UNLESS(!answeredChild->second->ActorAnswered);
    answeredChild->second->Response.Swap(&ev->Get()->Record);
    answeredChild->second->ActorAnswered = true;
    ++ChildrenAnswered;
    Y_ABORT_UNLESS(ChildrenAnswered <= Children.size());

    if (ReadyForAnswer(ctx)) {
        return AnswerAndDie(ctx);
    }
}

void TPersQueueBaseRequestProcessor::Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ListNodes);
    NodesInfo.reset(new TNodesInfo(ev->Release(), ctx));
    if (ReadyToCreateChildren()) {
        if (CreateChildren(ctx)) {
            return;
        }
    }
}

void TPersQueueBaseRequestProcessor::Handle(
        TEvPqNewMetaCache::TEvGetNodesMappingResponse::TPtr& ev, const TActorContext& ctx
) {
    NodesInfo->ProcessNodesMapping(ev, ctx);
    if (ReadyToCreateChildren()) {
        if (CreateChildren(ctx)) {
            return;
        }
    }
}

void TPersQueueBaseRequestProcessor::HandleTimeout(const TActorContext& ctx) {
    if (ctx.Now() - StartTimestamp > TIMEOUT) {
        SendErrorReplyAndDie(ctx, MSTATUS_TIMEOUT, NPersQueue::NErrorCode::ERROR, "Timeout while waiting for response, may be just slow, Marker# PQ16");
        return;
    }
    if (NeedChildrenCreation) {
        CreateChildrenIfNeeded(ctx);
    }
    ctx.Schedule(CHECK_INFLY_SEMAPHORE_DURATION, new TEvents::TEvWakeup());
}


void TPersQueueBaseRequestProcessor::GetTopicsListOrThrow(
        const ::google::protobuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& requests,
        THashMap<TString, std::shared_ptr<THashSet<ui64>>>& partitionsToRequest
) {
    for (const auto& topicRequest : requests) {
        if (topicRequest.GetTopic().empty()) {
            throw std::runtime_error("TopicRequest must have Topic field.");
        }
        std::shared_ptr<THashSet<ui64>> partitionsToRequestOnTopic(new THashSet<ui64>()); // nonconst
        partitionsToRequest[topicRequest.GetTopic()] = partitionsToRequestOnTopic;
        for (ui32 partition : topicRequest.GetPartition()) {
            const bool inserted = partitionsToRequestOnTopic->insert(partition).second;
            if (!inserted) {
                TStringBuilder desc;
                desc << "multiple partition " << partition
                     << " in TopicRequest for topic '" << topicRequest.GetTopic() << "'";
                throw std::runtime_error(desc);
            }
        }

        const bool res = TopicsToRequest.insert(topicRequest.GetTopic()).second;
        if (!res) {
            TStringBuilder desc;
            desc << "multiple TopicRequest for topic '" << topicRequest.GetTopic() << "'";
            throw std::runtime_error(desc);
        }
    }

}

void TPersQueueBaseRequestProcessor::Handle(
        NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse::TPtr&, const TActorContext&
) {
    Y_ABORT();
}

void TPersQueueBaseRequestProcessor::Handle(
        NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsResponse::TPtr& ev, const TActorContext& ctx
) {
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TPersQueueBaseRequestProcessor::Handle");

    auto& path = ev->Get()->Path;
    if (!ev->Get()->Success) {
        return SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                                    TStringBuilder() << "no path '" << path << "', Marker# PQ17");
    }

    TopicsDescription = std::move(ev->Get()->Result);
    TopicsConverters = std::move(ev->Get()->Topics);
    Y_ABORT_UNLESS(TopicsDescription->ResultSet.size() == TopicsConverters.size());
    if (ReadyToCreateChildren()) {
        if (CreateChildren(ctx)) {
            return;
        }
    }
}

bool TPersQueueBaseRequestProcessor::ReadyToCreateChildren() const {
    return TopicsDescription
           && (!ListNodes || (NodesInfo.get() != nullptr && NodesInfo->Ready));
}

bool TPersQueueBaseRequestProcessor::CreateChildren(const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TPersQueueBaseRequestProcessor::CreateChildren");

    if (ChildrenCreationDone)
        return false;
    ChildrenCreationDone = true;
    Y_ABORT_UNLESS(TopicsDescription->ResultSet.size() == TopicsConverters.size());
    ui32 i = 0;
    for (const auto& entry : TopicsDescription->ResultSet) {
        auto converter = TopicsConverters[i++];
        if (!converter) {
            continue;
        }
        if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic && entry.PQGroupInfo) {

            auto name = converter->GetClientsideName();

            if (name.empty() || !TopicsToRequest.empty() && !IsIn(TopicsToRequest, name)) {
                continue;
            }
            ChildrenToCreate.emplace_back(new TPerTopicInfo(entry, converter));
        }
    }
    NeedChildrenCreation = true;
    return CreateChildrenIfNeeded(ctx);
}


TPersQueueBaseRequestProcessor::~TPersQueueBaseRequestProcessor() {
    AtomicSub(Infly, ChildrenCreated);
}

bool TPersQueueBaseRequestProcessor::CreateChildrenIfNeeded(const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TPersQueueBaseRequestProcessor::CreateChildrenIfNeeded topics count = " << ChildrenToCreate.size());

    Y_ABORT_UNLESS(NeedChildrenCreation);

    if (AtomicAdd(Infly, ChildrenToCreate.size()) > MAX_INFLY) {
        AtomicSub(Infly, ChildrenToCreate.size());
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "topics count =" << ChildrenToCreate.size() << " is greater then MAX_INFLY=" << MAX_INFLY);
        return false;
    }

    ChildrenCreated = ChildrenToCreate.size();
    NeedChildrenCreation = false;

    THashSet<TString> topics;

    while (!ChildrenToCreate.empty()) {
        THolder<TPerTopicInfo> perTopicInfo(ChildrenToCreate.front().Release());
        ChildrenToCreate.pop_front();
        const auto& name = perTopicInfo->Converter->GetClientsideName();
        if (name.empty()) {
            continue;
        }

        if (topics.find(name) != topics.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "already present topic '" << name << "'");
            SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                                 TStringBuilder() << "already present topic '" << name  << "' Marker# PQ95");
            return true;
        }

        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "CreateTopicSubactor for topic " << name);

        THolder<IActor> childActor = CreateTopicSubactor(perTopicInfo->TopicEntry, name);
        if (childActor.Get() != nullptr) {
            const TActorId actorId = ctx.Register(childActor.Release());
            perTopicInfo->ActorId = actorId;
            topics.emplace(name);
            Children.emplace(actorId, std::move(perTopicInfo));
        }
        else
            LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE, "CreateTopicSubactor failed.");
    }
    Y_ABORT_UNLESS(topics.size() == Children.size());

    if (!TopicsToRequest.empty() && TopicsToRequest.size() != topics.size()) {
        // Write helpful error description
        Y_ABORT_UNLESS(topics.size() < TopicsToRequest.size());
        TStringBuilder errorDesc;
        errorDesc << "the following topics are not created: ";
        for (const TString& topic : TopicsToRequest) {
            if (!IsIn(topics, topic)) {
                errorDesc << topic << ", ";
            }
        }
        SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, errorDesc << "Marker# PQ95");
        return true;
    }
    if (ReadyForAnswer(ctx)) {
        AnswerAndDie(ctx);
        return true;
    }
    return false;
}

NKikimrClient::TResponse TPersQueueBaseRequestProcessor::MergeSubactorReplies() {
    NKikimrClient::TResponse response;
    response.SetStatus(MSTATUS_OK); // We need to have status event if we have no children
    response.SetErrorCode(NPersQueue::NErrorCode::OK);
    for (const auto& child : Children) {
        response.MergeFrom(child.second->Response);
    }
    return response;
}

TPersQueueBaseRequestProcessor::TNodesInfo::TNodesInfo(
        THolder<TEvInterconnect::TEvNodesInfo> nodesInfoReply, const TActorContext& ctx
)
    : NodesInfoReply(std::move(nodesInfoReply))
{
    const auto& pqConfig = AppData(ctx)->PQConfig;
    bool useMapping = pqConfig.GetPQDiscoveryConfig().GetUseDynNodesMapping();
    HostNames.reserve(NodesInfoReply->Nodes.size());
    for (const NActors::TEvInterconnect::TNodeInfo& info : NodesInfoReply->Nodes) {
        HostNames.emplace(info.NodeId, info.Host);
        if (useMapping) {
            ctx.Send(
                    CreatePersQueueMetaCacheV2Id(),
                    new TEvPqNewMetaCache::TEvGetNodesMappingRequest()
            );
        } else {
            Ready = true;
            auto insRes = MinNodeIdByHost.insert(std::make_pair(info.Host, info.NodeId));
            if (!insRes.second) {
                if (insRes.first->second > info.NodeId) {
                    insRes.first->second = info.NodeId;
                }
            }
        }
    }
}

TTopicInfoBasedActor::TTopicInfoBasedActor(const TSchemeEntry& topicEntry, const TString& topicName)
    : TActorBootstrapped<TTopicInfoBasedActor>()
    , SchemeEntry(topicEntry)
    , Name(topicName)
    , ProcessingResult(ProcessMetaCacheSingleTopicsResponse(SchemeEntry))
{
}

void TPersQueueBaseRequestProcessor::TNodesInfo::ProcessNodesMapping(
        TEvPqNewMetaCache::TEvGetNodesMappingResponse::TPtr& ev, const TActorContext&
) {
    DynToStaticNode = std::move(ev->Get()->NodesMapping);
    Ready = true;
}


void TTopicInfoBasedActor::Bootstrap(const TActorContext &ctx) {
    Become(&TTopicInfoBasedActor::StateFunc);
    BootstrapImpl(ctx);
}

STFUNC(TTopicInfoBasedActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    default:
        ALOG_WARN(NKikimrServices::PERSQUEUE, "Unexpected event type: " << ev->GetTypeRewrite() << ", " << ev->ToString());
    }
}


class TMessageBusServerPersQueueImpl : public TActorBootstrapped<TMessageBusServerPersQueueImpl> {
    using TEvDescribeAllTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsRequest;
    using TEvDescribeAllTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsResponse;

protected:
    NKikimrClient::TPersQueueRequest RequestProto;
    const TString RequestId;
    const bool IsMetaRequest;
    const bool IsFetchRequest;

    bool CanProcessFetchRequest; //any partitions answered that it has data or WaitMs timeout occured
    ui32 FetchRequestReadsDone;
    ui64 FetchRequestCurrentReadTablet; //if zero then no read at this time
    ui64 CurrentCookie;
    ui32 FetchRequestBytesLeft;
    NKikimrClient::TPersQueueFetchResponse FetchResponse;
    TVector<TActorId> PQClient;
    const TActorId SchemeCache;

    TAutoPtr<TEvInterconnect::TEvNodesInfo> NodesInfo;

    THashMap<TString, TTopicInfo> TopicInfo;
    THashMap<ui64, TTabletInfo> TabletInfo;

    ui32 TopicsAnswered;
    THashSet<ui64> TabletsDiscovered;
    THashSet<ui64> TabletsAnswered;
    ui32 AclRequests;
    ui32 DescribeRequests;
    ui32 PartTabletsRequested;
    TString ErrorReason;
    bool NoTopicsAtStart;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_PROXY_ACTOR;
    }

    virtual ~TMessageBusServerPersQueueImpl() {}

    virtual void SendReplyAndDie(NKikimrClient::TResponse&& response, const TActorContext& ctx) = 0;

    TMessageBusServerPersQueueImpl(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache)
        : RequestProto(request)
        , RequestId(RequestProto.HasRequestId() ? RequestProto.GetRequestId() : "<none>")
        , IsMetaRequest(RequestProto.HasMetaRequest())
        , IsFetchRequest(RequestProto.HasFetchRequest())
        , CanProcessFetchRequest(false)
        , FetchRequestReadsDone(0)
        , FetchRequestCurrentReadTablet(0)
        , CurrentCookie(1)
        , FetchRequestBytesLeft(0)
        , SchemeCache(schemeCache)
        , TopicsAnswered(0)
        , AclRequests(0)
        , DescribeRequests(0)
        , PartTabletsRequested(0)
        , NoTopicsAtStart(true)
    {
        const auto& record = RequestProto;

        if (record.HasMetaRequest() + record.HasPartitionRequest() + record.HasFetchRequest() > 1) {
            ErrorReason = "only one from meta partition or fetch requests must be filled";
            return;
        }
        if (record.HasMetaRequest()) {
            Y_ABORT_UNLESS(IsMetaRequest);
            auto& meta = record.GetMetaRequest();
            ui32 count = meta.HasCmdGetPartitionLocations() + meta.HasCmdGetPartitionOffsets() +
                         meta.HasCmdGetTopicMetadata() + meta.HasCmdGetPartitionStatus() + meta.HasCmdGetReadSessionsInfo();
            if (count != 1) {
                ErrorReason = "multiple or none requests in MetaRequest";
                return;
            }
            if (meta.HasCmdGetPartitionLocations()) {
                if (!GetTopicsList(meta.GetCmdGetPartitionLocations().topicrequest()))
                    return;
            } else if (meta.HasCmdGetPartitionOffsets()) {
                if (!GetTopicsList(meta.GetCmdGetPartitionOffsets().topicrequest()))
                    return;
            } else if (meta.HasCmdGetTopicMetadata()) {
                auto& d = meta.GetCmdGetTopicMetadata();
                for (ui32 i = 0; i < d.TopicSize(); ++i) {
                    if (d.GetTopic(i).empty()) {
                        ErrorReason = "empty topic in GetTopicMetadata request";
                        return;
                    }
                    TopicInfo[d.GetTopic(i)];
                }
            } else if (meta.HasCmdGetPartitionStatus()) {
                if (!GetTopicsList(meta.GetCmdGetPartitionStatus().topicrequest()))
                    return;
            } else if (meta.HasCmdGetReadSessionsInfo()) {
                auto& d = meta.GetCmdGetReadSessionsInfo();
                for (ui32 i = 0; i < d.TopicSize(); ++i) {
                    if (d.GetTopic(i).empty()) {
                        ErrorReason = "empty topic in GetReadSessionsInfo request";
                        return;
                    }
                    TopicInfo[d.GetTopic(i)];
                }
            }
             else
                ErrorReason = "Not implemented yet";
        } else if (record.HasPartitionRequest()) {
            auto& part = record.GetPartitionRequest();
            if (!part.HasTopic() || !part.HasPartition() || part.GetTopic().empty()) {
                ErrorReason = "no Topic or Partition in PartitionRequest";
                return;
            }
            TopicInfo[part.GetTopic()].PartitionsToRequest.insert(part.GetPartition());
        } else if (record.HasFetchRequest()) {
            auto& fetch = record.GetFetchRequest();
            ui64 deadline = TAppData::TimeProvider->Now().MilliSeconds() + Min<ui32>(fetch.GetWaitMs(), 30000);
            if (!fetch.HasWaitMs() || !fetch.HasTotalMaxBytes() || !fetch.HasClientId()) {
                ErrorReason = "no WaitMs, TotalMaxBytes or ClientId in FetchRequest";
                return;
            }
            FetchRequestBytesLeft = fetch.GetTotalMaxBytes();
            for (ui32 i = 0; i < fetch.PartitionSize(); ++i) {
                auto& part = fetch.GetPartition(i);
                if (!part.HasTopic() || part.GetTopic().empty() || !part.HasPartition() || !part.HasOffset() || !part.HasMaxBytes()) {
                    ErrorReason = "no Topic, Partition, Offset or MaxBytes in FetchRequest::Partition";
                    return;
                }
                bool res = TopicInfo[part.GetTopic()].PartitionsToRequest.insert(part.GetPartition()).second;
                if (!res) {
                    ErrorReason = "same partition specified multiple times";
                    return;
                }
                TAutoPtr<TEvPersQueue::TEvHasDataInfo> fetchInfo(new TEvPersQueue::TEvHasDataInfo());
                fetchInfo->Record.SetPartition(part.GetPartition());
                fetchInfo->Record.SetOffset(part.GetOffset());
                fetchInfo->Record.SetDeadline(deadline);
                fetchInfo->Record.SetClientId(fetch.GetClientId());
                TopicInfo[part.GetTopic()].FetchInfo[part.GetPartition()] = fetchInfo;
            }
        } else {
            ErrorReason = "empty request";
        }
    }


    //returns true if answered
    void AnswerGetPartitionLocations(const TActorContext& ctx)
    {
        auto& meta = RequestProto.GetMetaRequest();

        THashMap<ui32, TString> hostName(NodesInfo->Nodes.size());
        for (const auto& n : NodesInfo->Nodes)
            hostName.insert(std::make_pair(n.NodeId, n.Host));


        NKikimrClient::TResponse record;
        record.SetStatus(MSTATUS_OK);
        record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto res = record.MutableMetaResponse()->MutableCmdGetPartitionLocationsResult();
        for (const auto& p : TopicInfo) {
            auto topicResult = res->AddTopicResult();
            topicResult->SetTopic(p.first);
            if (p.second.PartitionToTablet.empty()) {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            } else {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::OK);
            }
            for (const auto& pp : p.second.PartitionToTablet) {
                auto it = TabletInfo.find(pp.second);
                Y_ABORT_UNLESS(it != TabletInfo.end());
                auto ht = hostName.find(it->second.NodeId);
                if (ht != hostName.end()) {
                    if (meta.GetCmdGetPartitionLocations().HasHost() && meta.GetCmdGetPartitionLocations().GetHost() != ht->second) {
                        continue;
                    }
                    auto partResult = topicResult->AddPartitionLocation();
                    partResult->SetPartition(pp.first);

                    partResult->SetHostId(it->second.NodeId);
                    partResult->SetHost(ht->second);
                    partResult->SetErrorCode(NPersQueue::NErrorCode::OK);
                } else {
                    auto partResult = topicResult->AddPartitionLocation();
                    partResult->SetPartition(pp.first);

                    partResult->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                    partResult->SetErrorReason("Tablet for that partition is no running");
                }
            }
        }
        SendReplyAndDie(std::move(record), ctx);
    }

    void AnswerGetTopicMetadata(const TActorContext& ctx)
    {
        NKikimrClient::TResponse record;
        record.SetStatus(MSTATUS_OK);
        record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto res = record.MutableMetaResponse()->MutableCmdGetTopicMetadataResult();
        for (const auto& ti : TopicInfo) {
            auto topicInfo = res->AddTopicInfo();
            topicInfo->SetTopic(ti.first);
            topicInfo->MutableConfig()->CopyFrom(ti.second.Config);
            topicInfo->SetNumPartitions(ti.second.NumParts);
            if (ti.second.NumParts == 0) {
                topicInfo->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            } else {
                topicInfo->SetErrorCode(NPersQueue::NErrorCode::OK);
            }

        }
        SendReplyAndDie(std::move(record), ctx);
    }

    void AnswerGetPartitionOffsets(const TActorContext& ctx)
    {
        NKikimrClient::TResponse record;
        record.SetStatus(MSTATUS_OK);
        record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto offs = record.MutableMetaResponse()->MutableCmdGetPartitionOffsetsResult();

        for (auto& p : TopicInfo) {
            auto topicResult = offs->AddTopicResult();
            topicResult->SetTopic(p.first);
            if (p.second.PartitionToTablet.empty()) {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            } else {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::OK);
            }
            for (const auto& tablet: p.second.Tablets) {
                auto it = TabletInfo.find(tablet);
                Y_ABORT_UNLESS(it != TabletInfo.end());
                for (const auto& r : it->second.OffsetResponses) {
                    if (p.second.PartitionToTablet.find(r.GetPartition()) == p.second.PartitionToTablet.end())
                        continue;
                    p.second.PartitionToTablet.erase(r.GetPartition());
                    auto res = topicResult->AddPartitionResult();
                    res->CopyFrom(r);
                }
            }
            for (const auto& part : p.second.PartitionToTablet) {
                auto res = topicResult->AddPartitionResult();
                res->SetPartition(part.first);
                res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                res->SetErrorReason("partition is not ready yet");
            }
        }
        SendReplyAndDie(std::move(record), ctx);
    }

    void AnswerGetPartitionStatus(const TActorContext& ctx)
    {
        NKikimrClient::TResponse record;
        record.SetStatus(MSTATUS_OK);
        record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto stat = record.MutableMetaResponse()->MutableCmdGetPartitionStatusResult();

        for (auto& p : TopicInfo) {
            auto topicResult = stat->AddTopicResult();
            topicResult->SetTopic(p.first);
            if (p.second.PartitionToTablet.empty()) {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            } else {
                topicResult->SetErrorCode(NPersQueue::NErrorCode::OK);
            }

            for (const auto& tablet: p.second.Tablets) {
                auto it = TabletInfo.find(tablet);
                Y_ABORT_UNLESS(it != TabletInfo.end());
                for (const auto& r : it->second.StatusResponses) {
                    if (p.second.PartitionToTablet.find(r.GetPartition()) == p.second.PartitionToTablet.end())
                        continue;
                    p.second.PartitionToTablet.erase(r.GetPartition());
                    auto res = topicResult->AddPartitionResult();
                    res->CopyFrom(r);
                }
            }
            for (const auto& part : p.second.PartitionToTablet) {
                auto res = topicResult->AddPartitionResult();
                res->SetPartition(part.first);
                res->SetStatus(NKikimrPQ::TStatusResponse::STATUS_UNKNOWN);
            }
        }
        SendReplyAndDie(std::move(record), ctx);
    }

    void AnswerGetReadSessionsInfo(const TActorContext& ctx)
    {
        NKikimrClient::TResponse record;
        record.SetStatus(MSTATUS_OK);
        record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto stat = record.MutableMetaResponse()->MutableCmdGetReadSessionsInfoResult();

        THashMap<ui32, TString> hostName(NodesInfo->Nodes.size());
        for (const auto& n : NodesInfo->Nodes)
            hostName.insert(std::make_pair(n.NodeId, n.Host));


        for (auto& p : TopicInfo) {
            auto topicRes = stat->AddTopicResult();
            topicRes->SetTopic(p.first);
            if (p.second.Tablets.empty()) {
                topicRes->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            } else {
                topicRes->SetErrorCode(NPersQueue::NErrorCode::OK);
            }

            THashMap<ui32, ui32> partitionToResp;
            ui32 sz = 0;
            Y_ABORT_UNLESS(p.second.ReadSessionsInfo);
            auto* sessionsInfo = p.second.ReadSessionsInfo.Get();
            for (ui32 i = 0; i < sessionsInfo->PartitionInfoSize(); ++i) {
                const auto& resp = sessionsInfo->GetPartitionInfo(i);
                partitionToResp[resp.GetPartition()] = sz++;
                auto res = topicRes->AddPartitionResult();
                res->SetPartition(resp.GetPartition());
                res->SetSession(resp.GetSession());
                res->SetClientNode(resp.GetClientNode());
                res->SetTimestamp(resp.GetTimestamp() > 0 ? TInstant::Seconds(resp.GetTimestamp()).ToString() : "");
                res->SetProxyNode(resp.GetProxyNodeId() > 0 ? hostName[resp.GetProxyNodeId()] : "");
            }
            for (const auto& tablet: p.second.Tablets) {
                auto it = TabletInfo.find(tablet);
                Y_ABORT_UNLESS(it != TabletInfo.end());
                for (const auto& r : it->second.OffsetResponses) {
                    if (p.second.PartitionToTablet.find(r.GetPartition()) == p.second.PartitionToTablet.end())
                        continue;
                    p.second.PartitionToTablet.erase(r.GetPartition());
                    ui32 part = r.GetPartition();
                    auto jt = partitionToResp.find(part);
                    auto res  = (jt == partitionToResp.end()) ? topicRes->AddPartitionResult() : topicRes->MutablePartitionResult(jt->second);
                    res->SetPartition(part);
                    res->SetClientOffset(r.HasClientOffset() ? r.GetClientOffset() : 0);
                    res->SetStartOffset(r.GetStartOffset());
                    res->SetEndOffset(r.GetEndOffset());
                    res->SetTimeLag(r.HasWriteTimestampMS() ? Max<i64>(TAppData::TimeProvider->Now().MilliSeconds() - r.GetWriteTimestampMS(), 0) : 0);
                    res->SetReadTimeLag(r.HasReadWriteTimestampMS() ? Max<i64>(TAppData::TimeProvider->Now().MilliSeconds() - r.GetReadWriteTimestampMS(), 0) : 0);
                    res->SetClientReadOffset(r.HasClientReadOffset() ? r.GetClientReadOffset() : 0);
                    res->SetTabletNode(it->second.NodeId > 0 ? hostName[it->second.NodeId] : "");
                }
            }
        }

        SendReplyAndDie(std::move(record), ctx);
    }


    bool AnswerIfCanForMeta(const TActorContext& ctx) {
        Y_ABORT_UNLESS(IsMetaRequest);
        Y_ABORT_UNLESS(RequestProto.HasMetaRequest());
        if (AclRequests)
            return false;
        if (DescribeRequests)
            return false;
        const auto& meta = RequestProto.GetMetaRequest();
        if (meta.HasCmdGetPartitionLocations()) {
            if (TopicsAnswered != TopicInfo.size() || TabletInfo.size() != TabletsDiscovered.size() || !NodesInfo)
                return false;
            AnswerGetPartitionLocations(ctx);
            return true;
        } else if (meta.HasCmdGetTopicMetadata()) {
            if (TopicsAnswered != TopicInfo.size())
                return false;
            AnswerGetTopicMetadata(ctx);
            return true;
        } else if (meta.HasCmdGetPartitionOffsets()) {
            if (TopicsAnswered != TopicInfo.size() || TabletsAnswered.size() < PartTabletsRequested)
                return false;
            Y_ABORT_UNLESS(PartTabletsRequested == TabletInfo.size());
            AnswerGetPartitionOffsets(ctx);
            return true;
        } else if (meta.HasCmdGetPartitionStatus()) {
            if (TopicsAnswered != TopicInfo.size() || TabletsAnswered.size() < PartTabletsRequested) //not all responses got
                return false;
            Y_ABORT_UNLESS(PartTabletsRequested == TabletInfo.size()); //there could be balancers and partTablets in TabletInfo only
            AnswerGetPartitionStatus(ctx);
            return true;
        } else if (meta.HasCmdGetReadSessionsInfo()) {
            if (TopicsAnswered != TopicInfo.size() || TabletsAnswered.size() < TabletInfo.size() || !NodesInfo) //not all responses got; waiting respose from all balancers and partitions
                return false;
            Y_ABORT_UNLESS(PartTabletsRequested + TopicInfo.size() >= TabletInfo.size()); //there could be balancers and partTablets in TabletInfo only
            AnswerGetReadSessionsInfo(ctx);
            return true;

        }
        Y_ABORT("UNKNOWN request");
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        if (IsFetchRequest) {
            ProcessFetchRequestResult(ev, ctx);
            return;
        }
        SendReplyAndDie(std::move(ev->Get()->Record), ctx);
    }

    void Handle(TEvPersQueue::TEvOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get()->Record;
        Y_ABORT_UNLESS(response.HasTabletId());
        auto it = TabletInfo.find(response.GetTabletId());
        Y_ABORT_UNLESS(it != TabletInfo.end());
        for (ui32 i = 0; i < response.PartResultSize(); ++i) {
            it->second.OffsetResponses.push_back(response.GetPartResult(i));
        }
        TabletsAnswered.insert(it->first);
        AnswerIfCanForMeta(ctx);
    }

    void Handle(TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get()->Record;
        Y_ABORT_UNLESS(response.HasTabletId());
        auto it = TabletInfo.find(response.GetTabletId());
        Y_ABORT_UNLESS(it != TabletInfo.end());
        TabletsAnswered.insert(it->first);

        auto jt = TopicInfo.find(it->second.Topic);
        Y_ABORT_UNLESS(jt != TopicInfo.end());
        jt->second.ReadSessionsInfo = MakeHolder<NKikimrPQ::TReadSessionsInfoResponse>(std::move(response));

        AnswerIfCanForMeta(ctx);
    }



    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get()->Record;
        Y_ABORT_UNLESS(response.HasTabletId());
        auto it = TabletInfo.find(response.GetTabletId());
        Y_ABORT_UNLESS(it != TabletInfo.end());
        for (ui32 i = 0; i < response.PartResultSize(); ++i) {
            it->second.StatusResponses.push_back(response.GetPartResult(i));
        }
        TabletsAnswered.insert(it->first);

        AnswerIfCanForMeta(ctx);
    }

    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr&, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "got HasDatainfoResponse");
        ProceedFetchRequest(ctx);
    }


    void Handle(TEvDescribeAllTopicsResponse::TPtr& ev, const TActorContext& ctx) {
        --DescribeRequests;
        auto& res = ev->Get()->Result->ResultSet;
        auto& topics = ev->Get()->Topics;
        auto processResult = ProcessMetaCacheAllTopicsResponse(ev);
        if (processResult.IsFatal) {
            ErrorReason = processResult.Reason;
            return SendReplyAndDie(CreateErrorReply(processResult.Status, processResult.ErrorCode, ctx), ctx);
        }

        NoTopicsAtStart = TopicInfo.empty();
        bool hasTopics = !NoTopicsAtStart;

        Y_ABORT_UNLESS(topics.size() == res.size());
        auto factory = NPersQueue::TTopicNamesConverterFactory(AppData(ctx)->PQConfig, {});
        for (auto i = 0u; i != res.size(); i++) {
            auto& entry = res[i];
            auto& converter = ev->Get()->Topics[i];
            if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic && entry.PQGroupInfo && converter) {
                auto& description = entry.PQGroupInfo->Description;
                if (!hasTopics || TopicInfo.find(converter->GetClientsideName()) != TopicInfo.end()) {
                    auto& topicInfo = TopicInfo[converter->GetClientsideName()];
                    topicInfo.BalancerTabletId = description.GetBalancerTabletID();
                    topicInfo.PQInfo = entry.PQGroupInfo;
                }
            }
        }

        for (auto& p: TopicInfo) {
            const TString& topic = p.first;

            if (!p.second.BalancerTabletId) {
                ErrorReason = Sprintf("topic '%s' is not created, Marker# PQ94", topic.c_str());
                return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx), ctx);
            }
            ProcessMetadata(p.first, p.second, ctx);
        }

        if (RequestProto.HasMetaRequest()) {
            AnswerIfCanForMeta(ctx); //if no topics at all
        }
    }

    void ProcessMetadata(const TString& name, TTopicInfo& info, const TActorContext& ctx) {
        if (!info.PQInfo) { //not supposed to happen anymore
            if (RequestProto.HasMetaRequest() && NoTopicsAtStart && !RequestProto.GetMetaRequest().HasCmdGetTopicMetadata()) {
                ++TopicsAnswered;
                AnswerIfCanForMeta(ctx);
            } else {
                ErrorReason = Sprintf("topic '%s' is not ready, Marker# PQ85", name.c_str());
                SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx), ctx);
            }
            return;
        }

        const auto& pqDescr = info.PQInfo->Description;

        bool mirrorerRequest = false;
        if (RequestProto.HasPartitionRequest()) {
            for (const auto& req : RequestProto.GetPartitionRequest().GetCmdWrite()) {
                if (req.HasDisableDeduplication() && req.GetDisableDeduplication()) mirrorerRequest = true;
            }
            if (RequestProto.GetPartitionRequest().HasCmdRead()) {
                mirrorerRequest = RequestProto.GetPartitionRequest().GetCmdRead().GetMirrorerRequest();
            }
            if (RequestProto.GetPartitionRequest().HasCmdGetMaxSeqNo()) {
                for (const auto& sid : RequestProto.GetPartitionRequest().GetCmdGetMaxSeqNo().GetSourceId()) {
                    mirrorerRequest = sid.substr(1).StartsWith("frontend-status");
                }
            }
            if (RequestProto.GetPartitionRequest().HasCmdSetClientOffset()) {
                mirrorerRequest = RequestProto.GetPartitionRequest().GetCmdSetClientOffset().GetMirrorerRequest();
            }
        } else if (RequestProto.HasFetchRequest()){
            mirrorerRequest = RequestProto.GetFetchRequest().GetMirrorerRequest();
        }

        TMaybe<NKikimrPQ::EOperation> operation = TMaybe<NKikimrPQ::EOperation>();
        if (RequestProto.HasFetchRequest()) {
            operation = NKikimrPQ::EOperation::READ_OP;
        } else if (RequestProto.HasPartitionRequest()) {
            auto& partitionRequest = RequestProto.GetPartitionRequest();
            if (partitionRequest.CmdWriteSize() > 0 || partitionRequest.HasCmdGetMaxSeqNo()) {
                operation = NKikimrPQ::EOperation::WRITE_OP;
            } else if (partitionRequest.HasCmdRead() || partitionRequest.HasCmdSetClientOffset()) {
                operation = NKikimrPQ::EOperation::READ_OP;
            }
        }

        if (AppData(ctx)->PQConfig.GetCheckACL() && operation && !mirrorerRequest) {
            if (*operation == NKikimrPQ::EOperation::WRITE_OP && pqDescr.GetPQTabletConfig().GetRequireAuthWrite() ||
                *operation == NKikimrPQ::EOperation::READ_OP && pqDescr.GetPQTabletConfig().GetRequireAuthRead()) {
                ErrorReason = Sprintf("unauthenticated access to '%s' is denied, Marker# PQ419", name.c_str());
                return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::ACCESS_DENIED, ctx), ctx);
            }
        }

        if (RequestProto.HasPartitionRequest()) {
            ui64 tabletId = 0;
            auto it = TopicInfo.find(name);
            Y_ABORT_UNLESS(it != TopicInfo.end());
            Y_ABORT_UNLESS(it->second.PartitionsToRequest.size() == 1);
            ui32 partition = *(it->second.PartitionsToRequest.begin());
            for (ui32 i = 0; i < pqDescr.PartitionsSize(); ++i) {
                const auto& pi = pqDescr.GetPartitions(i);
                if (pi.GetPartitionId() == partition) {
                    tabletId = pi.GetTabletId();
                    break;
                }
            }

            if (!tabletId) {
                ErrorReason = Sprintf("no partition %u in topic '%s', Marker# PQ4", partition, name.c_str());
                return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx), ctx);
            }

            auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            retryPolicy.RetryLimitCount = 5;
            NTabletPipe::TClientConfig clientConfig(retryPolicy);

            PQClient.push_back(ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig)));
            ActorIdToProto(PQClient.back(), RequestProto.MutablePartitionRequest()->MutablePipeClient());

            TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record.Swap(&RequestProto);
            NTabletPipe::SendData(ctx, PQClient.back(), req.Release());
            return;
        }


        if (RequestProto.HasMetaRequest() || RequestProto.HasFetchRequest()) { //answer or request locations

            bool needResolving = RequestProto.HasMetaRequest() && RequestProto.GetMetaRequest().HasCmdGetPartitionLocations();
            bool needAskOffset = RequestProto.HasMetaRequest() && (RequestProto.GetMetaRequest().HasCmdGetPartitionOffsets()
                                                                  || RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo());
            bool needAskStatus = RequestProto.HasMetaRequest() && RequestProto.GetMetaRequest().HasCmdGetPartitionStatus();
            bool needAskFetch = RequestProto.HasFetchRequest();
            bool metadataOnly = RequestProto.HasMetaRequest() && RequestProto.GetMetaRequest().HasCmdGetTopicMetadata();
            bool needAskBalancer = RequestProto.HasMetaRequest() && RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo();

            Y_ABORT_UNLESS((needResolving + needAskOffset + needAskStatus + needAskFetch + metadataOnly) == 1);
            ++TopicsAnswered;
            auto it = TopicInfo.find(name);
            Y_ABORT_UNLESS(it != TopicInfo.end(), "topic '%s'", name.c_str());
            it->second.Config = pqDescr.GetPQTabletConfig();
            it->second.Config.SetVersion(pqDescr.GetAlterVersion());
            it->second.NumParts = pqDescr.PartitionsSize();
            if (metadataOnly) {
                AnswerIfCanForMeta(ctx);
                return;
            }
            Y_ABORT_UNLESS(it->second.BalancerTabletId);

            if (needAskBalancer) {

                if (!RequestProto.GetMetaRequest().GetCmdGetReadSessionsInfo().HasClientId()) {
                    ErrorReason = "No clientId specified in CmdGetReadSessionsInfo";
                    return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx), ctx);
                }

                auto& tabletInfo = TabletInfo[it->second.BalancerTabletId];
                tabletInfo.IsBalancer = true;
                tabletInfo.Topic = name;

                auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
                retryPolicy.RetryLimitCount = 5;
                NTabletPipe::TClientConfig clientConfig(retryPolicy);

                TActorId pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, it->second.BalancerTabletId, clientConfig));
                tabletInfo.PipeClient = pipeClient;
                PQClient.push_back(pipeClient);

                THolder<TEvPersQueue::TEvGetReadSessionsInfo> ev(new TEvPersQueue::TEvGetReadSessionsInfo());
                ev->Record.SetClientId(RequestProto.GetMetaRequest().GetCmdGetReadSessionsInfo().GetClientId());
                NTabletPipe::SendData(ctx, pipeClient, ev.Release());
            }

            for (ui32 i = 0; i < pqDescr.PartitionsSize(); ++i) {
                ui32 part = pqDescr.GetPartitions(i).GetPartitionId();
                ui64 tabletId = pqDescr.GetPartitions(i).GetTabletId();
                if (!it->second.PartitionsToRequest.empty() && !it->second.PartitionsToRequest.contains(part)) {
                    continue;
                }
                bool res = it->second.PartitionToTablet.insert({part, tabletId}).second;
                Y_ABORT_UNLESS(res);
                if (TabletInfo.find(tabletId) == TabletInfo.end()) {
                    auto& tabletInfo = TabletInfo[tabletId];
                    tabletInfo.Topic = name;
                    it->second.Tablets.push_back(tabletId);
                        // Tablet node resolution relies on opening a pipe

                    auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
                    retryPolicy.RetryLimitCount = 5;
                    NTabletPipe::TClientConfig clientConfig(retryPolicy);

                    TActorId pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
                    tabletInfo.PipeClient = pipeClient;
                    PQClient.push_back(pipeClient);
                    if (needAskOffset) {
                        THolder<TEvPersQueue::TEvOffsets> ev(new TEvPersQueue::TEvOffsets());
                        TString clientId;
                        if (RequestProto.GetMetaRequest().HasCmdGetPartitionOffsets()
                            && RequestProto.GetMetaRequest().GetCmdGetPartitionOffsets().HasClientId())
                            clientId = RequestProto.GetMetaRequest().GetCmdGetPartitionOffsets().GetClientId();
                        if (RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo())
                            clientId = RequestProto.GetMetaRequest().GetCmdGetReadSessionsInfo().GetClientId();
                        if (!clientId.empty())
                            ev->Record.SetClientId(clientId);
                        NTabletPipe::SendData(ctx, pipeClient, ev.Release());
                    } else if (needAskStatus) {
                        TAutoPtr<TEvPersQueue::TEvStatus> ev = new TEvPersQueue::TEvStatus();
                        if (RequestProto.GetMetaRequest().GetCmdGetPartitionStatus().HasClientId())
                            ev->Record.SetClientId(RequestProto.GetMetaRequest().GetCmdGetPartitionStatus().GetClientId());
                        NTabletPipe::SendData(ctx, pipeClient, ev.Release());
                    }
                    ++PartTabletsRequested;
                }
                if (needAskFetch) {
                    if (CanProcessFetchRequest) {
                        ProceedFetchRequest(ctx);
                    } else {
                        const auto& tabletInfo = TabletInfo[tabletId];
                        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "sending HasDataInfoResponse " << it->second.FetchInfo[part]->Record);

                        NTabletPipe::SendData(ctx, tabletInfo.PipeClient, it->second.FetchInfo[part].Release());
                        ++PartTabletsRequested;
                    }
                }
            }
            if (!it->second.PartitionsToRequest.empty() && it->second.PartitionsToRequest.size() != it->second.PartitionToTablet.size()) {
                ErrorReason = Sprintf("no one of requested partitions in topic '%s', Marker# PQ12", name.c_str());
                return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx), ctx);
            }

            Y_ABORT_UNLESS(!TabletInfo.empty()); // if TabletInfo is empty - topic is empty
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        NodesInfo = ev->Release();
        AnswerIfCanForMeta(ctx);
    }

    bool HandlePipeError(const ui64 tabletId, const TActorContext& ctx)
    {
        if (IsMetaRequest) {
            auto it = TabletInfo.find(tabletId);
            if (it != TabletInfo.end()) {
                TabletsAnswered.insert(tabletId);
                if (RequestProto.HasMetaRequest() && (RequestProto.GetMetaRequest().HasCmdGetPartitionLocations() || RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo())) {
                    TabletsDiscovered.insert(tabletId); // Disconnect event can arrive after connect event and this hash set will take it into account.
                }
                AnswerIfCanForMeta(ctx);
                return true;
            }
        }
        if (IsFetchRequest) {
            auto it = TabletInfo.find(tabletId);
            if (it != TabletInfo.end()) {
                it->second.BrokenPipe = true;
                if (FetchRequestCurrentReadTablet == tabletId) {
                    //fail current read
                    ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
                }
                return true;
            }
        }
        return false;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        const ui64 tabletId = ev->Get()->TabletId;
        if (msg->Status != NKikimrProto::OK) {

            if (HandlePipeError(tabletId, ctx))
                return;

            ErrorReason = Sprintf("Client pipe to %" PRIu64 " connection error, Status# %s, Marker# PQ6",
                tabletId, NKikimrProto::EReplyStatus_Name(msg->Status).data());
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::ERROR, ctx), ctx);
        }

        // Update node resolution info for GetPartitionLocations request
        if (RequestProto.HasMetaRequest() && (RequestProto.GetMetaRequest().HasCmdGetPartitionLocations()
                                              || RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo())) {
            auto it = TabletInfo.find(ev->Get()->TabletId);
            if (it != TabletInfo.end()) {
                ui32 nodeId = ev->Get()->ServerId.NodeId();
                it->second.NodeId = nodeId;
                TabletsDiscovered.insert(tabletId);

                AnswerIfCanForMeta(ctx);
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {

        ui64 tabletId = ev->Get()->TabletId;
        if (HandlePipeError(tabletId, ctx))
            return;

        ErrorReason = Sprintf("Client pipe to %" PRIu64 " destroyed (connection lost), Marker# PQ7", tabletId);
        SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::ERROR, ctx), ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        ErrorReason = Sprintf("Timeout while waiting for response, may be just slow, Marker# PQ11");
        return SendReplyAndDie(CreateErrorReply(MSTATUS_TIMEOUT, NPersQueue::NErrorCode::ERROR, ctx), ctx);
    }

    void Die(const TActorContext& ctx) override {
        for (auto& actor: PQClient) {
            NTabletPipe::CloseClient(ctx, actor);
        }
        TActorBootstrapped<TMessageBusServerPersQueueImpl>::Die(ctx);
    }

    TAutoPtr<TEvPersQueue::TEvResponse> FormEmptyCurrentRead(ui64 cookie) {
        TAutoPtr<TEvPersQueue::TEvResponse> req(new TEvPersQueue::TEvResponse);
        auto read = req->Record.MutablePartitionResponse()->MutableCmdReadResult();
        req->Record.MutablePartitionResponse()->SetCookie(cookie);
        read->SetErrorCode(NPersQueue::NErrorCode::READ_NOT_DONE);
        return req.Release();
    }

    void ProceedFetchRequest(const TActorContext& ctx) {
        if (FetchRequestCurrentReadTablet) { //already got active read request
            return;
        }
        CanProcessFetchRequest = true;
        Y_ABORT_UNLESS(IsFetchRequest);
        const auto& fetch = RequestProto.GetFetchRequest();

        if (FetchRequestReadsDone == fetch.PartitionSize()) {
            NKikimrClient::TResponse record;
            record.MutableFetchResponse()->Swap(&FetchResponse);
            record.SetStatus(MSTATUS_OK);
            record.SetErrorCode(NPersQueue::NErrorCode::OK);
            return SendReplyAndDie(std::move(record), ctx);
        }
        const auto& clientId = fetch.GetClientId();
        Y_ABORT_UNLESS(FetchRequestReadsDone < fetch.PartitionSize());
        const auto& req = fetch.GetPartition(FetchRequestReadsDone);
        const auto& topic = req.GetTopic();
        const auto& offset = req.GetOffset();
        const auto& part = req.GetPartition();
        const auto& maxBytes = req.GetMaxBytes();
        const auto& readTimestampMs = req.GetReadTimestampMs();
        auto it = TopicInfo.find(topic);
        Y_ABORT_UNLESS(it != TopicInfo.end());
        if (it->second.PartitionToTablet.find(part) == it->second.PartitionToTablet.end()) { //tablet's info is not filled for this topic yet
            return;
        }
        ui64 tabletId = it->second.PartitionToTablet[part];
        Y_ABORT_UNLESS(tabletId);
        FetchRequestCurrentReadTablet = tabletId;
        ++CurrentCookie;
        auto jt = TabletInfo.find(tabletId);
        Y_ABORT_UNLESS(jt != TabletInfo.end());
        if (jt->second.BrokenPipe || FetchRequestBytesLeft == 0) { //answer right now
            ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
            return;
        }

        //Form read request
        TAutoPtr<TEvPersQueue::TEvRequest> preq(new TEvPersQueue::TEvRequest);
        TStringBuilder reqId;
        reqId << RequestId << "-id-" << FetchRequestReadsDone << "-" << fetch.PartitionSize();
        preq->Record.SetRequestId(reqId);
        auto partReq = preq->Record.MutablePartitionRequest();
        partReq->SetCookie(CurrentCookie);
        partReq->SetTopic(topic);
        partReq->SetPartition(part);
        auto read = partReq->MutableCmdRead();
        read->SetClientId(clientId);
        read->SetOffset(offset);
        read->SetCount(1000000);
        read->SetTimeoutMs(0);
        read->SetBytes(Min<ui32>(maxBytes, FetchRequestBytesLeft));
        read->SetReadTimestampMs(readTimestampMs);
        NTabletPipe::SendData(ctx, jt->second.PipeClient, preq.Release());
    }

    void ProcessFetchRequestResult(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasPartitionResponse());
        if (record.GetPartitionResponse().GetCookie() != CurrentCookie || FetchRequestCurrentReadTablet == 0) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "proxy fetch error: got response from tablet " << record.GetPartitionResponse().GetCookie()
                                << " while waiting from " << CurrentCookie << " and requested tablet is " << FetchRequestCurrentReadTablet);
            return;
        }

        if (FetchRequestBytesLeft >= (ui32)record.ByteSize())
            FetchRequestBytesLeft -= (ui32)record.ByteSize();
        else
            FetchRequestBytesLeft = 0;
        FetchRequestCurrentReadTablet = 0;

        auto res = FetchResponse.AddPartResult();
        auto& fetch = RequestProto.GetFetchRequest();
        Y_ABORT_UNLESS(FetchRequestReadsDone < fetch.PartitionSize());
        const auto& req = fetch.GetPartition(FetchRequestReadsDone);
        const auto& topic = req.GetTopic();
        const auto& part = req.GetPartition();

        res->SetTopic(topic);
        res->SetPartition(part);
        auto read = res->MutableReadResult();
        if (record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult())
            read->CopyFrom(record.GetPartitionResponse().GetCmdReadResult());
        if (record.HasErrorCode())
            read->SetErrorCode(record.GetErrorCode());
        if (record.HasErrorReason())
            read->SetErrorReason(record.GetErrorReason());

        ++FetchRequestReadsDone;
        ProceedFetchRequest(ctx);
    }


    NKikimrClient::TResponse CreateErrorReply(EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        NKikimrClient::TResponse rec;
        rec.SetStatus(status);
        rec.SetErrorCode(code);

        if (ErrorReason.size()) {
            rec.SetErrorReason(ErrorReason);
        } else {
            rec.SetErrorReason("Unknown, Marker# PQ12");
        }
        return rec;
    }


    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "proxy got request " << RequestId << " IsMetaRequest " << IsMetaRequest << " IsFetchRequest " << IsFetchRequest);

        // handle error from constructor
        if (!!ErrorReason) {
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::BAD_REQUEST, ctx), ctx);
        }
        if (IsFetchRequest) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "scheduling HasDataInfoResponse in " << RequestProto.GetFetchRequest().GetWaitMs());
            ctx.Schedule(TDuration::MilliSeconds(Min<ui32>(RequestProto.GetFetchRequest().GetWaitMs(), 30000)), new TEvPersQueue::TEvHasDataInfoResponse);
        }

        auto* request = new TEvDescribeAllTopicsRequest();
        ctx.Send(SchemeCache, request);
        ++DescribeRequests;

        if (RequestProto.HasMetaRequest() && (RequestProto.GetMetaRequest().HasCmdGetPartitionLocations()
                || RequestProto.GetMetaRequest().HasCmdGetReadSessionsInfo())) {
            //only for this request NodeId-s and Nodes names are required
            const TActorId nameserviceId = GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        }

        Become(&TMessageBusServerPersQueueImpl::StateFunc, ctx, TDuration::MilliSeconds(DefaultTimeout), new TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            HFunc(TEvDescribeAllTopicsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvPersQueue::TEvOffsetsResponse, Handle);
            HFunc(TEvPersQueue::TEvStatusResponse, Handle);
            HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);
            HFunc(TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    )
private:
    bool GetTopicsList(const ::google::protobuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& requests) {
        for (auto ri = requests.begin(); ri != requests.end(); ++ri) {
            const auto& topicRequest = *ri;
            if (!topicRequest.HasTopic() || topicRequest.GetTopic().empty()) {
                ErrorReason = "TopicRequest must have Topic field.";
                return false;

            }
            TTopicInfo topicInfo;
            for (ui32 j = 0; j < topicRequest.PartitionSize(); ++j) {
                bool res = topicInfo.PartitionsToRequest.insert(topicRequest.GetPartition(j)).second;
                if (!res) {
                    ErrorReason = Sprintf("multiple partition %d in TopicRequest for topic '%s'", topicRequest.GetPartition(j),
                                                                                                topicRequest.GetTopic().c_str());
                    return false;
                }
            }
            const auto& topic = topicRequest.GetTopic();
            if (TopicInfo.contains(topic)) {
                ErrorReason = Sprintf("multiple TopicRequest for topic '%s'", topic.c_str());
                return false;
            } else {
                TopicInfo[topic] = std::move(topicInfo);
            }
        }
        return true;
    }
};

class TErrorReplier : public TActorBootstrapped<TErrorReplier> {
public:
    TErrorReplier(const NKikimrClient::TPersQueueRequest& request, const TActorId& /*schemeCache*/)
        : RequestId(request.HasRequestId() ? request.GetRequestId() : "<none>")
    {
    }

    virtual void SendReplyAndDie(NKikimrClient::TResponse&& response, const TActorContext& ctx) = 0;

    void Bootstrap(const TActorContext& ctx) {
        SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, NPersQueue::NErrorCode::BAD_REQUEST, ErrorText), ctx);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_BASE_REQUEST_PROCESSOR;
    }

    TString ErrorText;
    TString RequestId;
};

template <template <class TImpl, class... TArgs> class TSenderImpl, class... T>
IActor* CreatePersQueueRequestProcessor(
    const NKikimrClient::TPersQueueRequest& request,
    T&&... constructorParams
) {
    try {
        if (request.HasMetaRequest() + request.HasPartitionRequest() + request.HasFetchRequest() > 1) {
            throw std::runtime_error("only one from meta partition or fetch requests must be filled");
        }
        if (request.HasMetaRequest()) {
            auto& meta = request.GetMetaRequest();
            const size_t count = meta.HasCmdGetPartitionLocations() + meta.HasCmdGetPartitionOffsets() +
                meta.HasCmdGetTopicMetadata() + meta.HasCmdGetPartitionStatus() + meta.HasCmdGetReadSessionsInfo();
            if (count != 1) {
                throw std::runtime_error("multiple or none requests in MetaRequest");
            }
            if (meta.HasCmdGetPartitionLocations()) {
                return new TSenderImpl<TPersQueueGetPartitionLocationsProcessor>(std::forward<T>(constructorParams)...);
            } else if (meta.HasCmdGetPartitionOffsets()) {
                return new TSenderImpl<TPersQueueGetPartitionOffsetsProcessor>(std::forward<T>(constructorParams)...);
            } else if (meta.HasCmdGetTopicMetadata()) {
                return new TSenderImpl<TPersQueueGetTopicMetadataProcessor>(std::forward<T>(constructorParams)...);
            } else if (meta.HasCmdGetPartitionStatus()) {
                return new TSenderImpl<TPersQueueGetPartitionStatusProcessor>(std::forward<T>(constructorParams)...);
            } else if (meta.HasCmdGetReadSessionsInfo()) {
                return new TSenderImpl<TPersQueueGetReadSessionsInfoProcessor>(
                    std::forward<T>(constructorParams)...
                );
            } else {
                throw std::runtime_error("Not implemented yet");
            }
        } else if (request.HasPartitionRequest()) {
            return new TSenderImpl<TMessageBusServerPersQueueImpl>(std::forward<T>(constructorParams)...);
        } else if (request.HasFetchRequest()) {
            return new TSenderImpl<TMessageBusServerPersQueueImpl>(std::forward<T>(constructorParams)...);
        } else {
            throw std::runtime_error("empty request");
        }
    } catch (const std::exception& ex) {
        auto* replier = new TSenderImpl<TErrorReplier>(std::forward<T>(constructorParams)...);
        replier->ErrorText = ex.what();
        return replier;
    }
}


template <class TImplActor>
class TMessageBusServerPersQueue : public TImplActor, TMessageBusSessionIdentHolder {
public:
    template <class... T>
    TMessageBusServerPersQueue(TBusMessageContext& msg, T&&... constructorParams)
        : TImplActor(static_cast<TBusPersQueue*>(msg.GetMessage())->Record, std::forward<T>(constructorParams)...)
        , TMessageBusSessionIdentHolder(msg)
    {}

    virtual ~TMessageBusServerPersQueue() = default;

    void SendReplyAndDie(NKikimrClient::TResponse&& record, const TActorContext& ctx) override {
        THolder<TBusResponse> result(new TBusResponse());
        result->Record.Swap(&record);
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "proxy answer " << TImplActor::RequestId);

        SendReplyMove(result.Release());

        TImplActor::Die(ctx);
    }
};


IActor* CreateMessageBusServerPersQueue(
    TBusMessageContext& msg,
    const TActorId& schemeCache
) {
    const NKikimrClient::TPersQueueRequest& request = static_cast<TBusPersQueue*>(msg.GetMessage())->Record;
    return CreatePersQueueRequestProcessor<TMessageBusServerPersQueue>(
        request,
        msg,
        schemeCache
    );
}

IActor* CreateActorServerPersQueue(
    const TActorId& parentId,
    const NKikimrClient::TPersQueueRequest& request,
    const TActorId& schemeCache
) {
    return CreatePersQueueRequestProcessor<TReplierToParent>(
        request,
        parentId,
        request,
        schemeCache
    );
}

}
}
