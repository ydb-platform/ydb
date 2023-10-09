#include "msgbus_server_pq_metarequest.h"
#include "msgbus_server_pq_read_session_info.h"

namespace NKikimr {
namespace NMsgBusProxy {
using namespace NSchemeCache;

template <class TTopicResult>
void SetErrorCode(
        TTopicResult* topicResult,
        const TSchemeCacheNavigate::TEntry& topicEntry
) {
    NPersQueue::NErrorCode::EErrorCode code = NPersQueue::NErrorCode::OK;
    if (topicEntry.Status != TSchemeCacheNavigate::EStatus::Ok || !topicEntry.PQGroupInfo) {
        code = NPersQueue::NErrorCode::UNKNOWN_TOPIC;
    }
    topicResult->SetErrorCode(code);
    if (code == NPersQueue::NErrorCode::UNKNOWN_TOPIC) {
        topicResult->SetErrorReason("topic not found");
    } else if (code == NPersQueue::NErrorCode::INITIALIZING) {
        topicResult->SetErrorReason("could not describe topic");
    } else if (code != NPersQueue::NErrorCode::OK) {
        topicResult->SetErrorReason("internal error");
    }
}

//
// GetTopicMetadata command
//

TPersQueueGetTopicMetadataProcessor::TPersQueueGetTopicMetadataProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache)
    : TPersQueueBaseRequestProcessor(request, schemeCache, false)
{
    const auto& topics = RequestProto->GetMetaRequest().GetCmdGetTopicMetadata().GetTopic();
    TopicsToRequest.insert(topics.begin(), topics.end());
    if (IsIn(TopicsToRequest, TString())) {
        throw std::runtime_error("empty topic in GetTopicMetadata request");
    }
}

THolder<IActor> TPersQueueGetTopicMetadataProcessor::CreateTopicSubactor(
        const TSchemeEntry& topicEntry, const TString& name
) {
    return MakeHolder<TPersQueueGetTopicMetadataTopicWorker>(SelfId(), topicEntry, name);
}

TPersQueueGetTopicMetadataTopicWorker::TPersQueueGetTopicMetadataTopicWorker(
        const TActorId& parent, const TSchemeEntry& topicEntry, const TString& name
)
    : TReplierToParent<TTopicInfoBasedActor>(parent, topicEntry, name)
{
    SetActivityType(NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR);
}


void TPersQueueGetTopicMetadataTopicWorker::BootstrapImpl(const TActorContext& ctx) {
    auto processingResult = ProcessMetaCacheSingleTopicsResponse(SchemeEntry);
    Answer(ctx, processingResult.Status, processingResult.ErrorCode, processingResult.Reason);
}

void TPersQueueGetTopicMetadataTopicWorker::Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
    NKikimrClient::TResponse response;
    response.SetStatus(status);
    response.SetErrorCode(code);
    if (!errorReason.empty())
        response.SetErrorReason(errorReason);
    if (code == NPersQueue::NErrorCode::OK) {
        auto* topicInfo = response.MutableMetaResponse()->MutableCmdGetTopicMetadataResult()->AddTopicInfo();
        SetErrorCode(topicInfo, SchemeEntry);
        if (SchemeEntry.PQGroupInfo != nullptr) {
            const auto& desc = SchemeEntry.PQGroupInfo->Description;
            topicInfo->SetTopic(Name);
            topicInfo->MutableConfig()->CopyFrom(desc.GetPQTabletConfig());
            topicInfo->MutableConfig()->SetVersion(desc.GetAlterVersion());
            topicInfo->SetNumPartitions(desc.PartitionsSize());
        }
    }
    SendReplyAndDie(std::move(response), ctx);
}


//
// GetPartitionOffsets command
//

TPersQueueGetPartitionOffsetsProcessor::TPersQueueGetPartitionOffsetsProcessor(
        const NKikimrClient::TPersQueueRequest& request, const TActorId& metaCacheId
)
    : TPersQueueBaseRequestProcessor(request, metaCacheId, false)
{
    GetTopicsListOrThrow(RequestProto->GetMetaRequest().GetCmdGetPartitionOffsets().GetTopicRequest(), PartitionsToRequest);
}

THolder<IActor> TPersQueueGetPartitionOffsetsProcessor::CreateTopicSubactor(
        const TSchemeEntry& topicEntry, const TString& name
) {
    return MakeHolder<TPersQueueGetPartitionOffsetsTopicWorker>(
            SelfId(), topicEntry, name, PartitionsToRequest[name], RequestProto
    );
}

TPersQueueGetPartitionOffsetsTopicWorker::TPersQueueGetPartitionOffsetsTopicWorker(
        const TActorId& parent, const TSchemeEntry& topicEntry, const TString& name,
        const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
        const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto
)
    : TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvOffsetsResponse>>(parent, topicEntry, name)
    , PartitionsToRequest(partitionsToRequest)
    , RequestProto(requestProto)
{
    SetActivityType(NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR);
}

void TPersQueueGetPartitionOffsetsTopicWorker::BootstrapImpl(const TActorContext &ctx) {
    size_t partitionsAsked = 0;
    THashSet<ui64> parts;
    if (SchemeEntry.PQGroupInfo) {
        const auto& pqDescr = SchemeEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDescr.GetPartitions()) {
            const ui32 partIndex = partition.GetPartitionId();
            const ui64 tabletId = partition.GetTabletId();
            if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && !PartitionsToRequest->contains(partIndex)) {
                continue;
            }
            parts.insert(partIndex);
            ++partitionsAsked;
            if (HasTabletPipe(tabletId)) { // Take all partitions for tablet from one TEvOffsetsResponse event
                continue;
            }
            THolder<TEvPersQueue::TEvOffsets> ev(new TEvPersQueue::TEvOffsets());
            const TString& clientId = RequestProto->GetMetaRequest().GetCmdGetPartitionOffsets().GetClientId();
            if (!clientId.empty()) {
                ev->Record.SetClientId(clientId);
            }
            CreatePipeAndSend(tabletId, ctx, std::move(ev));
        }
    }
    if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && PartitionsToRequest->size() != partitionsAsked) {
        SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                             TStringBuilder() << "no one of requested partitions in topic '" << Name << "', Marker# PQ96");
        return;
    }
    if (!PartitionsToRequest.get() || PartitionsToRequest->empty()) {
        PartitionsToRequest.reset(new THashSet<ui64>());
        PartitionsToRequest->swap(parts);
    }
    if(WaitAllPipeEvents(ctx)) {
        return;
    }
}

bool TPersQueueGetPartitionOffsetsTopicWorker::OnPipeEventsAreReady(const TActorContext& ctx) {
    auto processResult = ProcessMetaCacheSingleTopicsResponse(SchemeEntry);
    Answer(ctx, processResult.Status, processResult.ErrorCode, processResult.Reason);

    return true;
}

void TPersQueueGetPartitionOffsetsTopicWorker::Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
    NKikimrClient::TResponse response;
    response.SetStatus(status);
    response.SetErrorCode(code);
    if (!errorReason.empty())
        response.SetErrorReason(errorReason);
    if (code == NPersQueue::NErrorCode::OK) {
        auto& topicResult = *response.MutableMetaResponse()->MutableCmdGetPartitionOffsetsResult()->AddTopicResult();
        topicResult.SetTopic(Name);
        SetErrorCode(&topicResult, SchemeEntry);
        THashSet<ui64> partitionsInserted;
        for (auto& ans : PipeAnswers) {
            if (ans.second.Get() != nullptr) {
                for (auto& partResult : *ans.second->Get()->Record.MutablePartResult()) {
                    const ui64 partitionIndex = partResult.GetPartition();
                    if (PartitionsToRequest.get() == nullptr || PartitionsToRequest->empty() || PartitionsToRequest->contains(partitionIndex)) {
                        topicResult.AddPartitionResult()->Swap(&partResult);
                        partitionsInserted.insert(partitionIndex);
                    }
                }
            }
        }
        if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && PartitionsToRequest->size() != partitionsInserted.size() && topicResult.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK) {
            const TString reason = "partition is not ready yet";
            for (ui64 partitionIndex : *PartitionsToRequest) {
                if (!IsIn(partitionsInserted, partitionIndex)) {
                    auto res = topicResult.AddPartitionResult();
                    res->SetPartition(partitionIndex);
                    res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                    res->SetErrorReason(reason);
                }
            }
        }
    }
    SendReplyAndDie(std::move(response), ctx);
}


//
// GetPartitionStatus command
//

TPersQueueGetPartitionStatusProcessor::TPersQueueGetPartitionStatusProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache)
    : TPersQueueBaseRequestProcessor(request, schemeCache, false)
{
    GetTopicsListOrThrow(RequestProto->GetMetaRequest().GetCmdGetPartitionStatus().GetTopicRequest(), PartitionsToRequest);
}

THolder<IActor> TPersQueueGetPartitionStatusProcessor::CreateTopicSubactor(
        const TSchemeEntry& topicEntry, const TString& name
) {
    return MakeHolder<TPersQueueGetPartitionStatusTopicWorker>(
            SelfId(), topicEntry, name, PartitionsToRequest[name], RequestProto
    );
}

TPersQueueGetPartitionStatusTopicWorker::TPersQueueGetPartitionStatusTopicWorker(
        const TActorId& parent, const TSchemeEntry& topicEntry, const TString& name,
        const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
        const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto
)
    : TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvStatusResponse>>(parent, topicEntry, name)
    , PartitionsToRequest(partitionsToRequest)
    , RequestProto(requestProto)
{
    SetActivityType(NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR);
}

void TPersQueueGetPartitionStatusTopicWorker::BootstrapImpl(const TActorContext &ctx) {
    size_t partitionsAsked = 0;
    THashSet<ui64> parts;
    if (!ProcessingResult.IsFatal) {
        const auto& pqDescr = SchemeEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDescr.GetPartitions()) {
            const ui32 partIndex = partition.GetPartitionId();
            const ui64 tabletId = partition.GetTabletId();
            if (PartitionsToRequest != nullptr && !PartitionsToRequest->empty() && !PartitionsToRequest->contains(partIndex)) {
                continue;
            }
            parts.insert(partIndex);
            ++partitionsAsked;
            if (HasTabletPipe(tabletId)) { // Take all partitions for tablet from one TEvStatusResponse event
                continue;
            }
            THolder<TEvPersQueue::TEvStatus> ev(new TEvPersQueue::TEvStatus());
            if (RequestProto->GetMetaRequest().GetCmdGetPartitionStatus().HasClientId())
                ev->Record.SetClientId(RequestProto->GetMetaRequest().GetCmdGetPartitionStatus().GetClientId());
            CreatePipeAndSend(tabletId, ctx, std::move(ev));
        }
    } else {
        SendErrorReplyAndDie(ctx, ProcessingResult.Status, ProcessingResult.ErrorCode, ProcessingResult.Reason);
        return;
    }
    if (PartitionsToRequest != nullptr && !PartitionsToRequest->empty() && PartitionsToRequest->size() != partitionsAsked) {
        SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                             TStringBuilder() << "no one of requested partitions in topic '" << Name << "', Marker# PQ97");
        return;
    }
    if (!PartitionsToRequest.get() || PartitionsToRequest->empty()) {
        PartitionsToRequest.reset(new THashSet<ui64>());
        PartitionsToRequest->swap(parts);
    }

    if (WaitAllPipeEvents(ctx))
        return;
}

bool TPersQueueGetPartitionStatusTopicWorker::OnPipeEventsAreReady(const TActorContext& ctx) {
    auto processResult = ProcessMetaCacheSingleTopicsResponse(SchemeEntry);
    Answer(ctx, processResult.Status, processResult.ErrorCode, processResult.Reason);
    return true;
}

void TPersQueueGetPartitionStatusTopicWorker::Answer(
        const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code,
        const TString& errorReason
) {
    NKikimrClient::TResponse response;
    response.SetStatus(status);
    response.SetErrorCode(code);
    if (!errorReason.empty())
        response.SetErrorReason(errorReason);
    if (code == NPersQueue::NErrorCode::OK) {
        auto& topicResult = *response.MutableMetaResponse()->MutableCmdGetPartitionStatusResult()->AddTopicResult();
        topicResult.SetTopic(Name);
        SetErrorCode(&topicResult, SchemeEntry);
        THashSet<ui64> partitionsInserted;
        for (auto& ans : PipeAnswers) {
            if (ans.second.Get() != nullptr) {
                for (auto& partResult : *ans.second->Get()->Record.MutablePartResult()) {
                    const ui64 partitionIndex = partResult.GetPartition();
                    if (PartitionsToRequest.get() == nullptr || PartitionsToRequest->empty() || PartitionsToRequest->contains(partitionIndex)) {
                        topicResult.AddPartitionResult()->Swap(&partResult);
                        if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty()) {
                            partitionsInserted.insert(partitionIndex);
                        }
                    }
                }
            }
        }
        if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && PartitionsToRequest->size() != partitionsInserted.size() && topicResult.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK) {
            const TString reason = "partition is not ready yet";
            for (ui64 partitionIndex : *PartitionsToRequest) {
                if (!IsIn(partitionsInserted, partitionIndex)) {
                    auto res = topicResult.AddPartitionResult();
                    res->SetPartition(partitionIndex);
                    res->SetStatus(NKikimrPQ::TStatusResponse::STATUS_UNKNOWN);
                }
            }
        }
    }
    SendReplyAndDie(std::move(response), ctx);
}


//
// GetPartitionLocations command
//

TPersQueueGetPartitionLocationsProcessor::TPersQueueGetPartitionLocationsProcessor(
        const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache
)
    : TPersQueueBaseRequestProcessor(request, schemeCache, true)
{
    GetTopicsListOrThrow(RequestProto->GetMetaRequest().GetCmdGetPartitionLocations().GetTopicRequest(), PartitionsToRequest);
}

THolder<IActor> TPersQueueGetPartitionLocationsProcessor::CreateTopicSubactor(
        const TSchemeEntry& topicEntry, const TString& name
) {
    Y_ABORT_UNLESS(NodesInfo.get() != nullptr);
    return MakeHolder<TPersQueueGetPartitionLocationsTopicWorker>(
            SelfId(), topicEntry, name,
            PartitionsToRequest[name], RequestProto, NodesInfo
    );
}

TPersQueueGetPartitionLocationsTopicWorker::TPersQueueGetPartitionLocationsTopicWorker(
        const TActorId& parent,
        const TSchemeEntry& topicEntry, const TString& name,
        const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
        const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
)
    : TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvTabletPipe::TEvClientConnected>>(parent, topicEntry, name)
    , PartitionsToRequest(partitionsToRequest)
    , RequestProto(requestProto)
    , NodesInfo(nodesInfo)
{
    SetActivityType(NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR);
}

void TPersQueueGetPartitionLocationsTopicWorker::BootstrapImpl(const TActorContext& ctx) {
    size_t partitionsAsked = 0;
    THashSet<ui64> parts;
    if (SchemeEntry.PQGroupInfo) {
        const auto& pqDescr = SchemeEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDescr.GetPartitions()) {
            const ui32 partIndex = partition.GetPartitionId();
            const ui64 tabletId = partition.GetTabletId();
            if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && !PartitionsToRequest->contains(partIndex)) {
                continue;
            }
            PartitionToTablet[partIndex] = tabletId;
            ++partitionsAsked;
            parts.insert(partIndex);
            if (HasTabletPipe(tabletId)) { // Take all partitions for tablet from one TEvStatusResponse event
                continue;
            }
            CreatePipe(tabletId, ctx);
        }
    }
    if (PartitionsToRequest.get() != nullptr && !PartitionsToRequest->empty() && PartitionsToRequest->size() != partitionsAsked) {
        SendErrorReplyAndDie(ctx, MSTATUS_ERROR, NPersQueue::NErrorCode::UNKNOWN_TOPIC,
                             TStringBuilder() << "no one of requested partitions in topic '" << Name << "', Marker# PQ98");
        return;
    }
    if (!PartitionsToRequest.get() || PartitionsToRequest->empty()) {
        PartitionsToRequest.reset(new THashSet<ui64>());
        PartitionsToRequest->swap(parts);
    }

    if(WaitAllConnections(ctx))
        return;
}

bool TPersQueueGetPartitionLocationsTopicWorker::OnPipeEventsAreReady(const TActorContext& ctx) {
    auto processResult = ProcessMetaCacheSingleTopicsResponse(SchemeEntry);
    Answer(ctx, processResult.Status, processResult.ErrorCode, processResult.Reason);
    return true;
}

void TPersQueueGetPartitionLocationsTopicWorker::Answer(
        const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code,
        const TString& errorReason
) {
    NKikimrClient::TResponse response;
    response.SetStatus(status);
    response.SetErrorCode(code);
    if (!errorReason.empty())
        response.SetErrorReason(errorReason);

    const auto& pqConfig = AppData(ctx)->PQConfig;

    if (code == NPersQueue::NErrorCode::OK) {
        auto& topicResult = *response.MutableMetaResponse()->MutableCmdGetPartitionLocationsResult()->AddTopicResult();
        topicResult.SetTopic(Name);
        SetErrorCode(&topicResult, SchemeEntry);
        for (const auto& partitionToTablet : PartitionToTablet) {
            const ui32 partition = partitionToTablet.first;
            const ui64 tabletId = partitionToTablet.second;
            auto& location = *topicResult.AddPartitionLocation();
            location.SetPartition(partition);

            const auto ansIt = PipeAnswers.find(tabletId);
            Y_ABORT_UNLESS(ansIt != PipeAnswers.end());
            bool statusInitializing = false;
            if (ansIt->second.Get() != nullptr && ansIt->second->Get()->Status == NKikimrProto::OK) {
                const ui32 nodeId = ansIt->second->Get()->ServerId.NodeId();
                const auto hostNameIter = NodesInfo->HostNames.find(nodeId);
                if (hostNameIter != NodesInfo->HostNames.end()) {
                    const auto& hostName = hostNameIter->second;
                    location.SetHost(hostName);
                    location.SetHostId(nodeId);

                    location.SetErrorCode(NPersQueue::NErrorCode::OK);
                    if (pqConfig.GetPQDiscoveryConfig().GetUseDynNodesMapping()) {
                        auto dynNodeIdIter = NodesInfo->DynToStaticNode->find(nodeId);
                        if (!dynNodeIdIter.IsEnd()) {
                            auto statNodeIdIter = NodesInfo->HostNames.find(dynNodeIdIter->second);
                            if (statNodeIdIter.IsEnd()) {
                                statusInitializing = true;
                            } else {
                                location.SetHostId(statNodeIdIter->first);
                                location.SetHost(statNodeIdIter->second);
                            }
                        }
                    } else if (nodeId > pqConfig.GetMaxStorageNodeId()) {
                        auto minIter = NodesInfo->MinNodeIdByHost.find(hostName);
                        // location.SetHost(hostName); - already done before
                        if (minIter.IsEnd()) {
                            location.SetHostId(nodeId);
                        } else {
                            location.SetHostId(minIter->second);
                        }
                    }
                } else {
                    statusInitializing = true;
                }
            } else {
                statusInitializing = true;
            }
            if (statusInitializing) {
                location.SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                location.SetErrorReason("Tablet for that partition is not running");
            }
        }
    }
    SendReplyAndDie(std::move(response), ctx);
}


//
// GetReadSessionsInfo command
//

TPersQueueGetReadSessionsInfoProcessor::TPersQueueGetReadSessionsInfoProcessor(
    const NKikimrClient::TPersQueueRequest& request,
    const TActorId& schemeCache
)
    : TPersQueueBaseRequestProcessor(request, schemeCache, true)
{
    const auto& cmd = RequestProto->GetMetaRequest().GetCmdGetReadSessionsInfo();
    const auto& topics = cmd.GetTopic();
    TopicsToRequest.insert(topics.begin(), topics.end());
    if (IsIn(TopicsToRequest, TString())) {
        throw std::runtime_error("empty topic in GetReadSessionsInfo request");
    }

    if (!cmd.HasClientId()) {
        throw std::runtime_error("No clientId specified in CmdGetReadSessionsInfo");
    }
}

THolder<IActor> TPersQueueGetReadSessionsInfoProcessor::CreateTopicSubactor(
        const TSchemeEntry& topicEntry, const TString& name
) {
    Y_ABORT_UNLESS(NodesInfo.get() != nullptr);
    return MakeHolder<TPersQueueGetReadSessionsInfoTopicWorker>(
            SelfId(), topicEntry, name, RequestProto, NodesInfo);
}

TPersQueueGetReadSessionsInfoTopicWorker::TPersQueueGetReadSessionsInfoTopicWorker(
        const TActorId& parent, const TSchemeEntry& topicEntry, const TString& name,
        const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
)
    : TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvOffsetsResponse>>(parent, topicEntry, name)
    , RequestProto(requestProto)
    , NodesInfo(nodesInfo)
{
    SetActivityType(NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR);
}

void TPersQueueGetReadSessionsInfoTopicWorker::Die(const TActorContext& ctx) {
    if (BalancerPipe) {
        NTabletPipe::CloseClient(ctx, BalancerPipe);
    }
    TReplierToParent::Die(ctx);
}

void TPersQueueGetReadSessionsInfoTopicWorker::SendReadSessionsInfoToBalancer(const TActorContext& ctx) {
    auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    retryPolicy.RetryLimitCount = 5;

    NTabletPipe::TClientConfig clientConfig(retryPolicy);
    BalancerPipe = ctx.RegisterWithSameMailbox(
            NTabletPipe::CreateClient(ctx.SelfID, SchemeEntry.PQGroupInfo->Description.GetBalancerTabletID(), clientConfig)
    );

    THolder<TEvPersQueue::TEvGetReadSessionsInfo> ev(new TEvPersQueue::TEvGetReadSessionsInfo());
    ev->Record.SetClientId(RequestProto->GetMetaRequest().GetCmdGetReadSessionsInfo().GetClientId());
    NTabletPipe::SendData(ctx, BalancerPipe, ev.Release());
}

void TPersQueueGetReadSessionsInfoTopicWorker::BootstrapImpl(const TActorContext &ctx) {
    if (!ProcessingResult.IsFatal) {
        SendReadSessionsInfoToBalancer(ctx);
        for (const auto& partition : SchemeEntry.PQGroupInfo->Description.GetPartitions()) {
            const ui32 partitionIndex = partition.GetPartitionId();
            const ui64 tabletId = partition.GetTabletId();
            const bool inserted = PartitionToTablet.emplace(partitionIndex, tabletId).second;
            Y_ABORT_UNLESS(inserted);

            if (HasTabletPipe(tabletId)) {
                continue;
            }

            THolder<TEvPersQueue::TEvOffsets> ev(new TEvPersQueue::TEvOffsets());
            const TString& clientId = RequestProto->GetMetaRequest().GetCmdGetReadSessionsInfo().GetClientId();
            if (!clientId.empty()) {
                ev->Record.SetClientId(clientId);
            }
            CreatePipeAndSend(tabletId, ctx, std::move(ev));
        }
    } else {
        Answer(ctx, ProcessingResult.Status, ProcessingResult.ErrorCode, ProcessingResult.Reason);
    }
    if(WaitAllPipeEvents(ctx))
        return;
}

bool TPersQueueGetReadSessionsInfoTopicWorker::WaitAllPipeEvents(const TActorContext& ctx) {
    if (TPipesWaiterActor::WaitAllPipeEvents(ctx)) {
        return true;
    }
    Become(&TPersQueueGetReadSessionsInfoTopicWorker::WaitAllPipeEventsStateFunc);
    return false;
}

THolder<IActor> TPersQueueGetReadSessionsInfoProcessor::CreateSessionsSubactor(
    const THashMap<TString, TActorId>&& readSessions,
    const TActorContext& ctx
) {
    auto factory = AppData(ctx)->PersQueueGetReadSessionsInfoWorkerFactory;
    if (factory) {
        return factory->Create(SelfId(), std::move(readSessions), NodesInfo);
    }
    return MakeHolder<TPersQueueGetReadSessionsInfoWorker>(SelfId(), std::move(readSessions), NodesInfo);
}

STFUNC(TPersQueueGetReadSessionsInfoTopicWorker::WaitAllPipeEventsStateFunc) {
    auto ctx(this->ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
    case TEvTabletPipe::TEvClientDestroyed::EventType:
        if (!HandleDestroy(ev->Get<TEvTabletPipe::TEvClientDestroyed>(), ctx)) {
            TPipesWaiterActor::WaitAllPipeEventsStateFunc(ev);
        }
        break;
    case TEvTabletPipe::TEvClientConnected::EventType:
        if (!HandleConnect(ev->Get<TEvTabletPipe::TEvClientConnected>(), ctx)) {
            TPipesWaiterActor::WaitAllPipeEventsStateFunc(ev);
        }
        break;
    default:
        TPipesWaiterActor::WaitAllPipeEventsStateFunc(ev);
    }
}

bool TPersQueueGetReadSessionsInfoTopicWorker::HandleConnect(TEvTabletPipe::TEvClientConnected* ev, const TActorContext& ctx) {
    if (ev->ClientId != BalancerPipe) {
        return false;
    }
    if (ev->Status != NKikimrProto::OK) {
        BalancerReplied = true;
        if (ReadyToAnswer()) {
            Answer(ctx, ProcessingResult.Status, ProcessingResult.ErrorCode, ProcessingResult.Reason);
        }
    } else {
        TabletNodes[GetTabletId(ev)] = ev->ServerId.NodeId();
    }
    return true;
}

bool TPersQueueGetReadSessionsInfoTopicWorker::HandleDestroy(TEvTabletPipe::TEvClientDestroyed* ev, const TActorContext& ctx) {
    if (ev->ClientId != BalancerPipe) {
        return false;
    }
    BalancerReplied = true;
    if (ReadyToAnswer()) {
        Answer(ctx, ProcessingResult.Status, ProcessingResult.ErrorCode, ProcessingResult.Reason);
    }
    return true;
}

void TPersQueueGetReadSessionsInfoTopicWorker::Handle(TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    BalancerReplied = true;
    BalancerResponse = ev;
    if (ReadyToAnswer()) {
        Answer(ctx, MSTATUS_OK, NPersQueue::NErrorCode::OK, "");
    }
}



bool TPersQueueGetReadSessionsInfoTopicWorker::OnPipeEventsAreReady(const TActorContext& ctx) {
    PipeEventsAreReady = true;
    if (ReadyToAnswer()) {
        Answer(ctx, ProcessingResult.Status, ProcessingResult.ErrorCode, ProcessingResult.Reason);
        return true;
    }
    return false;
}

bool TPersQueueGetReadSessionsInfoTopicWorker::ReadyToAnswer() const {
    return PipeEventsAreReady && BalancerReplied;
}


TString TPersQueueGetReadSessionsInfoTopicWorker::GetHostName(ui32 hostId) const {
    const auto host = NodesInfo->HostNames.find(hostId);
    return host != NodesInfo->HostNames.end() ? host->second : TString();
}

void TPersQueueGetReadSessionsInfoTopicWorker::Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
    NKikimrClient::TResponse response;
    response.SetStatus(status);
    response.SetErrorCode(code);
    if (!errorReason.empty())
        response.SetErrorReason(errorReason);
    if (code == NPersQueue::NErrorCode::OK) {

        auto stat = response.MutableMetaResponse()->MutableCmdGetReadSessionsInfoResult();
        auto topicRes = stat->AddTopicResult();
        topicRes->SetTopic(Name);
        SetErrorCode(topicRes, SchemeEntry);
        THashMap<ui32, ui32> partitionToResp;
        ui32 index = 0;
        if (BalancerResponse.Get() != nullptr) {
            for (const auto& resp : BalancerResponse->Get()->Record.GetPartitionInfo()) {
                partitionToResp[resp.GetPartition()] = index++;
                auto res = topicRes->AddPartitionResult();
                res->SetPartition(resp.GetPartition());
                res->SetSession(resp.GetSession());
                res->SetClientNode(resp.GetClientNode());
                res->SetTimestamp(resp.GetTimestamp() > 0 ? TInstant::Seconds(resp.GetTimestamp()).ToString() : "");
                res->SetProxyNode(GetHostName(resp.GetProxyNodeId()));

                res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                res->SetErrorReason("Getting of session info failed");
            }
            THolder<TEvPersQueue::TEvReadSessionsInfoResponse> request = MakeHolder<TEvPersQueue::TEvReadSessionsInfoResponse>();
            request->Record.Swap(&(BalancerResponse->Get()->Record));
            request->Record.ClearPartitionInfo();

            ctx.Send(Parent, request.Release());
        } else if (topicRes->GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK) {
            for (const auto& partition : SchemeEntry.PQGroupInfo->Description.GetPartitions()) {
                const ui32 partitionIndex = partition.GetPartitionId();
                partitionToResp[partitionIndex] = index++;
                auto res = topicRes->AddPartitionResult();
                res->SetPartition(partitionIndex);
                res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                res->SetErrorReason("balancer tablet for partition is not running");
            }
            SendReplyAndDie(std::move(response), ctx);
            return;
        }

        for (const auto& partition : SchemeEntry.PQGroupInfo->Description.GetPartitions()) {
            const ui32 partitionIndex = partition.GetPartitionId();
            if (partitionToResp.find(partitionIndex) != partitionToResp.end())
                continue;
            partitionToResp[partitionIndex] = index++;
            auto res = topicRes->AddPartitionResult();
            res->SetPartition(partitionIndex);
            res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            res->SetErrorReason("tablet for partition is not running");
        }

        for (const auto& pipeAnswer : PipeAnswers) {
            if (!pipeAnswer.second) {
                continue;
            }
            const auto& offsetResp = pipeAnswer.second->Get()->Record;
            const ui64 tabletId = pipeAnswer.first;
            for (const auto& partResult : offsetResp.GetPartResult()) {
                const ui32 partitionIndex = partResult.GetPartition();
                if (PartitionToTablet.find(partitionIndex) == PartitionToTablet.end()) {
                    continue;
                }
                const auto responseIndex = partitionToResp.find(partitionIndex);
                auto res = responseIndex != partitionToResp.end() ? topicRes->MutablePartitionResult(responseIndex->second) : topicRes->AddPartitionResult();
                res->SetPartition(partitionIndex);
                res->SetClientOffset(partResult.GetClientOffset()); // 0 if there is no offset
                res->SetStartOffset(partResult.GetStartOffset());
                res->SetEndOffset(partResult.GetEndOffset());
                const ui64 nowMS = TAppData::TimeProvider->Now().MilliSeconds();
                res->SetTimeLag(partResult.HasWriteTimestampMS() ? Max<i64>(nowMS - partResult.GetWriteTimestampMS(), 0) : 0);
                res->SetReadTimeLag(partResult.HasReadWriteTimestampMS() ? Max<i64>(nowMS - partResult.GetReadWriteTimestampMS(), 0) : 0);
                res->SetClientReadOffset(partResult.GetClientReadOffset());
                res->SetErrorCode(NPersQueue::NErrorCode::OK);
                res->ClearErrorReason();

                auto itTabletNode = TabletNodes.find(tabletId);
                if (itTabletNode != TabletNodes.end()) {
                    res->SetTabletNode(GetHostName(itTabletNode->second));
                    res->SetTabletNodeId(itTabletNode->second);
                }
                if (res->GetTabletNode().empty()) {
                    res->SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
                    res->SetErrorReason("Tablet for partition is not running");
                }
            }
        }
    }
    SendReplyAndDie(std::move(response), ctx);
}

bool TPersQueueGetReadSessionsInfoTopicWorker::OnClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletNodes[GetTabletId(ev->Get())] = ev->Get()->ServerId.NodeId();
    return false;
}

} // namespace NMsgBusProxy
} // namespace NKikimr
