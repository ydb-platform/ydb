#include "mlp_writer.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

namespace NKikimr::NPQ::NMLP {

TWriterActor::TWriterActor(const TActorId& parentId, const TWriterSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_WRITER)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TWriterActor::Bootstrap() {
    DoDescribe();
}

void TWriterActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

void TWriterActor::DoDescribe() {
    LOG_D("Start describe");
    Become(&TWriterActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::UpdateRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TWriterActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    DescribeStatus = topic.Status;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            TopicInfo = topic.Info;
            return DoWrite();
        }
        default: {
            ReplyErrorAndDie();
        }
    }
}

STFUNC(TWriterActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

namespace {

size_t SerializeTo(TWriterSettings::TMessage& item, ::NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& cmdWrite) {
    size_t totalSize = item.MessageBody.size() + (item.SerializedMessageAttributes.has_value() ?
        item.SerializedMessageAttributes.value().size() : 0);

    cmdWrite.SetSourceId("");
    cmdWrite.SetDisableDeduplication(true);
    cmdWrite.SetCreateTimeMS(TInstant::Now().MilliSeconds());
    cmdWrite.SetUncompressedSize(item.MessageBody.size());
    cmdWrite.SetExternalOperation(true);

    NKikimrPQClient::TDataChunk proto;
    proto.SetCodec(0); // NPersQueue::CODEC_RAW
    proto.SetData(std::move(item.MessageBody));

    if (item.MessageGroupId) {
        auto* m = proto.AddMessageMeta();
        m->set_key(MESSAGE_ATTRIBUTE_KEY);
        m->set_value(std::move(*item.MessageGroupId));
    }
    if (item.MessageDeduplicationId.has_value()) {
        auto* m = proto.AddMessageMeta();
        m->set_key(MESSAGE_ATTRIBUTE_DEDUPLICATION_ID);
        m->set_value(std::move(*item.MessageDeduplicationId));
    }
    if (item.SerializedMessageAttributes.has_value()) {
        auto* m = proto.AddMessageMeta();
        m->set_key(MESSAGE_ATTRIBUTE_ATTRIBUTES);
        m->set_value(std::move(*item.SerializedMessageAttributes));
    }
    if (item.Delay != TDuration::Zero()) {
        auto* m = proto.AddMessageMeta();
        m->set_key(MESSAGE_ATTRIBUTE_DELAY_SECONDS);
        m->set_value(ToString(item.Delay.Seconds()));
    }

    TString dataStr;
    bool res = proto.SerializeToString(&dataStr);
    Y_ABORT_UNLESS(res);
    cmdWrite.SetData(dataStr);

    return totalSize;
}

}

void TWriterActor::DoWrite() {
    LOG_D("Start write");
    Become(&TWriterActor::WriteState);

    struct TInfo {
        std::unique_ptr<TEvPersQueue::TEvRequest> Request;
        size_t Size;
    };
    std::unordered_map<ui64, TInfo> requests;

    for (auto& message : Settings.Messages) {
        const IPartitionChooser::TPartitionInfo* partition;
        if (message.MessageGroupId.has_value()) {
            partition = TopicInfo->PartitionChooser->GetPartition(message.MessageGroupId.value());
        } else {
            partition = TopicInfo->PartitionChooser->GetRandomPartition();
        }

        AFL_ENSURE(partition)("t", TopicInfo->Description.GetName());

        PendingMessages.push_back({
            .Index = message.Index,
            .TabletId = partition->TabletId,
            .PartitionId = partition->PartitionId,
        });

        auto it = requests.find(partition->PartitionId);
        if (it == requests.end()) {
            it = requests.emplace(partition->PartitionId, TInfo{ std::make_unique<TEvPersQueue::TEvRequest>(), 0 }).first;

            auto* request = it->second.Request->Record.MutablePartitionRequest();
            request->SetTopic(Settings.TopicName);
            request->SetPartition(partition->PartitionId);
            request->SetIsDirectWrite(true);
            request->SetCookie(partition->PartitionId);
        }

        auto* write = it->second.Request->Record.MutablePartitionRequest()->AddCmdWrite();
        auto size = SerializeTo(message, *write);
        it->second.Size += size;
    }

    for (auto& [partitionId, info] : requests) {
        auto& request = info.Request;
        auto& totalSize = info.Size;

        if (Settings.ShouldBeCharged) {
            request->Record.MutablePartitionRequest()->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));
        }

        const auto* node = TopicInfo->PartitionGraph->GetPartition(partitionId);
        AFL_ENSURE(node)("p", partitionId);
        SendToTablet(node->TabletId, request.release());
        ++PendingRequests;
    }

    ReplyIfPossible();
}

void TWriterActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvResponse");

    bool alreadyReceived = false;
    auto& record = ev->Get()->Record;
    if (IsSuccess(record)) {
        size_t i = 0;
        auto& response = record.GetPartitionResponse();
        auto partitionId = response.GetCookie();
        for (auto& message : PendingMessages) {
            if (message.PartitionId != partitionId) {
                continue;
            }

            alreadyReceived = message.ResultReceived;
            message.ResultReceived = true;

            if (i < response.CmdWriteResultSize()) {
                auto& result = response.GetCmdWriteResult(i);
                message.Status = result.GetAlreadyWritten() ? Ydb::StatusIds::ALREADY_EXISTS : Ydb::StatusIds::SUCCESS;
                message.Offset = result.GetOffset();
                ++i;
            }
        }
    }

    if (!alreadyReceived) {
        --PendingRequests;
    }

    ReplyIfPossible();
}

void TWriterActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");

    const auto tabletId = ev->Get()->TabletId;

    std::unordered_set<ui32> partitions;
    for (auto& message : PendingMessages) {
        if (message.TabletId != tabletId || message.ResultReceived) {
            continue;
        }
        message.ResultReceived = true;
        message.Status = Ydb::StatusIds::INTERNAL_ERROR;
        partitions.insert(message.PartitionId);
    }

    PendingRequests -= std::min(partitions.size(), PendingRequests);
    ReplyIfPossible();
}

STFUNC(TWriterActor::WriteState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TWriterActor::SendToTablet(ui64 tabletId, IEventBase *ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, tabletId);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

bool TWriterActor::OnUnhandledException(const std::exception& exc) {
    LOG_C("unhandled exception " << TypeName(exc) << ": " << exc.what() << Endl
        << TBackTrace::FromCurrentException().PrintToString());

    PendingRequests = 0;
    ReplyIfPossible();

    return true;
}

bool TWriterActor::IsSuccess(const NKikimrClient::TResponse& record) {
    if (record.HasErrorCode() && record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        LOG_W("Write error: " << record.ShortDebugString());
        return false;
    }
    if (!record.HasPartitionResponse()) {
        LOG_W("Missing partition response: " << record.ShortDebugString());
        return false;
    }

    return true;
}

void TWriterActor::ReplyErrorAndDie() {
    PendingRequests = 0;
    ReplyIfPossible();
}

void TWriterActor::ReplyIfPossible() {
    if (PendingRequests > 0) {
        return;
    }

    auto response = std::make_unique<TEvWriteResponse>();
    response->DescribeStatus = DescribeStatus;
    for (auto& message : PendingMessages) {
        std::optional<TMessageId> messageId;
        if (message.Status == Ydb::StatusIds::SUCCESS || message.Status == Ydb::StatusIds::ALREADY_EXISTS) {
            messageId = {
                .PartitionId = message.PartitionId,
                .Offset = message.Offset
            };
        }
        response->Messages.push_back({
            .Index = message.Index,
            .Status = message.Status,
            .MessageId = messageId,
        });
    }

    Send(ParentId, std::move(response));
    PassAway();
}

IActor* CreateWriter(const NActors::TActorId& parentId, TWriterSettings&& settings) {
    return new TWriterActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NMLP
