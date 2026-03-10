#include "mlp_reader.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

namespace NKikimr::NPQ::NMLP {

TReaderActor::TReaderActor(const TActorId& parentId, const TReaderSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_READER)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TReaderActor::Bootstrap() {
    DoDescribe();
}

void TReaderActor::DoDescribe() {
    LOG_D("Start describe");
    Become(&TReaderActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::SelectRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TReaderActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            Info = topic.Info;
            ConsumerConfig = GetConsumer(Info->Description.GetPQTabletConfig(), Settings.Consumer);
            if (!ConsumerConfig) {
                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Consumer '" << Settings.Consumer << "' does not exist");
            }
            return DoSelectPartition();
        }
        default: {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TReaderActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TReaderActor::DoSelectPartition() {
    LOG_D("Start select partition");
    Become(&TReaderActor::SelectPartitionState);
    SendToTablet(Info->Description.GetBalancerTabletID(), new TEvPQ::TEvMLPGetPartitionRequest(Settings.TopicName, Settings.Consumer));
}

void TReaderActor::Handle(TEvPQ::TEvMLPGetPartitionResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPGetPartitionResponse " << ev->Get()->Record.ShortDebugString());
    auto* result = ev->Get();
    switch (result->GetStatus()) {
        case Ydb::StatusIds::SUCCESS: {
            PartitionId = result->GetPartitionId();
            PQTabletId = result->GetTabletId();
            return DoRead();
        }
        default:
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Partition choose error");
    }
}

void TReaderActor::HandleOnSelectPartition(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie != Cookie) {
        return;
    }
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    if (Backoff.HasMore()) {
        Backoff.Next();
        return DoSelectPartition();
    }
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Pipe error");
}

STFUNC(TReaderActor::SelectPartitionState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPGetPartitionResponse, Handle);
        hFunc(TEvPQ::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnSelectPartition);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TReaderActor::DoRead() {
    LOG_D("Start read");
    Become(&TReaderActor::ReadState);

    auto* request = new TEvPQ::TEvMLPReadRequest(
        Settings.TopicName,
        Settings.Consumer,
        PartitionId,
        Settings.WaitTime ? Settings.WaitTime->ToDeadLine() : TDuration::MilliSeconds(ConsumerConfig->GetDefaultReceiveMessageWaitTimeMs()).ToDeadLine(),
        Settings.ProcessingTimeout ? Settings.ProcessingTimeout.value() : TDuration::Seconds(ConsumerConfig->GetDefaultProcessingTimeoutSeconds()),
        Settings.MaxNumberOfMessage
    );
    SendToTablet(PQTabletId, request);
}

void TReaderActor::Handle(TEvPQ::TEvMLPReadResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPReadResponse");

    auto response = std::make_unique<TEvReadResponse>();
    for (auto& message : *ev->Get()->Record.MutableMessage()) {
        NKikimrPQClient::TDataChunk proto;
        bool res = proto.ParseFromString(message.GetData());
        if (!res) {
            LOG_W("Error parsing data. Offset " << message.GetId().GetOffset());
            // Skip message
            continue;
        }

        TString data;
        Ydb::Topic::Codec codec;
        if (Settings.UncompressMessages && proto.has_codec() && proto.codec() != Ydb::Topic::CODEC_RAW - 1) {
            const NYdb::NTopic::ICodec* codecImpl = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(proto.codec() + 1));
            data = codecImpl->Decompress(proto.GetData());
            codec = static_cast<Ydb::Topic::Codec>(proto.codec() + 1);
        } else {
            data = std::move(*proto.MutableData());
            codec = Ydb::Topic::CODEC_RAW;
        }

        THashMap<TString, TString> messageMetaAttr(proto.messagemeta_size());
        for (const auto& meta : proto.messagemeta()) {
            messageMetaAttr.try_emplace(meta.key(), meta.value());
        }

        TString messageGroupId;
        auto it = messageMetaAttr.find(MESSAGE_ATTRIBUTE_KEY);
        if (it != messageMetaAttr.end()) {
            messageGroupId = std::move(it->second);
        }
        response->Messages.push_back(TEvReadResponse::TMessage{
            .MessageId = {PartitionId, message.GetId().GetOffset()},
            .Codec = codec,
            .Data = std::move(data),
            .MessageMetaAttributes = std::move(messageMetaAttr),
            .SentTimestamp = TInstant::MilliSeconds(message.messagemeta().senttimestampmilliseconds()),
            .MessageGroupId = messageGroupId,
        });
    }

    Send(ParentId, std::move(response));
    PassAway();
}

void TReaderActor::Handle(TEvPQ::TEvMLPErrorResponse::TPtr& ev) {
    // TODO MLP Retry
    LOG_D("Handle TEvPQ::TEvMLPErrorResponse " << ev->Get()->Record.ShortDebugString());
    ReplyErrorAndDie(ev->Get()->GetStatus(), std::move(ev->Get()->GetErrorMessage()));
}

void TReaderActor::HandleOnRead(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie != Cookie) {
        return;
    }
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    if (Backoff.HasMore()) {
        Backoff.Next();
        return DoRead();
    }
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Pipe error");
}

STFUNC(TReaderActor::ReadState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadResponse, Handle);
        hFunc(TEvPQ::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnRead);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}


void TReaderActor::SendToTablet(ui64 tabletId, IEventBase *ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, ++Cookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

void TReaderActor::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    LOG_I("Reply error " << Ydb::StatusIds::StatusCode_Name(errorCode));
    Send(ParentId, new TEvReadResponse(errorCode, std::move(errorMessage)));
    PassAway();
}

void TReaderActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

bool TReaderActor::OnUnhandledException(const std::exception& exc) {
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR,
        TStringBuilder() <<"Unhandled exception: " << exc.what());
    return TBaseActor::OnUnhandledException(exc);
}

IActor* CreateReader(const NActors::TActorId& parentId, TReaderSettings&& settings) {
    return new TReaderActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NMLP
