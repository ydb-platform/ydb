#pragma once

#include "datastreams_proxy.h"
#include "events.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/digest/md5/md5.h>

namespace NKikimr::NDataStreams::V1 {

    struct TPutRecordsItem {
        TString Data;
        TString Key;
        TString ExplicitHash;
        TString Ip;
    };

    TString GetSerializedData(const TPutRecordsItem& item) {
        NKikimrPQClient::TDataChunk proto;

        //TODO: get ip from client, not grpc;
        // proto.SetIp(item.Ip);

        proto.SetCodec(0); // NPersQueue::CODEC_RAW
        proto.SetData(item.Data);

        TString str;
        bool res = proto.SerializeToString(&str);
        Y_ABORT_UNLESS(res);
        return str;
    }


    class TDatastreamsPartitionActor : public TActorBootstrapped<TDatastreamsPartitionActor> {
    public:
        using TBase = TActorBootstrapped<TDatastreamsPartitionActor>;

        TDatastreamsPartitionActor(NActors::TActorId parentId, ui64 tabletId, ui32 partition, const TString& topic, TVector<TPutRecordsItem> dataToWrite, bool shouldBeCharged)
            : ParentId(std::move(parentId))
            , TabletId(tabletId)
            , Partition(partition)
            , Topic(topic)
            , DataToWrite(std::move(dataToWrite))
            , ShouldBeCharged(shouldBeCharged)
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {
                .RetryLimitCount = 6,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(100),
                .BackoffMultiplier = 2,
                .DoFirstRetryInstantly = true
            };
            PipeClient = ctx.RegisterWithSameMailbox(
                NTabletPipe::CreateClient(ctx.SelfID, TabletId, clientConfig));

            SendWriteRequest(ctx);
            Become(&TDatastreamsPartitionActor::PartitionWriteFunc);
        }

    private:
        STFUNC(PartitionWriteFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPersQueue::TEvResponse, HandlePartitionWriteResult);
                HFunc(TEvTabletPipe::TEvClientConnected, Handle);
                HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            };
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);

            if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
                ReplyWithError(ctx, TStringBuilder() << "Cannot connect to tablet " << TabletId,
                               NPersQueue::NErrorCode::ERROR);
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);

            ReplyWithError(ctx, TStringBuilder() << "Connection to tablet " << TabletId << " is dead", NPersQueue::NErrorCode::TABLET_IS_DROPPED);
        }

        void SendWriteRequest(const TActorContext& ctx) {
            NKikimrClient::TPersQueueRequest request;
            request.MutablePartitionRequest()->SetTopic(Topic);
            request.MutablePartitionRequest()->SetPartition(Partition);
            request.MutablePartitionRequest()->SetIsDirectWrite(true);
            ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
            ui64 totalSize = 0;
            for (const auto& item : DataToWrite) {
                auto w = request.MutablePartitionRequest()->AddCmdWrite();
                w->SetData(GetSerializedData(item));
                w->SetPartitionKey(item.Key);
                w->SetExplicitHash(item.ExplicitHash);
                w->SetDisableDeduplication(true);
                w->SetCreateTimeMS(TInstant::Now().MilliSeconds());
                w->SetUncompressedSize(item.Data.size());
                w->SetExternalOperation(true);
                totalSize += (item.Data.size() + item.Key.size() + item.ExplicitHash.size());
            }

            if (ShouldBeCharged) {
                request.MutablePartitionRequest()->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));
            }

            TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record.Swap(&request);

            NTabletPipe::SendData(ctx, PipeClient, req.Release());
        }

        void HandlePartitionWriteResult(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
            if (CheckForError(ev, ctx)) {
                return;
            }

            Y_ABORT_UNLESS(ev->Get()->Record.HasPartitionResponse());
            Y_ENSURE(ev->Get()->Record.GetPartitionResponse().GetCmdWriteResult().size() > 0, "Wrong number of cmd write commands");
            auto offset = ev->Get()->Record.GetPartitionResponse().GetCmdWriteResult(0).GetOffset();
            ReplySuccessAndDie(ctx, offset);
        }

        void ReplyWithError(const NActors::TActorContext& ctx, const TString& errorText, NPersQueue::NErrorCode::EErrorCode errorCode) {
            auto result = MakeHolder<NDataStreams::V1::TEvDataStreams::TEvPartitionActorResult>();
            result->PartitionId = Partition;
            result->ErrorText = errorText;
            result->ErrorCode = errorCode;
            ctx.Send(ParentId, result.Release());
            Die(ctx);
        }

        bool CheckForError(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            if (record.HasErrorCode() && record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                ReplyWithError(ctx, record.GetErrorReason(), record.GetErrorCode());
                return true;
            }
            return false;
        }

        void ReplySuccessAndDie(const NActors::TActorContext& ctx, ui64 offset) {
            auto result = MakeHolder<NDataStreams::V1::TEvDataStreams::TEvPartitionActorResult>();
            result->PartitionId = Partition;
            result->CurrentOffset = offset;
            ctx.Send(ParentId, result.Release());
            Die(ctx);
        }

        void Die(const TActorContext& ctx) override {
            if (PipeClient)
                NTabletPipe::CloseClient(ctx, PipeClient);
            TBase::Die(ctx);
        }

    private:
        NActors::TActorId ParentId;
        ui64 TabletId = 0;
        ui32 Partition = 0;
        TString Topic;
        TVector<TPutRecordsItem> DataToWrite;
        NActors::TActorId PipeClient;
        bool ShouldBeCharged;
    };

    //------------------------------------------------------------------------------------

    namespace {
        TString CheckRequestIsValid(const Ydb::DataStreams::V1::PutRecordsRequest& putRecordsRequest) {
            if (putRecordsRequest.stream_name().empty()) {
                return "Stream name is empty";
            }
            if (putRecordsRequest.records_size() > 500) {
                return TStringBuilder() << "Too many records in a single PutRecords request: " << putRecordsRequest.records_size() << " > 500";
            }
            ui64 totalSize = 0;
            for (const auto& record : putRecordsRequest.records()) {
                totalSize += record.partition_key().size() + record.data().size();
                if (record.partition_key().empty()) {
                    return "Empty partition key";
                }
                if (record.data().size() > 1_MB) {
                    return TStringBuilder() << "Data of size of " << record.data().size() << " bytes exceed limit of " << 1_MB << " bytes";
                }
                if (record.partition_key().size() > 256) {
                    return TStringBuilder() << "Partition key is too long: " << record.partition_key();
                }
                if (!record.explicit_hash_key().empty() && !IsValidDecimal(record.explicit_hash_key())) {
                    return TStringBuilder() << record.explicit_hash_key() << " is not a valid 128 bit decimal";
                }
            }
            if (totalSize > 5_MB) {
                return TStringBuilder() << "Total size of PutRecords request of " << totalSize << " bytes exceed limit of " << 5_MB << " bytes";

            }
            return "";
        }
    }

    template<class TDerived, class TProto>
    class TPutRecordsActorBase
        : public NGRpcProxy::V1::TPQGrpcSchemaBase<TPutRecordsActorBase<TDerived, TProto>, TProto>
        , private NPQ::TRlHelpers
    {
        using TBase = NGRpcProxy::V1::TPQGrpcSchemaBase<TPutRecordsActorBase<TDerived, TProto>, TProto>;

    public:
        TPutRecordsActorBase(NGRpcService::IRequestOpCtx* request);
        ~TPutRecordsActorBase() = default;

        void Bootstrap(const NActors::TActorContext &ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
        void Die(const TActorContext& ctx) override;

    protected:
        void Write(const TActorContext& ctx);
        void AddRecord(THashMap<ui32, TVector<TPutRecordsItem>>& items, const std::shared_ptr<NPQ::IPartitionChooser>& chooser, int index);
        ui64 GetPayloadSize() const;

    private:
        struct TPartitionTask {
            NActors::TActorId ActorId;
            std::vector<ui32> RecordIndexes;
        };

        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
        THashMap<ui32, TPartitionTask> PartitionToActor;
        Ydb::DataStreams::V1::PutRecordsResult PutRecordsResult;

        TString Ip;
        bool ShouldBeCharged;

        void SendNavigateRequest(const TActorContext &ctx);
        void Handle(NDataStreams::V1::TEvDataStreams::TEvPartitionActorResult::TPtr& ev,
                    const TActorContext& ctx);
        void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
        void CheckFinish(const TActorContext& ctx);

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NDataStreams::V1::TEvDataStreams::TEvPartitionActorResult, Handle);
                HFunc(TEvents::TEvWakeup, Handle);
                default: TBase::StateWork(ev);
            };
        }
    };

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::Die(const TActorContext& ctx) {
        TRlHelpers::PassAway(TDerived::SelfId());
        TBase::Die(ctx);
    }

    template<class TDerived, class TProto>
    TPutRecordsActorBase<TDerived, TProto>::TPutRecordsActorBase(NGRpcService::IRequestOpCtx* request)
            : TBase(request, dynamic_cast<const typename TProto::TRequest*>(request->GetRequest())->stream_name())
            , TRlHelpers({}, request, 4_KB, false, TDuration::Seconds(1))
            , Ip(request->GetPeerName())
    {
        Y_ENSURE(request);
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::Bootstrap(const NActors::TActorContext& ctx) {
        TString error = CheckRequestIsValid(static_cast<TDerived*>(this)->GetPutRecordsRequest());

        if (!error.empty()) {
            return this->ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                                        Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                        error);
        }

        if (this->Request_->GetSerializedToken().empty()) {
            if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                            TStringBuilder() << "Access to stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied");
            }
        }
        NACLib::TUserToken token(this->Request_->GetSerializedToken());

        ShouldBeCharged = std::find(
            AppData(ctx)->PQConfig.GetNonChargeableUser().begin(),
            AppData(ctx)->PQConfig.GetNonChargeableUser().end(),
            token.GetUserSID()) == AppData(ctx)->PQConfig.GetNonChargeableUser().end();

        SendNavigateRequest(ctx);
        this->Become(&TPutRecordsActorBase<TDerived, TProto>::StateFunc);
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::SendNavigateRequest(const TActorContext& ctx) {
        auto schemeCacheRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(this->GetTopicPath());
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;
        schemeCacheRequest->ResultSet.emplace_back(entry);
        ctx.Send(MakeSchemeCacheID(), MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (TBase::ReplyIfNotTopic(ev)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        auto topicInfo = navigate->ResultSet.begin();
        if (AppData(this->ActorContext())->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetSerializedToken());
            if (!topicInfo->SecurityObject->CheckAccess(NACLib::EAccessRights::UpdateRow, token)) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                            TStringBuilder() << "Access for stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied for subject "
                                            << token.GetUserSID());
            }
        }


        PQGroupInfo = topicInfo->PQGroupInfo;
        SetMeteringMode(PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode());

        if (!AppData(this->ActorContext())->PQConfig.GetTopicsAreFirstClassCitizen() && !PQGroupInfo->Description.GetPQTabletConfig().GetLocalDC()) {

            return this->ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                                        Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                        TStringBuilder() << "write to mirrored stream "
                                        << this->GetProtoRequest()->stream_name()
                                        << " is forbidden");
        }


        if (IsQuotaRequired()) {
            const auto ru = 1 + CalcRuConsumption(GetPayloadSize());
            Y_ABORT_UNLESS(MaybeRequestQuota(ru, EWakeupTag::RlAllowed, this->ActorContext()));
        } else {
            Write(this->ActorContext());
        }
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::Write(const TActorContext& ctx) {
        const auto& pqDescription = PQGroupInfo->Description;
        auto chooser = NPQ::CreatePartitionChooser(pqDescription, true);

        THashMap<ui32, TVector<TPutRecordsItem>> items;
        for (int i = 0; i < static_cast<TDerived*>(this)->GetPutRecordsRequest().records_size(); ++i) {
            PutRecordsResult.add_records();
            AddRecord(items, chooser, i);
        }

        for (auto& partition : pqDescription.GetPartitions()) {
            auto part = partition.GetPartitionId();
            if (items[part].empty()) continue;
            PartitionToActor[part].ActorId = ctx.Register(
                new TDatastreamsPartitionActor(ctx.SelfID, partition.GetTabletId(), part,
                                               this->GetTopicPath(), std::move(items[part]),
                                               ShouldBeCharged));
        }
        this->CheckFinish(ctx);
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::CheckFinish(const TActorContext& ctx) {
        if (PartitionToActor.size() == 0) {
            static_cast<TDerived*>(this)->SendResult(PutRecordsResult, ctx);
        }
    }

    TString GetErrorText(const NPersQueue::NErrorCode::EErrorCode errorCode) {
        if (errorCode == NPersQueue::NErrorCode::OVERLOAD)
            return "ProvisionedThroughputExceededException";
        return "InternalFailure";
        //TODO: other codes https://docs.aws.amazon.com/kinesis/latest/APIReference/CommonErrors.html
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::Handle(NDataStreams::V1::TEvDataStreams::TEvPartitionActorResult::TPtr& ev, const TActorContext& ctx) {
        auto it = PartitionToActor.find(ev->Get()->PartitionId);
        Y_ENSURE(it != PartitionToActor.end());
        if (ev->Get()->ErrorText.Defined()) {
            PutRecordsResult.set_failed_record_count(
                    PutRecordsResult.failed_record_count() + it->second.RecordIndexes.size());
        }
        PutRecordsResult.set_encryption_type(Ydb::DataStreams::V1::EncryptionType::NONE);
        for (ui32 i = 0; i < it->second.RecordIndexes.size(); ++i) {
            ui32 index = it->second.RecordIndexes[i];
            auto& record = *PutRecordsResult.mutable_records(index);
            if (ev->Get()->ErrorText.Defined()) {
                record.set_error_message(*ev->Get()->ErrorText);
                record.set_error_code(GetErrorText(*(ev->Get()->ErrorCode))); //TODO: Throttling exception sometimes
            } else {
                record.set_shard_id(GetShardName(it->first));
                record.set_sequence_number(Sprintf("%lu", ev->Get()->CurrentOffset + i));
            }
        }

        PartitionToActor.erase(it);
        CheckFinish(ctx);
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        switch (static_cast<EWakeupTag>(ev->Get()->Tag)) {
            case EWakeupTag::RlAllowed:
                return Write(ctx);
            case EWakeupTag::RlNoResource:
                PutRecordsResult.set_failed_record_count(static_cast<TDerived*>(this)->GetPutRecordsRequest().records_size());
                for (int i = 0; i < PutRecordsResult.failed_record_count(); ++i) {
                    PutRecordsResult.add_records()->set_error_code("ThrottlingException");
                }
                return this->CheckFinish(ctx);
            default:
                return this->HandleWakeup(ev, ctx);
        }
    }

    inline NYql::NDecimal::TUint128 GetHashKey(const Ydb::DataStreams::V1::PutRecordsRequestEntry& record) {
        if (record.explicit_hash_key().empty()) {
            return HexBytesToDecimal(MD5::Calc(record.partition_key()));
        } else {
            return BytesToDecimal(record.explicit_hash_key());
        }
    }

    template<class TDerived, class TProto>
    void TPutRecordsActorBase<TDerived, TProto>::AddRecord(THashMap<ui32, TVector<TPutRecordsItem>>& items, const std::shared_ptr<NPQ::IPartitionChooser>& chooser, int index) {
        const auto& record = static_cast<TDerived*>(this)->GetPutRecordsRequest().records(index);

        TString hashKey = NPQ::AsKeyBound(GetHashKey(record));
        auto* partition = chooser->GetPartition(hashKey);
        items[partition->PartitionId].push_back(TPutRecordsItem{record.data(), record.partition_key(), record.explicit_hash_key(), Ip});
        PartitionToActor[partition->PartitionId].RecordIndexes.push_back(index);
    }

    template<class TDerived, class TProto>
    ui64 TPutRecordsActorBase<TDerived, TProto>::GetPayloadSize() const {
        ui64 result = 0;

        for (const auto& record : static_cast<const TDerived*>(this)->GetPutRecordsRequest().records()) {
            result += record.data().size()
                + record.explicit_hash_key().size()
                + record.partition_key().size();
        }

        return result;
    }

    class TPutRecordsActor : public TPutRecordsActorBase<TPutRecordsActor, NKikimr::NGRpcService::TEvDataStreamsPutRecordsRequest> {
    public:
        using TBase = TPutRecordsActorBase<TPutRecordsActor, NKikimr::NGRpcService::TEvDataStreamsPutRecordsRequest>;

        TPutRecordsActor(NGRpcService::IRequestOpCtx* request)
            : TBase(request)
        {}

        const Ydb::DataStreams::V1::PutRecordsRequest& GetPutRecordsRequest() const;
        void SendResult(const Ydb::DataStreams::V1::PutRecordsResult& result, const TActorContext& ctx);
    };

    const Ydb::DataStreams::V1::PutRecordsRequest& TPutRecordsActor::GetPutRecordsRequest() const {
        return *GetProtoRequest();
    }

    void TPutRecordsActor::SendResult(const Ydb::DataStreams::V1::PutRecordsResult& result, const TActorContext& ctx) {
        this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    //-------------------------------------------------------------------------------

    class TPutRecordActor : public TPutRecordsActorBase<TPutRecordActor, NKikimr::NGRpcService::TEvDataStreamsPutRecordRequest> {
    public:
        using TBase = TPutRecordsActorBase<TPutRecordActor, NKikimr::NGRpcService::TEvDataStreamsPutRecordRequest>;

        TPutRecordActor(NGRpcService::IRequestOpCtx* request)
                : TBase(request)
        {
            PutRecordsRequest.set_stream_name(GetProtoRequest()->stream_name());
            auto& record = *PutRecordsRequest.add_records();

            record.set_data(GetProtoRequest()->data());
            record.set_explicit_hash_key(GetProtoRequest()->explicit_hash_key());
            record.set_partition_key(GetProtoRequest()->partition_key());
        }

        const Ydb::DataStreams::V1::PutRecordsRequest& GetPutRecordsRequest() const;
        void SendResult(const Ydb::DataStreams::V1::PutRecordsResult& result, const TActorContext& ctx);

    private:
        Ydb::DataStreams::V1::PutRecordsRequest PutRecordsRequest;
    };

    const Ydb::DataStreams::V1::PutRecordsRequest& TPutRecordActor::GetPutRecordsRequest() const {
        return PutRecordsRequest;
    }

    void TPutRecordActor::SendResult(const Ydb::DataStreams::V1::PutRecordsResult& putRecordsResult, const TActorContext& ctx) {
        Ydb::DataStreams::V1::PutRecordResult result;

        if (putRecordsResult.failed_record_count() == 0) {
            result.set_sequence_number(putRecordsResult.records(0).sequence_number());
            result.set_shard_id(putRecordsResult.records(0).shard_id());
            result.set_encryption_type(Ydb::DataStreams::V1::EncryptionType::NONE);
            return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        } else {
            if (putRecordsResult.records(0).error_code() == "ProvisionedThroughputExceededException"
                || putRecordsResult.records(0).error_code() == "ThrottlingException")
            {
                return ReplyWithError(Ydb::StatusIds::OVERLOADED, Ydb::PersQueue::ErrorCode::OVERLOAD, putRecordsResult.records(0).error_message());
            }
            //TODO: other codes - access denied and so on
            return ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR, putRecordsResult.records(0).error_message());

        }
    }

}
