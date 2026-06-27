#pragma once

#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

    template<class T>
    void SetProtoTime(T* proto, const ui64 ms) {
        proto->set_seconds(ms / 1000);
        proto->set_nanos((ms % 1000) * 1'000'000);
    }
    
    template<class T>
    void UpdateProtoTime(T& proto, const T& time, bool storeMin) {
        bool cmp = proto.seconds() > time.seconds() || (proto.seconds() == time.seconds() && proto.nanos() > time.nanos());
        if (cmp == storeMin) {
            proto.CopyFrom(time);
        }
    }

    inline void AddWindowsStat(Ydb::Topic::MultipleWindowsStat *stat, ui64 perMin, ui64 perHour, ui64 perDay) {
        stat->set_per_minute(stat->per_minute() + perMin);
        stat->set_per_hour(stat->per_hour() + perHour);
        stat->set_per_day(stat->per_day() + perDay);
    }


    template<class TDerived, typename TRequest>
    class TDescribeBaseActor: public TGrpcProxyActor<TDerived, TRequest>
                            , protected NPQ::TPipeCacheClient
                            , public NPQ::TConstantLogPrefix {
        using TBase = TGrpcProxyActor<TDerived, TRequest>;

        static constexpr NKikimrServices::EServiceKikimr Service = NKikimrServices::EServiceKikimr::PQ_SCHEMA;

    public:
        TDescribeBaseActor(NGRpcService::IRequestOpCtx* request, NPQ::NDescriber::TAccessRights accessRights)
            : TBase(request)
            , NPQ::TPipeCacheClient(this)
            , AccessRights(std::move(accessRights))
        {
        }

        TStringBuilder LogBuilder() const {
            return TStringBuilder() << "[" << this->SelfId() << "]";
        }

        TString BuildLogPrefix() const override {
            return TStringBuilder() << "[" << typeid(TDerived).name() << "]";
        }

        virtual const google::protobuf::Message& MakeResult() = 0;
        virtual bool ValidateSchema() = 0;
        virtual bool NeedProcessPartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) = 0;
        virtual std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() = 0;
        virtual std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() = 0;

        void DoAction() {
            LOG_D("DoAction " << this->GetProtoRequest()->path());
            this->RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(
                this->SelfId(),
                this->GetDatabase(),
                { this->GetProtoRequest()->path() },
                {
                    .UserToken = this->GetUserToken(),
                    .AccessRights = AccessRights,
                }
            ));
            this->Become(&TDescribeBaseActor::StateDescribe);

            ReadSessionsReceived = !this->GetProtoRequest()->include_stats();
            LocationsReceived = !this->GetProtoRequest()->include_location() && !this->GetProtoRequest()->include_stats();
        }

        void PassAway() override {
            LOG_D("PassAway");
            TBase::PassAway();
            NPQ::TPipeCacheClient::Close();
        }

        STATEFN(StateDescribe) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
            default:
                this->StateFuncBase(ev);
            }
        }

        void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
            TopicInfo = std::move(ev->Get()->Topics.begin()->second);
            LOG_D("Handle TEvDescribeTopicsResponse. Status=" << TopicInfo.Status);

            if (TopicInfo.Status != NPQ::NDescriber::EStatus::SUCCESS) {
                auto asIssueCode = [](Ydb::StatusIds::StatusCode status) {
                    switch (status) {
                        case Ydb::StatusIds::SUCCESS:
                            return Ydb::PersQueue::ErrorCode::OK;
                        case Ydb::StatusIds::NOT_FOUND:
                        case Ydb::StatusIds::UNAUTHORIZED:
                            return Ydb::PersQueue::ErrorCode::ACCESS_DENIED;
                        default:
                            return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
                    }
                };

                return this->ReplyWithError(
                    Ydb::StatusIds::SCHEME_ERROR,
                    NPQ::NDescriber::Description(this->GetProtoRequest()->path(), TopicInfo.Status),
                    asIssueCode(NPQ::NDescriber::Convert(TopicInfo.Status))
                );
            }

            ReadBalancerTabletId = TopicInfo.Info->Description.GetBalancerTabletID();

            if (!ValidateSchema()) {
                return;
            }

            ConvertDirectoryEntry(TopicInfo.Self->Info, &SelfEntry, true);
            if (TopicInfo.CdcStream) {
                SelfEntry.set_name(std::move(TopicInfo.CdcStreamName));
            }

            RequestReadBalancer();

            if (this->GetProtoRequest()->include_stats()) {
                for (const auto& partition : TopicInfo.Info->Description.GetPartitions()) {
                    if (!NeedProcessPartition(partition)) {
                        continue;
                    }
                    if (TabletsInflight.contains(partition.GetTabletId())) {
                        continue;
                    }
                    RequestStats(partition.GetTabletId());
                }
            }

            if (this->ReplyIfPossible()) {
                return;
            }

            this->Become(&TDescribeBaseActor::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle);
                hFunc(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
                hFunc(NKikimr::TEvPersQueue::TEvStatusResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
            default:
                this->StateFuncBase(ev);
            }
        }

        void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev) {
            LOG_D("Handle TEvGetPartitionsLocationResponse");
            if (!TabletsInflight.contains(ReadBalancerTabletId)) {
                return;
            }

            const auto& record = ev->Get()->Record;
            if (!record.GetStatus()) {
                RequestReadBalancer();
                return;
            }

            for (const auto& location : record.GetLocations()) {
                auto& l = Partitions[location.GetPartitionId()].Location;
                l.set_node_id(location.GetNodeId());
                l.set_generation(location.GetGeneration());
            }

            LocationsReceived = true;

            if (LocationsReceived && ReadSessionsReceived) {
                TabletsInflight.erase(ReadBalancerTabletId);
                ReplyIfPossible();
            }
        }

        void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev) {
            const auto tabletId = ev->Cookie;
            LOG_D("Handle TEvStatusResponse. TabletId=" << tabletId);
            if (!TabletsInflight.contains(tabletId)) {
                return;
            }

            auto& record = ev->Get()->Record;
            for (const auto& partResult : record.GetPartResult()) {
                Ydb::Topic::DescribeConsumerResult::PartitionInfo& partRes = Partitions[partResult.GetPartition()].Stats;
                Ydb::Topic::PartitionStats* partStats = partRes.mutable_partition_stats();
        
                partStats->set_store_size_bytes(partResult.GetPartitionSize());
                partStats->mutable_partition_offsets()->set_start(partResult.GetStartOffset());
                partStats->mutable_partition_offsets()->set_end(partResult.GetEndOffset());
        
                SetProtoTime(partStats->mutable_last_write_time(), partResult.GetLastWriteTimestampMs());
                SetProtoTime(partStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs());

                AddWindowsStat(
                    partStats->mutable_bytes_written(),
                    partResult.GetAvgWriteSpeedPerMin(),
                    partResult.GetAvgWriteSpeedPerHour(),
                    partResult.GetAvgWriteSpeedPerDay()
                );
        
                const auto& lagInfo = partResult.GetLagsInfo();
        
                auto consStats = partRes.mutable_partition_consumer_stats();

                consStats->set_last_read_offset(lagInfo.GetReadPosition().GetOffset());
                consStats->set_committed_offset(lagInfo.GetWritePosition().GetOffset());

                SetProtoTime(consStats->mutable_last_read_time(), lagInfo.GetLastReadTimestampMs());
                SetProtoTime(consStats->mutable_max_read_time_lag(), lagInfo.GetReadLagMs());
                SetProtoTime(consStats->mutable_max_write_time_lag(), lagInfo.GetWriteLagMs());
                SetProtoTime(consStats->mutable_max_committed_time_lag(), lagInfo.GetCommitedLagMs());

                AddWindowsStat(
                    consStats->mutable_bytes_read(),
                    partResult.GetAvgReadSpeedPerMin(),
                    partResult.GetAvgReadSpeedPerHour(),
                    partResult.GetAvgReadSpeedPerDay()
                );
            }

            TabletsInflight.erase(tabletId);
            ReplyIfPossible();
        }

        void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev) {
            LOG_D("Handle TEvReadSessionsInfoResponse");
            if (!TabletsInflight.contains(ReadBalancerTabletId)) {
                return;
            }
            
            for (auto& partition : *ev->Get()->Record.MutablePartitionInfo()) {
                const auto partitionId = partition.GetPartition();
                Partitions[partitionId].ReadSession = std::move(partition);
            }

            ReadSessionsReceived = true;
            if (LocationsReceived && ReadSessionsReceived) {
                TabletsInflight.erase(ReadBalancerTabletId);
                ReplyIfPossible();
            }
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            LOG_D("Handle TEvDeliveryProblem. TabletId=" << ev->Get()->TabletId);
            if (OnUndelivered(ev)) {
                return;
            }

            if (!TabletsInflight.contains(ev->Get()->TabletId)) {
                return;
            }

            if (ev->Get()->TabletId == ReadBalancerTabletId) {
                RequestReadBalancer();
            } else {
                RequestStats(ev->Get()->TabletId);
            }
        }

        bool ReplyIfPossible() {
            if (TabletsInflight.empty()) {
                LOG_D("ReplyWithResult");
                this->ReplyWithResult(Ydb::StatusIds::SUCCESS, this->MakeResult());
                return true;
            }

            LOG_D("Waiting for tablets inflight: " << JoinSeq(", ", TabletsInflight));
            return false;
        }

        void RequestReadBalancer() {
            if (LocationsReceived && ReadSessionsReceived) {
                return;
            }
            if (!LocationsReceived) {
                LOG_D("PartitionsLocation " << ReadBalancerTabletId);
                SendToTablet(ReadBalancerTabletId, new TEvPersQueue::TEvGetPartitionsLocation());
            }
            if (!ReadSessionsReceived && this->GetProtoRequest()->include_stats()) {
                auto ev = CreateReadSessionsInfoRequest();
                if (ev) {
                    LOG_D("ReadSessionsInfo " << ReadBalancerTabletId);
                    SendToTablet(ReadBalancerTabletId, ev.release());
                } else {
                    ReadSessionsReceived = true;
                }
            }
            TabletsInflight.insert(ReadBalancerTabletId);
        }

        void RequestStats(ui64 tabletId) {
            LOG_D("Stats " << tabletId);
            SendToTablet(tabletId, CreateStatusRequest().release(), tabletId);
            TabletsInflight.insert(tabletId);
        }

        protected:
            const NPQ::NDescriber::TAccessRights AccessRights;
        
            NPQ::NDescriber::TTopicInfo TopicInfo;

            ui64 ReadBalancerTabletId = 0;
            absl::flat_hash_set<ui64> TabletsInflight;

            bool LocationsReceived = false;
            bool ReadSessionsReceived = false;

            Ydb::Scheme::Entry SelfEntry;

            struct TPartitionInfo {
                Ydb::Topic::PartitionLocation Location;
                Ydb::Topic::DescribeConsumerResult::PartitionInfo Stats;
                NKikimrPQ::TReadSessionsInfoResponse::TPartitionInfo ReadSession;
            };
            absl::flat_hash_map<ui32, TPartitionInfo> Partitions;
    };

} // namespace NKikimr::NGRpcProxy::V1::NTopic
