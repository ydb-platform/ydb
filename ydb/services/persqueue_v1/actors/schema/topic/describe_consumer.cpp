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

namespace {
    template<class T>
    void SetProtoTime(T* proto, const ui64 ms) {
        proto->set_seconds(ms / 1000);
        proto->set_nanos((ms % 1000) * 1'000'000);
    }
    
    template<class T>
    void UpdateProtoTime(T* proto, const ui64 ms, bool storeMin) {
        ui64 storedMs = proto->seconds() * 1000 + proto->nanos() / 1'000'000;
        if ((ms < storedMs) == storeMin) {
            SetProtoTime(proto, ms);
        }
    }

    void AddWindowsStat(Ydb::Topic::MultipleWindowsStat *stat, ui64 perMin, ui64 perHour, ui64 perDay) {
        stat->set_per_minute(stat->per_minute() + perMin);
        stat->set_per_hour(stat->per_hour() + perHour);
        stat->set_per_day(stat->per_day() + perDay);
    }


    class TDescribeConsumerActor: public TGrpcProxyActor<TDescribeConsumerActor, NGRpcService::TEvDescribeConsumerRequest>
                                , public NPQ::TPipeCacheClient {
        using TBase = TGrpcProxyActor<TDescribeConsumerActor, NGRpcService::TEvDescribeConsumerRequest>;

        public:
        TDescribeConsumerActor(NGRpcService::IRequestOpCtx* request)
            : TBase(request)
            , NPQ::TPipeCacheClient(this)
        {
        }

        void Bootstrap() {
            RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(
                SelfId(),
                GetDatabase(),
                { GetProtoRequest()->path() },
                {
                    .UserToken = GetUserToken(),
                    .AccessRights = NACLib::EAccessRights::DescribeSchema,
                }
            ));
            Become(&TDescribeConsumerActor::StateDescribe);

            ReadSessionsReceived = !GetProtoRequest()->include_stats();
            LocationsReceived = !GetProtoRequest()->include_location() && ReadSessionsReceived;
        }

        void PassAway() override {
            TBase::PassAway();
            NPQ::TPipeCacheClient::Close();
        }

        STATEFN(StateDescribe) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
            }
        }

        void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
            const auto& consumerName = GetProtoRequest()->consumer();
            const auto& topicInfo = ev->Get()->Topics.begin()->second;

            if (topicInfo.Status != NPQ::NDescriber::EStatus::SUCCESS) {
                return ReplyWithError(
                    Ydb::StatusIds::SCHEME_ERROR,
                    NPQ::NDescriber::Description(GetProtoRequest()->path(), topicInfo.Status)
                );
            }

            ReadBalancerTabletId = topicInfo.Info->Description.GetBalancerTabletID();

            const auto normalizedConsumerName = NPersQueue::ConvertNewConsumerName(consumerName, ActorContext());
            const auto* consumer = NPQ::GetConsumer(topicInfo.Info->Description.GetPQTabletConfig(), normalizedConsumerName);
            if (!consumer) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "no consumer '" << consumerName << "' in topic");
            }

            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumer(*Result.mutable_consumer(), *consumer, status, error, false)) {
                return ReplyWithError(status, error);
            }

            ConsumerName = consumer->GetName();

            Ydb::Scheme::Entry *selfEntry = Result.mutable_self();
            ConvertDirectoryEntry(topicInfo.Self->Info, selfEntry, true);
            //TODO: change entry
            //if (const auto& name = GetCdcStreamName()) {
            //    selfEntry->set_name(*name);
            //}
            selfEntry->set_name(selfEntry->name() + "/" + consumerName);
        

            for (const auto& partition : topicInfo.Info->Description.GetPartitions()) {
                auto part = Result.add_partitions();
                part->set_partition_id(partition.GetPartitionId());
                part->set_active(partition.GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);
            }

            RequestReadBalancer();

            if (GetProtoRequest()->include_stats()) {
                for (const auto& partition : topicInfo.Info->Description.GetPartitions()) {
                    if (TabletsInflight.contains(partition.GetTabletId())) {
                        continue;
                    }
                    RequestStats(partition.GetTabletId());
                }
            }

            if (ReplyIfPossible()) {
                return;
            }

            Become(&TDescribeConsumerActor::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle);
                hFunc(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
                hFunc(NKikimr::TEvPersQueue::TEvStatusResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
            default:
                Y_VERIFY_DEBUG_S(false, (TStringBuilder() << "Unexpected event " << ev->GetTypeName()).c_str());
            }
        }

        void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev) {
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
            if (!TabletsInflight.contains(tabletId)) {
                return;
            }

            auto& record = ev->Get()->Record;
            for (const auto& partResult : record.GetPartResult()) {
                Ydb::Topic::DescribeConsumerResult::PartitionInfo* partRes = &Partitions[partResult.GetPartition()].Stats;
                Ydb::Topic::PartitionStats* partStats = partRes->mutable_partition_stats();
        
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
        
                auto consStats = partRes->mutable_partition_consumer_stats();

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

                if (!Result.consumer().has_consumer_stats()) {
                    auto* stats = Result.mutable_consumer()->mutable_consumer_stats();

                    SetProtoTime(stats->mutable_min_partitions_last_read_time(), lagInfo.GetLastReadTimestampMs());
                    SetProtoTime(stats->mutable_max_read_time_lag(), lagInfo.GetReadLagMs());
                    SetProtoTime(stats->mutable_max_write_time_lag(), lagInfo.GetWriteLagMs());
                    SetProtoTime(stats->mutable_max_committed_time_lag(), lagInfo.GetCommitedLagMs());
                } else {
                    auto* stats = Result.mutable_consumer()->mutable_consumer_stats();

                    UpdateProtoTime(stats->mutable_min_partitions_last_read_time(), lagInfo.GetLastReadTimestampMs(), true);
                    UpdateProtoTime(stats->mutable_max_read_time_lag(), lagInfo.GetReadLagMs(), false);
                    UpdateProtoTime(stats->mutable_max_write_time_lag(), lagInfo.GetWriteLagMs(), false);
                    UpdateProtoTime(stats->mutable_max_committed_time_lag(), lagInfo.GetCommitedLagMs(), false);
                }
            }

            TabletsInflight.erase(tabletId);
            ReplyIfPossible();
        }

        void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev) {
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
                const auto includeLocation = GetProtoRequest()->include_location();
                const auto includeStats = GetProtoRequest()->include_stats();

                if (includeLocation || includeStats) {
                    for (auto& partition : *Result.mutable_partitions()) {
                        auto it = Partitions.find(partition.partition_id());
                        if (it == Partitions.end()) {
                            continue;
                        }

                        auto& partitionInfo = it->second;

                        if (includeLocation) {
                            *partition.mutable_partition_location() = partitionInfo.Location;
                        }

                        if (includeStats) {
                            auto* partitionStats = partition.mutable_partition_stats();
                            *partitionStats = partitionInfo.Stats.partition_stats();
                            partitionStats->set_partition_node_id(partitionInfo.Location.node_id());

                            auto* consumerStats = partition.mutable_partition_consumer_stats();
                            *consumerStats = partitionInfo.Stats.partition_consumer_stats();
                            consumerStats->set_read_session_id(std::move(partitionInfo.ReadSession.GetSession()));
                            SetProtoTime(consumerStats->mutable_partition_read_session_create_time(), std::move(partitionInfo.ReadSession.GetTimestampMs()));
                            consumerStats->set_connection_node_id(std::move(partitionInfo.ReadSession.GetProxyNodeId()));
                            consumerStats->set_reader_name(std::move(partitionInfo.ReadSession.GetClientNode()));
                        }
                    }
                }

                ReplyWithResult(Ydb::StatusIds::SUCCESS, Result);
                return true;
            }

            return false;
        }

        void RequestReadBalancer() {
            if (LocationsReceived && ReadSessionsReceived) {
                return;
            }
            if (!LocationsReceived) {
                SendToTablet(ReadBalancerTabletId, new TEvPersQueue::TEvGetPartitionsLocation());
            }
            if (!ReadSessionsReceived && GetProtoRequest()->include_stats()) {
                SendToTablet(ReadBalancerTabletId, new TEvPersQueue::TEvGetReadSessionsInfo(NPersQueue::ConvertNewConsumerName(ConsumerName, ActorContext())));
            }
            TabletsInflight.insert(ReadBalancerTabletId);
        }

        void RequestStats(ui64 tabletId) {
            auto ev = std::make_unique<TEvPersQueue::TEvStatus>();
            ev->Record.AddConsumers(ConsumerName);
            SendToTablet(tabletId, ev.release(), tabletId);
            TabletsInflight.insert(tabletId);
        }

        private:
            Ydb::Topic::DescribeConsumerResult Result;

            TString ConsumerName;
            ui64 ReadBalancerTabletId = 0;
            absl::flat_hash_set<ui64> TabletsInflight;

            bool LocationsReceived = false;
            bool ReadSessionsReceived = false;

            struct TPartitionInfo {
                Ydb::Topic::PartitionLocation Location;
                Ydb::Topic::DescribeConsumerResult::PartitionInfo Stats;
                NKikimrPQ::TReadSessionsInfoResponse::TPartitionInfo ReadSession;
            };

            absl::flat_hash_map<ui32, TPartitionInfo> Partitions;
    };

} // namespace

NActors::IActor* CreateDescribeConsumerActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribeConsumerActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
