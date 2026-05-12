#pragma once

#include "kafka_events.h"
#include "kafka_producer_instance_id.h"
#include "ydb/core/base/appdata_fwd.h"
#include "ydb/core/base/feature_flags.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include <ydb/core/kafka_proxy/kafka_consumer_members_metadata_initializers.h>
#include <ydb/core/kafka_proxy/kqp_helper.h>



namespace NKafka {
    /*
    This class serves as a proxy between Kafka SDK and TTransactionActor

    It validates that requester is not a zombie (by checking request's tranasactional_id+producer_id+producer_epoch)
    It does so by maintaining a set of the most relevant for this node tranasactional_id+producer_id+producer_epoch.
    Receives updates from init_producer_id_actors.
    */

    const ui32 TABLES_COUNT = 2;
    class TTransactionsCoordinator : public NActors::TActorBootstrapped<TTransactionsCoordinator> {

        using TBase = NActors::TActorBootstrapped<TTransactionsCoordinator>;

        struct TProducerInstance {
            TProducerInstanceId Id;
            ui64 TxnTimeoutMs;
        };

        struct TTransactionalRequest {
            TString TransactionalId;
            TProducerInstanceId ProducerState;
            ui64 CorrelationId;
            TActorId ConnectionId;
        };

        public:
            void Bootstrap(const TActorContext& ctx) {
                TAppData* appData = AppData(ctx);
                TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = appData->DynamicNameserviceConfig;
                if (dynamicNameserviceConfig && dynamicNameserviceConfig->MinDynamicNodeId <= ctx.SelfID.NodeId()) {
                    std::unique_ptr<NKafka::TKqpTxHelper> Kqp = std::make_unique<TKqpTxHelper>(AppData(ctx)->TenantName);
                    Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant());
                    Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant());
                }
                TBase::Become(&TTransactionsCoordinator::StateWork);
            }

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionsCoordinator ";
            }

            void PassAway() override;
        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvKafka::TEvSaveTxnProducerRequest, Handle);
                    HFunc(TEvKafka::TEvAddPartitionsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvAddOffsetsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvTxnOffsetCommitRequest, Handle);
                    HFunc(TEvKafka::TEvEndTxnRequest, Handle);
                    HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
                    HFunc(TEvKafka::TEvTransactionActorDied, Handle);
                    HFunc(TEvents::TEvPoison, Handle);
                }
            }

            // Handles new transactional_id+producer_id+producer_epoch: saves for validation of future requests
            void Handle(TEvKafka::TEvSaveTxnProducerRequest::TPtr& ev, const TActorContext& ctx);

            // Proxies requests to the relevant TTransactionActor
            void Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx);

            // tables creation responses
            void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);

            // remove transaction actor id from TxnActorByTransactionalId
            void Handle(TEvKafka::TEvTransactionActorDied::TPtr& ev, const TActorContext& ctx);
            // Will kill all txn actors
            void Handle(TEvents::TEvPoison::TPtr& ev, const TActorContext& ctx);

            template<class ErrorResponseType, class EventType>
            void HandleTransactionalRequest(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx);
            template<class ErrorResponseType, class RequestType>
            void SendProducerFencedResponse(TMessagePtr<RequestType> kafkaRequest, const TString& error, const TTransactionalRequest& request);
            template<class EventType>
            void ForwardToTransactionActor(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx);

            void DeleteTransactionActor(const TString& transactionalId);
            bool NewProducerStateIsOutdated(const TProducerInstanceId& currentProducerState, const TProducerInstanceId& newProducerState);
            TMaybe<TString> GetTxnRequestError(const TTransactionalRequest& request);
            TString GetProducerIsOutdatedError(const TString& transactionalId, const TProducerInstanceId& currentProducerState, const TProducerInstanceId& newProducerState);

            std::unordered_map<TString, TProducerInstance> ProducersByTransactionalId;
            std::unordered_map<TString, TActorId> TxnActorByTransactionalId;

            ui32 TablesInited = 0;
    };

    inline NActors::IActor* CreateTransactionsCoordinator() {
        return new TTransactionsCoordinator();
    };

    inline TActorId MakeTransactionsServiceID(ui32 nodeId) {
        static const char x[12] = "kafka_txns";
        return TActorId(nodeId, TStringBuf(x, 12));
    };

    inline bool IsTransactionalApiKey(i16 apiKey) {
        switch (apiKey) {
            case ADD_PARTITIONS_TO_TXN:
            case ADD_OFFSETS_TO_TXN:
            case TXN_OFFSET_COMMIT:
            case END_TXN:
                return true;
            default:
                return false;
        }
    }

    inline bool TransactionsEnabled() {
        return NKikimr::AppData()->FeatureFlags.GetEnableKafkaTransactions();
    }

} // namespace NKafka
