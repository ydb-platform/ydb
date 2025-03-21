#include "actors.h"
#include "../kqp_helper.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/service.h>

constexpr ui32 TX_ABORT_RETRY_MAX_COUNT = 3;
namespace NKafka {
    struct TProducerState {
        TString TransactionalId;
        i64 ProducerId;
        i16 ProducerEpoch;
        TInstant UpdatedAt;
    };

    class TKafkaInitProducerIdActor : public NActors::TActorBootstrapped<TKafkaInitProducerIdActor> {
        public:
            using TBase = NActors::TActorBootstrapped<TKafkaInitProducerIdActor>;

            enum EInitProducerIdKqpRequests : ui8 {
                NO_REQUEST = 0,
                
                SELECT,
                INSERT,
                UPDATE,
                DELETE,
                BEGIN_TRANSACTION
            };

            TKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TString& transactionalId);

            void Bootstrap(const NActors::TActorContext& ctx);

        private:
            const TContext::TPtr Context;
            // Kafka related fields
            const ui64 CorrelationId;
            const TString TransactionalId;
            
            // kqp related staff
            std::unique_ptr<NKafka::TKqpTxHelper> Kqp;
            ui64 KqpReqCookie = 0;
            ui32 CurrentTxAbortRetryNumber = 0;
            EInitProducerIdKqpRequests LastSentToKqpRequest;

            // not tx initialization methods
            TInitProducerIdResponseData::TPtr CreateResponseWithRandomProducerId(const NActors::TActorContext& ctx);

            // state mgmt
            using TActorContext = NActors::TActorContext;
            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
                    HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
                    HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);

                    SFunc(TEvents::TEvPoison, Die);
                }
            }
            void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);
            void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
            void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
            void RequestFullRetry(const TActorContext& ctx);
            void Die(const TActorContext& ctx);

            // methods with main logic
            void StartTxProducerInitCycle(const TActorContext& ctx);
            void HandleQueryResponseFromKqp(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
            void OnTxProducerStateReceived(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
            void OnSuccessfullProducerStateUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

            // requests to producer_state table
            void SendSelectRequest(const TActorContext& ctx);
            void SendInsertRequest(const TActorContext& ctx);
            void SendUpdateRequest(const TActorContext& ctx, ui16 newProducerEpoch);
            void SendDeleteByTransactionalIdRequest(const TActorContext& ctx);

            // params builders
            NYdb::TParams BuildSelectOrDeleteByTransactionalIdParams();
            NYdb::TParams BuildInsertNewProducerStateParams();
            NYdb::TParams BuildUpdateProducerStateParams(ui16 newProducerEpoch);

            // send responses methods
            void SendResponseFail(EKafkaErrors error, const TString& message);
            void SendSuccessfullResponseForTxProducer(const TProducerState& producerState, const TActorContext& ctx);

            // helper methods
            bool IsTransactionalProducerInitialization();
            EKafkaErrors KqpStatusToKafkaError(Ydb::StatusIds::StatusCode status);
            std::optional<TProducerState> ParseProducerState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev);
            TString GetYqlWithTableName(const TString& templateStr);
            TString LogPrefix();
            TString GetAsStr(EInitProducerIdKqpRequests request);
        };

} // NKafka
