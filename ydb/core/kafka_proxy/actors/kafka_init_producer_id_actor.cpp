#include "kafka_init_producer_id_actor.h"
#include "kafka_init_producer_id_actor_sql.cpp"
#include "../kafka_transactional_producers_initializers.h"
#include "../kqp_helper.h"

#include <util/random/random.h>
#include <ydb/public/sdk/cpp/src/client/params/impl.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/protos/kafka.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>
#include <util/datetime/base.h>
#include <regex>

namespace NKafka {

    NActors::IActor* CreateKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TInitProducerIdRequestData>& message) {
        return new TKafkaInitProducerIdActor(context, correlationId, message->TransactionalId.value_or(""));
    }

    // for non-transactional INIT_PRODUCER_ID request - just return random producer.id and 0 as epoch
    // for transactional INIT_PRODUCER_ID request algorythm is below: 
    // 1. Init tables in Bootstrap()
    // 2. Create KQP session in Handle(TEvManagerPrepared)
    // 3. Send Begin transaction in Handle(TEvCreateSessionResponse)
    // 4. Send Select in Handle(TEvQueryResponse) when LastSentToKqpRequest == BEGIN_TRANSACTION
    // 5. switch (SelectResult) in Handle(TEvQueryResponse) when LastSentToKqpRequest == SELECT
    //       case (RowDoesNotExist): Send Insert
    //       case (RowExistsAndEpochWillOverflow): 
    //          we need to obtain new producer_id for this transactional_id
    //          producer_id field is serial and thus we can just delete-insert row to obtain new producer_id
    //          a. First send Delete
    //          b. Then send Insert in Handle(TEvQueryResponse) when LastSentToKqpRequest == DELETE
    //       case (RowExistsAndEpochWillNotOverflow): Update epoch in table and return in to the client
    // 6. Send reponse in Handle(TEvQueryResponse) with new (if insert) producer.id and epoch
    TKafkaInitProducerIdActor::TKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TString& transactionalId)
        : Context(context)
        , CorrelationId(correlationId)
        , TransactionalId(std::move(transactionalId)) {
    }
        
    void TKafkaInitProducerIdActor::Bootstrap(const NActors::TActorContext& ctx) {
        if (IsTransactionalProducerInitialization()) {
            Kqp = std::make_unique<TKqpTxHelper>(Context->DatabasePath);
            Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TTransactionalProducersInitManager::GetInstant());
            Become(&TKafkaInitProducerIdActor::StateWork);
        } else {
            TInitProducerIdResponseData::TPtr response = CreateResponseWithRandomProducerId(ctx);
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
            Die(ctx);
        }
    }

    TInitProducerIdResponseData::TPtr TKafkaInitProducerIdActor::CreateResponseWithRandomProducerId(const NActors::TActorContext& ctx) {
        TInitProducerIdResponseData::TPtr response = std::make_shared<TInitProducerIdResponseData>();

        response->ProducerEpoch = 0;
        response->ProducerId = ((ctx.Now().MilliSeconds() << 16) & 0x7FFFFFFFFFFF) + RandomNumber<ui16>();
        response->ErrorCode = EKafkaErrors::NONE_ERROR;
        response->ThrottleTimeMs = 0;

        return response;
    }

    void TKafkaInitProducerIdActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
        Kqp->SendCreateSessionRequest(ctx);
    }

    void TKafkaInitProducerIdActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
            SendResponseFail(EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to create KQP session");
            Die(ctx);
            return;
        }

        StartTxProducerInitCycle(ctx);
    }

    void TKafkaInitProducerIdActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (ev->Cookie != KqpReqCookie) {
            KAFKA_LOG_CRIT(TStringBuilder() << "Unexpected cookie in TEvQueryResponse. Expected: " << KqpReqCookie << ", Actual: " << ev->Cookie << ".");
            SendResponseFail(EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to send request to producer_state table");
            Die(ctx);
            return;
        }

        const auto& record = ev->Get()->Record;
        auto status = record.GetYdbStatus();
        if (status == Ydb::StatusIds::ABORTED) {
            if (CurrentTxAbortRetryNumber < TX_ABORT_RETRY_MAX_COUNT) {
                KAFKA_LOG_ERROR(TStringBuilder() << "Retry after tx aborted. CurrentTxAbortRetryNumber# " << static_cast<int>(CurrentTxAbortRetryNumber));
                RequestFullRetry(ctx);
                return;
            }
        }

        auto kafkaErr = KqpStatusToKafkaError(status);

        if (kafkaErr != EKafkaErrors::NONE_ERROR) {
            auto kqpQueryError = TStringBuilder() <<" Kqp error. Status# " << status << ", ";

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
            kqpQueryError << issues.ToString();

            SendResponseFail(kafkaErr, kqpQueryError);
            Die(ctx);
            return;
        }

        HandleQueryResponseFromKqp(ev, ctx);
    }

    void TKafkaInitProducerIdActor::RequestFullRetry(const TActorContext& ctx) {
        CurrentTxAbortRetryNumber++;
        StartTxProducerInitCycle(ctx);
    }
    
    void TKafkaInitProducerIdActor::Die(const TActorContext& ctx) {
        KAFKA_LOG_D("Pass away.");
        if (Kqp) {
            Kqp->CloseKqpSession(ctx);
        }
        TBase::Die(ctx);
    }

    void TKafkaInitProducerIdActor::StartTxProducerInitCycle(const TActorContext& ctx) {
        Kqp->BeginTransaction(++KqpReqCookie, ctx);
        LastSentToKqpRequest = EInitProducerIdKqpRequests::BEGIN_TRANSACTION;
    }

    void TKafkaInitProducerIdActor::HandleQueryResponseFromKqp(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
        KAFKA_LOG_D(TStringBuilder() << "Handle kqp response for " << GetAsStr(LastSentToKqpRequest) << " request");

        try {
            switch (LastSentToKqpRequest) {
                case BEGIN_TRANSACTION:
                    SendSelectRequest(ctx);
                    break;
                case SELECT:
                    OnTxProducerStateReceived(ev, ctx);
                    break;
                case INSERT:
                case UPDATE:
                    OnSuccessfullProducerStateUpdate(ev, ctx);
                    break;
                case DELETE:
                    SendInsertRequest(ctx);
                    break;
                default:
                    KAFKA_LOG_ERROR("Unknown EInitProducerIdKqpRequests");
                    Die(ctx);
                    break;
            }
        } catch (const yexception& y) {
            SendResponseFail(EKafkaErrors::BROKER_NOT_AVAILABLE, TStringBuilder() << "Failed to handle reponse from KQP. Caused by: " << y.what());
            Die(ctx);
        }
    }

    void TKafkaInitProducerIdActor::OnTxProducerStateReceived(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
        auto producerState = ParseProducerState(ev);

        if (!producerState) {
            SendInsertRequest(ctx);
        } 
        // if epoch will overflow we need to delete-insert row in this transaction
        // so that new producer id (serial) is assigned to this transactional id
        else if (producerState->ProducerEpoch == std::numeric_limits<i16>::max() - 1) {
            SendDeleteByTransactionalIdRequest(ctx);
        } 
        // else we increment epoch and persist in the database
        else {
            SendUpdateRequest(ctx, producerState->ProducerEpoch + 1);
        }
    }

    void TKafkaInitProducerIdActor::OnSuccessfullProducerStateUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
        auto producerState = ParseProducerState(ev).value();

        SendSuccessfullResponseForTxProducer(producerState, ctx);
    }

    // requests to producer_state table
    void TKafkaInitProducerIdActor::SendSelectRequest(const TActorContext& ctx) {
        Kqp->SendYqlRequest(GetYqlWithTableName(NInitProducerIdSql::SELECT_BY_TRANSACTIONAL_ID), BuildSelectOrDeleteByTransactionalIdParams(), ++KqpReqCookie, ctx, false);
        
        LastSentToKqpRequest = EInitProducerIdKqpRequests::SELECT;
    }

    void TKafkaInitProducerIdActor::SendInsertRequest(const TActorContext& ctx) {
        Kqp->SendYqlRequest(GetYqlWithTableName(NInitProducerIdSql::INSERT_NEW_TRANSACTIONAL_ID), BuildInsertNewProducerStateParams(), ++KqpReqCookie, ctx, true);
        
        LastSentToKqpRequest = EInitProducerIdKqpRequests::INSERT;
    }

    void TKafkaInitProducerIdActor::SendUpdateRequest(const TActorContext& ctx, ui16 newProducerEpoch) {
        Kqp->SendYqlRequest(GetYqlWithTableName(NInitProducerIdSql::UPDATE_PRODUCER_EPOCH), BuildUpdateProducerStateParams(newProducerEpoch), ++KqpReqCookie, ctx, true);
        
        LastSentToKqpRequest = EInitProducerIdKqpRequests::UPDATE;
    }

    void TKafkaInitProducerIdActor::SendDeleteByTransactionalIdRequest(const TActorContext& ctx) {
        Kqp->SendYqlRequest(GetYqlWithTableName(NInitProducerIdSql::DELETE_BY_TRANSACTIONAL_ID), BuildSelectOrDeleteByTransactionalIdParams(), ++KqpReqCookie, ctx, false);
        
        LastSentToKqpRequest = EInitProducerIdKqpRequests::DELETE;
    }

    // params builders
    NYdb::TParams TKafkaInitProducerIdActor::BuildSelectOrDeleteByTransactionalIdParams() {
        NYdb::TParamsBuilder params;
        params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
        params.AddParam("$TransactionalId").Utf8(TransactionalId).Build();

        return params.Build();
    }

    NYdb::TParams TKafkaInitProducerIdActor::BuildInsertNewProducerStateParams() {
        NYdb::TParamsBuilder params;
        params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
        params.AddParam("$TransactionalId").Utf8(TransactionalId).Build();
        params.AddParam("$ProducerEpoch").Int16(0).Build();
        params.AddParam("$UpdatedAt").Datetime(TInstant::Now()).Build();

        return params.Build();
    }

    NYdb::TParams TKafkaInitProducerIdActor::BuildUpdateProducerStateParams(ui16 newProducerEpoch) {
        NYdb::TParamsBuilder params;
        params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
        params.AddParam("$TransactionalId").Utf8(TransactionalId).Build();
        params.AddParam("$ProducerEpoch").Int16(newProducerEpoch).Build();
        params.AddParam("$UpdatedAt").Datetime(TInstant::Now()).Build();

        return params.Build();
    }

    // send responses methods
    void TKafkaInitProducerIdActor::SendResponseFail(EKafkaErrors error, const TString& message) {
        KAFKA_LOG_ERROR(TStringBuilder() << "request failed. reason# " << message);
        auto response = std::make_shared<TInitProducerIdResponseData>();
        response->ErrorCode = error;
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, error));
    }

    void TKafkaInitProducerIdActor::SendSuccessfullResponseForTxProducer(const TProducerState& producerState, const TActorContext& ctx) {
        KAFKA_LOG_D("Sending succesfull response for transactional producer");
        auto response = std::make_shared<TInitProducerIdResponseData>();
        response->ErrorCode = EKafkaErrors::NONE_ERROR;
        response->ProducerId = producerState.ProducerId;
        response->ProducerEpoch = producerState.ProducerEpoch;
        
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, EKafkaErrors::NONE_ERROR));
        Die(ctx);
    }

    // helper methods
    bool TKafkaInitProducerIdActor::IsTransactionalProducerInitialization() {
        return NKikimr::AppData()->FeatureFlags.GetEnableKafkaTransactions() && !TransactionalId.empty();
    }

    EKafkaErrors TKafkaInitProducerIdActor::KqpStatusToKafkaError(Ydb::StatusIds::StatusCode status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            return EKafkaErrors::NONE_ERROR;
        } else if (status == Ydb::StatusIds::ABORTED) {
            return EKafkaErrors::BROKER_NOT_AVAILABLE;
        } 
        return EKafkaErrors::INVALID_REQUEST;
    }

    std::optional<TProducerState> TKafkaInitProducerIdActor::ParseProducerState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev) {
        if (!ev) {
            throw yexception() << "Event can't be null!";
        }

        auto& record = ev->Get()->Record;
        auto& resp = record.GetResponse();

        NYdb::TResultSetParser parser(resp.GetYdbResults(0));

        // for this transactional id there is no rows
        if (parser.RowsCount() == 0) {
            return {};
        } 
        // there are multiple rows for this transactional id. This is unexpected and should not happen
        else if (parser.RowsCount() > 1) {
            throw yexception() << "Request returned more than one row: " << resp.GetYdbResults().size();
        } else {
            parser.TryNextRow();

            TProducerState result; 

            result.TransactionalId = parser.ColumnParser("transactional_id").GetUtf8();
            result.ProducerId = parser.ColumnParser("producer_id").GetInt64();
            result.ProducerEpoch = parser.ColumnParser("producer_epoch").GetInt16();
            result.UpdatedAt = parser.ColumnParser("updated_at").GetDatetime();

            return result;
        }
    }

    TString TKafkaInitProducerIdActor::GetYqlWithTableName(const TString& templateStr) {
        return std::regex_replace(
            templateStr.c_str(),
            std::regex("<table_name>"), 
            NKikimr::NGRpcProxy::V1::TTransactionalProducersInitManager::GetInstant()->GetStorageTablePath().c_str()
        );
    }

    TString TKafkaInitProducerIdActor::LogPrefix() {
        return "InitProducerId actor: ";
    }

    TString TKafkaInitProducerIdActor::GetAsStr(EInitProducerIdKqpRequests request) {
        switch (request) {
            case BEGIN_TRANSACTION:
                return "BEGIN_TRANSACTION";
            case SELECT:
                return "SELECT";
            case INSERT:
                return "INSERT";
            case UPDATE:
                return "UPDATE";
            case DELETE:
                return "DELETE";
            case NO_REQUEST:
                return "NO_REQUEST";
            }
    }
} // namespace NKafka
