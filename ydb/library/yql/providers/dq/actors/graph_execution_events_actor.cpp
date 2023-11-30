#include "graph_execution_events_actor.h"
#include "actor_helpers.h"
#include "full_result_writer.h"
#include "events.h"

#include <ydb/library/actors/core/event_pb.h>

#include <util/generic/yexception.h>

namespace NYql::NDqs {

class TGraphExecutionEventsActor : public TRichActor<TGraphExecutionEventsActor> {
public:
    static constexpr const char ActorName[] = "YQL_DQ_GRAPH_EXECUTION_EVENTS_ACTOR";

    TGraphExecutionEventsActor(const TString& traceID, std::vector<IDqTaskPreprocessor::TPtr>&& taskPreprocessors)
        : TRichActor<TGraphExecutionEventsActor>(&TGraphExecutionEventsActor::Handler)
        , TraceID(traceID)
        , TaskPreprocessors(std::move(taskPreprocessors))
    {
    }

private:
    const TString TraceID;
    std::vector<IDqTaskPreprocessor::TPtr> TaskPreprocessors;

    STRICT_STFUNC(Handler, {
        HFunc(NDqs::TEvGraphExecutionEvent, OnEvent);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    })

    void DoPassAway() override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceID);
    }

    void OnEvent(NDqs::TEvGraphExecutionEvent::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceID);
        YQL_CLOG(DEBUG, ProviderDq)  << __FUNCTION__ << ' ' << NYql::NDqProto::EGraphExecutionEventType_Name(ev->Get()->Record.GetEventType());

        try {
            switch (ev->Get()->Record.GetEventType()) {
            case NYql::NDqProto::EGraphExecutionEventType::START: {
                NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor payload;
                ev->Get()->Record.GetMessage().UnpackTo(&payload);
                OnStart(ev->Sender, payload);
                break;
            }
            case NYql::NDqProto::EGraphExecutionEventType::SUCCESS:
                OnSuccess(ev->Sender);
                break;
            case NYql::NDqProto::EGraphExecutionEventType::FAIL:
                OnFail(ev->Sender);
                break;
            case NYql::NDqProto::EGraphExecutionEventType::FULL_RESULT: {
                NDqProto::TGraphExecutionEvent::TFullResultDescriptor payload;
                ev->Get()->Record.GetMessage().UnpackTo(&payload);
                OnFullResult(ev->Sender, payload);
                break;
            }
            default:
                Reply(ev->Sender);
                break;
            }
        } catch (...) {
            TString message = TStringBuilder() << "Error on TEvGraphExecutionEvent: " << CurrentExceptionMessage();
            YQL_CLOG(ERROR, ProviderDq) << message;
            Reply(ev->Sender, message);
        }
    }

    template <class TKey, class TValue>
    static THashMap<TKey, TValue> AsHashMap(const google::protobuf::Map<TKey, TValue>& map) {
        return THashMap<TKey, TValue>(map.begin(), map.end());
    }

    void OnStart(NActors::TActorId replyTo, const NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor& payload) {
        YQL_CLOG(DEBUG, ProviderDq)  << __FUNCTION__;
        const auto& secureParams = AsHashMap(payload.GetSecureParams().GetData());
        const auto& graphParams = AsHashMap(payload.GetGraphParams().GetData());

        NDqProto::TGraphExecutionEvent::TMap response;

        auto* addedParams = response.MutableData();
        for (const auto& preprocessor: TaskPreprocessors) {
            auto newParams = preprocessor->GetTaskParams(graphParams, secureParams);
            for (const auto& [k, v] : newParams) {
                addedParams->insert({k, v});
            }
        }

        Reply(replyTo, response);
    }

    void Reply(NActors::TActorId replyTo, TString msg = "") const {
        NYql::NDqProto::TGraphExecutionEvent sync;
        sync.SetEventType(NDqProto::EGraphExecutionEventType::SYNC);
        if (msg) {
            sync.SetErrorMessage(msg);
        }
        Send(replyTo, MakeHolder<NDqs::TEvGraphExecutionEvent>(sync));
    }

    template <class TPayload>
    void Reply(NActors::TActorId replyTo, const TPayload& resp) const {
        NYql::NDqProto::TGraphExecutionEvent sync;
        sync.SetEventType(NDqProto::EGraphExecutionEventType::SYNC);
        sync.MutableMessage()->PackFrom(resp);
        Send(replyTo, MakeHolder<NDqs::TEvGraphExecutionEvent>(sync));
    }

    void OnFail(NActors::TActorId replyTo) {
        YQL_CLOG(DEBUG, ProviderDq)  << __FUNCTION__;
        for (const auto& preprocessor: TaskPreprocessors) {
            preprocessor->Finish(false);
        }
        Reply(replyTo);
    }

    void OnSuccess(NActors::TActorId replyTo) {
        YQL_CLOG(DEBUG, ProviderDq)  << __FUNCTION__;
        for (const auto& preprocessor: TaskPreprocessors) {
            preprocessor->Finish(true);
        }
        Reply(replyTo);
    }

    void OnFullResult(NActors::TActorId replyTo, const NDqProto::TGraphExecutionEvent::TFullResultDescriptor& payload) {
        YQL_CLOG(DEBUG, ProviderDq)  << __FUNCTION__;
        THolder<IDqFullResultWriter> writer;
        for (const auto& preprocessor: TaskPreprocessors) {
            writer = preprocessor->CreateFullResultWriter();
            if (writer) {
                break;
            }
        }
        if (writer) {
            auto actor = MakeFullResultWriterActor(TraceID, payload.GetResultType(), std::move(writer), replyTo);
            auto fullResultWriterID = RegisterChild(actor.Release(), nullptr, EExecutorPoolType::FullResultWriter);
            NActorsProto::TActorId fullResultWriterProto;
            NActors::ActorIdToProto(fullResultWriterID, &fullResultWriterProto);
            Reply(replyTo, fullResultWriterProto);
        } else {
            Reply(replyTo, TString{"Failed to create full result writer"});
        }
    }
};

NActors::IActor* MakeGraphExecutionEventsActor(const TString& traceID, std::vector<IDqTaskPreprocessor::TPtr>&& taskPreprocessors) {
    return new TGraphExecutionEventsActor(traceID, std::move(taskPreprocessors));
}

} // namespace NYql::NDqs
