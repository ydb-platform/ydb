#pragma once

#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/actors/transform/dq_transform_actor.h>
#include <ydb/library/yql/dq/actors/transform/dq_transform_events.h>
#include <ydb/library/yql/dq/actors/transform/dq_transform_actor_factory.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor.h>

namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr;

class TCloudFunctionTransformActor : public NActors::TActor<TCloudFunctionTransformActor>
                                    , public IDqTransformActor
                                    , public TThrRefBase
{
public:
    struct TCFTransformEvent {
        enum {
            EV_BEGIN = EventSpaceBegin(TEvents::ES_PRIVATE),
            EV_TRANSFORM_SUCCESS = EV_BEGIN,
            EV_EXECUTE_TRANSFORM
        };

        struct TEvTransformSuccess : public TEventLocal<TEvTransformSuccess, EV_TRANSFORM_SUCCESS> {
            TEvTransformSuccess(IHTTPGateway::TContent&& result, bool lastBatch)
            : Result(std::move(result))
            , LastBatch(lastBatch)
            {}

            IHTTPGateway::TContent Result;
            bool LastBatch = false;
        };

        struct TEvExecuteTransform : public TEventLocal<TEvExecuteTransform, EV_EXECUTE_TRANSFORM> {
            TEvExecuteTransform() {}
        };
    };

public:
    using TPtr = TIntrusivePtr<TCloudFunctionTransformActor>;

    static constexpr char ActorName[] = "YQL_DQ_CLOUD_FUNC_TRANSFORM";

    TCloudFunctionTransformActor(TActorId owner,
                                 NDqProto::TDqTransform transform, IHTTPGateway::TPtr gateway,
                                 IDqOutputChannel::TPtr transformInput,
                                 IDqOutputConsumer::TPtr taskOutput,
                                 const NKikimr::NMiniKQL::THolderFactory& holderFactory,
                                 const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
                                 NKikimr::NMiniKQL::TProgramBuilder& programBuilder);
    void DoTransform();

private:
    STATEFN(StateFunc);
    void Handle(TCFTransformEvent::TEvTransformSuccess::TPtr& ev);
    void Handle(TCFTransformEvent::TEvExecuteTransform::TPtr& ev);
    void Handle(TEvDq::TEvAbortExecution::TPtr& ev);

    STATEFN(DeadState);
    void HandlePoison(NActors::TEvents::TEvPoison::TPtr&);

private:
    struct TCFReqContext {
        TCFReqContext() {}
        TCFReqContext(bool lastBatch): LastBatch(lastBatch) {}

        TString TransformName;
        bool LastBatch = false;
    };

    static void OnInvokeFinished(TActorSystem* actorSystem, TActorId selfId, TCFReqContext reqContext, IHTTPGateway::TResult&& result);

    void InternalError(const TIssues& issues);
    void InternalError(const TString& message, const TIssues& subIssues = {});

    void RuntimeError(const TIssues& issues);
    void RuntimeError(const TString& message, const TIssues& subIssues = {});

    void Aborted(const TIssues& issues);
    void Aborted(const TString& message, const TIssues& subIssues = {});

    void CompleteTransform();

public:
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator();

private:
    NActors::TActorId Owner; // compute actor Id

    NDqProto::TDqTransform Transform;
    IHTTPGateway::TPtr Gateway;

    IDqOutputChannel::TPtr TransformInput;
    IDqOutputConsumer::TPtr TaskOutput;

    TString TransformName;

    bool TransformInProgress = false;

    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;

    NMiniKQL::TType* InputRowType = nullptr;
    NMiniKQL::TType* OutputRowsType = nullptr;
};

std::pair<IDqTransformActor*, NActors::IActor*> CreateCloudFunctionTransformActor(const NDqProto::TDqTransform& transform, IHTTPGateway::TPtr gateway, TDqTransformActorFactory::TArguments&& args);

}