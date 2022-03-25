#pragma once

#include "dq_transform_actor.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <library/cpp/actors/core/actor.h>

namespace NYql::NDq {

class TDqTransformActorFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDqTransformActorFactory>;

    struct TArguments {
        const NActors::TActorId ComputeActorId;
        const IDqOutputChannel::TPtr TransformInput;
        const IDqOutputConsumer::TPtr TransformOutput;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
    };

    TDqTransformActorFactory();

    using TTransformCreator = std::function<std::pair<IDqTransformActor*, NActors::IActor*>(
            const NDqProto::TDqTransform& transform, TArguments&& args)>;

    std::pair<IDqTransformActor*, NActors::IActor*> CreateDqTransformActor(const NDqProto::TDqTransform& transform, TArguments&& args);
    void Register(NDqProto::ETransformType type, TTransformCreator creator);

private:
    std::unordered_map<NDqProto::ETransformType, TTransformCreator> CreatorsByType;
};
}