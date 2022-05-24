#pragma once
#include "dq_compute_actor_sources.h"
#include "dq_compute_actor_async_output.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <type_traits>

namespace NYql::NDq {

template <class T>
concept TCastsToAsyncInputPair =
    std::is_convertible_v<T, std::pair<IDqComputeActorAsyncInput*, NActors::IActor*>>;

template <class T, class TProto>
concept TSourceCreatorFunc = requires(T f, TProto&& settings, IDqSourceFactory::TArguments args) {
    { f(std::move(settings), std::move(args)) } -> TCastsToAsyncInputPair;
};

class TDqSourceFactory : public IDqSourceFactory {
public:
    using TCreatorFunction = std::function<std::pair<IDqComputeActorAsyncInput*, NActors::IActor*>(TArguments&& args)>;

    std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSource(TArguments&& args) const override;

    void Register(const TString& type, TCreatorFunction creator);

    template <class TProtoMsg, TSourceCreatorFunc<TProtoMsg> TCreatorFunc>
    void Register(const TString& type, TCreatorFunc creator) {
        Register(type,
            [creator = std::move(creator), type](TArguments&& args)
            {
                const google::protobuf::Any& settingsAny = args.InputDesc.GetSource().GetSettings();
                YQL_ENSURE(settingsAny.Is<TProtoMsg>(),
                    "Source \"" << type << "\" settings are expected to have protobuf type " << TProtoMsg::descriptor()->full_name()
                    << ", but got " << settingsAny.type_url());
                TProtoMsg settings;
                YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings of type \"" << type << "\"");
                return creator(std::move(settings), std::move(args));
        });
    }

private:
    THashMap<TString, TCreatorFunction> CreatorsByType;
};

template <class T>
concept TCastsToAsyncOutputPair =
    std::is_convertible_v<T, std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>>;

template <class T, class TProto>
concept TSinkCreatorFunc = requires(T f, TProto&& settings, IDqSinkFactory::TArguments&& args) {
    { f(std::move(settings), std::move(args)) } -> TCastsToAsyncOutputPair;
};

template <class T, class TProto>
concept TOutputTransformCreatorFunc = requires(T f, TProto&& settings, IDqOutputTransformFactory::TArguments&& args) {
    { f(std::move(settings), std::move(args)) } -> TCastsToAsyncOutputPair;
};

class TDqSinkFactory : public IDqSinkFactory {
public:
    using TCreatorFunction = std::function<std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>(TArguments&& args)>;

    std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSink(TArguments&& args) const override;

    void Register(const TString& type, TCreatorFunction creator);

    template <class TProtoMsg, TSinkCreatorFunc<TProtoMsg> TCreatorFunc>
    void Register(const TString& type, TCreatorFunc creator) {
        Register(type,
            [creator = std::move(creator), type](TArguments&& args)
            {
                const google::protobuf::Any& settingsAny = args.OutputDesc.GetSink().GetSettings();
                YQL_ENSURE(settingsAny.Is<TProtoMsg>(),
                    "Sink \"" << type << "\" settings are expected to have protobuf type " << TProtoMsg::descriptor()->full_name()
                    << ", but got " << settingsAny.type_url());
                TProtoMsg settings;
                YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings of type \"" << type << "\"");
                return creator(std::move(settings), std::move(args));
        });
    }

private:
    THashMap<TString, TCreatorFunction> CreatorsByType;
};

class TDqOutputTransformFactory : public IDqOutputTransformFactory {
public:
    using TCreatorFunction = std::function<std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>(TArguments&& args)>;

    std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqOutputTransform(TArguments&& args) override;

    void Register(const TString& type, TCreatorFunction creator);

    template <class TProtoMsg, TOutputTransformCreatorFunc<TProtoMsg> TCreatorFunc>
    void Register(const TString& type, TCreatorFunc creator) {
        Register(type,
            [creator = std::move(creator), type](TArguments&& args)
            {
                const google::protobuf::Any& settingsAny = args.OutputDesc.GetTransform().GetSettings();
                YQL_ENSURE(settingsAny.Is<TProtoMsg>(),
                    "Output transform \"" << type << "\" settings are expected to have protobuf type " << TProtoMsg::descriptor()->full_name()
                    << ", but got " << settingsAny.type_url());
                TProtoMsg settings;
                YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings of type \"" << type << "\"");
                return creator(std::move(settings), std::move(args));
        });
    }

private:
    std::unordered_map<TString, TCreatorFunction> CreatorsByType;
};

} // namespace NYql::NDq
