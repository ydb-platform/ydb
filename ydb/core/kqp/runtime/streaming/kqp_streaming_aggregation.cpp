#include "kqp_streaming_aggregation.h"

#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/helpers/future_callback.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <library/cpp/threading/future/future.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <list>
#include <optional>
#include <string>
#include <type_traits>

namespace NKikimr::NMiniKQL {

namespace {

template <typename TDerived, bool SerializableState = false>
class TStreamingAggregationFlowWrapperBase : public TStatefulFlowComputationNode<TDerived, SerializableState> {
    using TBaseComputation = TStatefulFlowComputationNode<TDerived, SerializableState>;

protected:
    template <typename TValue>
    using TMap = TMKQLHashMap<NUdf::TUnboxedValuePod, TValue, TValueHasher, TValueEqual>;

public:
    TStreamingAggregationFlowWrapperBase(
        TComputationMutables& mutables,
        const EValueRepresentation kind,
        IComputationNode* const flow,
        IComputationExternalNode* const itemArg,
        IComputationExternalNode* const stateArg,
        IComputationExternalNode* const keyArg,
        IComputationNode* const outKey,
        IComputationNode* const outInit,
        IComputationNode* const outUpdate,
        IComputationNode* const outFinish,
        TType* const keyType,
        IComputationExternalNode* const savedStateArg,
        IComputationNode* const outSave,
        IComputationNode* const outLoad,
        TType* const savedStateType)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Boxed)
        , Flow(flow)
        , ItemArg(itemArg)
        , StateArg(stateArg)
        , KeyArg(keyArg)
        , OutKey(outKey)
        , OutInit(outInit)
        , OutUpdate(outUpdate)
        , OutFinish(outFinish)
        , KeyType(keyType)
        , KeyTypeHelper(keyType)
        , KeyPacker(mutables)
        , SavedStateArg(savedStateArg)
        , OutSave(outSave)
        , OutLoad(outLoad)
        , SavedStateType(savedStateType)
        , StatePacker(mutables)
    {}

private:
    bool IsSuitableForCache() const final {
        return false;
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, ItemArg);
            this->Own(flow, StateArg);
            this->Own(flow, KeyArg);
            this->DependsOn(flow, OutKey);
            this->DependsOn(flow, OutInit);
            this->DependsOn(flow, OutUpdate);
            this->DependsOn(flow, OutFinish);

            this->Own(flow, SavedStateArg);
            this->DependsOn(flow, OutSave);
            this->DependsOn(flow, OutLoad);
        }
    }

protected:
    IComputationNode* const Flow;
    IComputationExternalNode* const ItemArg;
    IComputationExternalNode* const StateArg;
    IComputationExternalNode* const KeyArg;
    IComputationNode* const OutKey;
    IComputationNode* const OutInit;
    IComputationNode* const OutUpdate;
    IComputationNode* const OutFinish;
    TType* const KeyType;
    const TKeyTypeContanerHelper<true, true, false> KeyTypeHelper;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> KeyPacker;
    IComputationExternalNode* const SavedStateArg;
    IComputationNode* const OutSave;
    IComputationNode* const OutLoad;
    TType* const SavedStateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StatePacker;
};

class TInMemoryStreamingAggregationFlowWrapper final : public TStreamingAggregationFlowWrapperBase<TInMemoryStreamingAggregationFlowWrapper, true> {
    using TThis = TInMemoryStreamingAggregationFlowWrapper;
    using TBase = TStreamingAggregationFlowWrapperBase<TThis, true>;

    class TState final : public TComputationValue<TState> {
        static constexpr ui32 STATE_VERSION = 1;

    public:
        TState(TMemoryUsageInfo* const memInfo, const TThis& self, TComputationContext& ctx)
            : TComputationValue<TState>(memInfo)
            , Self(self)
            , Ctx(ctx)
            , Map(0, self.KeyTypeHelper.GetValueHash(), self.KeyTypeHelper.GetValueEqual())
        {}

        ~TState() final {
            ClearState();
        }

        NUdf::TUnboxedValue& GetOrCreateKeyState(const NUdf::TUnboxedValuePod& key) {
            const auto [it, inserted] = Map.try_emplace(key);
            if (inserted) {
                key.Ref();
            }
            return it->second;
        }

    private:
        bool HasListItems() const final {
            return false;
        }

        NUdf::TUnboxedValue Save() const final {
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, STATE_VERSION, Ctx);
            out.Write<ui64>(Map.size());

            const auto& keyPacker = Self.KeyPacker.RefMutableObject(Ctx, /* stable */ false, Self.KeyType);
            const auto& statePacker = Self.StatePacker.RefMutableObject(Ctx, /* stable */ false, Self.SavedStateType);
            for (const auto& [key, value] : Map) {
                out.WriteUnboxedValue(keyPacker, key);
                Self.StateArg->SetValue(Ctx, NUdf::TUnboxedValue(value));
                out.WriteUnboxedValue(statePacker, Self.OutSave->GetValue(Ctx));
            }

            return out.MakeState();
        }

        bool Load2(const NUdf::TUnboxedValue& state) final {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);
            MKQL_ENSURE(in.GetStateVersion() == STATE_VERSION, "Unsupported streaming aggregation checkpoint version");

            ClearState();
            const auto size = in.Read<ui64>();
            Map.reserve(size);

            const auto& keyPacker = Self.KeyPacker.RefMutableObject(Ctx, /* stable */ false, Self.KeyType);
            const auto& packer = Self.StatePacker.RefMutableObject(Ctx, /* stable */ false, Self.SavedStateType);
            for (ui64 i = 0; i < size; ++i) {
                const auto key = in.ReadUnboxedValue(keyPacker, Ctx);
                Self.SavedStateArg->SetValue(Ctx, in.ReadUnboxedValue(packer, Ctx));
                MKQL_ENSURE(Map.emplace(key, Self.OutLoad->GetValue(Ctx)).second, "Duplicate key in streaming aggregation checkpoint");
                key.Ref();
            }

            MKQL_ENSURE(in.Empty(), "Unexpected trailing data in streaming aggregation checkpoint");
            return true;
        }

        void ClearState() {
            for (const auto& entry : Map) {
                entry.first.UnRef();
            }
            Map.clear();
        }

        const TThis& Self;
        TComputationContext& Ctx;
        TMap<NUdf::TUnboxedValue> Map;
    };

public:
    using TBase::TBase;

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(*this, ctx);
        } else if (stateValue.HasListItems()) {
            auto restored = ctx.HolderFactory.Create<TState>(*this, ctx);
            restored.Load2(stateValue);
            stateValue = std::move(restored);
        }
        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());

        if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
            return item;
        } else {
            ItemArg->SetValue(ctx, std::move(item));
        }

        auto key = OutKey->GetValue(ctx);
        auto& slot = state.GetOrCreateKeyState(key);

        if (!slot.HasValue()) {
            slot = OutInit->GetValue(ctx);
        } else {
            StateArg->SetValue(ctx, std::move(slot));
            slot = OutUpdate->GetValue(ctx);
        }

        StateArg->SetValue(ctx, NUdf::TUnboxedValue(slot));
        KeyArg->SetValue(ctx, std::move(key));
        return OutFinish->GetValue(ctx);
    }
};

class TTableStreamingAggregationFlowWrapper final : public TStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper> {
    using TBase = TStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper>;

    // State helpers

    class TStreamingStateLruCache {
        static constexpr size_t MAX_SIZE = 1;
        static_assert(MAX_SIZE > 0, "Max size must be greater than 0");

        struct TEntry {
            NUdf::TUnboxedValue Key;
            NUdf::TUnboxedValue State;
        };

        using TUsageList = std::list<TEntry, TMKQLAllocator<TEntry>>;

    public:
        explicit TStreamingStateLruCache(const TTableStreamingAggregationFlowWrapper& self)
            : Map(0, self.KeyTypeHelper.GetValueHash(), self.KeyTypeHelper.GetValueEqual())
        {}

        NUdf::TUnboxedValue* Get(const NUdf::TUnboxedValuePod& key) {
            if (const auto it = Map.find(key); it != Map.end()) {
                Usage.splice(Usage.end(), Usage, it->second);
                return &it->second->State;
            }
            return nullptr;
        }

        std::optional<TEntry> Insert(NUdf::TUnboxedValue key, NUdf::TUnboxedValue state) {
            Usage.emplace_back(TEntry{std::move(key), std::move(state)});
            const auto last = std::prev(Usage.end());
            Y_VALIDATE(Map.emplace(last->Key, last).second, "Duplicated key");

            if (Map.size() <= MAX_SIZE) {
                return std::nullopt;
            }

            Y_VALIDATE(!Usage.empty(), "Usage list is empty");
            auto evicted = std::move(Usage.front());
            Usage.pop_front();
            Map.erase(evicted.Key);
            return std::move(evicted);
        }

    private:
        TUsageList Usage;
        TMap<TUsageList::iterator> Map;
    };

    // Persistent state table interaction

    struct TEvStateQueryResult : public NActors::TEventLocal<TEvStateQueryResult, EventSpaceBegin(NActors::TEvents::ES_PRIVATE)> {
        TEvStateQueryResult(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues, std::optional<std::string> state)
            : Status(status)
            , Issues(std::move(issues))
            , State(std::move(state))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        std::optional<std::string> State;
    };

    template <typename TDerived>
    class TStateQueryActorBase : public TQueryBase, public TQueryRetryActorMixin<TDerived, TEvStateQueryResult> {
    public:
        TStateQueryActorBase(const TString& operationName, const TString& database, const TMaybe<TString>& userToken, const TString& tablePath, const TString& key)
            : TQueryBase(NKikimrServices::KQP_COMPUTE, {}, database, /* isSystemUser */ false, /* isStreamingMode */ false, userToken)
            , TablePath(tablePath)
            , Key(key)
        {
            SetOperationInfo(operationName, "");
        }

    private:
        void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
            Send(Owner, new TEvStateQueryResult(status, std::move(issues), std::move(ResolvedResult)));
        }

    protected:
        const TString TablePath;
        const TString Key;
        std::optional<TString> ResolvedResult;
    };

    class TSelectStateActor final : public TStateQueryActorBase<TSelectStateActor> {
    public:
        using TResult = std::optional<std::string>;

        static constexpr TStringBuf OPERATION_NAME = "SELECT";

        TSelectStateActor(const TString& database, const TMaybe<TString>& userToken, const TString& tablePath, const TString& key)
            : TStateQueryActorBase(__func__, database, userToken, tablePath, key)
        {}

    private:
        void OnRunQuery() final {
            const TString sql = fmt::format(R"sql(
                DECLARE $key AS String;
                SELECT state FROM `{}` WHERE key = $key;
            )sql", TablePath);

            NYdb::TParamsBuilder params;
            params
                .AddParam("$key")
                    .String(Key)
                    .Build();

            RunDataQuery(sql, &params);
        }

        void OnQueryResult() final {
            Y_VALIDATE(ResultSets.size() == 1, "Unexpected state table SELECT result set count: " << ResultSets.size());

            NYdb::TResultSetParser parser(ResultSets.front());
            if (parser.TryNextRow()) {
                ResolvedResult = parser.ColumnParser("state").GetOptionalString();
                Y_VALIDATE(ResolvedResult, "State table contains a NULL aggregation state");
            }
            Finish();
        }
    };

    class TUpsertStateActor final : public TStateQueryActorBase<TUpsertStateActor> {
    public:
        using TResult = void;

        static constexpr TStringBuf OPERATION_NAME = "UPSERT";

        TUpsertStateActor(const TString& database, const TMaybe<TString>& userToken, const TString& tablePath, const TString& key, const TString& state)
            : TStateQueryActorBase(__func__, database, userToken, tablePath, key)
            , State(state)
        {}

    private:
        void OnRunQuery() final {
            const TString sql = fmt::format(R"sql(
                DECLARE $key AS String;
                DECLARE $state AS String;
                UPSERT INTO `{}` (key, state) VALUES ($key, $state);
            )sql", TablePath);

            NYdb::TParamsBuilder params;
            params
                .AddParam("$key")
                    .String(Key)
                    .Build()
                .AddParam("$state")
                    .String(State)
                    .Build();

            RunDataQuery(sql, &params);
        }

        void OnQueryResult() final {
            Y_VALIDATE(ResultSets.empty(), "Unexpected state table UPSERT result set count: " << ResultSets.size());
            Finish();
        }

    private:
        const TString State;
    };

    // Inmemory state

    class TState final : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        enum class EStep : ui8 {
            Fetch = 0,
            AwaitSelect,
            AwaitUpsert,
        };

        TState(TMemoryUsageInfo* const memInfo, const TTableStreamingAggregationFlowWrapper& self)
            : TBase(memInfo)
            , Cache(self)
        {}

        void Clear() {
            PendingItem = NUdf::TUnboxedValue();
            PendingKey = NUdf::TUnboxedValue();
            PendingNewState = NUdf::TUnboxedValue();
        }

        EStep Step = EStep::Fetch;
        NUdf::TUnboxedValue PendingItem;
        NUdf::TUnboxedValue PendingKey;
        NUdf::TUnboxedValue PendingNewState;
        NThreading::TFuture<std::optional<std::string>> SelectFuture;
        NThreading::TFuture<void> UpsertFuture;

        TStreamingStateLruCache Cache;
    };

public:
    TTableStreamingAggregationFlowWrapper(
        const TKqpComputeContextBase& computeCtx,
        TComputationMutables& mutables,
        const EValueRepresentation kind,
        IComputationNode* const flow,
        IComputationExternalNode* const itemArg,
        IComputationExternalNode* const stateArg,
        IComputationExternalNode* const keyArg,
        IComputationNode* const outKey,
        IComputationNode* const outInit,
        IComputationNode* const outUpdate,
        IComputationNode* const outFinish,
        TType* const keyType,
        IComputationExternalNode* const savedStateArg,
        IComputationNode* const outSave,
        IComputationNode* const outLoad,
        TType* const savedStateType,
        TString stateTablePath)
        : TBase(mutables, kind, flow, itemArg, stateArg, keyArg, outKey, outInit, outUpdate, outFinish, keyType, savedStateArg, outSave, outLoad, savedStateType)
        , ComputeCtx(computeCtx)
        , StateTablePath(std::move(stateTablePath))
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            stateValue = ctx.HolderFactory.Create<TState>(*this);
        }
        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());

        switch (state.Step) {
            case TState::EStep::Fetch: {
                return DoFetch(state, ctx);
            }
            case TState::EStep::AwaitSelect: {
                return FinishSelect(state, ctx);
            }
            case TState::EStep::AwaitUpsert: {
                return FinishUpsert(state, ctx);
            }
        }
    }

private:
    NUdf::TUnboxedValue DoFetch(TState& state, TComputationContext& ctx) const {
        auto item = Flow->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }

        ItemArg->SetValue(ctx, NUdf::TUnboxedValue(item));
        auto key = OutKey->GetValue(ctx);

        if (auto* const slot = state.Cache.Get(key)) {
            StateArg->SetValue(ctx, NUdf::TUnboxedValue(*slot));
            *slot = OutUpdate->GetValue(ctx);
            return EmitFinish(std::move(key), *slot, ctx);
        }

        const auto& keyPacker = KeyPacker.RefMutableObject(ctx, /* stable */ true, KeyType);
        TString keyBytes(keyPacker.Pack(key));
        state.PendingItem = std::move(item);
        state.PendingKey = std::move(key);

        state.SelectFuture = RunStateTableQuery<TSelectStateActor>(std::move(keyBytes));
        state.Step = TState::EStep::AwaitSelect;
        return NUdf::TUnboxedValuePod::MakeYield();
    }

    NUdf::TUnboxedValue FinishSelect(TState& state, TComputationContext& ctx) const {
        if (!state.SelectFuture.IsReady()) {
            return NUdf::TUnboxedValuePod::MakeYield();
        }

        ItemArg->SetValue(ctx, std::move(state.PendingItem));

        const auto packedPrev = state.SelectFuture.ExtractValue();
        state.SelectFuture = {};

        NUdf::TUnboxedValue newState;
        const auto& statePacker = StatePacker.RefMutableObject(ctx, /* stable */ false, SavedStateType);
        if (packedPrev) {
            SavedStateArg->SetValue(ctx, statePacker.Unpack(*packedPrev, ctx.HolderFactory));
            StateArg->SetValue(ctx, OutLoad->GetValue(ctx));
            newState = OutUpdate->GetValue(ctx);
        } else {
            newState = OutInit->GetValue(ctx);
        }

        state.PendingNewState = newState;

        if (auto evicted = state.Cache.Insert(NUdf::TUnboxedValue(state.PendingKey), std::move(newState))) {
            const auto& keyPacker = KeyPacker.RefMutableObject(ctx, /* stable */ true, KeyType);
            TString keyBytes(keyPacker.Pack(evicted->Key));

            StateArg->SetValue(ctx, std::move(evicted->State));
            TString packedState(statePacker.Pack(OutSave->GetValue(ctx)));

            state.UpsertFuture = RunStateTableQuery<TUpsertStateActor>(std::move(keyBytes), std::move(packedState));
            state.Step = TState::EStep::AwaitUpsert;
            return NUdf::TUnboxedValuePod::MakeYield();
        }

        auto out = EmitFinish(std::move(state.PendingKey), std::move(state.PendingNewState), ctx);
        state.Clear();
        state.Step = TState::EStep::Fetch;
        return out;
    }

    NUdf::TUnboxedValue FinishUpsert(TState& state, TComputationContext& ctx) const {
        if (!state.UpsertFuture.IsReady()) {
            return NUdf::TUnboxedValuePod::MakeYield();
        }

        state.UpsertFuture.GetValue();
        state.UpsertFuture = {};

        auto out = EmitFinish(std::move(state.PendingKey), std::move(state.PendingNewState), ctx);
        state.Clear();
        state.Step = TState::EStep::Fetch;
        return out;
    }

    NUdf::TUnboxedValue EmitFinish(NUdf::TUnboxedValue key, NUdf::TUnboxedValue state, TComputationContext& ctx) const {
        KeyArg->SetValue(ctx, std::move(key));
        StateArg->SetValue(ctx, std::move(state));
        return OutFinish->GetValue(ctx);
    }

    template <typename TQueryActor, typename... TArgs>
    NThreading::TFuture<typename TQueryActor::TResult> RunStateTableQuery(TArgs&&... args) const {
        Y_VALIDATE(NActors::TlsActivationContext, "KqpStreamingAggregation table backend requires actor TLS context");
        Y_VALIDATE(ComputeCtx.GetWakeupCallback(), "KqpStreamingAggregation table backend requires a wakeup callback in the KQP compute context");
        auto* const actorSystem = NActors::TlsActivationContext->ActorSystem();

        auto promise = NThreading::NewPromise<typename TQueryActor::TResult>();
        auto future = promise.GetFuture();
        future.Subscribe([wakeupCallback = ComputeCtx.GetWakeupCallback()](const auto&) {
            wakeupCallback();
        });

        const auto replyActor = actorSystem->Register(new NActors::TActorFutureCallback<TEvStateQueryResult>(
            [promise = std::move(promise), tablePath = StateTablePath](TEvStateQueryResult::TPtr& ev) mutable {
                auto& response = *ev->Get();
                if (response.Status != Ydb::StatusIds::SUCCESS) {
                    promise.SetException(TStringBuilder() << "Streaming aggregation " << TQueryActor::OPERATION_NAME
                        << " failed for table " << tablePath << ", status=" << static_cast<int>(response.Status)
                        << ": " << response.Issues.ToString());
                } else if constexpr (std::is_void_v<typename TQueryActor::TResult>) {
                    promise.SetValue();
                } else {
                    promise.SetValue(std::move(response.State));
                }
            }
        ));

        TMaybe<TString> userToken;
        if (const auto& token = ComputeCtx.GetUserToken()) {
            userToken = token->GetSerializedToken();
        }

        actorSystem->Register(TQueryActor::MakeRetry(replyActor, ComputeCtx.GetDatabase(), std::move(userToken), StateTablePath, std::forward<TArgs>(args)...));
        return future;
    }

    const TKqpComputeContextBase& ComputeCtx;
    const TString StateTablePath;
};

} // anonymous namespace

IComputationNode* WrapKqpStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    const TKqpComputeContextBase& computeCtx)
{
    MKQL_ENSURE(callable.GetInputsCount() == 12, "KqpStreamingAggregation expected 12 args, got " << callable.GetInputsCount());

    const auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsFlow(), "KqpStreamingAggregation expects flow return type");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto outKey = LocateNode(ctx.NodeLocator, callable, 4);
    const auto outInit = LocateNode(ctx.NodeLocator, callable, 5);
    const auto outUpdate = LocateNode(ctx.NodeLocator, callable, 6);
    const auto outFinish = LocateNode(ctx.NodeLocator, callable, 7);
    const auto outSave = LocateNode(ctx.NodeLocator, callable, 10);
    const auto outLoad = LocateNode(ctx.NodeLocator, callable, 11);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto stateArg = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto keyArg = LocateExternalNode(ctx.NodeLocator, callable, 3);
    const auto savedStateArg = LocateExternalNode(ctx.NodeLocator, callable, 9);

    const auto keyType = callable.GetInput(3).GetStaticType();
    const auto savedStateType = callable.GetInput(9).GetStaticType();

    const auto& stateTablePathLiteral = AS_VALUE(TDataLiteral, callable.GetInput(8));
    TString stateTablePath(stateTablePathLiteral->AsValue().AsStringRef());

    if (!stateTablePath) {
        return new TInMemoryStreamingAggregationFlowWrapper(
            ctx.Mutables, GetValueRepresentation(returnType), flow, itemArg, stateArg, keyArg,
            outKey, outInit, outUpdate, outFinish, keyType, savedStateArg, outSave, outLoad, savedStateType);
    }

    return new TTableStreamingAggregationFlowWrapper(
        computeCtx, ctx.Mutables, GetValueRepresentation(returnType), flow, itemArg, stateArg, keyArg,
        outKey, outInit, outUpdate, outFinish, keyType, savedStateArg, outSave, outLoad, savedStateType,
        std::move(stateTablePath));
}

} // namespace NKikimr::NMiniKQL
