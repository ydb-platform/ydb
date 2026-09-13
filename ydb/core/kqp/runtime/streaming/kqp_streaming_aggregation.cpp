#include "kqp_streaming_aggregation.h"

#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <list>
#include <unordered_map>

namespace NKikimr::NMiniKQL {

namespace {

template <typename TDerived, bool SerializableState = false>
class TStreamingAggregationFlowWrapperBase : public TStatefulFlowComputationNode<TDerived, SerializableState> {
    using TBaseComputation = TStatefulFlowComputationNode<TDerived, SerializableState>;

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
        IComputationExternalNode* const savedStateArg = nullptr,
        IComputationNode* const outSave = nullptr,
        IComputationNode* const outLoad = nullptr)
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
        , KeyPacker(mutables)
        , SavedStateArg(savedStateArg)
        , OutSave(outSave)
        , OutLoad(outLoad)
    {}

    bool IsSuitableForCache() const final {
        return false;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, ItemArg);
            this->Own(flow, StateArg);
            this->Own(flow, KeyArg);
            this->DependsOn(flow, OutKey);
            this->DependsOn(flow, OutInit);
            this->DependsOn(flow, OutUpdate);
            this->DependsOn(flow, OutFinish);
            if (SavedStateArg) {
                this->Own(flow, SavedStateArg);
                this->DependsOn(flow, OutSave);
                this->DependsOn(flow, OutLoad);
            }
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
    TMutableObjectOverBoxedValue<TValuePackerBoxed> KeyPacker;
    IComputationExternalNode* const SavedStateArg;
    IComputationNode* const OutSave;
    IComputationNode* const OutLoad;
};

class TInMemoryStreamingAggregationFlowWrapper final
    : public TStreamingAggregationFlowWrapperBase<TInMemoryStreamingAggregationFlowWrapper, true>
{
    using TBase = TStreamingAggregationFlowWrapperBase<TInMemoryStreamingAggregationFlowWrapper, true>;

    class TState final : public TComputationValue<TState> {
        using TMap = std::unordered_map<
            TString, NUdf::TUnboxedValue, std::hash<TString>, std::equal_to<>,
            TMKQLAllocator<std::pair<const TString, NUdf::TUnboxedValue>>>;

    public:
        TState(TMemoryUsageInfo* memInfo, const TInMemoryStreamingAggregationFlowWrapper& self, TComputationContext& ctx)
            : TComputationValue<TState>(memInfo)
            , Self(self)
            , Ctx(ctx)
        {}

        TMap Map;

    private:
        static constexpr ui32 StateVersion = 1;

        bool HasListItems() const override {
            return false;
        }

        NUdf::TUnboxedValue Save() const override {
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);
            out.Write<ui64>(Map.size());
            const auto& packer = Self.StatePacker.RefMutableObject(Ctx, false, Self.SavedStateType);
            for (const auto& [key, value] : Map) {
                out(key);
                Self.StateArg->SetValue(Ctx, NUdf::TUnboxedValue(value));
                out.WriteUnboxedValue(packer, Self.OutSave->GetValue(Ctx));
            }
            return out.MakeState();
        }

        bool Load2(const NUdf::TUnboxedValue& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);
            MKQL_ENSURE(in.GetStateVersion() == StateVersion, "Unsupported streaming aggregation checkpoint version");
            const auto size = in.Read<ui64>();
            Map.clear();
            const auto& packer = Self.StatePacker.RefMutableObject(Ctx, false, Self.SavedStateType);
            for (ui64 i = 0; i < size; ++i) {
                auto key = in.Read<TString>();
                Self.SavedStateArg->SetValue(Ctx, in.ReadUnboxedValue(packer, Ctx));
                const auto [it, inserted] = Map.emplace(std::move(key), Self.OutLoad->GetValue(Ctx));
                MKQL_ENSURE(inserted, "Duplicate key in streaming aggregation checkpoint");
            }
            MKQL_ENSURE(in.Empty(), "Unexpected trailing data in streaming aggregation checkpoint");
            return true;
        }

        const TInMemoryStreamingAggregationFlowWrapper& Self;
        TComputationContext& Ctx;
    };

public:
    TInMemoryStreamingAggregationFlowWrapper(
        TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,
        IComputationExternalNode* itemArg, IComputationExternalNode* stateArg, IComputationExternalNode* keyArg,
        IComputationNode* outKey, IComputationNode* outInit, IComputationNode* outUpdate, IComputationNode* outFinish,
        TType* keyType, IComputationExternalNode* savedStateArg,
        IComputationNode* outSave, IComputationNode* outLoad, TType* savedStateType)
        : TBase(mutables, kind, flow, itemArg, stateArg, keyArg, outKey, outInit, outUpdate, outFinish,
            keyType, savedStateArg, outSave, outLoad)
        , SavedStateType(savedStateType)
        , StatePacker(mutables)
    {}

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

        const auto& packer = KeyPacker.RefMutableObject(ctx, true, KeyType);
        const TString keyBytes(packer.Pack(key));

        auto& slot = state.Map[keyBytes];

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

private:
    TType* const SavedStateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StatePacker;
};

class TTableStreamingAggregationFlowWrapper final
    : public TStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper>
{
    using TBase = TStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper>;

    // Minimal LRU cache for the streaming aggregation state. Distinct from
    // TUnboxedKeyValueLruCacheWithTtl because we (a) have no TTL, (b) need to
    // surface the evicted entry on overflow so we can write it back to the table.
    class TStreamingStateLruCache final {
    public:
        struct TEntry {
            TString Key;
            NUdf::TUnboxedValue State;
        };

        // Hard cap on cached per-key entries before the oldest entry is offloaded to the
        // state table. No TTL — entries stay in cache until they fall off the LRU end.
        static constexpr size_t MaxSize = 1;

        NUdf::TUnboxedValue* Get(const TString& key) {
            if (const auto it = Map.find(key); it != Map.end()) {
                Usage.splice(Usage.end(), Usage, it->second);
                return &it->second->State;
            }
            return nullptr;
        }

        // Inserts (key, state) at the most-recently-used end. If the cache was at
        // capacity, returns the evicted least-recently-used entry so the caller can
        // persist it. Caller must guarantee the key is not already present.
        //
        // Special case MaxSize == 0: the cache is fully disabled — every Insert
        // returns its own argument as "evicted" (caller persists immediately) and
        // nothing is stored. This keeps the state-machine path uniform regardless
        // of whether the cache is on or off.
        TMaybe<TEntry> Insert(TString key, NUdf::TUnboxedValue state) {
            if constexpr (MaxSize == 0) {
                return TEntry{std::move(key), std::move(state)};
            }
            TMaybe<TEntry> evicted;
            if (Map.size() >= MaxSize && !Usage.empty()) {
                evicted.ConstructInPlace(std::move(Usage.front()));
                Map.erase(evicted->Key);
                Usage.pop_front();
            }
            Usage.emplace_back(TEntry{key, std::move(state)});
            const auto last = std::prev(Usage.end());
            Map.emplace(std::move(key), last);
            return evicted;
        }

    private:
        using TUsageList = std::list<TEntry, TMKQLAllocator<TEntry>>;
        using TMap = std::unordered_map<
            TString, TUsageList::iterator, std::hash<TString>, std::equal_to<>,
            TMKQLAllocator<std::pair<const TString, TUsageList::iterator>>>;

        TUsageList Usage;
        TMap Map;
    };

    class TSelectStateActor final : public TQueryBase {
    public:
        TSelectStateActor(
            const TString& tablePath,
            const TString& key,
            NThreading::TPromise<TMaybe<TString>> promise)
            : TQueryBase(/*logComponent=*/0)
            , TablePath(tablePath)
            , Key(key)
            , Promise(std::move(promise))
        {
            SetOperationInfo("StreamingAggregationSelect", "");
        }

        void OnRunQuery() override {
            const TString sql = TStringBuilder()
                << "DECLARE $key AS String;\n"
                << "SELECT state FROM `" << TablePath << "` WHERE key = $key;";

            NYdb::TParamsBuilder params;
            params.AddParam("$key").String(Key).Build();

            RunDataQuery(sql, &params);
        }

        void OnQueryResult() override {
            TMaybe<TString> result;
            if (!ResultSets.empty()) {
                NYdb::TResultSetParser parser(ResultSets.front());
                if (parser.TryNextRow()) {
                    auto& stateColumn = parser.ColumnParser("state");
                    if (!stateColumn.IsNull()) {
                        result = TString(stateColumn.GetOptionalString().value_or(""));
                    }
                }
            }
            ResolvedResult = std::move(result);
            Finish();
        }

        void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
            if (status == Ydb::StatusIds::SUCCESS) {
                Promise.SetValue(std::move(ResolvedResult));
            } else {
                Promise.SetException(TStringBuilder() << "Streaming aggregation SELECT failed for table "
                    << TablePath << ", status=" << static_cast<int>(status) << ": " << issues.ToString());
            }
        }

    private:
        const TString TablePath;
        const TString Key;
        NThreading::TPromise<TMaybe<TString>> Promise;
        TMaybe<TString> ResolvedResult;
    };

    class TUpsertStateActor final : public TQueryBase {
    public:
        TUpsertStateActor(
            const TString& tablePath,
            const TString& key,
            const TString& state,
            NThreading::TPromise<void> promise)
            : TQueryBase(/*logComponent=*/0)
            , TablePath(tablePath)
            , Key(key)
            , State(state)
            , Promise(std::move(promise))
        {
            SetOperationInfo("StreamingAggregationUpsert", "");
        }

        void OnRunQuery() override {
            const TString sql = TStringBuilder()
                << "DECLARE $key AS String;\n"
                << "DECLARE $state AS String;\n"
                << "UPSERT INTO `" << TablePath << "` (key, state) VALUES ($key, $state);";

            NYdb::TParamsBuilder params;
            params.AddParam("$key").String(Key).Build();
            params.AddParam("$state").String(State).Build();

            RunDataQuery(sql, &params);
        }

        void OnQueryResult() override {
            Finish();
        }

        void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
            if (status == Ydb::StatusIds::SUCCESS) {
                Promise.SetValue();
            } else {
                Promise.SetException(TStringBuilder() << "Streaming aggregation UPSERT failed for table "
                    << TablePath << ", status=" << static_cast<int>(status) << ": " << issues.ToString());
            }
        }

    private:
        const TString TablePath;
        const TString Key;
        const TString State;
        NThreading::TPromise<void> Promise;
    };

    enum class EStreamingStep : ui8 {
        Fetch = 0,
        AwaitSelect,
        AwaitUpsert,
    };

    class TState final : public TComputationValue<TState> {
    public:
        using TBase = TComputationValue<TState>;
        using TBase::TBase;

        EStreamingStep Step = EStreamingStep::Fetch;
        NUdf::TUnboxedValue PendingItem;
        NUdf::TUnboxedValue PendingKey;
        NUdf::TUnboxedValue PendingNewState;
        TString PendingKeyBytes;
        NThreading::TFuture<TMaybe<TString>> SelectFuture;
        NThreading::TFuture<void> UpsertFuture;

        TStreamingStateLruCache Cache;
    };

    template <typename T>
    static void SubscribeWakeUp(const NThreading::TFuture<T>& future, const std::function<void()>& wakeupCallback) {
        // Completion may outlive the computation graph and run outside the compute actor.
        future.Subscribe([wakeupCallback](const auto&) { wakeupCallback(); });
    }

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
        TType* const stateValueType,
        TString stateTablePath)
        : TBase(mutables, kind, flow, itemArg, stateArg, keyArg,
            outKey, outInit, outUpdate, outFinish, keyType)
        , ComputeCtx(computeCtx)
        , StateValueType(stateValueType)
        , StateTablePath(std::move(stateTablePath))
        , StateValuePacker(mutables)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            stateValue = ctx.HolderFactory.Create<TState>();
        }
        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());

        MKQL_ENSURE(NActors::TlsActivationContext, "StreamingAggregation table backend requires actor TLS context");
        MKQL_ENSURE(ComputeCtx.GetWakeupCallback(),
                    "StreamingAggregation table backend requires a wakeup callback in the KQP compute context");
        auto* const actorSystem = NActors::TlsActivationContext->ActorSystem();
        const auto& wakeupCallback = ComputeCtx.GetWakeupCallback();

        const auto clearPending = [&]() {
            state.PendingItem = NUdf::TUnboxedValue();
            state.PendingKey = NUdf::TUnboxedValue();
            state.PendingNewState = NUdf::TUnboxedValue();
            state.PendingKeyBytes.clear();
        };

        const auto emitFinish = [&](const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& curState) {
            StateArg->SetValue(ctx, NUdf::TUnboxedValue(curState));
            KeyArg->SetValue(ctx, NUdf::TUnboxedValue(key));
            return OutFinish->GetValue(ctx);
        };

        for (;;) {
            switch (state.Step) {
                case EStreamingStep::Fetch: {
                    auto item = Flow->GetValue(ctx);
                    if (item.IsSpecial()) {
                        return item;
                    }
                    ItemArg->SetValue(ctx, NUdf::TUnboxedValue(item));
                    auto key = OutKey->GetValue(ctx);

                    const auto& keyPacker = KeyPacker.RefMutableObject(ctx, true, KeyType);
                    const TString keyBytes(keyPacker.Pack(key));

                    if (auto* const slot = state.Cache.Get(keyBytes)) {
                        // Cache hit: in-place update, emit, no I/O, no yield.
                        StateArg->SetValue(ctx, NUdf::TUnboxedValue(*slot));
                        *slot = OutUpdate->GetValue(ctx);
                        return emitFinish(key, *slot);
                    }

                    // Cache miss: load previous state from the table.
                    state.PendingItem = std::move(item);
                    state.PendingKey = std::move(key);
                    state.PendingKeyBytes = keyBytes;

                    auto promise = NThreading::NewPromise<TMaybe<TString>>();
                    auto future = promise.GetFuture();
                    actorSystem->Register(new TSelectStateActor(StateTablePath, keyBytes, std::move(promise)));
                    SubscribeWakeUp(future, wakeupCallback);
                    state.SelectFuture = std::move(future);
                    state.Step = EStreamingStep::AwaitSelect;
                    return NUdf::TUnboxedValuePod::MakeYield();
                }
                case EStreamingStep::AwaitSelect: {
                    if (!state.SelectFuture.IsReady()) {
                        return NUdf::TUnboxedValuePod::MakeYield();
                    }
                    const auto packedPrev = state.SelectFuture.ExtractValue();
                    state.SelectFuture = {};

                    const auto& stateValuePacker = StateValuePacker.RefMutableObject(ctx, true, StateValueType);

                    NUdf::TUnboxedValue newState;
                    ItemArg->SetValue(ctx, NUdf::TUnboxedValue(state.PendingItem));
                    if (packedPrev) {
                        StateArg->SetValue(ctx, stateValuePacker.Unpack(*packedPrev, ctx.HolderFactory));
                        newState = OutUpdate->GetValue(ctx);
                    } else {
                        newState = OutInit->GetValue(ctx);
                    }
                    state.PendingNewState = newState;

                    if (auto evicted = state.Cache.Insert(state.PendingKeyBytes, NUdf::TUnboxedValue(newState))) {
                        // Cache full: persist the LRU entry we just kicked out, then
                        // emit current row when that write completes.
                        const TString packedEvicted(stateValuePacker.Pack(evicted->State));
                        auto promise = NThreading::NewPromise<void>();
                        auto future = promise.GetFuture();
                        actorSystem->Register(new TUpsertStateActor(StateTablePath, evicted->Key, packedEvicted, std::move(promise)));
                        SubscribeWakeUp(future, wakeupCallback);
                        state.UpsertFuture = std::move(future);
                        state.Step = EStreamingStep::AwaitUpsert;
                        return NUdf::TUnboxedValuePod::MakeYield();
                    }

                    auto out = emitFinish(state.PendingKey, state.PendingNewState);
                    clearPending();
                    state.Step = EStreamingStep::Fetch;
                    return out;
                }
                case EStreamingStep::AwaitUpsert: {
                    if (!state.UpsertFuture.IsReady()) {
                        return NUdf::TUnboxedValuePod::MakeYield();
                    }
                    state.UpsertFuture.GetValue();
                    state.UpsertFuture = {};

                    auto out = emitFinish(state.PendingKey, state.PendingNewState);
                    clearPending();
                    state.Step = EStreamingStep::Fetch;
                    return out;
                }
            }
        }
    }

private:
    const TKqpComputeContextBase& ComputeCtx;
    TType* const StateValueType;
    const TString StateTablePath;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StateValuePacker;
};

} // anonymous namespace

IComputationNode* WrapStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    const TKqpComputeContextBase& computeCtx)
{
    MKQL_ENSURE(callable.GetInputsCount() == 12, "StreamingAggregation expected 12 args, got " << callable.GetInputsCount());

    const auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsFlow(), "StreamingAggregation expects flow return type");

    // Locate lambda bodies before popping their arguments: identity handlers
    // refer to the argument node directly.
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

    const auto keyType = callable.GetInput(3).GetStaticType();
    const auto stateValueType = callable.GetInput(2).GetStaticType();

    const auto& stateTablePathLiteral = AS_VALUE(TDataLiteral, callable.GetInput(8));
    const TString stateTablePath(stateTablePathLiteral->AsValue().AsStringRef());

    if (stateTablePath.empty()) {
        auto savedStateArg = LocateExternalNode(ctx.NodeLocator, callable, 9);
        auto savedStateType = callable.GetInput(9).GetStaticType();
        return new TInMemoryStreamingAggregationFlowWrapper(
            ctx.Mutables, GetValueRepresentation(returnType), flow,
            itemArg, stateArg, keyArg,
            outKey, outInit, outUpdate, outFinish, keyType, savedStateArg, outSave, outLoad, savedStateType);
    }

    return new TTableStreamingAggregationFlowWrapper(
        computeCtx, ctx.Mutables, GetValueRepresentation(returnType), flow,
        itemArg, stateArg, keyArg,
        outKey, outInit, outUpdate, outFinish, keyType, stateValueType,
        stateTablePath);
}

} // namespace NKikimr::NMiniKQL
