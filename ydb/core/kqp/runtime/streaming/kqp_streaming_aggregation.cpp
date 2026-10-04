#include "kqp_streaming_aggregation.h"

#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/helpers/future_callback.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <library/cpp/threading/future/future.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/escape.h>
#include <util/string/subst.h>

#include <list>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>

namespace NKikimr::NMiniKQL {

namespace NPrivate {

TString QuoteStreamingAggregationIdentifier(const TStringBuf& name) {
    auto escaped = EscapeC(name);
    SubstGlobal(escaped, "`", "\\`");
    return TStringBuilder() << '`' << escaped << '`';
}

} // namespace NPrivate

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

template <typename TDerived>
class TTableStreamingAggregationFlowWrapperBase : public TStreamingAggregationFlowWrapperBase<TDerived> {
    using TBase = TStreamingAggregationFlowWrapperBase<TDerived>;

protected:
    template <typename TResult>
    struct TEvStateQueryResult : public NActors::TEventLocal<TEvStateQueryResult<TResult>, EventSpaceBegin(NActors::TEvents::ES_PRIVATE)> {
        using TValue = std::conditional_t<std::is_void_v<TResult>, std::monostate, TResult>;

        TEvStateQueryResult(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues, TValue result)
            : Status(status)
            , Issues(std::move(issues))
            , Result(std::move(result))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        TValue Result;
    };

    template <typename TQueryActor, typename TResultType>
    class TStateQueryActorBase : public TQueryBase, public TQueryRetryActorMixin<TQueryActor, TEvStateQueryResult<TResultType>> {
    public:
        using TResult = TResultType;
        using TResponseEvent = TEvStateQueryResult<TResult>;

        TStateQueryActorBase(const TString& operationName, TString database, TMaybe<TString> userToken, TString tablePath)
            : TQueryBase(NKikimrServices::KQP_COMPUTE, {}, std::move(database), /* isSystemUser */ false, /* isStreamingMode */ false, std::move(userToken))
            , TablePath(std::move(tablePath))
        {
            SetOperationInfo(operationName, /* traceId */ "");
        }

    private:
        void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
            Send(Owner, new TResponseEvent(status, std::move(issues), std::move(ResolvedResult)));
        }

    protected:
        const TString TablePath;
        typename TResponseEvent::TValue ResolvedResult;
    };

public:
    TTableStreamingAggregationFlowWrapperBase(
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
        , StateTablePath(NPrivate::QuoteStreamingAggregationIdentifier(stateTablePath))
    {
        MKQL_ENSURE(!stateTablePath.empty(), "Streaming aggregation output state table path is empty");
    }

protected:
    NUdf::TUnboxedValue EmitFinish(NUdf::TUnboxedValue key, NUdf::TUnboxedValue state, TComputationContext& ctx) const {
        this->KeyArg->SetValue(ctx, std::move(key));
        this->StateArg->SetValue(ctx, std::move(state));
        return this->OutFinish->GetValue(ctx);
    }

    template <typename TQueryActor, typename... TArgs>
    NThreading::TFuture<typename TQueryActor::TResult> RunStateTableQuery(TArgs&&... args) const {
        using TResponseEvent = typename TQueryActor::TResponseEvent;

        Y_VALIDATE(NActors::TlsActivationContext, "KqpStreamingAggregation table backend requires actor TLS context");
        Y_VALIDATE(ComputeCtx.GetWakeupCallback(), "KqpStreamingAggregation table backend requires a wakeup callback in the KQP compute context");
        auto* const actorSystem = NActors::TlsActivationContext->ActorSystem();

        auto promise = NThreading::NewPromise<typename TQueryActor::TResult>();
        auto future = promise.GetFuture();
        future.Subscribe([wakeupCallback = ComputeCtx.GetWakeupCallback()](const auto&) {
            wakeupCallback();
        });

        const auto replyActor = actorSystem->Register(new NActors::TActorFutureCallback<TResponseEvent>(
            [promise = std::move(promise), tablePath = StateTablePath](typename TResponseEvent::TPtr& ev) mutable {
                auto& response = *ev->Get();
                if (response.Status != Ydb::StatusIds::SUCCESS) {
                    promise.SetException(TStringBuilder() << "Streaming aggregation " << TQueryActor::OPERATION_NAME
                        << " failed for table " << tablePath << ", status=" << static_cast<int>(response.Status)
                        << ": " << response.Issues.ToString());
                } else if constexpr (std::is_void_v<typename TQueryActor::TResult>) {
                    promise.SetValue();
                } else {
                    promise.SetValue(std::move(response.Result));
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

class TTableStreamingAggregationFlowWrapper final : public TTableStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper> {
    using TBase = TTableStreamingAggregationFlowWrapperBase<TTableStreamingAggregationFlowWrapper>;

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

    class TSelectStateActor final : public TStateQueryActorBase<TSelectStateActor, std::optional<std::string>> {
    public:
        static constexpr TStringBuf OPERATION_NAME = "SELECT";

        TSelectStateActor(const TString& database, const TMaybe<TString>& userToken, const TString& tablePath, const TString& key)
            : TStateQueryActorBase(__func__, database, userToken, tablePath)
            , Key(key)
        {}

    private:
        void OnRunQuery() final {
            const TString sql = fmt::format(R"sql(
                DECLARE $key AS String;
                SELECT state FROM {} WHERE key = $key;
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
                MKQL_ENSURE(ResolvedResult, "State table contains a NULL aggregation state");
            }

            Finish();
        }

        const TString Key;
    };

    class TUpsertStateActor final : public TStateQueryActorBase<TUpsertStateActor, void> {
    public:
        static constexpr TStringBuf OPERATION_NAME = "UPSERT";

        TUpsertStateActor(const TString& database, const TMaybe<TString>& userToken, const TString& tablePath, const TString& key, const TString& state)
            : TStateQueryActorBase(__func__, database, userToken, tablePath)
            , Key(key)
            , State(state)
        {}

    private:
        void OnRunQuery() final {
            const TString sql = fmt::format(R"sql(
                DECLARE $key AS String;
                DECLARE $state AS String;
                UPSERT INTO {} (key, state) VALUES ($key, $state);
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
        const TString Key;
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
    using TBase::TBase;

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
};

class TOutputTableStreamingAggregationFlowWrapper final : public TTableStreamingAggregationFlowWrapperBase<TOutputTableStreamingAggregationFlowWrapper> {
    using TThis = TOutputTableStreamingAggregationFlowWrapper;
    using TBase = TTableStreamingAggregationFlowWrapperBase<TThis>;

    class TReadRowActor final : public TStateQueryActorBase<TReadRowActor, Ydb::ResultSet> {
    public:
        static constexpr TStringBuf OPERATION_NAME = "SELECT";

        TReadRowActor(TString database, TMaybe<TString> userToken, TString tablePath, TString sql, TVector<NYdb::TValue> keys)
            : TStateQueryActorBase("StreamingAggregationReadRow", std::move(database), std::move(userToken), std::move(tablePath))
            , Sql(std::move(sql))
            , Keys(std::move(keys))
        {}

    private:
        void OnRunQuery() final {
            NYdb::TParamsBuilder params;
            for (size_t i = 0; i < Keys.size(); ++i) {
                params.AddParam(TStringBuilder() << "$key" << i, Keys[i]);
            }
            RunDataQuery(Sql, &params);
        }

        void OnQueryResult() final {
            Y_VALIDATE(ResultSets.size() == 1, "Unexpected output state table SELECT result set count: " << ResultSets.size());
            ResolvedResult = NYdb::TProtoAccessor::GetProto(ResultSets.front());
            Finish();
        }

        const TString Sql;
        const TVector<NYdb::TValue> Keys;
    };

    class TState final : public TComputationValue<TState> {
    public:
        enum class EStep : ui8 {
            Work,
            ReadRow,
        };

        TState(TMemoryUsageInfo* const memInfo, const TThis& self)
            : TComputationValue<TState>(memInfo)
            , Map(0, self.KeyTypeHelper.GetValueHash(), self.KeyTypeHelper.GetValueEqual())
        {}

        ~TState() final {
            for (const auto& [key, value] : Map) {
                key.UnRef();
            }
        }

        TMap<NUdf::TUnboxedValue> Map;
        EStep Step = EStep::Work;
        NUdf::TUnboxedValue PendingItem;
        NUdf::TUnboxedValue PendingKey;
        NThreading::TFuture<Ydb::ResultSet> ReadFuture;
    };

public:
    TOutputTableStreamingAggregationFlowWrapper(
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
        const TTupleLiteral& binding)
        : TBase(computeCtx, mutables, kind, flow, itemArg, stateArg, keyArg, outKey, outInit, outUpdate, outFinish, keyType,
                savedStateArg, outSave, outLoad, savedStateType, TString(AS_VALUE(TDataLiteral, binding.GetValue(0))->AsValue().AsStringRef()))
    {
        const auto tableColumn = [columns = AS_VALUE(TStructLiteral, binding.GetValue(1))](const TStringBuf& name) {
            const auto index = columns->GetType()->GetMemberIndex(name);
            return NPrivate::QuoteStreamingAggregationIdentifier(TString(AS_VALUE(TDataLiteral, columns->GetValue(index))->AsValue().AsStringRef()));
        };

        const auto* const keys = AS_TYPE(TStructType, KeyType);
        const auto keyMembersCount = keys->GetMembersCount();
        TStringBuilder sql;
        KeyTypes.reserve(keyMembersCount);
        KeyColumns.reserve(keyMembersCount);
        for (ui32 i = 0; i < keyMembersCount; ++i) {
            Ydb::Type type;
            ExportTypeToProto(keys->GetMemberType(i), type);
            sql << "DECLARE $key" << i << " AS " << NYdb::FormatType(KeyTypes.emplace_back(std::move(type))) << ";\n";
            KeyColumns.emplace_back(tableColumn(keys->GetMemberName(i)));
        }

        sql << "SELECT ";
        const auto* saved = AS_TYPE(TStructType, SavedStateType);
        const auto savedMembersCount = saved->GetMembersCount();
        SavedTypes.reserve(savedMembersCount);
        for (ui32 i = 0; i < savedMembersCount; ++i) {
            sql << (i ? ", " : "") << tableColumn(saved->GetMemberName(i)) << " AS state" << i;

            Ydb::Type type;
            ExportTypeToProto(saved->GetMemberType(i), type);
            SavedTypes.emplace_back(WithoutOptional(type));
        }

        if (!saved->GetMembersCount()) {
            sql << "TRUE AS present";
        }

        SelectSql = sql << " FROM " << StateTablePath;
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(*this);
        }
        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());

        if (state.Step == TState::EStep::ReadRow) {
            return FinishReadRow(state, ctx);
        }

        auto item = Flow->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }

        ItemArg->SetValue(ctx, NUdf::TUnboxedValue(item));
        auto key = OutKey->GetValue(ctx);

        if (const auto it = state.Map.find(key); it != state.Map.end()) {
            StateArg->SetValue(ctx, NUdf::TUnboxedValue(it->second));
            it->second = OutUpdate->GetValue(ctx);
            return EmitFinish(std::move(key), it->second, ctx);
        }

        state.PendingItem = std::move(item);
        state.PendingKey = std::move(key);
        state.ReadFuture = ReadRow(state.PendingKey);
        state.Step = TState::EStep::ReadRow;
        return NUdf::TUnboxedValuePod::MakeYield();
    }

private:
    static const Ydb::Type& WithoutOptional(const Ydb::Type& type) {
        return type.has_optional_type() ? WithoutOptional(type.optional_type().item()) : type;
    }

    NUdf::TUnboxedValue FinishReadRow(TState& state, TComputationContext& ctx) const {
        if (!state.ReadFuture.IsReady()) {
            return NUdf::TUnboxedValuePod::MakeYield();
        }

        const auto result = state.ReadFuture.ExtractValue();
        state.ReadFuture = {};
        MKQL_ENSURE(result.rows_size() <= 1, "Streaming aggregation output state lookup returned more than one row");
        ItemArg->SetValue(ctx, std::move(state.PendingItem));

        NUdf::TUnboxedValue newState;
        if (result.rows_size()) {
            const auto* saved = AS_TYPE(TStructType, SavedStateType);
            const auto count = saved->GetMembersCount();
            const auto& resultRow = result.rows(0);
            MKQL_ENSURE(result.columns_size() == static_cast<int>(count ? count : 1) && resultRow.items_size() == result.columns_size(), "Unexpected output state table row shape");

            NUdf::TUnboxedValue* fields = nullptr;
            auto previous = ctx.HolderFactory.CreateDirectArrayHolder(count, fields);
            for (ui32 i = 0; i < count; ++i) {
                MKQL_ENSURE(NYdb::TypesEqual(NYdb::TType(WithoutOptional(result.columns(i).type())), SavedTypes[i]), "Unexpected output state table column type: " << result.columns(i).name());
                const auto* type = saved->GetMemberType(i);
                const auto& value = resultRow.items(i);
                MKQL_ENSURE(type->IsOptional() || type->IsPg() || value.value_case() != Ydb::Value::kNullFlagValue, "Output state table contains NULL for required aggregation state: " << saved->GetMemberName(i));
                fields[i] = ImportValueFromProto(saved->GetMemberType(i), value, ctx.TypeEnv, ctx.HolderFactory);
            }

            SavedStateArg->SetValue(ctx, std::move(previous));
            StateArg->SetValue(ctx, OutLoad->GetValue(ctx));
            newState = OutUpdate->GetValue(ctx);
        } else {
            newState = OutInit->GetValue(ctx);
        }

        const auto [it, inserted] = state.Map.emplace(state.PendingKey, std::move(newState));
        Y_VALIDATE(inserted, "Duplicate output table aggregation state");
        it->first.Ref();

        state.Step = TState::EStep::Work;
        return EmitFinish(std::move(state.PendingKey), it->second, ctx);
    }

    NThreading::TFuture<Ydb::ResultSet> ReadRow(const NUdf::TUnboxedValue& key) const {
        TStringBuilder sql;
        sql << SelectSql;

        TVector<NYdb::TValue> keys;
        const auto* const keyType = AS_TYPE(TStructType, KeyType);
        const auto keyMembersCount = keyType->GetMembersCount();
        keys.reserve(keyMembersCount);
        for (ui32 i = 0; i < keyType->GetMembersCount(); ++i) {
            Ydb::Value value;
            ExportValueToProto(keyType->GetMemberType(i), key.GetElement(i), value);

            sql << (i ? " AND " : " WHERE ") << KeyColumns[i];
            if (value.value_case() == Ydb::Value::kNullFlagValue) {
                sql << " IS NULL";
            } else {
                sql << " = $key" << i;
            }

            keys.emplace_back(KeyTypes[i], std::move(value));
        }

        sql << ';';
        return RunStateTableQuery<TReadRowActor>(TString(sql), std::move(keys));
    }

    TString SelectSql;
    TVector<TString> KeyColumns;
    TVector<NYdb::TType> KeyTypes;
    TVector<NYdb::TType> SavedTypes;
};

} // anonymous namespace

IComputationNode* WrapKqpStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    const TKqpComputeContextBase& computeCtx)
{
    MKQL_ENSURE(callable.GetInputsCount() >= 12, "KqpStreamingAggregation expected at least 12 args, got " << callable.GetInputsCount());

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

    const auto stateTable = callable.GetInput(8);
    if (stateTable.GetStaticType()->IsTuple()) {
        const auto* binding = AS_VALUE(TTupleLiteral, stateTable);
        MKQL_ENSURE(binding->GetValuesCount() == 2, "Expected output state table path and column mapping");
        return new TOutputTableStreamingAggregationFlowWrapper(
            computeCtx, ctx.Mutables, GetValueRepresentation(returnType), flow, itemArg, stateArg, keyArg,
            outKey, outInit, outUpdate, outFinish, keyType, savedStateArg, outSave, outLoad, savedStateType, *binding);
    }

    const auto& stateTablePathLiteral = AS_VALUE(TDataLiteral, stateTable);
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
