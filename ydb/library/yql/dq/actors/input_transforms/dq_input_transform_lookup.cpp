#include "dq_input_transform_lookup.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_key_payload_value_lru_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <chrono>

namespace NYql::NDq {

namespace {

enum class EOutputRowItemSource{None, InputKey, InputOther, LookupKey, LookupOther};
using TOutputRowColumnOrder = std::vector<std::pair<EOutputRowItemSource, ui64>>; //i -> {source, indexInSource}

//Design note: Base implementation is optimized for wide channels
class TInputTransformStreamLookupBase
        : public NActors::TActor<TInputTransformStreamLookupBase>
        , public NYql::NDq::IDqComputeActorAsyncInput
{
    using TActor = NActors::TActor<TInputTransformStreamLookupBase>;
public:
    TInputTransformStreamLookupBase(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NMiniKQL::THolderFactory& holderFactory,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        ui64 inputIndex,
        NUdf::TUnboxedValue inputFlow,
        NActors::TActorId computeActorId,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        IDqAsyncIoFactory* factory,
        NDqProto::TDqInputTransformLookupSettings&& settings,
        TVector<size_t>&& lookupInputIndexes,
        TVector<size_t>&& otherInputIndexes,
        const NMiniKQL::TMultiType* inputRowType,
        const NMiniKQL::TStructType* lookupKeyType,
        const NMiniKQL::TStructType* lookupPayloadType,
        const NMiniKQL::TMultiType* outputRowType,
        TOutputRowColumnOrder&& outputRowColumnOrder,
        size_t maxDelayedRows,
        size_t cacheLimit,
        std::chrono::seconds cacheTtl
    )
        : TActor(&TInputTransformStreamLookupBase::StateFunc)
        , Alloc(alloc)
        , HolderFactory(holderFactory)
        , TypeEnv(typeEnv)
        , InputIndex(inputIndex)
        , InputFlow(std::move(inputFlow))
        , ComputeActorId(std::move(computeActorId))
        , TaskCounters(taskCounters)
        , Factory(factory)
        , Settings(std::move(settings))
        , LookupInputIndexes(std::move(lookupInputIndexes))
        , OtherInputIndexes(std::move(otherInputIndexes))
        , InputRowType(inputRowType)
        , LookupKeyType(lookupKeyType)
        , KeyTypeHelper(std::make_shared<IDqAsyncLookupSource::TKeyTypeHelper>(lookupKeyType))
        , LookupPayloadType(lookupPayloadType)
        , OutputRowType(outputRowType)
        , OutputRowColumnOrder(std::move(outputRowColumnOrder))
        , InputFlowFetchStatus(NUdf::EFetchStatus::Yield)
        , LruCache(std::make_unique<NKikimr::NMiniKQL::TUnboxedKeyValueLruCacheWithTtl>(cacheLimit, lookupKeyType))
        , MaxDelayedRows(maxDelayedRows)
        , CacheTtl(cacheTtl)
        , MinimumRowSize(OutputRowColumnOrder.size()*sizeof(NUdf::TUnboxedValuePod))
        , PayloadExtraSize(0)
        , ReadyQueue(OutputRowType)
        , LastLruSize(0)
    {
        Y_ABORT_UNLESS(Alloc);
        for (size_t i = 0; i != LookupInputIndexes.size(); ++i) {
            Y_DEBUG_ABORT_UNLESS(LookupInputIndexes[i] < InputRowType->GetElementsCount());
        }
        for (size_t i = 0; i != OtherInputIndexes.size(); ++i) {
            Y_DEBUG_ABORT_UNLESS(OtherInputIndexes[i] < InputRowType->GetElementsCount());
        }
        Y_DEBUG_ABORT_UNLESS(LookupInputIndexes.size() == LookupKeyType->GetMembersCount());
        InitMonCounters(taskCounters);
    }

    ~TInputTransformStreamLookupBase() override {
        Free();
    }

protected:
    virtual NUdf::EFetchStatus FetchWideInputValue(NUdf::TUnboxedValue* inputRowItems) = 0;
    virtual void PushOutputValue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, NUdf::TUnboxedValue* outputRowItems) = 0;

private: //events
    STRICT_STFUNC(StateFunc,
        hFunc(IDqAsyncLookupSource::TEvLookupResult, Handle);
        hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, Handle);
    )

    void Handle(IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr ev) {
        auto evptr = ev->Get();
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(
                                  InputIndex,
                                  evptr->Issues,
                                  evptr->FatalCode));
    }

    void AddReadyQueue(NUdf::TUnboxedValue& lookupKey, NUdf::TUnboxedValue& inputOther, NUdf::TUnboxedValue *lookupPayload) {
            NUdf::TUnboxedValue* outputRowItems;
            NUdf::TUnboxedValue outputRow = HolderFactory.CreateDirectArrayHolder(OutputRowColumnOrder.size(), outputRowItems);
            for (size_t i = 0; i != OutputRowColumnOrder.size(); ++i) {
                const auto& [source, index] = OutputRowColumnOrder[i];
                switch (source) {
                    case EOutputRowItemSource::InputKey:
                        outputRowItems[i] = lookupKey.GetElement(index);
                        break;
                    case EOutputRowItemSource::InputOther:
                        outputRowItems[i] = inputOther.GetElement(index);
                        break;
                    case EOutputRowItemSource::LookupKey:
                        outputRowItems[i] = lookupPayload && *lookupPayload ? lookupKey.GetElement(index) : NUdf::TUnboxedValue {};
                        break;
                    case EOutputRowItemSource::LookupOther:
                        if (lookupPayload && *lookupPayload) {
                            outputRowItems[i] = lookupPayload->GetElement(index);
                        }
                        break;
                    case EOutputRowItemSource::None:
                        Y_ABORT();
                        break;
                }
                if (outputRowItems[i].IsString()) {
                    PayloadExtraSize += outputRowItems[i].AsStringRef().size();
                }
            }
            ReadyQueue.PushRow(outputRowItems, OutputRowType->GetElementsCount());
    }

    void Handle(IDqAsyncLookupSource::TEvLookupResult::TPtr ev) {
        auto startCycleCount = GetCycleCountFast();
        if (!KeysForLookup) {
            return;
        }
        auto guard = BindAllocator();
        const auto now = std::chrono::steady_clock::now();
        auto lookupResult = ev->Get()->Result.lock();
        Y_ABORT_UNLESS(lookupResult == KeysForLookup);
        for (; !AwaitingQueue.empty(); AwaitingQueue.pop_front()) {
            auto& [lookupKey, inputOther] = AwaitingQueue.front();
            auto lookupPayload = lookupResult->FindPtr(lookupKey);
            if (lookupPayload == nullptr) {
                continue;
            }
            AddReadyQueue(lookupKey, inputOther, lookupPayload);
        }
        for (auto&& [k, v]: *lookupResult) {
            LruCache->Update(NUdf::TUnboxedValue(const_cast<NUdf::TUnboxedValue&&>(k)), std::move(v), now + CacheTtl);
        }
        KeysForLookup->clear();
        auto deltaLruSize = (i64)LruCache->Size() - LastLruSize;
        auto deltaTime = GetCpuTimeDelta(startCycleCount);
        CpuTime += deltaTime;
        if (CpuTimeUs) {
            LruSize->Add(deltaLruSize); // Note: there can be several streamlookup tied to same counter, so Add instead of Set
            CpuTimeUs->Add(deltaTime.MicroSeconds());
        }
        LastLruSize += deltaLruSize;
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived{InputIndex});
    }

    //TODO implement checkpoints
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) final {}
    void LoadState(const NYql::NDq::TSourceState&) final {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}

private: //IDqComputeActorAsyncInput
    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    void PassAway() final {
        InputFlowFetchStatus = NUdf::EFetchStatus::Finish;
        Send(LookupSourceId, new NActors::TEvents::TEvPoison{});
        Free();
    }

    void Free() {
        if (LruSize && LastLruSize) {
            LruSize->Add(-LastLruSize);
            LastLruSize = 0;
        }
        auto guard = BindAllocator();
        //All resources, held by this class, that have been created with mkql allocator, must be deallocated here
        KeysForLookup.reset();
        InputFlow.Clear();
        KeyTypeHelper.reset();
        decltype(AwaitingQueue){}.swap(AwaitingQueue);
        decltype(ReadyQueue){}.swap(ReadyQueue);
        LruCache.reset();
    }

    void DrainReadyQueue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) {
        while (!ReadyQueue.empty()) {
            PushOutputValue(batch, ReadyQueue.Head());
            ReadyQueue.Pop();
        }
    }

    std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> GetKeysForLookup() { // must be called with mkql allocator
        if (!KeysForLookup) {
            Y_ENSURE(SelfId());
            Y_ENSURE(!LookupSourceId);
            NDq::IDqAsyncIoFactory::TLookupSourceArguments lookupSourceArgs {
                .Alloc = Alloc,
                .KeyTypeHelper = KeyTypeHelper,
                .ParentId = SelfId(),
                .TaskCounters = TaskCounters,
                .LookupSource = Settings.GetRightSource().GetLookupSource(),
                .KeyType = LookupKeyType,
                .PayloadType = LookupPayloadType,
                .TypeEnv = TypeEnv,
                .HolderFactory = HolderFactory,
                .MaxKeysInRequest = 1000 // TODO configure me
            };
            auto [lookupSource, lookupSourceActor] = Factory->CreateDqLookupSource(Settings.GetRightSource().GetProviderName(), std::move(lookupSourceArgs));
            MaxKeysInRequest = lookupSource->GetMaxSupportedKeysInRequest();
            LookupSourceId = RegisterWithSameMailbox(lookupSourceActor);
            KeysForLookup = std::make_shared<IDqAsyncLookupSource::TUnboxedValueMap>(MaxKeysInRequest, KeyTypeHelper->GetValueHash(), KeyTypeHelper->GetValueEqual());
        }
        return KeysForLookup;
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        auto startCycleCount = GetCycleCountFast();
        auto guard = BindAllocator();

        DrainReadyQueue(batch);

        if (InputFlowFetchStatus != NUdf::EFetchStatus::Finish && GetKeysForLookup()->empty()) {
            Y_DEBUG_ABORT_UNLESS(AwaitingQueue.empty());
            NUdf::TUnboxedValue* inputRowItems;
            NUdf::TUnboxedValue inputRow = HolderFactory.CreateDirectArrayHolder(InputRowType->GetElementsCount(), inputRowItems);
            const auto now = std::chrono::steady_clock::now();
            LruCache->Prune(now);
            size_t rowLimit = std::numeric_limits<size_t>::max();
            size_t row = 0;
            while (
                row < rowLimit &&
                (KeysForLookup->size() < MaxKeysInRequest) &&
                ((InputFlowFetchStatus = FetchWideInputValue(inputRowItems)) == NUdf::EFetchStatus::Ok)) {
                NUdf::TUnboxedValue* keyItems;
                NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(LookupInputIndexes.size(), keyItems);
                NUdf::TUnboxedValue* otherItems;
                NUdf::TUnboxedValue other = HolderFactory.CreateDirectArrayHolder(OtherInputIndexes.size(), otherItems);
                bool nullsInKey = false;
                for (size_t i = 0; i != LookupInputIndexes.size(); ++i) {
                    keyItems[i] = inputRowItems[LookupInputIndexes[i]];
                    if (!keyItems[i]) {
                        nullsInKey = true;
                    }
                }
                for (size_t i = 0; i != OtherInputIndexes.size(); ++i) {
                    otherItems[i] = inputRowItems[OtherInputIndexes[i]];
                }
                if (nullsInKey) {
                    AddReadyQueue(key, other, nullptr);
                } else if (auto lookupPayload = LruCache->Get(key, now)) {
                    AddReadyQueue(key, other, &*lookupPayload);
                } else {
                    if (AwaitingQueue.empty()) {
                        // look ahead at most MaxDelayedRows after first missing
                        rowLimit = row + MaxDelayedRows;
                    }
                    AwaitingQueue.emplace_back(key, std::move(other));
                    KeysForLookup->emplace(std::move(key), NUdf::TUnboxedValue{});
                }
                ++row;
            }
            if (Batches && (!KeysForLookup->empty() || ReadyQueue.RowCount())) {
                Batches->Inc();
                LruHits->Add(ReadyQueue.RowCount());
                LruMiss->Add(AwaitingQueue.size());
            }
            if (!KeysForLookup->empty()) {
                Send(LookupSourceId, new IDqAsyncLookupSource::TEvLookupRequest(KeysForLookup));
            }
            DrainReadyQueue(batch);
        }
        auto deltaTime = GetCpuTimeDelta(startCycleCount);
        CpuTime += deltaTime;
        if (CpuTimeUs) {
            CpuTimeUs->Add(deltaTime.MicroSeconds());
        }
        finished = IsFinished();
        if (batch.empty()) {
            // Use non-zero value to signal presence of pending request
            // (value is NOT used for used space accounting)
            return !AwaitingQueue.empty();
        } else {
            // Attempt to estimate actual byte size;
            // May be over-estimated for shared strings;
            // May be under-estimated for complex types;
            auto usedSpace = batch.RowCount() * MinimumRowSize + PayloadExtraSize;
            PayloadExtraSize = 0;
            return usedSpace;
        }
    }

    void InitMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
        if (!taskCounters) {
            return;
        }
        auto component = taskCounters->GetSubgroup("component", "Lookup");
        LruHits = component->GetCounter("Hits");
        LruMiss = component->GetCounter("Miss");
        LruSize = component->GetCounter("Size");
        CpuTimeUs = component->GetCounter("CpuUs");
        Batches = component->GetCounter("Batches");
    }

    static TDuration GetCpuTimeDelta(ui64 startCycleCount) {
        return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - startCycleCount));
    }

    TDuration GetCpuTime() override {
        return CpuTime;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        //TODO fill me
        return result;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return Guard(*Alloc);
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(issue);
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }
private:
    bool IsFinished() const {
        return NUdf::EFetchStatus::Finish == InputFlowFetchStatus && AwaitingQueue.empty();
    }
protected:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NMiniKQL::THolderFactory& HolderFactory;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    ui64 InputIndex; // NYql::NDq::IDqComputeActorAsyncInput
    NUdf::TUnboxedValue InputFlow;
    const NActors::TActorId ComputeActorId;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    IDqAsyncIoFactory::TPtr Factory;
    NDqProto::TDqInputTransformLookupSettings Settings;
protected:
    NActors::TActorId LookupSourceId;
    size_t MaxKeysInRequest;
    const TVector<size_t> LookupInputIndexes;
    const TVector<size_t> OtherInputIndexes;
    const NMiniKQL::TMultiType* const InputRowType;
    const NMiniKQL::TStructType* const LookupKeyType; //key column types in LookupTable
    std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> KeyTypeHelper;
    const NMiniKQL::TStructType* const LookupPayloadType; //other column types in LookupTable
    const NMiniKQL::TMultiType* const OutputRowType;
    const TOutputRowColumnOrder OutputRowColumnOrder;

    NUdf::EFetchStatus InputFlowFetchStatus;
    std::unique_ptr<NKikimr::NMiniKQL::TUnboxedKeyValueLruCacheWithTtl> LruCache;
    size_t MaxDelayedRows;
    std::chrono::seconds CacheTtl;
    using TInputKeyOtherPair = std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>;
    using TAwaitingQueue = std::deque<TInputKeyOtherPair, NKikimr::NMiniKQL::TMKQLAllocator<TInputKeyOtherPair>>; //input row split in two parts: key columns and other columns
    TAwaitingQueue AwaitingQueue;
    size_t MinimumRowSize; // only account for unboxed parts
    size_t PayloadExtraSize; // non-embedded part of strings in ReadyQueue
    NKikimr::NMiniKQL::TUnboxedValueBatch ReadyQueue;
    NYql::NDq::TDqAsyncStats IngressStats;
    std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> KeysForLookup;
    i64 LastLruSize;

    ::NMonitoring::TDynamicCounters::TCounterPtr LruHits;
    ::NMonitoring::TDynamicCounters::TCounterPtr LruMiss;
    ::NMonitoring::TDynamicCounters::TCounterPtr LruSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr CpuTimeUs;
    ::NMonitoring::TDynamicCounters::TCounterPtr Batches;
    TDuration CpuTime;
};

class TInputTransformStreamLookupWide: public TInputTransformStreamLookupBase {
    using TBase = TInputTransformStreamLookupBase;
public:
    using TBase::TBase;
protected:
    NUdf::EFetchStatus FetchWideInputValue(NUdf::TUnboxedValue* inputRowItems) override {
        return InputFlow.WideFetch(inputRowItems, InputRowType->GetElementsCount());
    }
    void PushOutputValue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, NUdf::TUnboxedValue* outputRowItems) override {
        batch.PushRow(outputRowItems, OutputRowType->GetElementsCount());
    }
};

class TInputTransformStreamLookupNarrow: public TInputTransformStreamLookupBase {
    using TBase = TInputTransformStreamLookupBase;
public:
    using TBase::TBase;
protected:
    NUdf::EFetchStatus FetchWideInputValue(NUdf::TUnboxedValue* inputRowItems) override {
        NUdf::TUnboxedValue row;
        auto result = InputFlow.Fetch(row);
        if (NUdf::EFetchStatus::Ok == result) {
            for (size_t i = 0; i != row.GetListLength(); ++i) {
                inputRowItems[i] = row.GetElement(i);
            }
        }
        return result;
    }
    void PushOutputValue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, NUdf::TUnboxedValue* outputRowItems) override {
        NUdf::TUnboxedValue* narrowOutputRowItems;
        NUdf::TUnboxedValue narrowOutputRow = HolderFactory.CreateDirectArrayHolder(OutputRowType->GetElementsCount(), narrowOutputRowItems);
        for (size_t i = 0; i != OutputRowType->GetElementsCount(); ++i) {
            narrowOutputRowItems[i] = std::move(outputRowItems[i]);
        }
        batch.emplace_back(std::move(narrowOutputRow));
    }
};


std::pair<
    const NMiniKQL::TStructType*, //lookup key, may contain several columns
    const NMiniKQL::TStructType*  //lookup result(payload) the rest columns
> SplitLookupTableColumns(
    const NMiniKQL::TStructType* rowType,
    const THashMap<TStringBuf, size_t>& keyColumns,
    const NMiniKQL::TTypeEnvironment& typeEnv
) {
    NKikimr::NMiniKQL::TStructTypeBuilder key{typeEnv};
    NKikimr::NMiniKQL::TStructTypeBuilder payload{typeEnv};
    for (ui32 i = 0; i != rowType->GetMembersCount(); ++i) {
        if (keyColumns.find(rowType->GetMemberName(i)) != keyColumns.end()) {
            key.Add(rowType->GetMemberName(i), rowType->GetMemberType(i));
        } else {
            payload.Add(rowType->GetMemberName(i), rowType->GetMemberType(i));
        }
    }
    return {key.Build(), payload.Build()};
}

const NMiniKQL::TType* DeserializeType(TStringBuf s, const NMiniKQL::TTypeEnvironment& env) {
    const auto node = NMiniKQL::DeserializeNode(s, env);
    return static_cast<const  NMiniKQL::TType*>(node);
}

const NMiniKQL::TStructType* DeserializeStructType(TStringBuf s, const NMiniKQL::TTypeEnvironment& env) {
    const auto type = DeserializeType(s, env);
    MKQL_ENSURE(type->IsStruct(), "Expected struct type");
    return static_cast<const NMiniKQL::TStructType*>(type);
}

std::tuple<const NMiniKQL::TMultiType*, const NMiniKQL::TMultiType*, bool> DeserializeAsMulti(TStringBuf input, TStringBuf output, const NMiniKQL::TTypeEnvironment& env) {
    const auto inputType = DeserializeType(input, env);
    const auto outputType = DeserializeType(output, env);
    Y_ABORT_UNLESS(inputType->IsMulti() == outputType->IsMulti());
    if (inputType->IsMulti()) {
        return {
            static_cast<const NMiniKQL::TMultiType*>(inputType),
            static_cast<const NMiniKQL::TMultiType*>(outputType),
            true
        };
    } else {
        Y_ABORT_UNLESS(inputType->IsStruct() && outputType->IsStruct());
        const auto inputStruct = static_cast<const NMiniKQL::TStructType*>(inputType);
        std::vector<const NMiniKQL::TType*> inputItems(inputStruct->GetMembersCount());
        for(size_t i = 0; i != inputItems.size(); ++i) {
            inputItems[i] = inputStruct->GetMemberType(i);
        }
        const auto outputStruct = static_cast<const NMiniKQL::TStructType*>(outputType);
        std::vector<const NMiniKQL::TType*> outputItems(outputStruct->GetMembersCount());
        for(size_t i = 0; i != outputItems.size(); ++i) {
            outputItems[i] = outputStruct->GetMemberType(i);
        }

        return {
            NMiniKQL::TMultiType::Create(inputItems.size(), const_cast<NMiniKQL::TType *const*>(inputItems.data()), env),
            NMiniKQL::TMultiType::Create(outputItems.size(), const_cast<NMiniKQL::TType *const*>(outputItems.data()), env),
            false
        };
    }
}

std::pair<
    TOutputRowColumnOrder,
    TVector<size_t>
> CategorizeOutputRowItems(
    const NMiniKQL::TStructType* type,
    TStringBuf leftLabel,
    TStringBuf rightLabel,
    const auto& rightNames,
    const THashMap<TStringBuf, size_t>& leftJoinColumns,
    const THashMap<TStringBuf, size_t>& lookupKeyColumns,
    const THashMap<TStringBuf, size_t>& lookupPayloadColumns,
    const THashMap<TStringBuf, size_t>& inputColumns
)
{
    TOutputRowColumnOrder result(type->GetMembersCount());
    TVector<size_t> otherInputIndexes;
    for (ui32 i = 0; i != type->GetMembersCount(); ++i) {
        const auto prefixedName = type->GetMemberName(i);
        if (prefixedName.starts_with(leftLabel) &&
            prefixedName.length() > leftLabel.length() &&
            prefixedName[leftLabel.length()] == '.') {
            const auto name = prefixedName.SubStr(leftLabel.length() + 1); //skip prefix and dot
            if (auto j = leftJoinColumns.FindPtr(name)) {
                result[i] = { EOutputRowItemSource::InputKey, lookupKeyColumns.at(rightNames[*j]) };
            } else {
                result[i] = { EOutputRowItemSource::InputOther, otherInputIndexes.size() };
                otherInputIndexes.push_back(inputColumns.at(name));
            }
        } else if (prefixedName.starts_with(rightLabel) &&
                   prefixedName.length() > rightLabel.length() &&
                   prefixedName[rightLabel.length()] == '.') {
            const auto name = prefixedName.SubStr(rightLabel.length() + 1); //skip prefix and dot
            if (auto j = lookupKeyColumns.FindPtr(name)) {
                result[i] = { EOutputRowItemSource::LookupKey, *j };
            } else {
                result[i] = { EOutputRowItemSource::LookupOther, lookupPayloadColumns.at(name) };
            }
        } else if (leftLabel.empty()) {
            const auto name = prefixedName;
            if (auto j = leftJoinColumns.FindPtr(name)) {
                result[i] = { EOutputRowItemSource::InputKey, lookupKeyColumns.at(rightNames[*j]) };
            } else if (auto k = inputColumns.FindPtr(name)) {
                result[i] = { EOutputRowItemSource::InputOther, otherInputIndexes.size() };
                otherInputIndexes.push_back(*k);
            } else {
                Y_ABORT();
            }
        } else {
            Y_ABORT();
        }
    }
    return { std::move(result), std::move(otherInputIndexes) };
}

template <typename TIndex, typename TGetter>
THashMap<TStringBuf, size_t> GetNameToIndex(TIndex size, TGetter&& getter) {
    THashMap<TStringBuf, size_t> result;
    for (TIndex i = 0; i != size; ++i) {
        result[getter(i)] = i;
    }
    return result;
}

THashMap<TStringBuf, size_t> GetNameToIndex(const ::google::protobuf::RepeatedPtrField<TProtoStringType>& names) {
    return GetNameToIndex(names.size(), [&names](auto idx) {
        return names[idx];
    });
}

THashMap<TStringBuf, size_t> GetNameToIndex(const NMiniKQL::TStructType* type) {
    return GetNameToIndex(type->GetMembersCount(), [type](auto idx) {
        return type->GetMemberName(idx);
    });
}

template <typename TIndex, typename TGetter>
TVector<size_t> GetJoinColumnIndexes(TIndex size, TGetter&& getter, const THashMap<TStringBuf, size_t>& joinColumns) {
    TVector<size_t> result;
    result.reserve(size);
    for (TIndex i = 0; i != size; ++i) {
        if (auto p = joinColumns.FindPtr(getter(i))) {
            result.push_back(*p);
        }
    }
    return result;
}

[[maybe_unused]]
TVector<size_t> GetJoinColumnIndexes(const ::google::protobuf::RepeatedPtrField<TProtoStringType>& names, const THashMap<TStringBuf, size_t>& joinColumns) {
    return GetJoinColumnIndexes(names.size(), [&names](auto idx) {
        return names[idx];
    }, joinColumns);
}

TVector<size_t> GetJoinColumnIndexes(const NMiniKQL::TStructType* type, const THashMap<TStringBuf, size_t>& joinColumns) {
    return GetJoinColumnIndexes(type->GetMembersCount(), [type](auto idx) {
        return type->GetMemberName(idx);
    }, joinColumns);
}

} // namespace

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateInputTransformStreamLookup(
    IDqAsyncIoFactory* factory,
    NDqProto::TDqInputTransformLookupSettings&& settings,
    IDqAsyncIoFactory::TInputTransformArguments&& args
)
{
    const auto narrowInputRowType = DeserializeStructType(settings.GetNarrowInputRowType(), args.TypeEnv);
    const auto narrowOutputRowType = DeserializeStructType(settings.GetNarrowOutputRowType(), args.TypeEnv);

    const auto& transform = args.InputDesc.transform();
    const auto [inputRowType, outputRowType, isWide] = DeserializeAsMulti(transform.GetInputType(), transform.GetOutputType(), args.TypeEnv);

    const auto rightRowType = DeserializeStructType(settings.GetRightSource().GetSerializedRowType(), args.TypeEnv);

    auto inputColumns = GetNameToIndex(narrowInputRowType);
    auto leftJoinColumns = GetNameToIndex(settings.GetLeftJoinKeyNames());
    auto rightJoinColumns = GetNameToIndex(settings.GetRightJoinKeyNames());

    auto rightJoinColumnIndexes  = GetJoinColumnIndexes(rightRowType, rightJoinColumns);
    Y_ABORT_UNLESS(rightJoinColumnIndexes.size() == rightJoinColumns.size());

    auto&& [lookupKeyType, lookupPayloadType] = SplitLookupTableColumns(rightRowType, rightJoinColumns, args.TypeEnv);

    auto lookupKeyColumns = GetNameToIndex(lookupKeyType);
    auto lookupPayloadColumns = GetNameToIndex(lookupPayloadType);

    auto lookupKeyInputIndexes = GetJoinColumnIndexes(
            lookupKeyType->GetMembersCount(),
            [&leftJoinKeyNames = settings.GetLeftJoinKeyNames(),
             &rightJoinColumns, &lookupKeyType = lookupKeyType](auto idx) {
                return leftJoinKeyNames[rightJoinColumns.at(lookupKeyType->GetMemberName(idx))];
            }, inputColumns);

    auto&& [outputColumnsOrder, otherInputIndexes] = CategorizeOutputRowItems(
        narrowOutputRowType,
        settings.GetLeftLabel(),
        settings.GetRightLabel(),
        settings.GetRightJoinKeyNames(),
        leftJoinColumns,
        lookupKeyColumns,
        lookupPayloadColumns,
        inputColumns
    );
    auto actor = isWide ?
        (TInputTransformStreamLookupBase*)new TInputTransformStreamLookupWide(
            args.Alloc,
            args.HolderFactory,
            args.TypeEnv,
            args.InputIndex,
            args.TransformInput,
            args.ComputeActorId,
            args.TaskCounters,
            factory,
            std::move(settings),
            std::move(lookupKeyInputIndexes),
            std::move(otherInputIndexes),
            inputRowType,
            lookupKeyType,
            lookupPayloadType,
            outputRowType,
            std::move(outputColumnsOrder),
            settings.GetMaxDelayedRows(),
            settings.GetCacheLimit(),
            std::chrono::seconds(settings.GetCacheTtlSeconds())
        ) :
        (TInputTransformStreamLookupBase*)new TInputTransformStreamLookupNarrow(
            args.Alloc,
            args.HolderFactory,
            args.TypeEnv,
            args.InputIndex,
            args.TransformInput,
            args.ComputeActorId,
            args.TaskCounters,
            factory,
            std::move(settings),
            std::move(lookupKeyInputIndexes),
            std::move(otherInputIndexes),
            inputRowType,
            lookupKeyType,
            lookupPayloadType,
            outputRowType,
            std::move(outputColumnsOrder),
            settings.GetMaxDelayedRows(),
            settings.GetCacheLimit(),
            std::chrono::seconds(settings.GetCacheTtlSeconds())
        );
    return {actor, actor};
}

} // namespace NYql::NDq
