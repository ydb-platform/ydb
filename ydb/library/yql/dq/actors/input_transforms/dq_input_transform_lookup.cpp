#include "dq_input_transform_lookup.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_key_payload_value_lru_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <chrono>

namespace NYql::NDq {

namespace {

enum class EOutputRowItemSource{None, InputKey, InputOther, LookupKey, LookupOther};
using TOutputRowColumnOrder = std::vector<std::pair<EOutputRowItemSource, ui64>>; //i -> {source, indexInSource}

//Design note: Base implementation is optimized for wide channels
class TInputTransformStreamLookupBase
        : public NActors::TActorBootstrapped<TInputTransformStreamLookupBase>
        , public NYql::NDq::IDqComputeActorAsyncInput
{
public:
    TInputTransformStreamLookupBase(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NMiniKQL::THolderFactory& holderFactory,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        ui64 inputIndex,
        NUdf::TUnboxedValue inputFlow,
        NActors::TActorId computeActorId,
        IDqAsyncIoFactory* factory,
        NDqProto::TDqInputTransformLookupSettings&& settings,
        TVector<bool>&& inputJoinColumnBitmap,
        TVector<size_t>&& lookupJoinColumns,
        const NMiniKQL::TMultiType* inputRowType,
        const NMiniKQL::TStructType* lookupKeyType,
        const NMiniKQL::TStructType* lookupPayloadType,
        const NMiniKQL::TMultiType* outputRowType,
        const TOutputRowColumnOrder& outputRowColumnOrder,
        const size_t cacheLimit,
        const  std::chrono::seconds cacheTtl
    )
        : Alloc(alloc)
        , HolderFactory(holderFactory)
        , TypeEnv(typeEnv)
        , InputIndex(inputIndex)
        , InputFlow(std::move(inputFlow))
        , ComputeActorId(std::move(computeActorId))
        , Factory(factory)
        , Settings(std::move(settings))
        , InputJoinColumnBitmap(std::move(inputJoinColumnBitmap))
        , LookupJoinColumns(std::move(lookupJoinColumns))
        , InputRowType(inputRowType)
        , LookupKeyType(lookupKeyType)
        , KeyTypeHelper(std::make_shared<IDqAsyncLookupSource::TKeyTypeHelper>(lookupKeyType))
        , LookupPayloadType(lookupPayloadType)
        , OutputRowType(outputRowType)
        , OutputRowColumnOrder(outputRowColumnOrder)
        , InputFlowFetchStatus(NUdf::EFetchStatus::Yield)
        , LruCache(std::make_unique<NKikimr::NMiniKQL::TUnboxedKeyValueLruCacheWithTtl>(cacheLimit, lookupKeyType))
        , CacheTtl(cacheTtl)
        , AwaitingQueue()
        , ReadyQueue(OutputRowType)
        , WaitingForLookupResults(false)
    {
        Y_ABORT_UNLESS(Alloc);
    }

    void Bootstrap() {
        Become(&TInputTransformStreamLookupBase::StateFunc);
        NDq::IDqAsyncIoFactory::TLookupSourceArguments lookupSourceArgs {
            .Alloc = Alloc,
            .KeyTypeHelper = KeyTypeHelper,
            .ParentId = SelfId(),
            .LookupSource = Settings.GetRightSource().GetLookupSource(),
            .KeyType = LookupKeyType,
            .PayloadType = LookupPayloadType,
            .TypeEnv = TypeEnv,
            .HolderFactory = HolderFactory,
            .MaxKeysInRequest = 1000 // TODO configure me
        };
        auto guard = Guard(*Alloc);
        LookupSource = Factory->CreateDqLookupSource(Settings.GetRightSource().GetProviderName(), std::move(lookupSourceArgs));
        RegisterWithSameMailbox(LookupSource.second);
    }
protected:
    virtual NUdf::EFetchStatus FetchWideInputValue(NUdf::TUnboxedValue* inputRowItems) = 0;
    virtual void PushOutputValue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, NUdf::TUnboxedValue* outputRowItems) = 0;

private: //events
    STRICT_STFUNC(StateFunc,
        hFunc(IDqAsyncLookupSource::TEvLookupResult, Handle);
    )

    void Handle(IDqAsyncLookupSource::TEvLookupResult::TPtr ev) {
        auto guard = BindAllocator();
        const auto now = std::chrono::steady_clock::now();
        auto lookupResult = std::move(ev->Get()->Result);
        for (auto&& [k, v]: lookupResult) {
            LruCache->Update(NUdf::TUnboxedValue(const_cast<NUdf::TUnboxedValue&&>(k)), std::move(v), now + CacheTtl);
        }
        while (!AwaitingQueue.empty()) {
            const auto& [inputKey, inputOther] = AwaitingQueue.front();
            auto lookupPayload = LruCache->Get(inputKey, now);
            if (!lookupPayload) {
                //NB some element in cache may become stale since creating current lookup request
                break;
            }
            NUdf::TUnboxedValue* outputRowItems;
            NUdf::TUnboxedValue outputRow = HolderFactory.CreateDirectArrayHolder(OutputRowColumnOrder.size(), outputRowItems);
            for (size_t i = 0; i != OutputRowColumnOrder.size(); ++i) {
                const auto& [source, index] = OutputRowColumnOrder[i];
                switch(source) {
                    case EOutputRowItemSource::InputKey:
                        outputRowItems[i] = inputKey.GetElement(index);
                        break;
                    case EOutputRowItemSource::InputOther:
                        outputRowItems[i] = inputOther.GetElement(index);
                        break;
                    case EOutputRowItemSource::LookupKey:
                        outputRowItems[i] = inputKey.GetElement(index);
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
            }
            AwaitingQueue.pop_front();
            ReadyQueue.PushRow(outputRowItems, OutputRowType->GetElementsCount());
        }
        WaitingForLookupResults = false;
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
        Send(LookupSource.second->SelfId(), new NActors::TEvents::TEvPoison{});
        auto guard = BindAllocator();
        //All resources, held by this class, that have been created with mkql allocator, must be deallocated here
        InputFlow.Clear();
        KeyTypeHelper.reset();
        TAwaitingQueue{}.swap(AwaitingQueue);
        NMiniKQL::TUnboxedValueBatch{}.swap(ReadyQueue);
        LruCache.reset();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        auto guard = BindAllocator();
        while (!ReadyQueue.empty()) {
            PushOutputValue(batch, ReadyQueue.Head());
            ReadyQueue.Pop();
        }
        if (!WaitingForLookupResults) {
            const auto maxKeysInRequest = LookupSource.first->GetMaxSupportedKeysInRequest();
            IDqAsyncLookupSource::TUnboxedValueMap keysForLookup{maxKeysInRequest, KeyTypeHelper->GetValueHash(), KeyTypeHelper->GetValueEqual()};
            const auto now = std::chrono::steady_clock::now();
            if (!AwaitingQueue.empty()) {
                //Some elements in cache may become stale during a lookup request and evected from the cache
                //So we need to re-request these keys
                for(size_t i = 0; i!= AwaitingQueue.size() && keysForLookup.size() < maxKeysInRequest; ++i) {
                    const auto& key = AwaitingQueue[i].first;
                    if (const auto item = LruCache->Get(key, now); !item) {
                        keysForLookup.emplace(key, NUdf::TUnboxedValue{});
                    }
                }
            } else if (InputFlowFetchStatus != NUdf::EFetchStatus::Finish) {
                NUdf::TUnboxedValue* inputRowItems;
                NUdf::TUnboxedValue inputRow = HolderFactory.CreateDirectArrayHolder(InputRowType->GetElementsCount(), inputRowItems);
                while (
                    ((InputFlowFetchStatus = FetchWideInputValue(inputRowItems)) == NUdf::EFetchStatus::Ok) && 
                    (keysForLookup.size() < maxKeysInRequest)
                ) {
                    NUdf::TUnboxedValue* keyItems;
                    NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(LookupKeyType->GetMembersCount(), keyItems);
                    NUdf::TUnboxedValue* otherItems;
                    NUdf::TUnboxedValue other = HolderFactory.CreateDirectArrayHolder(InputJoinColumnBitmap.size() - LookupKeyType->GetMembersCount(), otherItems);
                    for (size_t i = 0; i != InputJoinColumnBitmap.size(); ++i) {
                        if (InputJoinColumnBitmap[i]) {
                            *keyItems++ = inputRowItems[i];
                        } else {
                            *otherItems++ = inputRowItems[i];
                        }
                    }                
                    keysForLookup.emplace(key, NUdf::TUnboxedValue{});
                    AwaitingQueue.emplace_back(std::move(key), std::move(other));
                }
            }
            if (!keysForLookup.empty()) {
                LookupSource.first->AsyncLookup(std::move(keysForLookup));
                WaitingForLookupResults = true;
            }
        }
        finished = IsFinished();
        return 0;
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
    IDqAsyncIoFactory::TPtr Factory;
    NDqProto::TDqInputTransformLookupSettings Settings;
protected:
    std::pair<IDqAsyncLookupSource*, NActors::IActor*> LookupSource;
    const TVector<bool> InputJoinColumnBitmap;
    const TVector<size_t> LookupJoinColumns;
    const NMiniKQL::TMultiType* const InputRowType;
    const NMiniKQL::TStructType* const LookupKeyType; //key column types in LookupTable
    std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> KeyTypeHelper;
    const NMiniKQL::TStructType* const LookupPayloadType; //other column types in LookupTable
    const NMiniKQL::TMultiType* const OutputRowType;
    const TOutputRowColumnOrder OutputRowColumnOrder;

    NUdf::EFetchStatus InputFlowFetchStatus;
    std::unique_ptr<NKikimr::NMiniKQL::TUnboxedKeyValueLruCacheWithTtl> LruCache;
    std::chrono::seconds CacheTtl;
    using TInputKeyOtherPair = std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>;
    using TAwaitingQueue = std::deque<TInputKeyOtherPair, NKikimr::NMiniKQL::TMKQLAllocator<TInputKeyOtherPair>>; //input row split in two parts: key columns and other columns
    TAwaitingQueue AwaitingQueue;
    NKikimr::NMiniKQL::TUnboxedValueBatch ReadyQueue;
    std::atomic<bool> WaitingForLookupResults;
    NYql::NDq::TDqAsyncStats IngressStats;
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

TOutputRowColumnOrder CategorizeOutputRowItems(
    const NMiniKQL::TStructType* type,
    TStringBuf leftLabel,
    TStringBuf rightLabel,
    const THashSet<TString>& leftJoinColumns,
    const THashSet<TString>& rightJoinColumns)
{
    TOutputRowColumnOrder result(type->GetMembersCount());
    size_t idxLeftKey = 0;
    size_t idxLeftOther = 0;
    size_t idxRightKey = 0;
    size_t idxRightPayload = 0;
    for (ui32 i = 0; i != type->GetMembersCount(); ++i) {
        const auto prefixedName = type->GetMemberName(i);
        if (prefixedName.starts_with(leftLabel)) {
            Y_ABORT_IF(prefixedName.length() == leftLabel.length());
            const auto name = prefixedName.SubStr(leftLabel.length() + 1); //skip prefix and dot
            if (leftJoinColumns.contains(name)) {
                result[i] = {EOutputRowItemSource::InputKey, idxLeftKey++};
            } else {
                result[i] = {EOutputRowItemSource::InputOther, idxLeftOther++};
            }
        } else if (prefixedName.starts_with(rightLabel)) {
            Y_ABORT_IF(prefixedName.length() == rightLabel.length());
            const auto name = prefixedName.SubStr(rightLabel.length() + 1); //skip prefix and dot
            //presume that indexes in LookupKey, LookupOther has the same relative position as in OutputRow
            if (rightJoinColumns.contains(name)) {
                result[i] = {EOutputRowItemSource::LookupKey, idxRightKey++};
            } else {
                result[i] = {EOutputRowItemSource::LookupOther, idxRightPayload++};
            }
        } else {
            Y_ABORT();
        }
    }
    return result;
}

THashMap<TStringBuf, size_t> GetNameToIndex(const ::google::protobuf::RepeatedPtrField<TProtoStringType>& names) {
    THashMap<TStringBuf, size_t> result;
    for (int i = 0; i != names.size(); ++i) {
        result[names[i]] = i;
    }
    return result;
}

TVector<size_t> GetJoinColumnIndexes(const NMiniKQL::TStructType* type, const THashMap<TStringBuf, size_t>& joinColumns) {
    TVector<size_t> result;
    result.reserve(joinColumns.size());
    for (ui32 i = 0; i != type->GetMembersCount(); ++i) {
        if (auto p = joinColumns.FindPtr(type->GetMemberName(i))) {
            result.push_back(*p);
        }
    }
    return result;
}

TVector<bool> GetJoinColumnIndexesBitmap(const TVector<size_t>& indexes, size_t size) {
    TVector<bool> result(size);
    for (const auto& i: indexes) {
        result[i] = true;
    }
    return result;
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

    auto leftJoinColumns = GetNameToIndex(settings.GetLeftJoinKeyNames());
    auto rightJoinColumns = GetNameToIndex(settings.GetRightJoinKeyNames());
    Y_ABORT_UNLESS(leftJoinColumns.size() == rightJoinColumns.size());

    auto leftJoinColumnIndexes = GetJoinColumnIndexes(narrowInputRowType, leftJoinColumns);
    Y_ABORT_UNLESS(leftJoinColumnIndexes.size() == leftJoinColumns.size());
    auto leftJoinColumnIndexesBitmap = GetJoinColumnIndexesBitmap(leftJoinColumnIndexes, narrowInputRowType->GetMembersCount());

    auto rightJoinColumnIndexes  = GetJoinColumnIndexes(rightRowType, rightJoinColumns);
    Y_ABORT_UNLESS(rightJoinColumnIndexes.size() == rightJoinColumns.size());
    
    const auto& [lookupKeyType, lookupPayloadType] = SplitLookupTableColumns(rightRowType, rightJoinColumns, args.TypeEnv);
    const auto& outputColumnsOrder = CategorizeOutputRowItems(
        narrowOutputRowType,
        settings.GetLeftLabel(),
        settings.GetRightLabel(),
        {settings.GetLeftJoinKeyNames().cbegin(), settings.GetLeftJoinKeyNames().cend()},
        {settings.GetRightJoinKeyNames().cbegin(), settings.GetRightJoinKeyNames().cend()}
    );
    auto actor = isWide ?
        (TInputTransformStreamLookupBase*)new TInputTransformStreamLookupWide(
            args.Alloc,
            args.HolderFactory,
            args.TypeEnv,
            args.InputIndex,
            args.TransformInput,
            args.ComputeActorId,
            factory,
            std::move(settings),
            std::move(leftJoinColumnIndexesBitmap),
            std::move(rightJoinColumnIndexes),
            inputRowType,
            lookupKeyType,
            lookupPayloadType,
            outputRowType,
            outputColumnsOrder,
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
            factory,
            std::move(settings),
            std::move(leftJoinColumnIndexesBitmap),
            std::move(rightJoinColumnIndexes),
            inputRowType,
            lookupKeyType,
            lookupPayloadType,
            outputRowType,
            outputColumnsOrder,
            settings.GetCacheLimit(),
            std::chrono::seconds(settings.GetCacheTtlSeconds())
        );
    return {actor, actor};
}

} // namespace NYql::NDq
