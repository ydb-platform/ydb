#include "dq_input_transform_lookup.h"
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

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
        ui64 inputIndex,
        NUdf::TUnboxedValue inputFlow,
        NActors::TActorId computeActorId,
        NDqProto::TDqInputTransformLookupSettings&& settings,
        TVector<size_t>&& inputJoinColumns,
        TVector<size_t>&& lookupJoinColumns,
        const NMiniKQL::TMultiType* const inputRowType,
        const NMiniKQL::TMultiType* const outputRowType,
        const TOutputRowColumnOrder& outputRowColumnOrder
    )
        : Alloc(alloc)
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , InputFlow(std::move(inputFlow))
        , ComputeActorId(std::move(computeActorId))
        , Settings(std::move(settings))
        , InputJoinColumns(std::move(inputJoinColumns))
        , LookupJoinColumns(std::move(lookupJoinColumns))
        , InputRowType(inputRowType)
        , OutputRowType(outputRowType)
        , OutputRowColumnOrder(outputRowColumnOrder)
        , InputFlowFetchStatus(NUdf::EFetchStatus::Yield)
        , AwaitingQueue(InputRowType)
        , ReadyQueue(OutputRowType)
    {
        Y_ABORT_UNLESS(Alloc);
    }

    void Bootstrap() {
        //TODO implement me
    }
protected:
    virtual NUdf::EFetchStatus FetchWideInputValue(NUdf::TUnboxedValue* inputRowItems) = 0;
    virtual void PushOutputValue(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, NUdf::TUnboxedValue* outputRowItems) = 0;

private:
    void HandleLookup() {
        //TODO fixme. Temp passthrow mode
        auto guard = BindAllocator();
        while(!AwaitingQueue.empty()) {
            const auto wideInputRow = AwaitingQueue.Head();
            NUdf::TUnboxedValue* outputRowItems;
            NUdf::TUnboxedValue outputRow = HolderFactory.CreateDirectArrayHolder(OutputRowColumnOrder.size(), outputRowItems);
            for (size_t i = 0; i != OutputRowColumnOrder.size(); ++i) {
                const auto& [source, index] = OutputRowColumnOrder[i];
                switch(source) {
                    case EOutputRowItemSource::InputKey:
                    case EOutputRowItemSource::InputOther:
                        outputRowItems[i] = wideInputRow[index];
                        break;
                    case EOutputRowItemSource::LookupKey:
                        //TODO fixme. Switch to values from lookup table
                        for (size_t k = 0; k != LookupJoinColumns.size(); ++k) {
                            if (LookupJoinColumns[k] == index) {
                                outputRowItems[i] = wideInputRow[InputJoinColumns[k]];
                                break;        
                            }
                        }
                        break;
                    case EOutputRowItemSource::LookupOther:
                        //TODO fix me. Fill with lookup results
                        outputRowItems[i] = NUdf::TUnboxedValuePod{}; // NKikimr::NMiniKQL::MakeString("QQQ");
                        break;
                    case EOutputRowItemSource::None:
                        Y_ABORT();
                        break;
                }
            }
            AwaitingQueue.Pop();
            ReadyQueue.PushRow(outputRowItems, OutputRowType->GetElementsCount());
        }
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
        auto guard = BindAllocator();
        //All resources, hold by this class, that have been created with mkql allocator, must be deallocated here
        InputFlow.Clear();
        NMiniKQL::TUnboxedValueBatch{}.swap(AwaitingQueue);
        NMiniKQL::TUnboxedValueBatch{}.swap(ReadyQueue);
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        auto guard = BindAllocator();
        while (!ReadyQueue.empty()) {
            PushOutputValue(batch, ReadyQueue.Head());
            ReadyQueue.Pop();
        }

        if (InputFlowFetchStatus != NUdf::EFetchStatus::Finish) {
            NUdf::TUnboxedValue* inputRowItems;
            NUdf::TUnboxedValue inputRow = HolderFactory.CreateDirectArrayHolder(InputRowType->GetElementsCount(), inputRowItems);
            while (
                (InputFlowFetchStatus = FetchWideInputValue(inputRowItems)) == NUdf::EFetchStatus::Ok
            ) {
                AwaitingQueue.PushRow(inputRowItems, InputRowType->GetElementsCount());
            }
        }
        if (!AwaitingQueue.empty()) {
            HandleLookup();
        }
        finished = IsFinished() && ReadyQueue.empty();
        return 0;
    }

private:
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return Guard(*Alloc);
    }

    bool IsFinished() const {
        return NUdf::EFetchStatus::Finish == InputFlowFetchStatus && AwaitingQueue.empty();
    }
protected:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NMiniKQL::THolderFactory& HolderFactory;
    ui64 InputIndex; // NYql::NDq::IDqComputeActorAsyncInput
    NUdf::TUnboxedValue InputFlow;
    const NActors::TActorId ComputeActorId; 

    NDqProto::TDqInputTransformLookupSettings Settings;
    const TVector<size_t> InputJoinColumns;
    const TVector<size_t> LookupJoinColumns;
    const NMiniKQL::TMultiType* const InputRowType;
    const NMiniKQL::TMultiType* const OutputRowType;
    const TOutputRowColumnOrder OutputRowColumnOrder;

    NUdf::EFetchStatus InputFlowFetchStatus;
    NKikimr::NMiniKQL::TUnboxedValueBatch AwaitingQueue;
    NKikimr::NMiniKQL::TUnboxedValueBatch ReadyQueue;
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
    size_t idxLeft = 0;
    size_t idxRight = 0;
    for (ui32 i = 0; i != type->GetMembersCount(); ++i) {
        const auto prefixedName = type->GetMemberName(i);
        if (prefixedName.starts_with(leftLabel)) {
            Y_ABORT_IF(prefixedName.length() == leftLabel.length());
            const auto name = prefixedName.SubStr(leftLabel.length() + 1); //skip prefix and dot
            result[i] = {
                leftJoinColumns.contains(name) ? EOutputRowItemSource::InputKey : EOutputRowItemSource::InputOther,
                idxLeft++
            };
        } else if (prefixedName.starts_with(rightLabel)) {
            Y_ABORT_IF(prefixedName.length() == rightLabel.length());
            const auto name = prefixedName.SubStr(rightLabel.length() + 1); //skip prefix and dot
            result[i] = {
                rightJoinColumns.contains(name) ? EOutputRowItemSource::LookupKey : EOutputRowItemSource::LookupOther,
                idxRight++
            };
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
    for (ui32 i = 0; i != type->GetMembersCount(); ++i) {
        if (auto p = joinColumns.FindPtr(type->GetMemberName(i))) {
            result.push_back(*p);
        }
    }
    return result;
}

} // namespace

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateInputTransformStreamLookup(
    NDqProto::TDqInputTransformLookupSettings&& settings,
    IDqAsyncIoFactory::TInputTransformArguments&& args //TODO expand me
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
    auto rightJoinColumnIndexes  = GetJoinColumnIndexes(rightRowType, rightJoinColumns);
    Y_ABORT_UNLESS(rightJoinColumnIndexes.size() == rightJoinColumns.size());
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
            //args.TypeEnv,
            args.InputIndex,
            args.TransformInput,
            args.ComputeActorId,
            std::move(settings),
            std::move(leftJoinColumnIndexes),
            std::move(rightJoinColumnIndexes),
            inputRowType,
            outputRowType,
            outputColumnsOrder
        ) :
        (TInputTransformStreamLookupBase*)new TInputTransformStreamLookupNarrow(
            args.Alloc,
            args.HolderFactory,
            //args.TypeEnv,
            args.InputIndex,
            args.TransformInput,
            args.ComputeActorId,
            std::move(settings),
            std::move(leftJoinColumnIndexes),
            std::move(rightJoinColumnIndexes),
            inputRowType,
            outputRowType,
            outputColumnsOrder
        );
    return {actor, actor};
}

} // namespace NYql::NDq
