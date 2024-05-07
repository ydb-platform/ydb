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

class TInputTransformStreamLookup
        : public NActors::TActorBootstrapped<TInputTransformStreamLookup>
        , public NYql::NDq::IDqComputeActorAsyncInput
{
public:
    TInputTransformStreamLookup(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NMiniKQL::THolderFactory& holderFactory,
        ui64 inputIndex,
        NUdf::TUnboxedValue inputFlow,
        NActors::TActorId computeActorId,
        NDqProto::TDqInputTransformLookupSettings&& settings,
        TVector<size_t>&& inputJoinColumns,
        TVector<size_t>&& lookupJoinColumns,
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
        , OutputRowColumnOrder(outputRowColumnOrder)
        , InputFlowFetchStatus(NUdf::EFetchStatus::Yield)
    {
        Y_ABORT_UNLESS(Alloc);
    }

    void Bootstrap() {
        //TODO implement me
    }

private: //IDqComputeActorAsyncInput
    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }


    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");
        auto guard = BindAllocator();
        while(!AwaitingQueue.empty()) {
            const auto& row = AwaitingQueue.front();
            NUdf::TUnboxedValue* outputRowItems;
            auto outputRow = HolderFactory.CreateDirectArrayHolder(OutputRowColumnOrder.size(), outputRowItems);
            for (size_t i = 0; i != OutputRowColumnOrder.size(); ++i) {
                const auto& [source, index] = OutputRowColumnOrder[i];
                switch(source) {
                    case EOutputRowItemSource::InputKey:
                    case EOutputRowItemSource::InputOther:
                        outputRowItems[i] = row.GetElement(index);
                        break;
                    case EOutputRowItemSource::LookupKey:
                        //TODO fixme. Switch to values from lookup table
                        for (size_t k = 0; k != LookupJoinColumns.size(); ++k) {
                            if (LookupJoinColumns[k] == index) {
                                outputRowItems[i] = row.GetElement(InputJoinColumns[k]);
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
            batch.push_back(std::move(outputRow));
            AwaitingQueue.pop_front();
            //TODO check space
        }
        if (InputFlowFetchStatus != NUdf::EFetchStatus::Finish) {
            NUdf::TUnboxedValue row;
            while ((InputFlowFetchStatus = InputFlow.Fetch(row)) == NUdf::EFetchStatus::Ok) {
                AwaitingQueue.push_back(std::move(row));
            }
        }
        //TODO fixme. Temp passthrow mode
        if (!AwaitingQueue.empty()) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived{InputIndex});
        }
        finished = IsFinished();
        return 0;
    }

        //No checkpointg required
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) final {}
    void LoadState(const NYql::NDq::TSourceState&) final {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}

    void PassAway() final {
        auto guard = BindAllocator();
        //All resources, hold by this class, that have been created with mkql allocator, must be deallocated here
        InputFlow.Clear();
        NMiniKQL::TUnboxedValueDeque{}.swap(AwaitingQueue);
    }

private:
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return Guard(*Alloc);
    }

    bool IsFinished() const {
        return NUdf::EFetchStatus::Finish == InputFlowFetchStatus && AwaitingQueue.empty();
    }
private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NMiniKQL::THolderFactory& HolderFactory;
    ui64 InputIndex; // NYql::NDq::IDqComputeActorAsyncInput
    NUdf::TUnboxedValue InputFlow;
    const NActors::TActorId ComputeActorId; 

    NDqProto::TDqInputTransformLookupSettings Settings;
    const TVector<size_t> InputJoinColumns;
    const TVector<size_t> LookupJoinColumns;
    const TOutputRowColumnOrder OutputRowColumnOrder;

    NUdf::EFetchStatus InputFlowFetchStatus;
    NMiniKQL::TUnboxedValueDeque AwaitingQueue;
    NYql::NDq::TDqAsyncStats IngressStats;
};

const NMiniKQL::TStructType* DeserializeStructType(TStringBuf s, const NMiniKQL::TTypeEnvironment& env) {
    const auto node = NMiniKQL::DeserializeNode(s, env);
    auto type = static_cast<const  NMiniKQL::TType*>(node);
    MKQL_ENSURE(type->IsStruct(), "Expected struct type");
    return static_cast<const NMiniKQL::TStructType*>(type);
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
    const auto& transform = args.InputDesc.transform();
    const auto inputRowType = DeserializeStructType(transform.GetInputType(), args.TypeEnv);
    const auto outputRowType = DeserializeStructType(transform.GetOutputType(), args.TypeEnv);
    const auto rightRowType = DeserializeStructType(settings.GetRightSource().GetSerializedRowType(), args.TypeEnv);

    auto leftJoinColumns = GetNameToIndex(settings.GetLeftJoinKeyNames());
    auto rightJoinColumns = GetNameToIndex(settings.GetRightJoinKeyNames());
    Y_ABORT_UNLESS(leftJoinColumns.size() == rightJoinColumns.size());

    auto leftJoinColumnIndexes = GetJoinColumnIndexes(inputRowType, leftJoinColumns);
    Y_ABORT_UNLESS(leftJoinColumnIndexes.size() == leftJoinColumns.size());
    auto rightJoinColumnIndexes  = GetJoinColumnIndexes(rightRowType, rightJoinColumns);
    Y_ABORT_UNLESS(rightJoinColumnIndexes.size() == rightJoinColumns.size());
    auto columnOrder = CategorizeOutputRowItems(
        outputRowType, 
        settings.GetLeftLabel(),
        settings.GetRightLabel(),
        {settings.GetLeftJoinKeyNames().cbegin(), settings.GetLeftJoinKeyNames().cend()},
        {settings.GetRightJoinKeyNames().cbegin(), settings.GetRightJoinKeyNames().cend()}
    );

    auto actor = new TInputTransformStreamLookup(
        args.Alloc,
        args.HolderFactory,
        args.InputIndex,
        args.TransformInput,
        args.ComputeActorId,
        std::move(settings),
        std::move(leftJoinColumnIndexes),
        std::move(rightJoinColumnIndexes),
        std::move(columnOrder)
    );
    return {actor, actor};
}

} // namespace NYql::NDq
