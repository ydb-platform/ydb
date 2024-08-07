#include "mkql_join.h"

#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>
#include <util/generic/ylimits.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const ui64 DEFAULT_STACK_ITEMS = 16;

static const TStatKey Join_Spill_Count("Join_Spill_Count", true);
static const TStatKey Join_Spill_MaxFileSize("Join_Spill_MaxFileSize", false);
static const TStatKey Join_Spill_MaxRowsCount("Join_Spill_MaxRowsCount", false);

enum class EOutputMode {
    Unknown,
    LeftNull,
    RightNull,
    BothNull,
    Cross,
    CrossSwap,
    None
};

std::vector<bool> FillRequiredStructColumn(const ui32 inputWidth, const std::vector<ui32>& requiredColumns) {
    std::vector<bool> result(inputWidth, false);
    for (const auto i : requiredColumns) {
        result[i] = true;
    }
    return result;
}

enum ETableIndex : ui32 {
    LeftIndex = 0U,
    RightIndex = 1U
};

namespace NFlow {

using TFetcher = std::function<EFetchResult(TComputationContext&, NUdf::TUnboxedValue*const*)>;
using TLiveFetcher = std::function<EFetchResult(TComputationContext&, NUdf::TUnboxedValue*)>;

class TSpillList {
public:
    TSpillList(TValuePacker& itemPacker, bool singleShot, size_t width = 0ULL)
        : Width(width)
        , ItemPacker(itemPacker)
        , Count(0)
#ifndef NDEBUG
        , IsSealed(false)
#endif
        , Index(ui64(-1))
        , SingleShot(singleShot)
    {}

    TSpillList(TSpillList&& rhs) = delete;
    TSpillList(const TSpillList& rhs) = delete;
    void operator=(const TSpillList& rhs) = delete;

    void Init() {
        Count = 0;
#ifndef NDEBUG
        IsSealed = false;
#endif
        Index = ui64(-1);
        FileState = nullptr;
        Heap.clear();
        LiveFlow = nullptr;
        LiveValue = NUdf::TUnboxedValue();
    }

    bool Spill() {
        if (FileState) {
            return false;
        }

        FileState.reset(new TFileState);
        OpenWrite();
        for (ui32 i = 0; i < Count; ++i) {
            Write(std::move(InMemory(i)));
        }

        Heap.clear();
        return true;
    }

    void Live(IComputationNode* flow, NUdf::TUnboxedValue&& liveValue) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count == 0);
        LiveFlow = flow;
        LiveValue = std::move(liveValue);
    }

    void Live(TLiveFetcher&& fetcher, NUdf::TUnboxedValue* liveValues) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count == 0);
        Fetcher = std::move(fetcher);
        LiveValues = liveValues;
    }

    void Add(NUdf::TUnboxedValue&& value) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(!IsSealed);
#endif
        if (SingleShot && Count > 0) {
            MKQL_ENSURE(Count == 1, "Counter inconsistent");
            return;
        }

        if (FileState) {
            Write(std::move(value));
        } else {
            if (Count < DEFAULT_STACK_ITEMS) {
                Stack[Count] = std::move(value);
            }
            else {
                if (Count == DEFAULT_STACK_ITEMS) {
                    Y_DEBUG_ABORT_UNLESS(Heap.empty());
                    Heap.assign(Stack, Stack + DEFAULT_STACK_ITEMS);
                }

                Heap.push_back(std::move(value));
            }
        }

        ++Count;
    }

    void Seal(TComputationContext& ctx) {
#ifndef NDEBUG
        IsSealed = true;
#endif
        if (FileState) {
            FileState->Output->Finish();
            Cerr << "Spill finished at " << Count << " items" << Endl;
            FileState->Output.reset();
            Cerr << "File size: " << GetFileLength(FileState->File.GetName()) << ", expected: " << FileState->TotalSize << Endl;

            MKQL_INC_STAT(ctx.Stats, Join_Spill_Count);
            MKQL_SET_MAX_STAT(ctx.Stats, Join_Spill_MaxFileSize, static_cast<i64>(FileState->TotalSize));
            MKQL_SET_MAX_STAT(ctx.Stats, Join_Spill_MaxRowsCount, static_cast<i64>(Count));
        }
    }

    bool IsLive() const {
        return bool(LiveFlow) || bool(Fetcher);
    }

    ui64 GetCount() const {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        return Count;
    }

    bool Empty() const {
        return !IsLive() && (Count == 0);
    }

    NUdf::TUnboxedValue Next(TComputationContext& ctx) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed);
#endif
        if (IsLive()) {
            if ((Index + 1) == 0) {
                ++Index;
                return std::move(LiveValue);
            }

            auto value = LiveFlow->GetValue(ctx);
            while (SingleShot && !value.IsSpecial()) {
                // skip all remaining values
                value = LiveFlow->GetValue(ctx);
            }

            if (!value.IsSpecial()) {
                ++Index;
            }
            return value;
        }

        if ((Index + 1) == Count) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        ++Index;
        if (FileState) {
            if (Index == 0) {
                OpenRead();
            }

            return Read(ctx);
        }

        return InMemory(Index);
    }

    EFetchResult Next(TComputationContext& ctx, NUdf::TUnboxedValue* values) {
        if (IsLive()) {
            if ((Index + 1) == 0) {
                ++Index;

                if (values != LiveValues)
                    for (auto i = 0U; i < Width; ++i)
                        *values++ = std::move(*LiveValues++);

                LiveValues = nullptr;
                return EFetchResult::One;
            }

            auto result = Fetcher(ctx, values);
            while (SingleShot && EFetchResult::One == result) {
                // skip all remaining values
                result = Fetcher(ctx, values);
            }

            if (EFetchResult::One == result) {
                ++Index;
            }
            return result;
        }

        if ((Index + 1) == Count) {
            return EFetchResult::Finish;
        }

        ++Index;
        if (FileState) {
            if (Index == 0) {
                OpenRead();
            }

            std::copy_n(Read(ctx).GetElements(), Width, values);
            return EFetchResult::One;
        }

        std::copy_n(InMemory(Index).GetElements(), Width, values);
        return EFetchResult::One;
    }

    void Rewind() {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed);
#endif
        Index = ui64(-1);
        if (FileState) {
            OpenRead();
        }
    }

private:
    NUdf::TUnboxedValue& InMemory(ui32 index) {
        return !Heap.empty() ? Heap[index] : Stack[index];
    }

    const NUdf::TUnboxedValue& InMemory(ui32 index) const {
        return !Heap.empty() ? Heap[index] : Stack[index];
    }

    void OpenWrite() {
        Cerr << "Spill started at " << Count << " items to " << FileState->File.GetName() << Endl;
        FileState->Output.reset(new TFixedBufferFileOutput(FileState->File.GetName()));
        FileState->Output->SetFlushPropagateMode(false);
        FileState->Output->SetFinishPropagateMode(false);
    }

    void Write(NUdf::TUnboxedValue&& value) {
        Y_DEBUG_ABORT_UNLESS(FileState->Output);
        TStringBuf serialized = ItemPacker.Pack(value);
        ui32 length = serialized.size();
        FileState->Output->Write(&length, sizeof(length));
        FileState->Output->Write(serialized.data(), length);
        FileState->TotalSize += sizeof(length);
        FileState->TotalSize += length;
    }

    void OpenRead() {
        FileState->Input.reset();
        FileState->Input.reset(new TFileInput(FileState->File.GetName()));
    }

    NUdf::TUnboxedValue Read(TComputationContext& ctx) {
        ui32 length = 0;
        auto wasRead = FileState->Input->Load(&length, sizeof(length));
        Y_ABORT_UNLESS(wasRead == sizeof(length));
        FileState->Buffer.Reserve(length);
        wasRead = FileState->Input->Load((void*)FileState->Buffer.Data(), length);
        Y_ABORT_UNLESS(wasRead == length);
        return ReadValue = ItemPacker.Unpack(TStringBuf(FileState->Buffer.Data(), length), ctx.HolderFactory);
    }

private:
    const size_t Width;
    TValuePacker& ItemPacker;
    ui64 Count;
    NUdf::TUnboxedValue ReadValue;
    NUdf::TUnboxedValue Stack[DEFAULT_STACK_ITEMS];
    TUnboxedValueVector Heap;
#ifndef NDEBUG
    bool IsSealed;
#endif
    ui64 Index;
    const bool SingleShot;
    struct TFileState {
        TFileState()
            : File(TTempFileHandle::InCurrentDir())
            , TotalSize(0)
        {}

        TTempFileHandle File;
        ui64 TotalSize;
        std::unique_ptr<TFileInput> Input;
        std::unique_ptr<TFixedBufferFileOutput> Output;
        TBuffer Buffer;
    };

    std::unique_ptr<TFileState> FileState;
    IComputationNode* LiveFlow = nullptr;
    TLiveFetcher Fetcher;
    NUdf::TUnboxedValue LiveValue;
    NUdf::TUnboxedValue* LiveValues = nullptr;
};

template <EJoinKind Kind, bool TTrackRss>
class TCommonJoinCoreWrapper : public TStatefulFlowComputationNode<TCommonJoinCoreWrapper<Kind, TTrackRss>> {
    using TSelf = TCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TStatefulFlowComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    friend TSelf;
    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TSelf* self)
            : TBase(memInfo)
            , Self(self)
            , List1(Self->Packer.RefMutableObject(ctx, false, Self->InputStructType), IsAnyJoinLeft(Self->AnyJoinSettings))
            , List2(Self->Packer.RefMutableObject(ctx, false, Self->InputStructType), IsAnyJoinRight(Self->AnyJoinSettings))
        {
            Init();
        }

        void Init() {
            List1.Init();
            List2.Init();
            CrossMove1 = true;
            EatInput = true;
            KeyHasNulls = false;
            OutputMode = EOutputMode::Unknown;
            InitialUsage = std::nullopt;
        }

    private:
        // copypaste to resolve -Woverloaded-virtual
        bool Next(NUdf::TUnboxedValue&) override {
            this->ThrowNotSupported(__func__);
            return false;
        }

        NUdf::TUnboxedValue Next(IComputationNode* flow, TComputationContext& ctx) {
            while (EatInput) {
                if (!InitialUsage) {
                    InitialUsage = ctx.HolderFactory.GetPagePool().GetUsed();
                }

                if (auto value = flow->GetValue(ctx); value.IsYield()) {
                    return value;
                } else if (value.IsFinish()) {
                    EatInput = false;
                } else {
                    if (!KeyHasNulls && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                        for (ui32 i = 0U; i < Self->KeyColumns.size(); ++i) {
                            if (!value.GetElement(Self->KeyColumns[i])) {
                                KeyHasNulls = true;
                                break;
                            }
                        }
                    }

                    switch (const auto tableIndex = value.GetElement(Self->TableIndexPos).template Get<ui32>()) {
                        case LeftIndex:
                            if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2.Empty() && !KeyHasNulls)) {
                                EatInput = false;
                                OutputMode = EOutputMode::None;
                                break;
                            }

                            if (Self->SortedTableOrder && *Self->SortedTableOrder == RightIndex) {
                                List1.Live(flow, std::move(value));
                                EatInput = false;
                            } else {
                                List1.Add(std::move(value));
                                if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                    List1.Spill();
                                }
                            }
                            break;
                        case RightIndex:
                            if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1.Empty() && !KeyHasNulls)) {
                                EatInput = false;
                                OutputMode = EOutputMode::None;
                                break;
                            }

                            if (Self->SortedTableOrder && *Self->SortedTableOrder == LeftIndex) {
                                List2.Live(flow, std::move(value));
                                EatInput = false;
                            } else {
                                List2.Add(std::move(value));
                                if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                    List2.Spill();
                                }
                            }
                            break;
                        default: THROW yexception() << "Bad table index: " << tableIndex;
                    }
                }
            }

            while (true) {
                switch (OutputMode) {
                case EOutputMode::Unknown: {
                        List1.Seal(ctx);
                        List2.Seal(ctx);
                        switch (Kind) {
                        case EJoinKind::Cross:
                        case EJoinKind::Inner:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }

                            break;
                        case EJoinKind::Left:
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::LeftOnly:
                            if (List1.Empty() || !List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::Right:
                            if (List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::RightOnly:
                            if (List2.Empty() || !List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        case EJoinKind::Exclusion:
                            if (!List1.Empty() && !List2.Empty() && !KeyHasNulls) {
                                OutputMode = EOutputMode::None;
                            } else if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else {
                                OutputMode = EOutputMode::BothNull;
                            }
                            break;

                        case EJoinKind::Full:
                            break;

                        case EJoinKind::LeftSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::RightSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        default:
                            Y_ABORT("Unknown kind");
                        }

                        if (OutputMode == EOutputMode::Unknown) {
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else if (List1.IsLive()) {
                                OutputMode = EOutputMode::Cross;
                            } else if (List2.IsLive()) {
                                OutputMode = EOutputMode::CrossSwap;
                            } else {
                                OutputMode = List1.GetCount() >= List2.GetCount() ?
                                    EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                    continue;
                case EOutputMode::LeftNull:
                    if (const auto item = List2.Next(ctx); item.IsSpecial()) {
                        return item;
                    } else {
                        return PrepareNullItem<true>(ctx, item);
                    }
                case EOutputMode::RightNull:
                    if (const auto item = List1.Next(ctx); item.IsSpecial()) {
                        return item;
                    } else {
                        return PrepareNullItem<false>(ctx, item);
                    }
                case EOutputMode::BothNull:
                    if (CrossMove1) {
                        if (const auto item = List1.Next(ctx); item.IsFinish()) {
                            CrossMove1 = false;
                        } else if (item.IsYield()) {
                            return item;
                        } else {
                            return PrepareNullItem<false>(ctx, item);
                        }
                    }

                    if (const auto item = List2.Next(ctx); item.IsSpecial()) {
                        return item;
                    } else {
                        return PrepareNullItem<true>(ctx, item);
                    }
                case EOutputMode::Cross:
                    return PrepareCrossItem<false>(ctx);
                case EOutputMode::CrossSwap:
                    return PrepareCrossItem<true>(ctx);
                case EOutputMode::None:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                default:
                    Y_ABORT("Unknown output mode");
                }
            }
        }

        template <bool IsLeftNull>
        NUdf::TUnboxedValue PrepareNullItem(TComputationContext& ctx, const NUdf::TUnboxedValue& value) {
            const auto structObj = Self->ResStruct.NewArray(ctx, Self->LeftInputColumns.size() + Self->RightInputColumns.size(), ResItems);

            for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                ui32 inIndex = Self->LeftInputColumns[i];
                ui32 outIndex = Self->LeftOutputColumns[i];
                if constexpr (IsLeftNull) {
                    ResItems[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self->IsRequiredColumn[inIndex]) {
                    ResItems[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems[outIndex] = std::move(member);
                }
            }

            for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                ui32 inIndex = Self->RightInputColumns[i];
                ui32 outIndex = Self->RightOutputColumns[i];
                if constexpr (!IsLeftNull) {
                    ResItems[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self->IsRequiredColumn[inIndex]) {
                    ResItems[outIndex] = member.Release().GetOptionalValue();
                }
                else {
                    ResItems[outIndex] = std::move(member);
                }
            }

            return structObj;
        }

        template <bool SwapLists>
        NUdf::TUnboxedValue PrepareCrossItem(TComputationContext& ctx) {
            if (KeyHasNulls) {
                for (;;) {
                    const auto& value = (CrossMove1 == SwapLists ? List2 : List1).Next(ctx);
                    if (value.IsFinish() && CrossMove1) {
                        CrossMove1 = false;
                        continue;
                    }

                    if (value.IsSpecial()) {
                        return value;
                    }

                    return (CrossMove1 == SwapLists) ? PrepareNullItem<true>(ctx, value) : PrepareNullItem<false>(ctx, value);
                }
            }

            for (;;) {
                if (CrossMove1) {
                    CrossValue1 = (SwapLists ? List2 : List1).Next(ctx);
                    if (CrossValue1.IsSpecial()) {
                        return CrossValue1;
                    }

                    CrossMove1 = false;
                    (SwapLists ? List1 : List2).Rewind();
                }

                CrossValue2 = (SwapLists ? List1 : List2).Next(ctx);
                if (CrossValue2.IsFinish()) {
                    CrossMove1 = true;
                    continue;
                }

                auto structObj = Self->ResStruct.NewArray(ctx, Self->LeftInputColumns.size() + Self->RightInputColumns.size(), ResItems);

                for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                    ui32 inIndex = Self->LeftInputColumns[i];
                    ui32 outIndex = Self->LeftOutputColumns[i];
                    auto member = (SwapLists ? CrossValue2 : CrossValue1).GetElement(inIndex);
                    if (Self->IsRequiredColumn[inIndex]) {
                        ResItems[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems[outIndex] = std::move(member);
                    }
                }

                for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                    ui32 inIndex = Self->RightInputColumns[i];
                    ui32 outIndex = Self->RightOutputColumns[i];
                    auto member = (SwapLists ? CrossValue1 : CrossValue2).GetElement(inIndex);
                    if (Self->IsRequiredColumn[inIndex]) {
                        ResItems[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems[outIndex] = std::move(member);
                    }
                }

                return std::move(structObj);
            }
        }


    private:
        const TSelf* const Self;
        bool EatInput;
        bool KeyHasNulls;
        std::optional<ui64> InitialUsage;
        EOutputMode OutputMode;

        bool CrossMove1;
        NUdf::TUnboxedValue CrossValue1;
        NUdf::TUnboxedValue CrossValue2;

        TSpillList List1;
        TSpillList List2;

        NUdf::TUnboxedValue* ResItems = nullptr;
    };

    TCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationNode* flow, const TType* inputStructType, ui32 inputWidth, ui32 tableIndexPos,
        std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
        std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
        std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , Flow(flow)
        , InputStructType(inputStructType)
        , Packer(mutables)
        , TableIndexPos(tableIndexPos)
        , LeftInputColumns(std::move(leftInputColumns))
        , RightInputColumns(std::move(rightInputColumns))
        , RequiredColumns(std::move(requiredColumns))
        , LeftOutputColumns(std::move(leftOutputColumns))
        , RightOutputColumns(std::move(rightOutputColumns))
        , MemLimit(memLimit)
        , SortedTableOrder(sortedTableOrder)
        , KeyColumns(std::move(keyColumns))
        , IsRequiredColumn(FillRequiredStructColumn(inputWidth, RequiredColumns))
        , ResStruct(mutables)
        , ResStreamIndex(mutables.CurValueIndex++)
        , AnyJoinSettings(anyJoinSettings)
    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TValue>(ctx, this);
        }

        return static_cast<TValue*>(state.AsBoxed().Get())->Next(Flow, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow);
    }

    IComputationNode* const Flow;
    const TType* const InputStructType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer;
    const ui32 TableIndexPos;
    const std::vector<ui32> LeftInputColumns;
    const std::vector<ui32> RightInputColumns;
    const std::vector<ui32> RequiredColumns;
    const std::vector<ui32> LeftOutputColumns;
    const std::vector<ui32> RightOutputColumns;
    const ui64 MemLimit;
    const std::optional<ui32> SortedTableOrder;
    const std::vector<ui32> KeyColumns;
    const std::vector<bool> IsRequiredColumn;

    const TContainerCacheOnContext ResStruct;
    const ui32 ResStreamIndex;
    const EAnyJoinSettings AnyJoinSettings;
};

template <EJoinKind Kind, bool TTrackRss>
class TWideCommonJoinCoreWrapper : public TStatefulWideFlowCodegeneratorNode<TWideCommonJoinCoreWrapper<Kind, TTrackRss>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
    using TSelf = TWideCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TStatefulWideFlowCodegeneratorNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    friend TSelf;
    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TSelf* self, TFetcher&& fetcher)
            : TBase(memInfo)
            , Self(self)
            , Fetcher(std::move(fetcher))
            , Values(Self->InputRepresentations.size(), NUdf::TUnboxedValuePod())
            , CrossValues1(std::max(Self->LeftInputColumns.size(), Self->RightInputColumns.size()), NUdf::TUnboxedValuePod())
            , CrossValues2(std::max(Self->LeftInputColumns.size(), Self->RightInputColumns.size()), NUdf::TUnboxedValuePod())
            , List1(Self->PackerLeft.RefMutableObject(ctx, false, Self->InputLeftType), IsAnyJoinLeft(Self->AnyJoinSettings), Self->InputLeftType->GetElementsCount())
            , List2(Self->PackerRight.RefMutableObject(ctx, false, Self->InputRightType), IsAnyJoinRight(Self->AnyJoinSettings), Self->InputRightType->GetElementsCount())
            , Fields(GetPointers(Values))
            , Stubs(Values.size(), nullptr)
        {
            Init();
        }

        void Init() {
            List1.Init();
            List2.Init();
            CrossMove1 = true;
            EatInput = true;
            KeyHasNulls = false;
            OutputMode = EOutputMode::Unknown;
            InitialUsage = std::nullopt;
        }

    private:
        // copypaste to resolve -Woverloaded-virtual
        bool Next(NUdf::TUnboxedValue&) override {
            this->ThrowNotSupported(__func__);
            return false;
        }

        EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
            while (EatInput) {
                if (!InitialUsage) {
                    InitialUsage = ctx.HolderFactory.GetPagePool().GetUsed();
                }

                switch (Fetcher(ctx, Fields.data())) {
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        EatInput = false;
                        continue;
                    default:
                        break;
                }

                if (!KeyHasNulls && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                    for (ui32 i = 0U; i < Self->KeyColumns.size(); ++i) {
                        if (!*Fields[Self->KeyColumns[i]]) {
                            KeyHasNulls = true;
                            break;
                        }
                    }
                }

                switch (const auto tableIndex = Fields[Self->TableIndexPos]->template Get<ui32>()) {
                    case LeftIndex:
                        if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2.Empty() && !KeyHasNulls)) {
                            EatInput = false;
                            OutputMode = EOutputMode::None;
                            break;
                        }

                        if (Self->SortedTableOrder && *Self->SortedTableOrder == RightIndex) {
                            auto fetcher = IsAnyJoinLeft(Self->AnyJoinSettings) ?
                                TLiveFetcher(std::bind(Fetcher, std::placeholders::_1, Stubs.data())):
                                [this] (TComputationContext& ctx, NUdf::TUnboxedValue* output) {
                                    if (const auto status = Fetcher(ctx, Fields.data()); EFetchResult::One != status)
                                        return status;
                                    std::transform(Self->LeftInputColumns.cbegin(), Self->LeftInputColumns.cend(), output, [this] (ui32 index) { return std::move(this->Values[index]); });
                                    return EFetchResult::One;
                                };
                            std::transform(Self->LeftInputColumns.cbegin(), Self->LeftInputColumns.cend(), Values.data(), [this] (ui32 index) { return std::move(this->Values[index]); });
                            List1.Live(std::move(fetcher), Values.data());
                            EatInput = false;
                        } else {
                            NUdf::TUnboxedValue* items = nullptr;
                            auto value = ctx.HolderFactory.CreateDirectArrayHolder(Self->LeftInputColumns.size(), items);
                            std::transform(Self->LeftInputColumns.cbegin(), Self->LeftInputColumns.cend(), items, [this] (ui32 index) { return std::move(this->Values[index]); });
                            List1.Add(std::move(value));
                            if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                List1.Spill();
                            }
                        }
                        break;
                    case RightIndex:
                        if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1.Empty() && !KeyHasNulls)) {
                            EatInput = false;
                            OutputMode = EOutputMode::None;
                            break;
                        }

                        if (Self->SortedTableOrder && *Self->SortedTableOrder == LeftIndex) {
                            auto fetcher = IsAnyJoinRight(Self->AnyJoinSettings) ?
                                TLiveFetcher(std::bind(Fetcher, std::placeholders::_1, Stubs.data())):
                                [this] (TComputationContext& ctx, NUdf::TUnboxedValue* output) {
                                    if (const auto status = Fetcher(ctx, Fields.data()); EFetchResult::One != status)
                                        return status;
                                    std::transform(Self->RightInputColumns.cbegin(), Self->RightInputColumns.cend(), output, [this] (ui32 index) { return std::move(this->Values[index]); });
                                    return EFetchResult::One;
                                };
                            std::transform(Self->RightInputColumns.cbegin(), Self->RightInputColumns.cend(), Values.data(), [this] (ui32 index) { return std::move(this->Values[index]); });
                            List2.Live(std::move(fetcher), Values.data());
                            EatInput = false;
                        } else {
                            NUdf::TUnboxedValue* items = nullptr;
                            auto value = ctx.HolderFactory.CreateDirectArrayHolder(Self->RightInputColumns.size(), items);
                            std::transform(Self->RightInputColumns.cbegin(), Self->RightInputColumns.cend(), items, [this] (ui32 index) { return std::move(this->Values[index]); });
                            List2.Add(std::move(value));
                            if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                List2.Spill();
                            }
                        }
                        break;
                    default: THROW yexception() << "Bad table index: " << tableIndex;
                }
            }

            while (true) {
                switch (OutputMode) {
                case EOutputMode::Unknown: {
                        List1.Seal(ctx);
                        List2.Seal(ctx);
                        switch (Kind) {
                        case EJoinKind::Cross:
                        case EJoinKind::Inner:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }

                            break;
                        case EJoinKind::Left:
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::LeftOnly:
                            if (List1.Empty() || !List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::Right:
                            if (List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::RightOnly:
                            if (List2.Empty() || !List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        case EJoinKind::Exclusion:
                            if (!List1.Empty() && !List2.Empty() && !KeyHasNulls) {
                                OutputMode = EOutputMode::None;
                            } else if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else {
                                OutputMode = EOutputMode::BothNull;
                            }
                            break;

                        case EJoinKind::Full:
                            break;

                        case EJoinKind::LeftSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::RightSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        default:
                            Y_ABORT("Unknown kind");
                        }

                        if (OutputMode == EOutputMode::Unknown) {
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else if (List1.IsLive()) {
                                OutputMode = EOutputMode::Cross;
                            } else if (List2.IsLive()) {
                                OutputMode = EOutputMode::CrossSwap;
                            } else {
                                OutputMode = List1.GetCount() >= List2.GetCount() ?
                                    EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                    continue;
                case EOutputMode::LeftNull:
                    if (const auto res = List2.Next(ctx, Values.data()); EFetchResult::One != res) {
                        return res;
                    }

                    PrepareNullItem<true>(ctx, output);
                    return EFetchResult::One;

                case EOutputMode::RightNull:
                    if (const auto res = List1.Next(ctx, Values.data()); EFetchResult::One != res) {
                        return res;
                    }

                    PrepareNullItem<false>(ctx, output);
                    return EFetchResult::One;
                case EOutputMode::BothNull:
                    if (CrossMove1) {
                        switch (List1.Next(ctx, Values.data())) {
                            case EFetchResult::Finish: CrossMove1 = false; break;
                            case EFetchResult::Yield: return EFetchResult::Yield;
                            case EFetchResult::One:
                                PrepareNullItem<false>(ctx, output);
                                return EFetchResult::One;
                        }
                    }

                    if (const auto res = List2.Next(ctx, Values.data()); EFetchResult::One != res) {
                        return res;
                    }

                    PrepareNullItem<true>(ctx, output);
                    return EFetchResult::One;
                case EOutputMode::Cross:
                    return PrepareCrossItem<false>(ctx, output);
                case EOutputMode::CrossSwap:
                    return PrepareCrossItem<true>(ctx, output);
                case EOutputMode::None:
                    return EFetchResult::Finish;
                default:
                    Y_ABORT("Unknown output mode");
                }
            }
        }

        template <bool IsLeftNull>
        void PrepareNullItem(TComputationContext&, NUdf::TUnboxedValue*const* output) {
            for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                if (const auto out = output[Self->LeftOutputColumns[i]]) {
                    if constexpr (IsLeftNull) {
                        *out = NUdf::TUnboxedValuePod();
                    } else if (Self->IsRequiredColumn[Self->LeftInputColumns[i]]) {
                        *out = Values[i].Release().GetOptionalValue();
                    } else {
                        *out = std::move(Values[i]);
                    }
                }
            }

            for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                if (const auto out = output[Self->RightOutputColumns[i]]) {
                    if constexpr (!IsLeftNull) {
                        *out = NUdf::TUnboxedValuePod();
                    } else if (Self->IsRequiredColumn[Self->RightInputColumns[i]]) {
                        *out = Values[i].Release().GetOptionalValue();
                    } else {
                        *out = std::move(Values[i]);
                    }
                }
            }
        }

        template <bool SwapLists>
        EFetchResult PrepareCrossItem(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
            if (KeyHasNulls) {
                for (;;) {
                    if (const auto res = (CrossMove1 == SwapLists ? List2 : List1).Next(ctx, Values.data()); EFetchResult::Finish == res && CrossMove1) {
                        CrossMove1 = false;
                        continue;
                    } else if (EFetchResult::One != res) {
                        return res;
                    }

                    if (CrossMove1 == SwapLists)
                        PrepareNullItem<true>(ctx, output);
                    else
                        PrepareNullItem<false>(ctx, output);

                    return EFetchResult::One;
                }
            }

            for (;;) {
                if (CrossMove1) {
                    if (const auto res = (SwapLists ? List2 : List1).Next(ctx, CrossValues1.data()); EFetchResult::One != res) {
                        return res;
                    }

                    CrossMove1 = false;
                    (SwapLists ? List1 : List2).Rewind();
                }

                if (const auto res = (SwapLists ? List1 : List2).Next(ctx, CrossValues2.data()); EFetchResult::Finish == res) {
                    CrossMove1 = true;
                    continue;
                } else if (EFetchResult::Yield == res) {
                    return EFetchResult::Yield;
                }

                const auto& lValues = SwapLists ? CrossValues2 : CrossValues1;
                const auto& rValues = SwapLists ? CrossValues1 : CrossValues2;

                for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                    if (const auto out = output[Self->LeftOutputColumns[i]]) {
                        if (Self->IsRequiredColumn[Self->LeftInputColumns[i]]) {
                            *out = NUdf::TUnboxedValue(lValues[i]).Release().GetOptionalValue();
                        } else {
                            *out = lValues[i];
                        }
                    }
                }

                for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                    if (const auto out = output[Self->RightOutputColumns[i]]) {
                        if (Self->IsRequiredColumn[Self->RightInputColumns[i]]) {
                            *out = NUdf::TUnboxedValue(rValues[i]).Release().GetOptionalValue();
                        } else {
                            *out = rValues[i];
                        }
                    }
                }

                return EFetchResult::One;
            }
        }

    private:
        static std::vector<NUdf::TUnboxedValue*> GetPointers(std::vector<NUdf::TUnboxedValue>& array) {
            std::vector<NUdf::TUnboxedValue*> pointers;
            pointers.reserve(array.size());
            std::transform(array.begin(), array.end(), std::back_inserter(pointers), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });
            return pointers;
        }


        const TSelf* const Self;
        TFetcher Fetcher;
        bool EatInput;
        bool KeyHasNulls;
        std::optional<ui64> InitialUsage;
        EOutputMode OutputMode;

        bool CrossMove1;

        std::vector<NUdf::TUnboxedValue> Values, CrossValues1, CrossValues2;

        TSpillList List1, List2;

        NUdf::TUnboxedValue* ResItems = nullptr;
        const std::vector<NUdf::TUnboxedValue*> Fields;
        const std::vector<NUdf::TUnboxedValue*> Stubs;
    };

    TWideCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, const TTupleType* inputLeftType, const TTupleType* inputRightType,
        std::vector<EValueRepresentation>&& inputRepresentations, std::vector<EValueRepresentation>&& outputRepresentations, ui32 tableIndexPos,
        std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
        std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
        std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow(flow), InputRepresentations(std::move(inputRepresentations)), OutputRepresentations(std::move(outputRepresentations))
        , InputLeftType(inputLeftType), InputRightType(inputRightType)
        , PackerLeft(mutables), PackerRight(mutables)
        , TableIndexPos(tableIndexPos)
        , LeftInputColumns(std::move(leftInputColumns))
        , RightInputColumns(std::move(rightInputColumns))
        , RequiredColumns(std::move(requiredColumns))
        , LeftOutputColumns(std::move(leftOutputColumns))
        , RightOutputColumns(std::move(rightOutputColumns))
        , MemLimit(memLimit)
        , SortedTableOrder(sortedTableOrder)
        , KeyColumns(std::move(keyColumns))
        , IsRequiredColumn(FillRequiredStructColumn(InputRepresentations.size(), RequiredColumns))
        , AnyJoinSettings(anyJoinSettings)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        return static_cast<TValue*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto size = LeftOutputColumns.size() + RightOutputColumns.size();
        const auto arrayType = ArrayType::get(valueType, size);
        const auto fieldsType = ArrayType::get(PointerType::getUnqual(valueType), size);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto values = new AllocaInst(arrayType, 0U, "values", atTop);
        const auto fields = new AllocaInst(fieldsType, 0U, "fields", atTop);

        ICodegeneratorInlineWideNode::TGettersList getters(size);

        Value* initV = UndefValue::get(arrayType);
        Value* initF = UndefValue::get(fieldsType);
        std::vector<Value*> pointers;
        pointers.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            pointers.emplace_back(GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), atTop));
            initV = InsertValueInst::Create(initV, ConstantInt::get(valueType, 0), {i}, (TString("zero_") += ToString(i)).c_str(), atTop);
            initF = InsertValueInst::Create(initF, pointers.back(), {i}, (TString("insert_") += ToString(i)).c_str(), atTop);

            getters[i] = [i, values, valueType, indexType, arrayType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                return new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block);
            };
        }

        new StoreInst(initV, values, atTop);
        new StoreInst(initF, fields, atTop);

        TLLVMFieldsStructure<TComputationValue<TNull>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);

        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideCommonJoinCoreWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        for (ui32 i = 0U; i < OutputRepresentations.size(); ++i) {
            ValueCleanup(OutputRepresentations[i], pointers[i], ctx, block);
        }

        new StoreInst(initV, values, block);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TValue::FetchValues));
        const auto funcType = FunctionType::get(Type::getInt32Ty(context), { statePtrType, ctx.Ctx->getType(), fields->getType() }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto result = CallInst::Create(funcType, funcPtr, { stateArg, ctx.Ctx, fields }, "fetch", block);

        for (ui32 i = 0U; i < OutputRepresentations.size(); ++i) {
            ValueRelease(OutputRepresentations[i], pointers[i], ctx, block);
        }

        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TValue>(ctx, this, std::bind(&IComputationWideFlowNode::FetchValues, Flow, std::placeholders::_1, std::placeholders::_2));
#else
        state = ctx.ExecuteLLVM && Fetch ?
            ctx.HolderFactory.Create<TValue>(ctx, this, Fetch):
            ctx.HolderFactory.Create<TValue>(ctx, this, std::bind(&IComputationWideFlowNode::FetchValues, Flow, std::placeholders::_1, std::placeholders::_2));
#endif
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow);
    }

    IComputationWideFlowNode* const Flow;
    const std::vector<EValueRepresentation> InputRepresentations;
    const std::vector<EValueRepresentation> OutputRepresentations;
    const TTupleType* const InputLeftType;
    const TTupleType* const InputRightType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> PackerLeft, PackerRight;
    const ui32 TableIndexPos;
    const std::vector<ui32> LeftInputColumns;
    const std::vector<ui32> RightInputColumns;
    const std::vector<ui32> RequiredColumns;
    const std::vector<ui32> LeftOutputColumns;
    const std::vector<ui32> RightOutputColumns;
    const ui64 MemLimit;
    const std::optional<ui32> SortedTableOrder;
    const std::vector<ui32> KeyColumns;
    const std::vector<bool> IsRequiredColumn;
    const EAnyJoinSettings AnyJoinSettings;
#ifndef MKQL_DISABLE_CODEGEN
    typedef EFetchResult (*TFetchPtr)(TComputationContext&, NUdf::TUnboxedValue*const*);

    TFetchPtr Fetch = nullptr;

    Function* FetchFunc = nullptr;

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc) {
            Fetch = reinterpret_cast<TFetchPtr>(codegen.GetPointerToFunction(FetchFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(FetchFunc = GenerateFetchFunction(codegen));
    }

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Fetch_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    Function* GenerateFetchFunction(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = MakeName();
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto pointerType = PointerType::getUnqual(valueType);
        const auto arrayType = ArrayType::get(pointerType, InputRepresentations.size());
        const auto contextType = GetCompContextType(context);
        const auto resultType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(resultType, {PointerType::getUnqual(contextType), PointerType::getUnqual(arrayType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto outputArg = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        auto block = main;

        const auto result = GetNodeValues(Flow, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, result.first, ConstantInt::get(result.first->getType(), 0), "special", block);

        BranchInst::Create(exit, good, special, block);

        block = good;

        const auto fields = new LoadInst(arrayType, outputArg, "fields", block);

        for (ui32 i = 0U; i < InputRepresentations.size(); ++i) {
            const auto save = BasicBlock::Create(context, (TString("save_") += ToString(i)).c_str(), ctx.Func);
            const auto skip = BasicBlock::Create(context, (TString("skip_") += ToString(i)).c_str(), ctx.Func);

            const auto pointer = ExtractValueInst::Create(fields, i, (TString("pointer_") += ToString(i)).c_str(), block);
            const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, pointer, ConstantPointerNull::get(pointerType), (TString("null_") += ToString(i)).c_str(), block);

            BranchInst::Create(skip, save, null, block);

            block = save;

            const auto value = result.second[i](ctx, block);
            ValueUnRef(InputRepresentations[i], pointer, ctx, block);
            new StoreInst(value, pointer, block);
            ValueAddRef(InputRepresentations[i], value, ctx, block);

            BranchInst::Create(skip, block);

            block = skip;
        }
        BranchInst::Create(exit, block);

        block = exit;
        ReturnInst::Create(context, result.first, block);
        return ctx.Func;
    }
#endif
};

}

namespace NStream {

class TSpillList {
public:
    TSpillList(TValuePacker& itemPacker, bool singleShot)
        : ItemPacker(itemPacker)
        , Ctx(nullptr)
        , Count(0)
#ifndef NDEBUG
        , IsSealed(false)
#endif
        , Index(ui64(-1))
        , SingleShot(singleShot)
    {}

    TSpillList(TSpillList&& rhs) = delete;
    TSpillList(const TSpillList& rhs) = delete;
    void operator=(const TSpillList& rhs) = delete;

    void Init(TComputationContext& ctx) {
        Ctx = &ctx;
        Count = 0;
#ifndef NDEBUG
        IsSealed = false;
#endif
        Index = ui64(-1);
        FileState = nullptr;
        Heap.clear();
        LiveStream = NUdf::TUnboxedValue();
        LiveValue = NUdf::TUnboxedValue();
    }

    TComputationContext& GetCtx() const {
        return *Ctx;
    }

    bool Spill() {
        if (FileState) {
            return false;
        }

        FileState.reset(new TFileState);
        OpenWrite();
        for (ui32 i = 0; i < Count; ++i) {
            Write(std::move(InMemory(i)));
        }

        Heap.clear();
        return true;
    }

    void Live(NUdf::TUnboxedValue& stream, NUdf::TUnboxedValue&& liveValue) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count == 0);
        LiveStream = stream;
        LiveValue = std::move(liveValue);
    }

    void Add(NUdf::TUnboxedValue&& value) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(!IsSealed);
#endif
        if (SingleShot && Count > 0) {
            MKQL_ENSURE(Count == 1, "Counter inconsistent");
            return;
        }

        if (FileState) {
            Write(std::move(value));
        } else {
            if (Count < DEFAULT_STACK_ITEMS) {
                Stack[Count] = std::move(value);
            }
            else {
                if (Count == DEFAULT_STACK_ITEMS) {
                    Y_DEBUG_ABORT_UNLESS(Heap.empty());
                    Heap.assign(Stack, Stack + DEFAULT_STACK_ITEMS);
                }

                Heap.push_back(std::move(value));
            }
        }

        ++Count;
    }

    void Seal() {
#ifndef NDEBUG
        IsSealed = true;
#endif
        if (FileState) {
            FileState->Output->Finish();
            Cerr << "Spill finished at " << Count << " items" << Endl;
            FileState->Output.reset();
            Cerr << "File size: " << GetFileLength(FileState->File.GetName()) << ", expected: " << FileState->TotalSize << Endl;

            MKQL_INC_STAT(Ctx->Stats, Join_Spill_Count);
            MKQL_SET_MAX_STAT(Ctx->Stats, Join_Spill_MaxFileSize, static_cast<i64>(FileState->TotalSize));
            MKQL_SET_MAX_STAT(Ctx->Stats, Join_Spill_MaxRowsCount, static_cast<i64>(Count));
        }
    }

    bool IsLive() const {
        return bool(LiveStream);
    }

    ui64 GetCount() const {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        return Count;
    }

    bool Empty() const {
        return !IsLive() && (Count == 0);
    }

    NUdf::EFetchStatus Next(NUdf::TUnboxedValue& result) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed);
#endif
        if (IsLive()) {
            auto status = NUdf::EFetchStatus::Ok;
            NUdf::TUnboxedValue value;
            if ((Index + 1) == 0) {
                value = std::move(LiveValue);
            } else {
                status = LiveStream.Fetch(value);
                while (SingleShot && status == NUdf::EFetchStatus::Ok) {
                    // skip all remaining values
                    status = LiveStream.Fetch(value);
                }
            }

            if (status == NUdf::EFetchStatus::Ok) {
                result = std::move(value);
                ++Index;
            }
            return status;
        }

        if ((Index + 1) == Count) {
            return NUdf::EFetchStatus::Finish;
        }

        ++Index;
        if (FileState) {
            if (Index == 0) {
                OpenRead();
            }

            result = Read();
            return NUdf::EFetchStatus::Ok;
        }

        result = InMemory(Index);
        return NUdf::EFetchStatus::Ok;
    }

    void Rewind() {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed);
#endif
        Index = ui64(-1);
        if (FileState) {
            OpenRead();
        }
    }

private:
    NUdf::TUnboxedValue& InMemory(ui32 index) {
        return !Heap.empty() ? Heap[index] : Stack[index];
    }

    const NUdf::TUnboxedValue& InMemory(ui32 index) const {
        return !Heap.empty() ? Heap[index] : Stack[index];
    }

    void OpenWrite() {
        Cerr << "Spill started at " << Count << " items to " << FileState->File.GetName() << Endl;
        FileState->Output.reset(new TFixedBufferFileOutput(FileState->File.GetName()));
        FileState->Output->SetFlushPropagateMode(false);
        FileState->Output->SetFinishPropagateMode(false);
    }

    void Write(NUdf::TUnboxedValue&& value) {
        Y_DEBUG_ABORT_UNLESS(FileState->Output);
        TStringBuf serialized = ItemPacker.Pack(value);
        ui32 length = serialized.size();
        FileState->Output->Write(&length, sizeof(length));
        FileState->Output->Write(serialized.data(), length);
        FileState->TotalSize += sizeof(length);
        FileState->TotalSize += length;
    }

    void OpenRead() {
        FileState->Input.reset();
        FileState->Input.reset(new TFileInput(FileState->File.GetName()));
    }

    NUdf::TUnboxedValue Read() {
        ui32 length = 0;
        auto wasRead = FileState->Input->Load(&length, sizeof(length));
        Y_ABORT_UNLESS(wasRead == sizeof(length));
        FileState->Buffer.Reserve(length);
        wasRead = FileState->Input->Load((void*)FileState->Buffer.Data(), length);
        Y_ABORT_UNLESS(wasRead == length);
        return ItemPacker.Unpack(TStringBuf(FileState->Buffer.Data(), length), Ctx->HolderFactory);
    }

private:
    TValuePacker& ItemPacker;
    TComputationContext* Ctx;
    ui64 Count;
    NUdf::TUnboxedValue Stack[DEFAULT_STACK_ITEMS];
    TUnboxedValueVector Heap;
#ifndef NDEBUG
    bool IsSealed;
#endif
    ui64 Index;
    const bool SingleShot;
    struct TFileState {
        TFileState()
            : File(TTempFileHandle::InCurrentDir())
            , TotalSize(0)
        {}

        TTempFileHandle File;
        ui64 TotalSize;
        std::unique_ptr<TFileInput> Input;
        std::unique_ptr<TFixedBufferFileOutput> Output;
        TBuffer Buffer;
    };

    std::unique_ptr<TFileState> FileState;
    NUdf::TUnboxedValue LiveStream;
    NUdf::TUnboxedValue LiveValue;
};

template <EJoinKind Kind, bool TTrackRss>
class TCommonJoinCoreWrapper : public TMutableComputationNode<TCommonJoinCoreWrapper<Kind, TTrackRss>> {
    using TSelf = TCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream,
            TComputationContext& ctx, const TSelf* self)
            : TBase(memInfo)
            , Stream(std::move(stream))
            , Ctx(ctx)
            , Self(self)
            , List1(Self->Packer.RefMutableObject(ctx, false, Self->InputStructType), IsAnyJoinLeft(Self->AnyJoinSettings))
            , List2(Self->Packer.RefMutableObject(ctx, false, Self->InputStructType), IsAnyJoinRight(Self->AnyJoinSettings))
        {
            Init();
        }

        void Reset(NUdf::TUnboxedValue&& stream) {
            Stream = std::move(stream);
            Init();
        }

        void Init() {
            List1.Init(Ctx);
            List2.Init(Ctx);
            CrossMove1 = true;
            EatInput = true;
            KeyHasNulls = false;
            OutputMode = EOutputMode::Unknown;
            InitialUsage = std::nullopt;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            while (EatInput) {
                if (!InitialUsage) {
                    InitialUsage = Ctx.HolderFactory.GetPagePool().GetUsed();
                }

                NUdf::TUnboxedValue value;
                const auto status = Stream.Fetch(value);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }

                if (status == NUdf::EFetchStatus::Finish) {
                    EatInput = false;
                } else {
                    if (!KeyHasNulls && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                        for (ui32 i = 0U; i < Self->KeyColumns.size(); ++i) {
                            if (!value.GetElement(Self->KeyColumns[i])) {
                                KeyHasNulls = true;
                                break;
                            }
                        }
                    }

                    switch (const auto tableIndex = value.GetElement(Self->TableIndexPos).template Get<ui32>()) {
                        case LeftIndex:
                            if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2.Empty() && !KeyHasNulls)) {
                                EatInput = false;
                                OutputMode = EOutputMode::None;
                                break;
                            }

                            if (Self->SortedTableOrder && *Self->SortedTableOrder == RightIndex) {
                                List1.Live(Stream, std::move(value));
                                EatInput = false;
                            } else {
                                List1.Add(std::move(value));
                                if (Ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                    List1.Spill();
                                }
                            }
                            break;
                        case RightIndex:
                            if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1.Empty() && !KeyHasNulls)) {
                                EatInput = false;
                                OutputMode = EOutputMode::None;
                                break;
                            }

                            if (Self->SortedTableOrder && *Self->SortedTableOrder == LeftIndex) {
                                List2.Live(Stream, std::move(value));
                                EatInput = false;
                            } else {
                                List2.Add(std::move(value));
                                if (Ctx.CheckAdjustedMemLimit<TTrackRss>(Self->MemLimit, *InitialUsage)) {
                                    List2.Spill();
                                }
                            }
                            break;
                        default: THROW yexception() << "Bad table index: " << tableIndex;
                    }
                }
            }

            while (true) {
                switch (OutputMode) {
                case EOutputMode::Unknown: {
                        List1.Seal();
                        List2.Seal();
                        switch (Kind) {
                        case EJoinKind::Cross:
                        case EJoinKind::Inner:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }

                            break;
                        case EJoinKind::Left:
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::LeftOnly:
                            if (List1.Empty() || !List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::Right:
                            if (List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            }
                            break;

                        case EJoinKind::RightOnly:
                            if (List2.Empty() || !List1.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        case EJoinKind::Exclusion:
                            if (!List1.Empty() && !List2.Empty() && !KeyHasNulls) {
                                OutputMode = EOutputMode::None;
                            } else if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else {
                                OutputMode = EOutputMode::BothNull;
                            }
                            break;

                        case EJoinKind::Full:
                            break;

                        case EJoinKind::LeftSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::RightNull;
                            }
                            break;

                        case EJoinKind::RightSemi:
                            if (List1.Empty() || List2.Empty()) {
                                OutputMode = EOutputMode::None;
                            } else {
                                OutputMode = EOutputMode::LeftNull;
                            }
                            break;

                        default:
                            Y_ABORT("Unknown kind");
                        }

                        if (OutputMode == EOutputMode::Unknown) {
                            if (List1.Empty()) {
                                OutputMode = EOutputMode::LeftNull;
                            } else if (List2.Empty()) {
                                OutputMode = EOutputMode::RightNull;
                            } else if (List1.IsLive()) {
                                OutputMode = EOutputMode::Cross;
                            } else if (List2.IsLive()) {
                                OutputMode = EOutputMode::CrossSwap;
                            } else {
                                OutputMode = List1.GetCount() >= List2.GetCount() ?
                                    EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                    continue;
                case EOutputMode::LeftNull: {
                        NUdf::TUnboxedValue value;
                        auto status = List2.Next(value);
                        if (status != NUdf::EFetchStatus::Ok) {
                            return status;
                        }

                        result = PrepareNullItem<true>(value);
                        return NUdf::EFetchStatus::Ok;
                    }
                    break;
                case EOutputMode::RightNull: {
                        NUdf::TUnboxedValue value;
                        auto status = List1.Next(value);
                        if (status != NUdf::EFetchStatus::Ok) {
                            return status;
                        }

                        result = PrepareNullItem<false>(value);
                        return NUdf::EFetchStatus::Ok;
                    }
                    break;
                case EOutputMode::BothNull: {
                        NUdf::TUnboxedValue value;

                        if (CrossMove1) {
                            switch (const auto status = List1.Next(value)) {
                            case NUdf::EFetchStatus::Finish: CrossMove1 = false; break;
                            case NUdf::EFetchStatus::Yield: return status;
                            case NUdf::EFetchStatus::Ok:
                                result = PrepareNullItem<false>(value);
                                return NUdf::EFetchStatus::Ok;
                            }
                        }

                        switch (const auto status = List2.Next(value)) {
                        case NUdf::EFetchStatus::Yield:
                        case NUdf::EFetchStatus::Finish: return status;
                        case NUdf::EFetchStatus::Ok:
                            result = PrepareNullItem<true>(value);
                            return NUdf::EFetchStatus::Ok;
                        }
                    }
                    break;
                case EOutputMode::Cross:
                    return PrepareCrossItem<false>(result);
                case EOutputMode::CrossSwap:
                    return PrepareCrossItem<true>(result);
                case EOutputMode::None:
                    return NUdf::EFetchStatus::Finish;
                default:
                    Y_ABORT("Unknown output mode");
                }
            }
        }

        template <bool IsLeftNull>
        NUdf::TUnboxedValue PrepareNullItem(const NUdf::TUnboxedValue& value) {
            const auto structObj = Self->ResStruct.NewArray(Ctx, Self->LeftInputColumns.size() + Self->RightInputColumns.size(), ResItems);

            for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                ui32 inIndex = Self->LeftInputColumns[i];
                ui32 outIndex = Self->LeftOutputColumns[i];
                if (IsLeftNull) {
                    ResItems[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self->IsRequiredColumn[inIndex]) {
                    ResItems[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems[outIndex] = std::move(member);
                }
            }

            for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                ui32 inIndex = Self->RightInputColumns[i];
                ui32 outIndex = Self->RightOutputColumns[i];
                if (!IsLeftNull) {
                    ResItems[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self->IsRequiredColumn[inIndex]) {
                    ResItems[outIndex] = member.Release().GetOptionalValue();
                }
                else {
                    ResItems[outIndex] = std::move(member);
                }
            }

            return structObj;
        }

        template <bool SwapLists>
        NUdf::EFetchStatus PrepareCrossItem(NUdf::TUnboxedValue& result) {
            if (KeyHasNulls) {
                for (;;) {
                    NUdf::TUnboxedValue value;
                    auto status = (CrossMove1 == SwapLists ? List2 : List1).Next(value);
                    if (status == NUdf::EFetchStatus::Finish && CrossMove1) {
                        CrossMove1 = false;
                        continue;
                    }

                    if (status != NUdf::EFetchStatus::Ok) {
                        return status;
                    }

                    result = (CrossMove1 == SwapLists) ? PrepareNullItem<true>(value) : PrepareNullItem<false>(value);
                    return status;
                }
            }

            for (;;) {
                if (CrossMove1) {
                    auto status = (SwapLists ? List2 : List1).Next(CrossValue1);
                    if (status != NUdf::EFetchStatus::Ok) {
                        return status;
                    }

                    CrossMove1 = false;
                    (SwapLists ? List1 : List2).Rewind();
                }

                auto status = (SwapLists ? List1 : List2).Next(CrossValue2);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status");
                if (status == NUdf::EFetchStatus::Finish) {
                    CrossMove1 = true;
                    continue;
                }

                auto structObj = Self->ResStruct.NewArray(Ctx, Self->LeftInputColumns.size() + Self->RightInputColumns.size(), ResItems);

                for (ui32 i = 0; i < Self->LeftInputColumns.size(); ++i) {
                    ui32 inIndex = Self->LeftInputColumns[i];
                    ui32 outIndex = Self->LeftOutputColumns[i];
                    auto member = (SwapLists ? CrossValue2 : CrossValue1).GetElement(inIndex);
                    if (Self->IsRequiredColumn[inIndex]) {
                        ResItems[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems[outIndex] = std::move(member);
                    }
                }

                for (ui32 i = 0; i < Self->RightInputColumns.size(); ++i) {
                    ui32 inIndex = Self->RightInputColumns[i];
                    ui32 outIndex = Self->RightOutputColumns[i];
                    auto member = (SwapLists ? CrossValue1 : CrossValue2).GetElement(inIndex);
                    if (Self->IsRequiredColumn[inIndex]) {
                        ResItems[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems[outIndex] = std::move(member);
                    }
                }

                result = std::move(structObj);
                return NUdf::EFetchStatus::Ok;
            }
        }


    private:
        NUdf::TUnboxedValue Stream;
        TComputationContext& Ctx;
        const TSelf* const Self;
        bool EatInput;
        bool KeyHasNulls;
        std::optional<ui64> InitialUsage;
        EOutputMode OutputMode;

        bool CrossMove1;
        NUdf::TUnboxedValue CrossValue1;
        NUdf::TUnboxedValue CrossValue2;

        TSpillList List1;
        TSpillList List2;

        NUdf::TUnboxedValue* ResItems = nullptr;
    };

    TCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationNode* stream, const TType* inputStructType, ui32 inputWidth, ui32 tableIndexPos,
        std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
        std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
        std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables)
        , Stream(stream)
        , InputStructType(inputStructType)
        , Packer(mutables)
        , TableIndexPos(tableIndexPos)
        , LeftInputColumns(std::move(leftInputColumns))
        , RightInputColumns(std::move(rightInputColumns))
        , RequiredColumns(std::move(requiredColumns))
        , LeftOutputColumns(std::move(leftOutputColumns))
        , RightOutputColumns(std::move(rightOutputColumns))
        , MemLimit(memLimit)
        , SortedTableOrder(sortedTableOrder)
        , KeyColumns(std::move(keyColumns))
        , IsRequiredColumn(FillRequiredStructColumn(inputWidth, RequiredColumns))
        , ResStruct(mutables)
        , ResStreamIndex(mutables.CurValueIndex++)
        , AnyJoinSettings(anyJoinSettings)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& resStream = ctx.MutableValues[ResStreamIndex];
        if (!resStream || resStream.IsInvalid() || !resStream.UniqueBoxed()) {
            resStream = ctx.HolderFactory.Create<TValue>(Stream->GetValue(ctx), ctx, this);
        } else {
            static_cast<TValue&>(*resStream.AsBoxed()).Reset(Stream->GetValue(ctx));
        }

        return static_cast<const NUdf::TUnboxedValuePod&>(resStream);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
    }

    IComputationNode* const Stream;
    const TType* const InputStructType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer;
    const ui32 TableIndexPos;
    const std::vector<ui32> LeftInputColumns;
    const std::vector<ui32> RightInputColumns;
    const std::vector<ui32> RequiredColumns;
    const std::vector<ui32> LeftOutputColumns;
    const std::vector<ui32> RightOutputColumns;
    const ui64 MemLimit;
    const std::optional<ui32> SortedTableOrder;
    const std::vector<ui32> KeyColumns;
    const std::vector<bool> IsRequiredColumn;

    const TContainerCacheOnContext ResStruct;
    const ui32 ResStreamIndex;
    const EAnyJoinSettings AnyJoinSettings;
};

}

}

IComputationNode* WrapCommonJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 11U || callable.GetInputsCount() == 12U, "Expected 12 args");
    const auto type = callable.GetType()->GetReturnType();

    const auto inputRowType = type->IsFlow() ?
        AS_TYPE(TFlowType, callable.GetInput(0))->GetItemType():
        AS_TYPE(TStreamType, callable.GetInput(0))->GetItemType();

    std::vector<EValueRepresentation> inputRepresentations;
    std::vector<TType*> fieldTypes;
    if (inputRowType->IsTuple()) {
        const auto tupleType = AS_TYPE(TTupleType, inputRowType);
        inputRepresentations.reserve(tupleType->GetElementsCount());
        fieldTypes.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i) {
            fieldTypes.emplace_back(tupleType->GetElementType(i));
            inputRepresentations.emplace_back(GetValueRepresentation(fieldTypes.back()));
        }
    } else if (inputRowType->IsMulti()) {
        const auto tupleType = AS_TYPE(TMultiType, inputRowType);
        inputRepresentations.reserve(tupleType->GetElementsCount());
        fieldTypes.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i) {
            fieldTypes.emplace_back(tupleType->GetElementType(i));
            inputRepresentations.emplace_back(GetValueRepresentation(fieldTypes.back()));
        }

    } else if (inputRowType->IsStruct()) {
        const auto structType = AS_TYPE(TStructType, inputRowType);
        inputRepresentations.reserve(structType->GetMembersCount());
        fieldTypes.reserve(structType->GetMembersCount());
        for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
            fieldTypes.emplace_back(structType->GetMemberType(i));
            inputRepresentations.emplace_back(GetValueRepresentation(fieldTypes.back()));
        }
    }

    const auto outputRowType = type->IsFlow() ?
        AS_TYPE(TFlowType, type)->GetItemType():
        AS_TYPE(TStreamType, type)->GetItemType();

    std::vector<EValueRepresentation> outputRepresentations;
    if (outputRowType->IsTuple()) {
        const auto tupleType = AS_TYPE(TTupleType, outputRowType);
        outputRepresentations.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i)
            outputRepresentations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));
    } else if (outputRowType->IsMulti()) {
        const auto tupleType = AS_TYPE(TMultiType, outputRowType);
        outputRepresentations.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i)
            outputRepresentations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));
    } else if (outputRowType->IsStruct()) {
        const auto structType = AS_TYPE(TStructType, outputRowType);
        outputRepresentations.reserve(structType->GetMembersCount());
        for (ui32 i = 0U; i < structType->GetMembersCount(); ++i)
            outputRepresentations.emplace_back(GetValueRepresentation(structType->GetMemberType(i)));
    }

    const auto rawKind = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    const auto kind = GetJoinKind(rawKind);

    std::vector<ui32> leftInputColumns;
    std::vector<ui32> rightInputColumns;
    std::vector<ui32> requiredColumns;
    std::vector<ui32> leftOutputColumns;
    std::vector<ui32> rightOutputColumns;
    std::vector<ui32> keyColumns;
    const auto leftInputColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    const auto rightInputColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    const auto requiredColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(4));
    const auto leftOutputColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    const auto rightOutputColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(6));
    const auto keyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(7));

    std::vector<TType*> leftTypes;
    leftTypes.reserve(leftInputColumnsNode->GetValuesCount());
    leftInputColumns.reserve(leftInputColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < leftInputColumnsNode->GetValuesCount(); ++i) {
        leftInputColumns.push_back(AS_VALUE(TDataLiteral, leftInputColumnsNode->GetValue(i))->AsValue().Get<ui32>());
        leftTypes.emplace_back(fieldTypes[leftInputColumns.back()]);
    }

    std::vector<TType*> rightTypes;
    rightTypes.reserve(rightInputColumnsNode->GetValuesCount());
    rightInputColumns.reserve(rightInputColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < rightInputColumnsNode->GetValuesCount(); ++i) {
        rightInputColumns.push_back(AS_VALUE(TDataLiteral, rightInputColumnsNode->GetValue(i))->AsValue().Get<ui32>());
        rightTypes.emplace_back(fieldTypes[rightInputColumns.back()]);
    }

    requiredColumns.reserve(requiredColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < requiredColumnsNode->GetValuesCount(); ++i) {
        requiredColumns.push_back(AS_VALUE(TDataLiteral, requiredColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    leftOutputColumns.reserve(leftOutputColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < leftOutputColumnsNode->GetValuesCount(); ++i) {
        leftOutputColumns.push_back(AS_VALUE(TDataLiteral, leftOutputColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    rightOutputColumns.reserve(rightOutputColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < rightOutputColumnsNode->GetValuesCount(); ++i) {
        rightOutputColumns.push_back(AS_VALUE(TDataLiteral, rightOutputColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    keyColumns.reserve(keyColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < keyColumnsNode->GetValuesCount(); ++i) {
        keyColumns.push_back(AS_VALUE(TDataLiteral, keyColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    const ui64 memLimit = AS_VALUE(TDataLiteral, callable.GetInput(8))->AsValue().Get<ui64>();

    std::optional<ui32> sortedTableOrder;
    if (!callable.GetInput(9).GetStaticType()->IsVoid()) {
        sortedTableOrder = AS_VALUE(TDataLiteral, callable.GetInput(9))->AsValue().Get<ui32>();
        MKQL_ENSURE(*sortedTableOrder < 2, "Bad sorted table order");
    }

    const EAnyJoinSettings anyJoinSettings = GetAnyJoinSettings(AS_VALUE(TDataLiteral, callable.GetInput(10))->AsValue().Get<ui32>());

    const auto tableIndexPos = 12U == callable.GetInputsCount() ?
        AS_VALUE(TDataLiteral, callable.GetInput(11U))->AsValue().Get<ui32>():
        AS_TYPE(TStructType, inputRowType)->GetMemberIndex("_yql_table_index");

    const bool trackRss = EGraphPerProcess::Single == ctx.GraphPerProcess;
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    const auto leftInputType = TTupleType::Create(leftTypes.size(), leftTypes.data(), ctx.Env);
    const auto rightInputType = TTupleType::Create(rightTypes.size(), rightTypes.data(), ctx.Env);

#define MAKE_COMMON_JOIN_CORE_WRAPPER(KIND)\
    case EJoinKind::KIND: \
    if (type->IsFlow()) { \
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) \
            if (trackRss) \
                return new NFlow::TWideCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, wide, leftInputType, rightInputType, std::move(inputRepresentations), std::move(outputRepresentations), tableIndexPos, \
                    std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                    std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
            else \
                return new NFlow::TWideCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, wide, leftInputType, rightInputType, std::move(inputRepresentations), std::move(outputRepresentations), tableIndexPos, \
                    std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                    std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
        else \
            if (trackRss) \
                return new NFlow::TCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos, \
                    std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                    std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
            else \
                return new NFlow::TCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos, \
                    std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                    std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
    } else { \
        if (trackRss) \
            return new NStream::TCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos, \
                std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
        else \
            return new NStream::TCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos, \
                std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns), \
                std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings); \
    }

    switch (kind) {
        MAKE_COMMON_JOIN_CORE_WRAPPER(Inner)
        MAKE_COMMON_JOIN_CORE_WRAPPER(Left)
        MAKE_COMMON_JOIN_CORE_WRAPPER(Right)
        MAKE_COMMON_JOIN_CORE_WRAPPER(Full)
        MAKE_COMMON_JOIN_CORE_WRAPPER(LeftOnly)
        MAKE_COMMON_JOIN_CORE_WRAPPER(RightOnly)
        MAKE_COMMON_JOIN_CORE_WRAPPER(Exclusion)
        MAKE_COMMON_JOIN_CORE_WRAPPER(LeftSemi)
        MAKE_COMMON_JOIN_CORE_WRAPPER(RightSemi)
        MAKE_COMMON_JOIN_CORE_WRAPPER(Cross)
    default:
        Y_ABORT("Unknown kind");
    }
#undef MAKE_COMMON_JOIN_CORE_WRAPPER
}

}
}
