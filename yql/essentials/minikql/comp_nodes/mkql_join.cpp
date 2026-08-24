#include "mkql_join.h"

#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_llvm_base.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>
#include <util/generic/ylimits.h>

#include <array>
#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

const ui64 DEFAULT_STACK_ITEMS = 16;

const TStatKey Join_Spill_Count("Join_Spill_Count", /*deriv=*/true);
const TStatKey Join_Spill_MaxFileSize("Join_Spill_MaxFileSize", /*deriv=*/false);
const TStatKey Join_Spill_MaxRowsCount("Join_Spill_MaxRowsCount", /*deriv=*/false);

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

enum ETableIndex: ui32 {
    LeftIndex = 0U,
    RightIndex = 1U
};

namespace NFlow {

using TFetcher = std::function<EFetchResult(TComputationContext&, NUdf::TUnboxedValue* const*)>;
using TLiveFetcher = std::function<EFetchResult(TComputationContext&, NUdf::TUnboxedValue*)>;

class TSpillList {
public:
    TSpillList(NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent,
               TValuePacker& itemPacker, bool singleShot, size_t width = 0ULL)
        : Logger_(std::move(logger))
        , LogComponent_(logComponent)
        , Width_(width)
        , ItemPacker_(itemPacker)
        , Count_(0)
#ifndef NDEBUG
        , IsSealed_(false)
#endif
        , Index_(ui64(-1))
        , SingleShot_(singleShot)
    {
    }

    TSpillList(TSpillList&& rhs) = delete;
    TSpillList(const TSpillList& rhs) = delete;
    void operator=(const TSpillList& rhs) = delete;

    void Init() {
        Count_ = 0;
#ifndef NDEBUG
        IsSealed_ = false;
#endif
        Index_ = ui64(-1);
        FileState_ = nullptr;
        Heap_.clear();
        LiveFlow_ = nullptr;
        LiveValue_ = NUdf::TUnboxedValue();
    }

    bool Spill() {
        if (FileState_) {
            return false;
        }

        FileState_ = std::make_unique<TFileState>();
        OpenWrite();
        for (ui32 i = 0; i < Count_; ++i) {
            Write(std::move(InMemory(i)));
        }

        Heap_.clear();
        return true;
    }

    void Live(IComputationNode* flow, NUdf::TUnboxedValue&& liveValue) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count_ == 0);
        LiveFlow_ = flow;
        LiveValue_ = std::move(liveValue);
    }

    void Live(TLiveFetcher&& fetcher, NUdf::TUnboxedValue* liveValues) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count_ == 0);
        Fetcher_ = std::move(fetcher);
        LiveValues_ = liveValues;
    }

    void Add(NUdf::TUnboxedValue&& value) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(!IsSealed_);
#endif
        if (SingleShot_ && Count_ > 0) {
            MKQL_ENSURE(Count_ == 1, "Counter inconsistent");
            return;
        }

        if (FileState_) {
            Write(std::move(value));
        } else {
            if (Count_ < DEFAULT_STACK_ITEMS) {
                Stack_[Count_] = std::move(value);
            } else {
                if (Count_ == DEFAULT_STACK_ITEMS) {
                    Y_DEBUG_ABORT_UNLESS(Heap_.empty());
                    Heap_.assign(Stack_.begin(), Stack_.end());
                }

                Heap_.push_back(std::move(value));
            }
        }

        ++Count_;
    }

    void Seal(TComputationContext& ctx) {
#ifndef NDEBUG
        IsSealed_ = true;
#endif
        if (FileState_) {
            FileState_->Output->Finish();
            Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "Spill finished at " << Count_ << " items");
            FileState_->Output.reset();
            Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "File size: " << GetFileLength(FileState_->File.GetName()) << ", expected: " << FileState_->TotalSize);

            MKQL_INC_STAT(ctx.Stats, Join_Spill_Count);
            MKQL_SET_MAX_STAT(ctx.Stats, Join_Spill_MaxFileSize, static_cast<i64>(FileState_->TotalSize));
            MKQL_SET_MAX_STAT(ctx.Stats, Join_Spill_MaxRowsCount, static_cast<i64>(Count_));
        }
    }

    bool IsLive() const {
        return bool(LiveFlow_) || bool(Fetcher_);
    }

    ui64 GetCount() const {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        return Count_;
    }

    bool Empty() const {
        return !IsLive() && (Count_ == 0);
    }

    NUdf::TUnboxedValue Next(TComputationContext& ctx) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed_);
#endif
        if (IsLive()) {
            if ((Index_ + 1) == 0) {
                ++Index_;
                return std::move(LiveValue_);
            }

            auto value = LiveFlow_->GetValue(ctx);
            while (SingleShot_ && !value.IsSpecial()) {
                // skip all remaining values
                value = LiveFlow_->GetValue(ctx);
            }

            if (!value.IsSpecial()) {
                ++Index_;
            }
            return value;
        }

        if ((Index_ + 1) == Count_) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        ++Index_;
        if (FileState_) {
            if (Index_ == 0) {
                OpenRead();
            }

            return Read(ctx);
        }

        return InMemory(Index_);
    }

    EFetchResult Next(TComputationContext& ctx, NUdf::TUnboxedValue* values) {
        if (IsLive()) {
            if ((Index_ + 1) == 0) {
                ++Index_;

                if (values != LiveValues_) {
                    for (auto i = 0U; i < Width_; ++i) {
                        *values++ = std::move(*LiveValues_++);
                    }
                }

                LiveValues_ = nullptr;
                return EFetchResult::One;
            }

            auto result = Fetcher_(ctx, values);
            while (SingleShot_ && EFetchResult::One == result) {
                // skip all remaining values
                result = Fetcher_(ctx, values);
            }

            if (EFetchResult::One == result) {
                ++Index_;
            }
            return result;
        }

        if ((Index_ + 1) == Count_) {
            return EFetchResult::Finish;
        }

        ++Index_;
        if (FileState_) {
            if (Index_ == 0) {
                OpenRead();
            }

            std::copy_n(Read(ctx).GetElements(), Width_, values);
            return EFetchResult::One;
        }

        std::copy_n(InMemory(Index_).GetElements(), Width_, values);
        return EFetchResult::One;
    }

    void Rewind() {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed_);
#endif
        Index_ = ui64(-1);
        if (FileState_) {
            OpenRead();
        }
    }

private:
    NUdf::TUnboxedValue& InMemory(ui32 index) {
        return !Heap_.empty() ? Heap_[index] : Stack_[index];
    }

    const NUdf::TUnboxedValue& InMemory(ui32 index) const {
        return !Heap_.empty() ? Heap_[index] : Stack_[index];
    }

    void OpenWrite() {
        Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "Spill started at " << Count_ << " items to " << FileState_->File.GetName());
        FileState_->Output = std::make_unique<TFixedBufferFileOutput>(FileState_->File.GetName());
        FileState_->Output->SetFlushPropagateMode(false);
        FileState_->Output->SetFinishPropagateMode(false);
    }

    void Write(NUdf::TUnboxedValue&& value) {
        Y_DEBUG_ABORT_UNLESS(FileState_->Output);
        TStringBuf serialized = ItemPacker_.Pack(value);
        ui32 length = serialized.size();
        FileState_->Output->Write(&length, sizeof(length));
        FileState_->Output->Write(serialized.data(), length);
        FileState_->TotalSize += sizeof(length);
        FileState_->TotalSize += length;
    }

    void OpenRead() {
        FileState_->Input.reset();
        FileState_->Input = std::make_unique<TFileInput>(FileState_->File.GetName());
    }

    NUdf::TUnboxedValue Read(TComputationContext& ctx) {
        ui32 length = 0;
        auto wasRead = FileState_->Input->Load(&length, sizeof(length));
        Y_ABORT_UNLESS(wasRead == sizeof(length));
        FileState_->Buffer.Reserve(length);
        wasRead = FileState_->Input->Load((void*)FileState_->Buffer.Data(), length);
        Y_ABORT_UNLESS(wasRead == length);
        return ReadValue_ = ItemPacker_.Unpack(TStringBuf(FileState_->Buffer.Data(), length), ctx.HolderFactory);
    }

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    const size_t Width_;
    TValuePacker& ItemPacker_;
    ui64 Count_;
    NUdf::TUnboxedValue ReadValue_;
    std::array<NUdf::TUnboxedValue, DEFAULT_STACK_ITEMS> Stack_;
    TUnboxedValueVector Heap_;
#ifndef NDEBUG
    bool IsSealed_;
#endif
    ui64 Index_;
    const bool SingleShot_;
    struct TFileState {
        TFileState()
            : File(TTempFileHandle::InCurrentDir())
            , TotalSize(0)
        {
        }

        TTempFileHandle File;
        ui64 TotalSize;
        std::unique_ptr<TFileInput> Input;
        std::unique_ptr<TFixedBufferFileOutput> Output;
        TBuffer Buffer;
    };

    std::unique_ptr<TFileState> FileState_;
    IComputationNode* LiveFlow_ = nullptr;
    TLiveFetcher Fetcher_;
    NUdf::TUnboxedValue LiveValue_;
    NUdf::TUnboxedValue* LiveValues_ = nullptr;
};

template <EJoinKind Kind, bool TTrackRss>
class TCommonJoinCoreWrapper: public TStatefulFlowComputationNode<TCommonJoinCoreWrapper<Kind, TTrackRss>> {
    using TSelf = TCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TStatefulFlowComputationNode<TSelf>;
    using TBaseComputation = TBase;

public:
    class TValue: public TComputationValue<TValue> {
        friend TSelf;

    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TSelf* self)
            : TBase(memInfo)
            , Self_(self)
            , List1_(Self_->GetLogger(ctx), Self_->GetLogComponent(ctx), Self_->Packer_.RefMutableObject(ctx, false, Self_->InputStructType_), IsAnyJoinLeft(Self_->AnyJoinSettings_))
            , List2_(Self_->GetLogger(ctx), Self_->GetLogComponent(ctx), Self_->Packer_.RefMutableObject(ctx, false, Self_->InputStructType_), IsAnyJoinRight(Self_->AnyJoinSettings_))
        {
            Init();
        }

        void Init() {
            List1_.Init();
            List2_.Init();
            CrossMove1_ = true;
            EatInput_ = true;
            KeyHasNulls_ = false;
            OutputMode_ = EOutputMode::Unknown;
            InitialUsage_ = std::nullopt;
        }

    private:
        // copypaste to resolve -Woverloaded-virtual
        bool Next(NUdf::TUnboxedValue&) override {
            this->ThrowNotSupported(__func__);
            return false;
        }

        NUdf::TUnboxedValue Next(IComputationNode* flow, TComputationContext& ctx) {
            while (EatInput_) {
                if (!InitialUsage_) {
                    InitialUsage_ = ctx.HolderFactory.GetPagePool().GetUsed();
                }

                if (auto value = flow->GetValue(ctx); value.IsYield()) {
                    return value;
                } else if (value.IsFinish()) {
                    EatInput_ = false;
                } else {
                    if (!KeyHasNulls_ && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                        for (const auto& keyColumn : Self_->KeyColumns_) {
                            if (!value.GetElement(keyColumn)) {
                                KeyHasNulls_ = true;
                                break;
                            }
                        }
                    }

                    switch (const auto tableIndex = value.GetElement(Self_->TableIndexPos_).template Get<ui32>()) {
                        case LeftIndex:
                            if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2_.Empty() && !KeyHasNulls_)) {
                                EatInput_ = false;
                                OutputMode_ = EOutputMode::None;
                                break;
                            }

                            if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == RightIndex) {
                                List1_.Live(flow, std::move(value));
                                EatInput_ = false;
                            } else {
                                List1_.Add(std::move(value));
                                if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                    List1_.Spill();
                                }
                            }
                            break;
                        case RightIndex:
                            if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1_.Empty() && !KeyHasNulls_)) {
                                EatInput_ = false;
                                OutputMode_ = EOutputMode::None;
                                break;
                            }

                            if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == LeftIndex) {
                                List2_.Live(flow, std::move(value));
                                EatInput_ = false;
                            } else {
                                List2_.Add(std::move(value));
                                if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                    List2_.Spill();
                                }
                            }
                            break;
                        default:
                            THROW yexception() << "Bad table index: " << tableIndex;
                    }
                }
            }

            while (true) {
                switch (OutputMode_) {
                    case EOutputMode::Unknown: {
                        List1_.Seal(ctx);
                        List2_.Seal(ctx);
                        switch (Kind) {
                            case EJoinKind::Cross:
                            case EJoinKind::Inner:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }

                                break;
                            case EJoinKind::Left:
                                if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::LeftOnly:
                                if (List1_.Empty() || !List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::Right:
                                if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::RightOnly:
                                if (List2_.Empty() || !List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            case EJoinKind::Exclusion:
                                if (!List1_.Empty() && !List2_.Empty() && !KeyHasNulls_) {
                                    OutputMode_ = EOutputMode::None;
                                } else if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::LeftNull;
                                } else if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::RightNull;
                                } else {
                                    OutputMode_ = EOutputMode::BothNull;
                                }
                                break;

                            case EJoinKind::Full:
                                break;

                            case EJoinKind::LeftSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::RightSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            default:
                                Y_ABORT("Unknown kind");
                        }

                        if (OutputMode_ == EOutputMode::Unknown) {
                            if (List1_.Empty()) {
                                OutputMode_ = EOutputMode::LeftNull;
                            } else if (List2_.Empty()) {
                                OutputMode_ = EOutputMode::RightNull;
                            } else if (List1_.IsLive()) {
                                OutputMode_ = EOutputMode::Cross;
                            } else if (List2_.IsLive()) {
                                OutputMode_ = EOutputMode::CrossSwap;
                            } else {
                                OutputMode_ = List1_.GetCount() >= List2_.GetCount() ? EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                        continue;
                    case EOutputMode::LeftNull:
                        if (const auto item = List2_.Next(ctx); item.IsSpecial()) {
                            return item;
                        } else {
                            return PrepareNullItem<true>(ctx, item);
                        }
                    case EOutputMode::RightNull:
                        if (const auto item = List1_.Next(ctx); item.IsSpecial()) {
                            return item;
                        } else {
                            return PrepareNullItem<false>(ctx, item);
                        }
                    case EOutputMode::BothNull:
                        if (CrossMove1_) {
                            if (const auto item = List1_.Next(ctx); item.IsFinish()) {
                                CrossMove1_ = false;
                            } else if (item.IsYield()) {
                                return item;
                            } else {
                                return PrepareNullItem<false>(ctx, item);
                            }
                        }

                        if (const auto item = List2_.Next(ctx); item.IsSpecial()) {
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
            const auto structObj = Self_->ResStruct_.NewArray(ctx, Self_->LeftInputColumns_.size() + Self_->RightInputColumns_.size(), ResItems_);

            for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                ui32 inIndex = Self_->LeftInputColumns_[i];
                ui32 outIndex = Self_->LeftOutputColumns_[i];
                if constexpr (IsLeftNull) {
                    ResItems_[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self_->IsRequiredColumn_[inIndex]) {
                    ResItems_[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems_[outIndex] = std::move(member);
                }
            }

            for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                ui32 inIndex = Self_->RightInputColumns_[i];
                ui32 outIndex = Self_->RightOutputColumns_[i];
                if constexpr (!IsLeftNull) {
                    ResItems_[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self_->IsRequiredColumn_[inIndex]) {
                    ResItems_[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems_[outIndex] = std::move(member);
                }
            }

            return structObj;
        }

        template <bool SwapLists>
        NUdf::TUnboxedValue PrepareCrossItem(TComputationContext& ctx) {
            if (KeyHasNulls_) {
                for (;;) {
                    const auto& value = (CrossMove1_ == SwapLists ? List2_ : List1_).Next(ctx);
                    if (value.IsFinish() && CrossMove1_) {
                        CrossMove1_ = false;
                        continue;
                    }

                    if (value.IsSpecial()) {
                        return value;
                    }

                    return (CrossMove1_ == SwapLists) ? PrepareNullItem<true>(ctx, value) : PrepareNullItem<false>(ctx, value);
                }
            }

            for (;;) {
                if (CrossMove1_) {
                    CrossValue1_ = (SwapLists ? List2_ : List1_).Next(ctx);
                    if (CrossValue1_.IsSpecial()) {
                        return CrossValue1_;
                    }

                    CrossMove1_ = false;
                    (SwapLists ? List1_ : List2_).Rewind();
                }

                CrossValue2_ = (SwapLists ? List1_ : List2_).Next(ctx);
                if (CrossValue2_.IsFinish()) {
                    CrossMove1_ = true;
                    continue;
                }

                auto structObj = Self_->ResStruct_.NewArray(ctx, Self_->LeftInputColumns_.size() + Self_->RightInputColumns_.size(), ResItems_);

                for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                    ui32 inIndex = Self_->LeftInputColumns_[i];
                    ui32 outIndex = Self_->LeftOutputColumns_[i];
                    auto member = (SwapLists ? CrossValue2_ : CrossValue1_).GetElement(inIndex);
                    if (Self_->IsRequiredColumn_[inIndex]) {
                        ResItems_[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems_[outIndex] = std::move(member);
                    }
                }

                for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                    ui32 inIndex = Self_->RightInputColumns_[i];
                    ui32 outIndex = Self_->RightOutputColumns_[i];
                    auto member = (SwapLists ? CrossValue1_ : CrossValue2_).GetElement(inIndex);
                    if (Self_->IsRequiredColumn_[inIndex]) {
                        ResItems_[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems_[outIndex] = std::move(member);
                    }
                }

                return std::move(structObj);
            }
        }

        const TSelf* const Self_;
        bool EatInput_;
        bool KeyHasNulls_;
        std::optional<ui64> InitialUsage_;
        EOutputMode OutputMode_;

        bool CrossMove1_;
        NUdf::TUnboxedValue CrossValue1_;
        NUdf::TUnboxedValue CrossValue2_;

        TSpillList List1_;
        TSpillList List2_;

        NUdf::TUnboxedValue* ResItems_ = nullptr;
    };

    TCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationNode* flow, const TType* inputStructType, ui32 inputWidth, ui32 tableIndexPos,
                           std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
                           std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
                           std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , Flow_(flow)
        , InputStructType_(inputStructType)
        , Packer_(mutables)
        , TableIndexPos_(tableIndexPos)
        , LeftInputColumns_(std::move(leftInputColumns))
        , RightInputColumns_(std::move(rightInputColumns))
        , RequiredColumns_(std::move(requiredColumns))
        , LeftOutputColumns_(std::move(leftOutputColumns))
        , RightOutputColumns_(std::move(rightOutputColumns))
        , MemLimit_(memLimit)
        , SortedTableOrder_(sortedTableOrder)
        , KeyColumns_(std::move(keyColumns))
        , IsRequiredColumn_(FillRequiredStructColumn(inputWidth, RequiredColumns_))
        , ResStruct_(mutables)
        , ResStreamIndex_(mutables.CurValueIndex++)
        , AnyJoinSettings_(anyJoinSettings)
        , Logger_(mutables)
        , LogComponent_(mutables)
    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = ctx.HolderFactory.Create<TValue>(ctx, this);
        }

        return static_cast<TValue*>(state.AsBoxed().Get())->Next(Flow_, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow_);
    }

    NUdf::TLoggerPtr GetLogger(TComputationContext& ctx) const {
        if (Logger_.Empty(ctx)) {
            return Logger_.GetOrCreate(ctx, ctx.MakeLogger());
        }
        return Logger_.Get(ctx);
    }

    NUdf::TLogComponentId GetLogComponent(TComputationContext& ctx) const {
        if (LogComponent_.Empty(ctx)) {
            return LogComponent_.GetOrCreate(ctx, GetLogger(ctx)->RegisterComponent("CommonJoinCore"));
        }
        return LogComponent_.Get(ctx);
    }

    IComputationNode* const Flow_;
    const TType* const InputStructType_;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer_;
    const ui32 TableIndexPos_;
    const std::vector<ui32> LeftInputColumns_;
    const std::vector<ui32> RightInputColumns_;
    const std::vector<ui32> RequiredColumns_;
    const std::vector<ui32> LeftOutputColumns_;
    const std::vector<ui32> RightOutputColumns_;
    const ui64 MemLimit_;
    const std::optional<ui32> SortedTableOrder_;
    const std::vector<ui32> KeyColumns_;
    const std::vector<bool> IsRequiredColumn_;

    const TContainerCacheOnContext ResStruct_;
    const ui32 ResStreamIndex_;
    const EAnyJoinSettings AnyJoinSettings_;
    const TMutableDataOnContext<NUdf::TLoggerPtr> Logger_;
    const TMutableDataOnContext<NUdf::TLogComponentId> LogComponent_;
};

// Staging buffers for the wide CommonJoinCore state. They live in a single boxed
// value on the context (TMutableDataOnContext) rather than as raw ctx.MutableValues
// slots: the state (TValue) pins the box with a strong reference, so releasing the
// staged values from ~TValue never touches sibling context slots that the context's
// own MutableValues teardown may already have destroyed.
struct TWideJoinTempValues {
    TWideJoinTempValues(size_t valuesSize, size_t crossValuesSize)
        : Values(valuesSize)
        , CrossValues1(crossValuesSize)
        , CrossValues2(crossValuesSize)
    {
    }

    TUnboxedValueVector Values;
    TUnboxedValueVector CrossValues1;
    TUnboxedValueVector CrossValues2;
};

template <EJoinKind Kind, bool TTrackRss>
class TWideCommonJoinCoreWrapper: public TStatefulWideFlowCodegeneratorNode<TWideCommonJoinCoreWrapper<Kind, TTrackRss>>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                                  public ICodegeneratorRootNode
#endif
{
    using TSelf = TWideCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TStatefulWideFlowCodegeneratorNode<TSelf>;
    using TBaseComputation = TBase;

public:
    class TValue: public TComputationValue<TValue> {
        friend TSelf;

    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TSelf* self, TFetcher&& fetcher)
            : TBase(memInfo)
            , Self_(self)
            , Fetcher_(std::move(fetcher))
            , TempValuesPin_(Self_->GetTempValuesBox(ctx))
            , Values_(Self_->GetValues(ctx))
            , CrossValues1_(Self_->GetCrossValues(ctx, /*one=*/true))
            , CrossValues2_(Self_->GetCrossValues(ctx, /*one=*/false))
            , List1_(Self_->GetLogger(ctx), Self_->GetLogComponent(ctx), Self_->PackerLeft_.RefMutableObject(ctx, false, Self_->InputLeftType_), IsAnyJoinLeft(Self_->AnyJoinSettings_), Self_->InputLeftType_->GetElementsCount())
            , List2_(Self_->GetLogger(ctx), Self_->GetLogComponent(ctx), Self_->PackerRight_.RefMutableObject(ctx, false, Self_->InputRightType_), IsAnyJoinRight(Self_->AnyJoinSettings_), Self_->InputRightType_->GetElementsCount())
            , Fields_(Self_->GetFields(ctx))
            , Stubs_(Self_->Stubs_)
        {
            Init();
        }

        void Init() {
            List1_.Init();
            List2_.Init();
            CrossMove1_ = true;
            EatInput_ = true;
            KeyHasNulls_ = false;
            OutputMode_ = EOutputMode::Unknown;
            InitialUsage_ = std::nullopt;
        }

        ~TValue() override {
            std::fill(Values_.begin(), Values_.end(), NUdf::TUnboxedValuePod());
            std::fill(CrossValues1_.begin(), CrossValues1_.end(), NUdf::TUnboxedValuePod());
            std::fill(CrossValues2_.begin(), CrossValues2_.end(), NUdf::TUnboxedValuePod());
        }

    private:
        // copypaste to resolve -Woverloaded-virtual
        bool Next(NUdf::TUnboxedValue&) override {
            this->ThrowNotSupported(__func__);
            return false;
        }

        EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
            while (EatInput_) {
                if (!InitialUsage_) {
                    InitialUsage_ = ctx.HolderFactory.GetPagePool().GetUsed();
                }

                switch (Fetcher_(ctx, Fields_.data())) {
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        EatInput_ = false;
                        continue;
                    default:
                        break;
                }

                if (!KeyHasNulls_ && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                    for (const auto& keyColumn : Self_->KeyColumns_) {
                        if (!*Fields_[keyColumn]) {
                            KeyHasNulls_ = true;
                            break;
                        }
                    }
                }

                switch (const auto tableIndex = Fields_[Self_->TableIndexPos_]->template Get<ui32>()) {
                    case LeftIndex:
                        if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2_.Empty() && !KeyHasNulls_)) {
                            EatInput_ = false;
                            OutputMode_ = EOutputMode::None;
                            break;
                        }

                        if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == RightIndex) {
                            auto fetcher = IsAnyJoinLeft(Self_->AnyJoinSettings_) ? TLiveFetcher(std::bind(Fetcher_, std::placeholders::_1, Stubs_.data())) : [this](TComputationContext& ctx, NUdf::TUnboxedValue* output) {
                                if (const auto status = Fetcher_(ctx, Fields_.data()); EFetchResult::One != status) {
                                    return status;
                                }
                                std::transform(Self_->LeftInputColumns_.cbegin(), Self_->LeftInputColumns_.cend(), output, [this](ui32 index) { return std::move(this->Values_[index]); });
                                return EFetchResult::One;
                            };
                            std::transform(Self_->LeftInputColumns_.cbegin(), Self_->LeftInputColumns_.cend(), Values_.data(), [this](ui32 index) { return std::move(this->Values_[index]); });
                            List1_.Live(std::move(fetcher), Values_.data());
                            EatInput_ = false;
                        } else {
                            NUdf::TUnboxedValue* items = nullptr;
                            auto value = ctx.HolderFactory.CreateDirectArrayHolder(Self_->LeftInputColumns_.size(), items);
                            std::transform(Self_->LeftInputColumns_.cbegin(), Self_->LeftInputColumns_.cend(), items, [this](ui32 index) { return std::move(this->Values_[index]); });
                            List1_.Add(std::move(value));
                            if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                List1_.Spill();
                            }
                        }
                        break;
                    case RightIndex:
                        if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1_.Empty() && !KeyHasNulls_)) {
                            EatInput_ = false;
                            OutputMode_ = EOutputMode::None;
                            break;
                        }

                        if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == LeftIndex) {
                            auto fetcher = IsAnyJoinRight(Self_->AnyJoinSettings_) ? TLiveFetcher(std::bind(Fetcher_, std::placeholders::_1, Stubs_.data())) : [this](TComputationContext& ctx, NUdf::TUnboxedValue* output) {
                                if (const auto status = Fetcher_(ctx, Fields_.data()); EFetchResult::One != status) {
                                    return status;
                                }
                                std::transform(Self_->RightInputColumns_.cbegin(), Self_->RightInputColumns_.cend(), output, [this](ui32 index) { return std::move(this->Values_[index]); });
                                return EFetchResult::One;
                            };
                            std::transform(Self_->RightInputColumns_.cbegin(), Self_->RightInputColumns_.cend(), Values_.data(), [this](ui32 index) { return std::move(this->Values_[index]); });
                            List2_.Live(std::move(fetcher), Values_.data());
                            EatInput_ = false;
                        } else {
                            NUdf::TUnboxedValue* items = nullptr;
                            auto value = ctx.HolderFactory.CreateDirectArrayHolder(Self_->RightInputColumns_.size(), items);
                            std::transform(Self_->RightInputColumns_.cbegin(), Self_->RightInputColumns_.cend(), items, [this](ui32 index) { return std::move(this->Values_[index]); });
                            List2_.Add(std::move(value));
                            if (ctx.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                List2_.Spill();
                            }
                        }
                        break;
                    default:
                        THROW yexception() << "Bad table index: " << tableIndex;
                }
            }

            while (true) {
                switch (OutputMode_) {
                    case EOutputMode::Unknown: {
                        List1_.Seal(ctx);
                        List2_.Seal(ctx);
                        switch (Kind) {
                            case EJoinKind::Cross:
                            case EJoinKind::Inner:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }

                                break;
                            case EJoinKind::Left:
                                if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::LeftOnly:
                                if (List1_.Empty() || !List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::Right:
                                if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::RightOnly:
                                if (List2_.Empty() || !List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            case EJoinKind::Exclusion:
                                if (!List1_.Empty() && !List2_.Empty() && !KeyHasNulls_) {
                                    OutputMode_ = EOutputMode::None;
                                } else if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::LeftNull;
                                } else if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::RightNull;
                                } else {
                                    OutputMode_ = EOutputMode::BothNull;
                                }
                                break;

                            case EJoinKind::Full:
                                break;

                            case EJoinKind::LeftSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::RightSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            default:
                                Y_ABORT("Unknown kind");
                        }

                        if (OutputMode_ == EOutputMode::Unknown) {
                            if (List1_.Empty()) {
                                OutputMode_ = EOutputMode::LeftNull;
                            } else if (List2_.Empty()) {
                                OutputMode_ = EOutputMode::RightNull;
                            } else if (List1_.IsLive()) {
                                OutputMode_ = EOutputMode::Cross;
                            } else if (List2_.IsLive()) {
                                OutputMode_ = EOutputMode::CrossSwap;
                            } else {
                                OutputMode_ = List1_.GetCount() >= List2_.GetCount() ? EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                        continue;
                    case EOutputMode::LeftNull:
                        if (const auto res = List2_.Next(ctx, Values_.data()); EFetchResult::One != res) {
                            return res;
                        }

                        PrepareNullItem<true>(ctx, output);
                        return EFetchResult::One;

                    case EOutputMode::RightNull:
                        if (const auto res = List1_.Next(ctx, Values_.data()); EFetchResult::One != res) {
                            return res;
                        }

                        PrepareNullItem<false>(ctx, output);
                        return EFetchResult::One;
                    case EOutputMode::BothNull:
                        if (CrossMove1_) {
                            switch (List1_.Next(ctx, Values_.data())) {
                                case EFetchResult::Finish:
                                    CrossMove1_ = false;
                                    break;
                                case EFetchResult::Yield:
                                    return EFetchResult::Yield;
                                case EFetchResult::One:
                                    PrepareNullItem<false>(ctx, output);
                                    return EFetchResult::One;
                            }
                        }

                        if (const auto res = List2_.Next(ctx, Values_.data()); EFetchResult::One != res) {
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
        void PrepareNullItem(TComputationContext&, NUdf::TUnboxedValue* const* output) {
            for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                if (const auto out = output[Self_->LeftOutputColumns_[i]]) {
                    if constexpr (IsLeftNull) {
                        *out = NUdf::TUnboxedValuePod();
                    } else if (Self_->IsRequiredColumn_[Self_->LeftInputColumns_[i]]) {
                        *out = Values_[i].Release().GetOptionalValue();
                    } else {
                        *out = std::move(Values_[i]);
                    }
                }
            }

            for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                if (const auto out = output[Self_->RightOutputColumns_[i]]) {
                    if constexpr (!IsLeftNull) {
                        *out = NUdf::TUnboxedValuePod();
                    } else if (Self_->IsRequiredColumn_[Self_->RightInputColumns_[i]]) {
                        *out = Values_[i].Release().GetOptionalValue();
                    } else {
                        *out = std::move(Values_[i]);
                    }
                }
            }
        }

        template <bool SwapLists>
        EFetchResult PrepareCrossItem(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
            if (KeyHasNulls_) {
                for (;;) {
                    if (const auto res = (CrossMove1_ == SwapLists ? List2_ : List1_).Next(ctx, Values_.data()); EFetchResult::Finish == res && CrossMove1_) {
                        CrossMove1_ = false;
                        continue;
                    } else if (EFetchResult::One != res) {
                        return res;
                    }

                    if (CrossMove1_ == SwapLists) {
                        PrepareNullItem<true>(ctx, output);
                    } else {
                        PrepareNullItem<false>(ctx, output);
                    }

                    return EFetchResult::One;
                }
            }

            for (;;) {
                if (CrossMove1_) {
                    if (const auto res = (SwapLists ? List2_ : List1_).Next(ctx, CrossValues1_.data()); EFetchResult::One != res) {
                        return res;
                    }

                    CrossMove1_ = false;
                    (SwapLists ? List1_ : List2_).Rewind();
                }

                if (const auto res = (SwapLists ? List1_ : List2_).Next(ctx, CrossValues2_.data()); EFetchResult::Finish == res) {
                    CrossMove1_ = true;
                    continue;
                } else if (EFetchResult::Yield == res) {
                    return EFetchResult::Yield;
                }

                const auto& lValues = SwapLists ? CrossValues2_ : CrossValues1_;
                const auto& rValues = SwapLists ? CrossValues1_ : CrossValues2_;

                for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                    if (const auto out = output[Self_->LeftOutputColumns_[i]]) {
                        if (Self_->IsRequiredColumn_[Self_->LeftInputColumns_[i]]) {
                            *out = NUdf::TUnboxedValue(lValues[i]).Release().GetOptionalValue();
                        } else {
                            *out = lValues[i];
                        }
                    }
                }

                for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                    if (const auto out = output[Self_->RightOutputColumns_[i]]) {
                        if (Self_->IsRequiredColumn_[Self_->RightInputColumns_[i]]) {
                            *out = NUdf::TUnboxedValue(rValues[i]).Release().GetOptionalValue();
                        } else {
                            *out = rValues[i];
                        }
                    }
                }

                return EFetchResult::One;
            }
        }

        const TSelf* const Self_;
        TFetcher Fetcher_;
        bool EatInput_;
        bool KeyHasNulls_;
        std::optional<ui64> InitialUsage_;
        EOutputMode OutputMode_;

        bool CrossMove1_;

        NUdf::TUnboxedValue TempValuesPin_;

        TArrayRef<NUdf::TUnboxedValue> Values_;
        TArrayRef<NUdf::TUnboxedValue> CrossValues1_;
        TArrayRef<NUdf::TUnboxedValue> CrossValues2_;

        TSpillList List1_, List2_;

        NUdf::TUnboxedValue* ResItems_ = nullptr;
        const std::vector<NUdf::TUnboxedValue*>& Fields_;
        const std::vector<NUdf::TUnboxedValue*>& Stubs_;
    };

    TWideCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, const TTupleType* inputLeftType, const TTupleType* inputRightType,
                               std::vector<EValueRepresentation>&& inputRepresentations, std::vector<EValueRepresentation>&& outputRepresentations, ui32 tableIndexPos,
                               std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
                               std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
                               std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , InputRepresentations_(std::move(inputRepresentations))
        , OutputRepresentations_(std::move(outputRepresentations))
        , InputLeftType_(inputLeftType)
        , InputRightType_(inputRightType)
        , PackerLeft_(mutables)
        , PackerRight_(mutables)
        , TableIndexPos_(tableIndexPos)
        , LeftInputColumns_(std::move(leftInputColumns))
        , RightInputColumns_(std::move(rightInputColumns))
        , RequiredColumns_(std::move(requiredColumns))
        , LeftOutputColumns_(std::move(leftOutputColumns))
        , RightOutputColumns_(std::move(rightOutputColumns))
        , MemLimit_(memLimit)
        , SortedTableOrder_(sortedTableOrder)
        , KeyColumns_(std::move(keyColumns))
        , IsRequiredColumn_(FillRequiredStructColumn(InputRepresentations_.size(), RequiredColumns_))
        , AnyJoinSettings_(anyJoinSettings)
        , InputColumnsSize_(std::max(LeftInputColumns_.size(), RightInputColumns_.size()))
        , Logger_(mutables)
        , LogComponent_(mutables)
        , Fields_(mutables)
        , TempValues_(mutables)
        , Stubs_(InputRepresentations_.size(), nullptr)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        return static_cast<TValue*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto size = LeftOutputColumns_.size() + RightOutputColumns_.size();
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);

        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWideCommonJoinCoreWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        for (ui32 i = 0U; i < OutputRepresentations_.size(); ++i) {
            ValueCleanup(OutputRepresentations_[i], pointers[i], ctx, block);
        }

        new StoreInst(initV, values, block);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto result = EmitFunctionCall<&TValue::FetchValues>(Type::getInt32Ty(context), {stateArg, ctx.Ctx, fields}, ctx, block);

        for (ui32 i = 0U; i < OutputRepresentations_.size(); ++i) {
            ValueRelease(OutputRepresentations_[i], pointers[i], ctx, block);
        }

        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TValue>(ctx, this, std::bind(&IComputationWideFlowNode::FetchValues, Flow_, std::placeholders::_1, std::placeholders::_2));
#else
        state = ctx.ExecuteLLVM && Fetch_ ? ctx.HolderFactory.Create<TValue>(ctx, this, Fetch_) : ctx.HolderFactory.Create<TValue>(ctx, this, std::bind(&IComputationWideFlowNode::FetchValues, Flow_, std::placeholders::_1, std::placeholders::_2));
#endif
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow_);
    }

    NUdf::TLoggerPtr GetLogger(TComputationContext& ctx) const {
        if (Logger_.Empty(ctx)) {
            return Logger_.GetOrCreate(ctx, ctx.MakeLogger());
        }
        return Logger_.Get(ctx);
    }

    NUdf::TLogComponentId GetLogComponent(TComputationContext& ctx) const {
        if (LogComponent_.Empty(ctx)) {
            return LogComponent_.GetOrCreate(ctx, GetLogger(ctx)->RegisterComponent("WideCommonJoinCore"));
        }
        return LogComponent_.Get(ctx);
    }

    TWideJoinTempValues& GetTempValues(TComputationContext& ctx) const {
        return TempValues_.GetOrCreate(ctx, InputRepresentations_.size(), InputColumnsSize_);
    }

    NUdf::TUnboxedValue GetTempValuesBox(TComputationContext& ctx) const {
        GetTempValues(ctx);
        return TempValues_.GetValue(ctx);
    }

    TArrayRef<NUdf::TUnboxedValue> GetValues(TComputationContext& ctx) const {
        return GetTempValues(ctx).Values;
    }

    TArrayRef<NUdf::TUnboxedValue> GetCrossValues(TComputationContext& ctx, bool one) const {
        auto& tempValues = GetTempValues(ctx);
        return one ? tempValues.CrossValues1 : tempValues.CrossValues2;
    }

    const std::vector<NUdf::TUnboxedValue*>& GetFields(TComputationContext& ctx) const {
        if (Fields_.Empty(ctx)) {
            auto values = GetValues(ctx);
            auto& ptrs = Fields_.GetOrCreate(ctx, values.size());
            for (size_t i = 0; i < ptrs.size(); ++i) {
                ptrs[i] = &values[i];
            }
            return ptrs;
        }
        return Fields_.Get(ctx);
    }

    IComputationWideFlowNode* const Flow_;
    const std::vector<EValueRepresentation> InputRepresentations_;
    const std::vector<EValueRepresentation> OutputRepresentations_;
    const TTupleType* const InputLeftType_;
    const TTupleType* const InputRightType_;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> PackerLeft_, PackerRight_;
    const ui32 TableIndexPos_;
    const std::vector<ui32> LeftInputColumns_;
    const std::vector<ui32> RightInputColumns_;
    const std::vector<ui32> RequiredColumns_;
    const std::vector<ui32> LeftOutputColumns_;
    const std::vector<ui32> RightOutputColumns_;
    const ui64 MemLimit_;
    const std::optional<ui32> SortedTableOrder_;
    const std::vector<ui32> KeyColumns_;
    const std::vector<bool> IsRequiredColumn_;
    const EAnyJoinSettings AnyJoinSettings_;
    const ui32 InputColumnsSize_;
    const TMutableDataOnContext<NUdf::TLoggerPtr> Logger_;
    const TMutableDataOnContext<NUdf::TLogComponentId> LogComponent_;
    const TMutableDataOnContext<std::vector<NUdf::TUnboxedValue*>> Fields_;
    const TMutableDataOnContext<TWideJoinTempValues> TempValues_;
    const std::vector<NUdf::TUnboxedValue*> Stubs_;
#ifndef MKQL_DISABLE_CODEGEN
    using TFetchPtr = EFetchResult (*)(TComputationContext&, NUdf::TUnboxedValue* const*);

    TFetchPtr Fetch_ = nullptr;

    Function* FetchFunc_ = nullptr;

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc_) {
            Fetch_ = reinterpret_cast<TFetchPtr>(codegen.GetPointerToFunction(FetchFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(FetchFunc_ = GenerateFetchFunction(codegen));
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
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto pointerType = PointerType::getUnqual(valueType);
        const auto arrayType = ArrayType::get(pointerType, InputRepresentations_.size());
        const auto contextType = GetCompContextType(context);
        const auto resultType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(resultType, {PointerType::getUnqual(contextType), PointerType::getUnqual(arrayType)}, /*isVarArg=*/false);

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

        const auto result = GetNodeValues(Flow_, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, result.first, ConstantInt::get(result.first->getType(), 0), "special", block);

        BranchInst::Create(exit, good, special, block);

        block = good;

        const auto fields = new LoadInst(arrayType, outputArg, "fields", block);

        for (ui32 i = 0U; i < InputRepresentations_.size(); ++i) {
            const auto save = BasicBlock::Create(context, (TString("save_") += ToString(i)).c_str(), ctx.Func);
            const auto skip = BasicBlock::Create(context, (TString("skip_") += ToString(i)).c_str(), ctx.Func);

            const auto pointer = ExtractValueInst::Create(fields, i, (TString("pointer_") += ToString(i)).c_str(), block);
            const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, pointer, ConstantPointerNull::get(pointerType), (TString("null_") += ToString(i)).c_str(), block);

            BranchInst::Create(skip, save, null, block);

            block = save;

            const auto value = result.second[i](ctx, block);
            ValueUnRef(InputRepresentations_[i], pointer, ctx, block);
            new StoreInst(value, pointer, block);
            ValueAddRef(InputRepresentations_[i], value, ctx, block);

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

} // namespace NFlow

namespace NStream {

class TSpillList {
public:
    TSpillList(NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent, TValuePacker& itemPacker, bool singleShot)
        : Logger_(std::move(logger))
        , LogComponent_(logComponent)
        , ItemPacker_(itemPacker)
        , Ctx_(nullptr)
        , Count_(0)
#ifndef NDEBUG
        , IsSealed_(false)
#endif
        , Index_(ui64(-1))
        , SingleShot_(singleShot)
    {
    }

    TSpillList(TSpillList&& rhs) = delete;
    TSpillList(const TSpillList& rhs) = delete;
    void operator=(const TSpillList& rhs) = delete;

    void Init(TComputationContext& ctx) {
        Ctx_ = &ctx;
        Count_ = 0;
#ifndef NDEBUG
        IsSealed_ = false;
#endif
        Index_ = ui64(-1);
        FileState_ = nullptr;
        Heap_.clear();
        LiveStream_ = NUdf::TUnboxedValue();
        LiveValue_ = NUdf::TUnboxedValue();
    }

    TComputationContext& GetCtx() const {
        return *Ctx_;
    }

    bool Spill() {
        if (FileState_) {
            return false;
        }

        FileState_ = std::make_unique<TFileState>();
        OpenWrite();
        for (ui32 i = 0; i < Count_; ++i) {
            Write(std::move(InMemory(i)));
        }

        Heap_.clear();
        return true;
    }

    void Live(NUdf::TUnboxedValue& stream, NUdf::TUnboxedValue&& liveValue) {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        Y_DEBUG_ABORT_UNLESS(Count_ == 0);
        LiveStream_ = stream;
        LiveValue_ = std::move(liveValue);
    }

    void Add(NUdf::TUnboxedValue&& value) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(!IsSealed_);
#endif
        if (SingleShot_ && Count_ > 0) {
            MKQL_ENSURE(Count_ == 1, "Counter inconsistent");
            return;
        }

        if (FileState_) {
            Write(std::move(value));
        } else {
            if (Count_ < DEFAULT_STACK_ITEMS) {
                Stack_[Count_] = std::move(value);
            } else {
                if (Count_ == DEFAULT_STACK_ITEMS) {
                    Y_DEBUG_ABORT_UNLESS(Heap_.empty());
                    Heap_.assign(Stack_.begin(), Stack_.end());
                }

                Heap_.push_back(std::move(value));
            }
        }

        ++Count_;
    }

    void Seal() {
#ifndef NDEBUG
        IsSealed_ = true;
#endif
        if (FileState_) {
            FileState_->Output->Finish();
            Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "Spill finished at " << Count_ << " items");
            FileState_->Output.reset();
            Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "File size: " << GetFileLength(FileState_->File.GetName()) << ", expected: " << FileState_->TotalSize);

            MKQL_INC_STAT(Ctx_->Stats, Join_Spill_Count);
            MKQL_SET_MAX_STAT(Ctx_->Stats, Join_Spill_MaxFileSize, static_cast<i64>(FileState_->TotalSize));
            MKQL_SET_MAX_STAT(Ctx_->Stats, Join_Spill_MaxRowsCount, static_cast<i64>(Count_));
        }
    }

    bool IsLive() const {
        return bool(LiveStream_);
    }

    ui64 GetCount() const {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
        return Count_;
    }

    bool Empty() const {
        return !IsLive() && (Count_ == 0);
    }

    NUdf::EFetchStatus Next(NUdf::TUnboxedValue& result) {
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed_);
#endif
        if (IsLive()) {
            auto status = NUdf::EFetchStatus::Ok;
            NUdf::TUnboxedValue value;
            if ((Index_ + 1) == 0) {
                value = std::move(LiveValue_);
            } else {
                status = LiveStream_.Fetch(value);
                while (SingleShot_ && status == NUdf::EFetchStatus::Ok) {
                    // skip all remaining values
                    status = LiveStream_.Fetch(value);
                }
            }

            if (status == NUdf::EFetchStatus::Ok) {
                result = std::move(value);
                ++Index_;
            }
            return status;
        }

        if ((Index_ + 1) == Count_) {
            return NUdf::EFetchStatus::Finish;
        }

        ++Index_;
        if (FileState_) {
            if (Index_ == 0) {
                OpenRead();
            }

            result = Read();
            return NUdf::EFetchStatus::Ok;
        }

        result = InMemory(Index_);
        return NUdf::EFetchStatus::Ok;
    }

    void Rewind() {
        Y_DEBUG_ABORT_UNLESS(!IsLive());
#ifndef NDEBUG
        Y_DEBUG_ABORT_UNLESS(IsSealed_);
#endif
        Index_ = ui64(-1);
        if (FileState_) {
            OpenRead();
        }
    }

private:
    NUdf::TUnboxedValue& InMemory(ui32 index) {
        return !Heap_.empty() ? Heap_[index] : Stack_[index];
    }

    const NUdf::TUnboxedValue& InMemory(ui32 index) const {
        return !Heap_.empty() ? Heap_[index] : Stack_[index];
    }

    void OpenWrite() {
        Logger_->Log(LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "Spill started at " << Count_ << " items to " << FileState_->File.GetName());
        FileState_->Output = std::make_unique<TFixedBufferFileOutput>(FileState_->File.GetName());
        FileState_->Output->SetFlushPropagateMode(false);
        FileState_->Output->SetFinishPropagateMode(false);
    }

    void Write(NUdf::TUnboxedValue&& value) {
        Y_DEBUG_ABORT_UNLESS(FileState_->Output);
        TStringBuf serialized = ItemPacker_.Pack(value);
        ui32 length = serialized.size();
        FileState_->Output->Write(&length, sizeof(length));
        FileState_->Output->Write(serialized.data(), length);
        FileState_->TotalSize += sizeof(length);
        FileState_->TotalSize += length;
    }

    void OpenRead() {
        FileState_->Input.reset();
        FileState_->Input = std::make_unique<TFileInput>(FileState_->File.GetName());
    }

    NUdf::TUnboxedValue Read() {
        ui32 length = 0;
        auto wasRead = FileState_->Input->Load(&length, sizeof(length));
        Y_ABORT_UNLESS(wasRead == sizeof(length));
        FileState_->Buffer.Reserve(length);
        wasRead = FileState_->Input->Load((void*)FileState_->Buffer.Data(), length);
        Y_ABORT_UNLESS(wasRead == length);
        return ItemPacker_.Unpack(TStringBuf(FileState_->Buffer.Data(), length), Ctx_->HolderFactory);
    }

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    TValuePacker& ItemPacker_;
    TComputationContext* Ctx_;
    ui64 Count_;
    std::array<NUdf::TUnboxedValue, DEFAULT_STACK_ITEMS> Stack_;
    TUnboxedValueVector Heap_;
#ifndef NDEBUG
    bool IsSealed_;
#endif
    ui64 Index_;
    const bool SingleShot_;
    struct TFileState {
        TFileState()
            : File(TTempFileHandle::InCurrentDir())
            , TotalSize(0)
        {
        }

        TTempFileHandle File;
        ui64 TotalSize;
        std::unique_ptr<TFileInput> Input;
        std::unique_ptr<TFixedBufferFileOutput> Output;
        TBuffer Buffer;
    };

    std::unique_ptr<TFileState> FileState_;
    NUdf::TUnboxedValue LiveStream_;
    NUdf::TUnboxedValue LiveValue_;
};

template <EJoinKind Kind, bool TTrackRss>
class TCommonJoinCoreWrapper: public TMutableComputationNode<TCommonJoinCoreWrapper<Kind, TTrackRss>> {
    using TSelf = TCommonJoinCoreWrapper<Kind, TTrackRss>;
    using TBase = TMutableComputationNode<TSelf>;
    using TBaseComputation = TBase;

public:
    class TValue: public TComputationValue<TValue> {
    public:
        using TBase = TComputationValue<TValue>;

        TValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream,
               TComputationContext& ctx, const TSelf* self)
            : TBase(memInfo)
            , Stream_(std::move(stream))
            , Ctx_(ctx)
            , Self_(self)
            , Logger_(ctx.MakeLogger())
            , LogComponent_(Logger_->RegisterComponent("CommonJoinCore"))
            , List1_(Logger_, LogComponent_, Self_->Packer_.RefMutableObject(ctx, false, Self_->InputStructType_), IsAnyJoinLeft(Self_->AnyJoinSettings_))
            , List2_(Logger_, LogComponent_, Self_->Packer_.RefMutableObject(ctx, false, Self_->InputStructType_), IsAnyJoinRight(Self_->AnyJoinSettings_))
        {
            Init();
        }

        void Reset(NUdf::TUnboxedValue&& stream) {
            Stream_ = std::move(stream);
            Init();
        }

        void Init() {
            List1_.Init(Ctx_);
            List2_.Init(Ctx_);
            CrossMove1_ = true;
            EatInput_ = true;
            KeyHasNulls_ = false;
            OutputMode_ = EOutputMode::Unknown;
            InitialUsage_ = std::nullopt;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            while (EatInput_) {
                if (!InitialUsage_) {
                    InitialUsage_ = Ctx_.HolderFactory.GetPagePool().GetUsed();
                }

                NUdf::TUnboxedValue value;
                const auto status = Stream_.Fetch(value);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }

                if (status == NUdf::EFetchStatus::Finish) {
                    EatInput_ = false;
                } else {
                    if (!KeyHasNulls_ && (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Full)) {
                        for (const auto& keyColumn : Self_->KeyColumns_) {
                            if (!value.GetElement(keyColumn)) {
                                KeyHasNulls_ = true;
                                break;
                            }
                        }
                    }

                    switch (const auto tableIndex = value.GetElement(Self_->TableIndexPos_).template Get<ui32>()) {
                        case LeftIndex:
                            if (Kind == EJoinKind::RightOnly || (Kind == EJoinKind::Exclusion && !List2_.Empty() && !KeyHasNulls_)) {
                                EatInput_ = false;
                                OutputMode_ = EOutputMode::None;
                                break;
                            }

                            if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == RightIndex) {
                                List1_.Live(Stream_, std::move(value));
                                EatInput_ = false;
                            } else {
                                List1_.Add(std::move(value));
                                if (Ctx_.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                    List1_.Spill();
                                }
                            }
                            break;
                        case RightIndex:
                            if (Kind == EJoinKind::LeftOnly || (Kind == EJoinKind::Exclusion && !List1_.Empty() && !KeyHasNulls_)) {
                                EatInput_ = false;
                                OutputMode_ = EOutputMode::None;
                                break;
                            }

                            if (Self_->SortedTableOrder_ && *Self_->SortedTableOrder_ == LeftIndex) {
                                List2_.Live(Stream_, std::move(value));
                                EatInput_ = false;
                            } else {
                                List2_.Add(std::move(value));
                                if (Ctx_.CheckAdjustedMemLimit<TTrackRss>(Self_->MemLimit_, *InitialUsage_)) {
                                    List2_.Spill();
                                }
                            }
                            break;
                        default:
                            THROW yexception() << "Bad table index: " << tableIndex;
                    }
                }
            }

            while (true) {
                switch (OutputMode_) {
                    case EOutputMode::Unknown: {
                        List1_.Seal();
                        List2_.Seal();
                        switch (Kind) {
                            case EJoinKind::Cross:
                            case EJoinKind::Inner:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }

                                break;
                            case EJoinKind::Left:
                                if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::LeftOnly:
                                if (List1_.Empty() || !List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::Right:
                                if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                }
                                break;

                            case EJoinKind::RightOnly:
                                if (List2_.Empty() || !List1_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            case EJoinKind::Exclusion:
                                if (!List1_.Empty() && !List2_.Empty() && !KeyHasNulls_) {
                                    OutputMode_ = EOutputMode::None;
                                } else if (List1_.Empty()) {
                                    OutputMode_ = EOutputMode::LeftNull;
                                } else if (List2_.Empty()) {
                                    OutputMode_ = EOutputMode::RightNull;
                                } else {
                                    OutputMode_ = EOutputMode::BothNull;
                                }
                                break;

                            case EJoinKind::Full:
                                break;

                            case EJoinKind::LeftSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::RightNull;
                                }
                                break;

                            case EJoinKind::RightSemi:
                                if (List1_.Empty() || List2_.Empty()) {
                                    OutputMode_ = EOutputMode::None;
                                } else {
                                    OutputMode_ = EOutputMode::LeftNull;
                                }
                                break;

                            default:
                                Y_ABORT("Unknown kind");
                        }

                        if (OutputMode_ == EOutputMode::Unknown) {
                            if (List1_.Empty()) {
                                OutputMode_ = EOutputMode::LeftNull;
                            } else if (List2_.Empty()) {
                                OutputMode_ = EOutputMode::RightNull;
                            } else if (List1_.IsLive()) {
                                OutputMode_ = EOutputMode::Cross;
                            } else if (List2_.IsLive()) {
                                OutputMode_ = EOutputMode::CrossSwap;
                            } else {
                                OutputMode_ = List1_.GetCount() >= List2_.GetCount() ? EOutputMode::Cross : EOutputMode::CrossSwap;
                            }
                        }
                    }
                        continue;
                    case EOutputMode::LeftNull: {
                        NUdf::TUnboxedValue value;
                        auto status = List2_.Next(value);
                        if (status != NUdf::EFetchStatus::Ok) {
                            return status;
                        }

                        result = PrepareNullItem<true>(value);
                        return NUdf::EFetchStatus::Ok;
                    } break;
                    case EOutputMode::RightNull: {
                        NUdf::TUnboxedValue value;
                        auto status = List1_.Next(value);
                        if (status != NUdf::EFetchStatus::Ok) {
                            return status;
                        }

                        result = PrepareNullItem<false>(value);
                        return NUdf::EFetchStatus::Ok;
                    } break;
                    case EOutputMode::BothNull: {
                        NUdf::TUnboxedValue value;

                        if (CrossMove1_) {
                            switch (const auto status = List1_.Next(value)) {
                                case NUdf::EFetchStatus::Finish:
                                    CrossMove1_ = false;
                                    break;
                                case NUdf::EFetchStatus::Yield:
                                    return status;
                                case NUdf::EFetchStatus::Ok:
                                    result = PrepareNullItem<false>(value);
                                    return NUdf::EFetchStatus::Ok;
                            }
                        }

                        switch (const auto status = List2_.Next(value)) {
                            case NUdf::EFetchStatus::Yield:
                            case NUdf::EFetchStatus::Finish:
                                return status;
                            case NUdf::EFetchStatus::Ok:
                                result = PrepareNullItem<true>(value);
                                return NUdf::EFetchStatus::Ok;
                        }
                    } break;
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
            const auto structObj = Self_->ResStruct_.NewArray(Ctx_, Self_->LeftInputColumns_.size() + Self_->RightInputColumns_.size(), ResItems_);

            for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                ui32 inIndex = Self_->LeftInputColumns_[i];
                ui32 outIndex = Self_->LeftOutputColumns_[i];
                if (IsLeftNull) {
                    ResItems_[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self_->IsRequiredColumn_[inIndex]) {
                    ResItems_[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems_[outIndex] = std::move(member);
                }
            }

            for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                ui32 inIndex = Self_->RightInputColumns_[i];
                ui32 outIndex = Self_->RightOutputColumns_[i];
                if (!IsLeftNull) {
                    ResItems_[outIndex] = NUdf::TUnboxedValuePod();
                    continue;
                }

                auto member = value.GetElement(inIndex);
                if (Self_->IsRequiredColumn_[inIndex]) {
                    ResItems_[outIndex] = member.Release().GetOptionalValue();
                } else {
                    ResItems_[outIndex] = std::move(member);
                }
            }

            return structObj;
        }

        template <bool SwapLists>
        NUdf::EFetchStatus PrepareCrossItem(NUdf::TUnboxedValue& result) {
            if (KeyHasNulls_) {
                for (;;) {
                    NUdf::TUnboxedValue value;
                    auto status = (CrossMove1_ == SwapLists ? List2_ : List1_).Next(value);
                    if (status == NUdf::EFetchStatus::Finish && CrossMove1_) {
                        CrossMove1_ = false;
                        continue;
                    }

                    if (status != NUdf::EFetchStatus::Ok) {
                        return status;
                    }

                    result = (CrossMove1_ == SwapLists) ? PrepareNullItem<true>(value) : PrepareNullItem<false>(value);
                    return status;
                }
            }

            for (;;) {
                if (CrossMove1_) {
                    auto status = (SwapLists ? List2_ : List1_).Next(CrossValue1_);
                    if (status != NUdf::EFetchStatus::Ok) {
                        return status;
                    }

                    CrossMove1_ = false;
                    (SwapLists ? List1_ : List2_).Rewind();
                }

                auto status = (SwapLists ? List1_ : List2_).Next(CrossValue2_);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status");
                if (status == NUdf::EFetchStatus::Finish) {
                    CrossMove1_ = true;
                    continue;
                }

                auto structObj = Self_->ResStruct_.NewArray(Ctx_, Self_->LeftInputColumns_.size() + Self_->RightInputColumns_.size(), ResItems_);

                for (ui32 i = 0; i < Self_->LeftInputColumns_.size(); ++i) {
                    ui32 inIndex = Self_->LeftInputColumns_[i];
                    ui32 outIndex = Self_->LeftOutputColumns_[i];
                    auto member = (SwapLists ? CrossValue2_ : CrossValue1_).GetElement(inIndex);
                    if (Self_->IsRequiredColumn_[inIndex]) {
                        ResItems_[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems_[outIndex] = std::move(member);
                    }
                }

                for (ui32 i = 0; i < Self_->RightInputColumns_.size(); ++i) {
                    ui32 inIndex = Self_->RightInputColumns_[i];
                    ui32 outIndex = Self_->RightOutputColumns_[i];
                    auto member = (SwapLists ? CrossValue1_ : CrossValue2_).GetElement(inIndex);
                    if (Self_->IsRequiredColumn_[inIndex]) {
                        ResItems_[outIndex] = member.Release().GetOptionalValue();
                    } else {
                        ResItems_[outIndex] = std::move(member);
                    }
                }

                result = std::move(structObj);
                return NUdf::EFetchStatus::Ok;
            }
        }

        NUdf::TUnboxedValue Stream_;
        TComputationContext& Ctx_;
        const TSelf* const Self_;
        const NUdf::TLoggerPtr Logger_;
        const NUdf::TLogComponentId LogComponent_;
        bool EatInput_;
        bool KeyHasNulls_;
        std::optional<ui64> InitialUsage_;
        EOutputMode OutputMode_;

        bool CrossMove1_;
        NUdf::TUnboxedValue CrossValue1_;
        NUdf::TUnboxedValue CrossValue2_;

        TSpillList List1_;
        TSpillList List2_;

        NUdf::TUnboxedValue* ResItems_ = nullptr;
    };

    TCommonJoinCoreWrapper(TComputationMutables& mutables, IComputationNode* stream, const TType* inputStructType, ui32 inputWidth, ui32 tableIndexPos,
                           std::vector<ui32>&& leftInputColumns, std::vector<ui32>&& rightInputColumns, std::vector<ui32>&& requiredColumns,
                           std::vector<ui32>&& leftOutputColumns, std::vector<ui32>&& rightOutputColumns, ui64 memLimit,
                           std::optional<ui32> sortedTableOrder, std::vector<ui32>&& keyColumns, EAnyJoinSettings anyJoinSettings)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , InputStructType_(inputStructType)
        , Packer_(mutables)
        , TableIndexPos_(tableIndexPos)
        , LeftInputColumns_(std::move(leftInputColumns))
        , RightInputColumns_(std::move(rightInputColumns))
        , RequiredColumns_(std::move(requiredColumns))
        , LeftOutputColumns_(std::move(leftOutputColumns))
        , RightOutputColumns_(std::move(rightOutputColumns))
        , MemLimit_(memLimit)
        , SortedTableOrder_(sortedTableOrder)
        , KeyColumns_(std::move(keyColumns))
        , IsRequiredColumn_(FillRequiredStructColumn(inputWidth, RequiredColumns_))
        , ResStruct_(mutables)
        , ResStreamIndex_(mutables.CurValueIndex++)
        , AnyJoinSettings_(anyJoinSettings)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& resStream = ctx.MutableValues[ResStreamIndex_];
        if (!resStream || resStream.IsInvalid() || !resStream.UniqueBoxed()) {
            resStream = ctx.HolderFactory.Create<TValue>(Stream_->GetValue(ctx), ctx, this);
        } else {
            static_cast<TValue&>(*resStream.AsBoxed()).Reset(Stream_->GetValue(ctx));
        }

        return static_cast<const NUdf::TUnboxedValuePod&>(resStream);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const TType* const InputStructType_;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer_;
    const ui32 TableIndexPos_;
    const std::vector<ui32> LeftInputColumns_;
    const std::vector<ui32> RightInputColumns_;
    const std::vector<ui32> RequiredColumns_;
    const std::vector<ui32> LeftOutputColumns_;
    const std::vector<ui32> RightOutputColumns_;
    const ui64 MemLimit_;
    const std::optional<ui32> SortedTableOrder_;
    const std::vector<ui32> KeyColumns_;
    const std::vector<bool> IsRequiredColumn_;

    const TContainerCacheOnContext ResStruct_;
    const ui32 ResStreamIndex_;
    const EAnyJoinSettings AnyJoinSettings_;
};

} // namespace NStream

} // namespace

IComputationNode* WrapCommonJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 11U || callable.GetInputsCount() == 12U, "Expected 12 args");
    const auto type = callable.GetType()->GetReturnType();

    const auto inputRowType = type->IsFlow() ? AS_TYPE(TFlowType, callable.GetInput(0))->GetItemType() : AS_TYPE(TStreamType, callable.GetInput(0))->GetItemType();

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

    const auto outputRowType = type->IsFlow() ? AS_TYPE(TFlowType, type)->GetItemType() : AS_TYPE(TStreamType, type)->GetItemType();

    std::vector<EValueRepresentation> outputRepresentations;
    if (outputRowType->IsTuple()) {
        const auto tupleType = AS_TYPE(TTupleType, outputRowType);
        outputRepresentations.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i) {
            outputRepresentations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));
        }
    } else if (outputRowType->IsMulti()) {
        const auto tupleType = AS_TYPE(TMultiType, outputRowType);
        outputRepresentations.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i) {
            outputRepresentations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));
        }
    } else if (outputRowType->IsStruct()) {
        const auto structType = AS_TYPE(TStructType, outputRowType);
        outputRepresentations.reserve(structType->GetMembersCount());
        for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
            outputRepresentations.emplace_back(GetValueRepresentation(structType->GetMemberType(i)));
        }
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

    const auto tableIndexPos = 12U == callable.GetInputsCount() ? AS_VALUE(TDataLiteral, callable.GetInput(11U))->AsValue().Get<ui32>() : AS_TYPE(TStructType, inputRowType)->GetMemberIndex("_yql_table_index");

    const bool trackRss = EGraphPerProcess::Single == ctx.GraphPerProcess;
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    const auto leftInputType = TTupleType::Create(leftTypes.size(), leftTypes.data(), ctx.Env);
    const auto rightInputType = TTupleType::Create(rightTypes.size(), rightTypes.data(), ctx.Env);

#define MAKE_COMMON_JOIN_CORE_WRAPPER(KIND)                                                                                                                                                                                   \
    case EJoinKind::KIND:                                                                                                                                                                                                     \
        if (type->IsFlow()) {                                                                                                                                                                                                 \
            if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow))                                                                                                                                              \
                if (trackRss)                                                                                                                                                                                                 \
                    return new NFlow::TWideCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, wide, leftInputType, rightInputType, std::move(inputRepresentations), std::move(outputRepresentations), tableIndexPos,  \
                                                                                        std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                                \
                                                                                        std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);     \
                else                                                                                                                                                                                                          \
                    return new NFlow::TWideCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, wide, leftInputType, rightInputType, std::move(inputRepresentations), std::move(outputRepresentations), tableIndexPos, \
                                                                                         std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                               \
                                                                                         std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);    \
            else if (trackRss)                                                                                                                                                                                                \
                return new NFlow::TCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos,                                                                 \
                                                                                std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                                        \
                                                                                std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);             \
            else                                                                                                                                                                                                              \
                return new NFlow::TCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos,                                                                \
                                                                                 std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                                       \
                                                                                 std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);            \
        } else {                                                                                                                                                                                                              \
            if (trackRss)                                                                                                                                                                                                     \
                return new NStream::TCommonJoinCoreWrapper<EJoinKind::KIND, true>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos,                                                               \
                                                                                  std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                                      \
                                                                                  std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);           \
            else                                                                                                                                                                                                              \
                return new NStream::TCommonJoinCoreWrapper<EJoinKind::KIND, false>(ctx.Mutables, flow, inputRowType, inputRepresentations.size(), tableIndexPos,                                                              \
                                                                                   std::move(leftInputColumns), std::move(rightInputColumns), std::move(requiredColumns),                                                     \
                                                                                   std::move(leftOutputColumns), std::move(rightOutputColumns), memLimit, sortedTableOrder, std::move(keyColumns), anyJoinSettings);          \
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

} // namespace NKikimr::NMiniKQL
