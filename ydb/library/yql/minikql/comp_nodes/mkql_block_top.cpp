#include "mkql_block_top.h"

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl_codegen.h>  // Y_IGNORE

#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/utils/sort.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool Sort, bool HasCount>
class TTopOrSortBlocksWrapper : public TStatefulWideFlowCodegeneratorNode<TTopOrSortBlocksWrapper<Sort, HasCount>> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TTopOrSortBlocksWrapper<Sort, HasCount>>;
using TChunkedArrayIndex = std::vector<IArrayBuilder::TArrayDataItem>;
public:
    TTopOrSortBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TArrayRef<TType* const> wideComponents, IComputationNode* count,
        TComputationNodePtrVector&& directions, std::vector<ui32>&& keyIndicies)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Count_(count)
        , Directions_(std::move(directions))
        , KeyIndicies_(std::move(keyIndicies))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(wideComponents.size()))
    {
        for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
            Columns_.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_ABORT_UNLESS(output[Columns_.size()]);
        auto& s = GetState(state, ctx);

        if (!s.Count) {
            if (s.IsFinished_)
                return EFetchResult::Finish;

            if (!s.WritingOutput_) {
                for (const auto fields = ctx.WideFields.data() + WideFieldsIndex_;;) {
                    switch (Flow_->FetchValues(ctx, fields)) {
                        case EFetchResult::Yield:
                            return EFetchResult::Yield;
                        case EFetchResult::One:
                            s.ProcessInput();
                            continue;
                        case EFetchResult::Finish:
                            break;
                    }
                    break;
                }
            }

            if (!s.FillOutput(ctx.HolderFactory))
                return EFetchResult::Finish;
        }

        const auto sliceSize = s.Slice();
        for (size_t i = 0; i <= Columns_.size(); ++i) {
            if (const auto out = output[i]) {
                *out = s.Get(sliceSize, ctx.HolderFactory, i);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto width = Columns_.size() + 1U;

        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto flagType = Type::getInt1Ty(context);
        const auto arrayType = ArrayType::get(valueType, width);
        const auto ptrValuesType = PointerType::getUnqual(arrayType);

        TLLVMFieldsStructureState stateFields(context, width);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, indexType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", atTop);

        const auto heightPtr = new AllocaInst(indexType, 0U, "height_ptr", atTop);
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", atTop);

        new StoreInst(ConstantInt::get(indexType, 0), heightPtr, atTop);
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, atTop);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto read = BasicBlock::Create(context, "read", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        llvm::Value* trunc;
        if constexpr (HasCount) {
            const auto count = GetNodeValue(Count_, ctx, block);
            trunc = GetterFor<ui64>(count, context, block);
        } else {
            trunc = ConstantInt::get(Type::getInt64Ty(context), 0U);
        }

        const auto dirsType = ArrayType::get(flagType, Directions_.size());
        const auto dirs = new AllocaInst(dirsType, 0U, "dirs", block);
        for (auto i = 0U; i < Directions_.size(); ++i) {
            const auto dir = GetNodeValue(Directions_[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = GetElementPtrInst::CreateInBounds(dirsType, dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block);
            new StoreInst(cut, ptr, block);
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TTopOrSortBlocksWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType(), dirs->getType(), trunc->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr, dirs, trunc}, "", block);

        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCount() }, "count_ptr", block);

        const auto count = new LoadInst(indexType, countPtr, "count", block);
        const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "none", block);

        BranchInst::Create(more, fill, none, block);

        block = more;

        const auto finishedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIsFinished() }, "is_finished_ptr", block);
        const auto finished = new LoadInst(flagType, finishedPtr, "finished", block);

        const auto result = PHINode::Create(statusType, 4U, "result", over);
        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(over, test, finished, block);

        block = test;

        const auto writingOutputPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetWritingOutput() }, "writing_output_ptr", block);
        const auto writingOutput = new LoadInst(flagType, writingOutputPtr, "writing_output", block);

        BranchInst::Create(work, read, writingOutput, block);

        block = read;

        const auto getres = GetNodeValues(Flow_, ctx, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        const auto way = SwitchInst::Create(getres.first, good, 2U, block);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Finish)), work);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Yield)), over);

        block = good;

        const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetPointer() }, "values_ptr", block);
        const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
        Value* array = UndefValue::get(arrayType);
        for (auto idx = 0U; idx < getres.second.size(); ++idx) {
            const auto value = getres.second[idx](ctx, block);
            AddRefBoxed(value, ctx, block);
            array = InsertValueInst::Create(array, value, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
        }
        new StoreInst(array, values, block);

        const auto processBlockFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::ProcessInput));
        const auto processBlockType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);
        const auto processBlockPtr = CastInst::Create(Instruction::IntToPtr, processBlockFunc, PointerType::getUnqual(processBlockType), "process_inputs_func", block);
        CallInst::Create(processBlockType, processBlockPtr, {stateArg}, "", block);

        BranchInst::Create(read, block);

        block = work;

        const auto fillBlockFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::FillOutput));
        const auto fillBlockType = FunctionType::get(flagType, {statePtrType, ctx.GetFactory()->getType()}, false);
        const auto fillBlockPtr = CastInst::Create(Instruction::IntToPtr, fillBlockFunc, PointerType::getUnqual(fillBlockType), "fill_output_func", block);
        const auto hasData = CallInst::Create(fillBlockType, fillBlockPtr, {stateArg, ctx.GetFactory()}, "fill_output", block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(fill, over, hasData, block);

        block = fill;

        const auto sliceFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Slice));
        const auto sliceType = FunctionType::get(indexType, {statePtrType}, false);
        const auto slicePtr = CastInst::Create(Instruction::IntToPtr, sliceFunc, PointerType::getUnqual(sliceType), "slice_func", block);
        const auto slice = CallInst::Create(sliceType, slicePtr, {stateArg}, "slice", block);
        new StoreInst(slice, heightPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(width);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, getType, getPtr, heightPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto heightArg = new LoadInst(indexType, heightPtr, "height", block);
                return CallInst::Create(getType, getPtr, {stateArg, heightArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, "get", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            if constexpr (HasCount) {
                this->DependsOn(flow, Count_);
            }

            for (auto dir : Directions_) {
                this->DependsOn(flow, dir);
            }
        }
    }

    class TState : public TBlockState {
    public:
        bool WritingOutput_ = false;
        bool IsFinished_ = false;

        ui64 OutputLength_ = 0;
        ui64 Written_ = 0;
        const std::vector<bool> Directions_;
        const ui64 Count_;
        const std::vector<TBlockType*> Columns_;
        const std::vector<ui32> KeyIndicies_;
        std::vector<std::vector<arrow::Datum>> SortInput_;
        std::vector<ui64> SortPermutation_;
        std::vector<TChunkedArrayIndex> SortArrays_;

        bool ScalarsFilled_ = false;
        TUnboxedValueVector ScalarValues_;
        std::vector<std::unique_ptr<IBlockReader>> LeftReaders_;
        std::vector<std::unique_ptr<IBlockReader>> RightReaders_;
        std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
        ui64 BuilderMaxLength_ = 0;
        ui64 BuilderLength_ = 0;
        std::vector<NUdf::IBlockItemComparator::TPtr> Comparators_; // by key columns only

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const std::vector<ui32>& keyIndicies, const std::vector<TBlockType*>& columns, const bool* directions, ui64 count)
            : TBlockState(memInfo, columns.size() + 1U)
            , IsFinished_(HasCount && !count)
            , Directions_(directions, directions + keyIndicies.size())
            , Count_(count)
            , Columns_(columns)
            , KeyIndicies_(keyIndicies)
            , SortInput_(Columns_.size())
            , SortArrays_(Columns_.size())
            , LeftReaders_(Columns_.size())
            , RightReaders_(Columns_.size())
            , Builders_(Columns_.size())
            , Comparators_(KeyIndicies_.size())
        {
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                LeftReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
                RightReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
            }

            for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                Comparators_[k] = TBlockTypeHelper().MakeComparator(Columns_[KeyIndicies_[k]]->GetItemType());
            }

            BuilderMaxLength_ = GetStorageLength();
            size_t maxBlockItemSize = 0;
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                maxBlockItemSize = Max(maxBlockItemSize, CalcMaxBlockItemSize(Columns_[i]->GetItemType()));
            };

            BuilderMaxLength_ = Max(BuilderMaxLength_, CalcBlockLen(maxBlockItemSize));

            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), Columns_[i]->GetItemType(), ctx.ArrowMemoryPool, BuilderMaxLength_, &ctx.Builder->GetPgBuilder());
            }
        }

        void Add(const NUdf::TUnboxedValuePod value, size_t idx) {
            Values[idx] = value;
        }

        void ProcessInput() {
            const ui64 blockLen = TArrowBlock::From(Values.back()).GetDatum().template scalar_as<arrow::UInt64Scalar>().value;

            if (!ScalarsFilled_) {
                for (ui32 i = 0; i < Columns_.size(); ++i) {
                    if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                        ScalarValues_[i] = std::move(Values[i]);
                    }
                }

                ScalarsFilled_ = true;
            }

            if constexpr (!HasCount) {
                for (ui32 i = 0; i < Columns_.size(); ++i) {
                    auto datum = TArrowBlock::From(Values[i]).GetDatum();
                    if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                        SortInput_[i].emplace_back(datum);
                    }
                }

                OutputLength_ += blockLen;
                Values.assign(Values.size(), NUdf::TUnboxedValuePod());
                return;
            }

            // shrink input block
            std::optional<std::vector<ui64>> blockIndicies;
            if (blockLen > Count_) {
                blockIndicies.emplace();
                blockIndicies->reserve(blockLen);
                for (ui64 row = 0; row < blockLen; ++row) {
                    blockIndicies->emplace_back(row);
                }

                std::vector<TChunkedArrayIndex> arrayIndicies(Columns_.size());
                for (ui32 i = 0; i < Columns_.size(); ++i) {
                    if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                        auto datum = TArrowBlock::From(Values[i]).GetDatum();
                        arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                    }
                }

                const TBlockLess cmp(KeyIndicies_, *this, arrayIndicies);
                NYql::FastNthElement(blockIndicies->begin(), blockIndicies->begin() + Count_, blockIndicies->end(), cmp);
            }

            // copy all to builders
            AddTop(Columns_, blockIndicies, blockLen);
            if (BuilderLength_ + Count_ > BuilderMaxLength_) {
                CompressBuilders(false);
            }

            Values.assign(Values.size(), NUdf::TUnboxedValuePod());
        }

        ui64 GetStorageLength() const {
            return 2 * Count_;
        }

        void CompressBuilders(bool sort) {
            Y_ABORT_UNLESS(ScalarsFilled_);
            std::vector<TChunkedArrayIndex> arrayIndicies(Columns_.size());
            std::vector<arrow::Datum> tmpDatums(Columns_.size());
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = Builders_[i]->Build(false);
                    arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                    tmpDatums[i] = std::move(datum);
                }
            }

            std::vector<ui64> blockIndicies;
            blockIndicies.reserve(BuilderLength_);
            for (ui64 row = 0; row < BuilderLength_; ++row) {
                blockIndicies.push_back(row);
            }

            const ui64 blockLen = Min(BuilderLength_, Count_);
            const TBlockLess cmp(KeyIndicies_, *this, arrayIndicies);
            if (BuilderLength_ <= Count_) {
                if (sort) {
                    std::sort(blockIndicies.begin(), blockIndicies.end(), cmp);
                }
            } else {
                if (sort) {
                    NYql::FastPartialSort(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                } else {
                    NYql::FastNthElement(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                }
            }

            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                auto& arrayIndex = arrayIndicies[i];
                Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies.data(), blockLen);
            }

            BuilderLength_ = blockLen;
        }

        void SortAll() {
            SortPermutation_.reserve(OutputLength_);
            for (ui64 i = 0; i < OutputLength_; ++i) {
                SortPermutation_.emplace_back(i);
            }

            for (ui32 i = 0; i < Columns_.size(); ++i) {
                ui64 offset = 0;
                for (const auto& datum : SortInput_[i]) {
                    if (datum.is_scalar()) {
                        continue;
                    } else if (datum.is_array()) {
                        auto arrayData = datum.array();
                        SortArrays_[i].push_back({ arrayData.get(), offset });
                        offset += arrayData->length;
                    } else {
                        auto chunks = datum.chunks();
                        for (auto& chunk : chunks) {
                            auto arrayData = chunk->data();
                            SortArrays_[i].push_back({ arrayData.get(), offset });
                            offset += arrayData->length;
                        }
                    }
                }
            }

            TBlockLess cmp(KeyIndicies_, *this, SortArrays_);
            std::sort(SortPermutation_.begin(), SortPermutation_.end(), cmp);
        }

        bool FillOutput(const THolderFactory& holderFactory) {
            if (WritingOutput_) {
                FillSortOutputPart(holderFactory);
            } else if constexpr (!HasCount) {
                if (!OutputLength_) {
                    IsFinished_ = true;
                    return false;
                }

                SortAll();
                WritingOutput_ = true;
                FillSortOutputPart(holderFactory);
            } else {
                IsFinished_ = true;
                if (!BuilderLength_) {
                    return false;
                }

                if (BuilderLength_ > Count_ || Sort) {
                    CompressBuilders(Sort);
                }

                for (ui32 i = 0; i < Columns_.size(); ++i) {
                    if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                        Values[i] = ScalarValues_[i];
                    } else {
                        Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(true)));
                    }
                }

                Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BuilderLength_)));
            }
            FillArrays();
            return true;
        }

        void FillSortOutputPart(const THolderFactory& holderFactory) {
            auto blockLen = Min(BuilderMaxLength_, OutputLength_ - Written_);
            const bool isLast = (Written_ + blockLen == OutputLength_);

            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    Values[i] = ScalarValues_[i];
                } else {
                    Builders_[i]->AddMany(SortArrays_[i].data(), SortArrays_[i].size(), SortPermutation_.data() + Written_, blockLen);
                    Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(isLast)));
                }
            }

            Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
            Written_ += blockLen;
            if (Written_ >= OutputLength_)
                IsFinished_ = true;
        }

        void AddTop(const std::vector<TBlockType*>& columns, const std::optional<std::vector<ui64>>& blockIndicies, ui64 blockLen) {
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                const auto& datum = TArrowBlock::From(Values[i]).GetDatum();
                auto arrayIndex = MakeChunkedArrayIndex(datum);
                if (blockIndicies) {
                    Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies->data(), Count_);
                } else {
                    Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), ui64(0), blockLen);
                }
            }

            if (blockIndicies) {
                BuilderLength_ += Count_;
            } else {
                BuilderLength_ += blockLen;
            }
        }
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructureBlockState {
    private:
        using TBase = TLLVMFieldsStructureBlockState;
        llvm::IntegerType*const WritingOutputType;
        llvm::IntegerType*const IsFinishedType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFieldsArray();
            result.emplace_back(WritingOutputType);
            result.emplace_back(IsFinishedType);
            return result;
        }

        llvm::Constant* GetWritingOutput() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields);
        }

        llvm::Constant* GetIsFinished() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 1);
        }

        TLLVMFieldsStructureState(llvm::LLVMContext& context, size_t width)
            : TBase(context, width)
            , WritingOutputType(Type::getInt1Ty(Context))
            , IsFinishedType(Type::getInt1Ty(Context))
        {}
    };
#endif
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, const bool* directions, ui64 count = 0ULL) const {
        state = ctx.HolderFactory.Create<TState>(ctx, KeyIndicies_, Columns_, directions, count);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            std::vector<bool> dirs(Directions_.size());
            std::transform(Directions_.cbegin(), Directions_.cend(), dirs.begin(), [&ctx](IComputationNode* dir){ return dir->GetValue(ctx).Get<bool>(); });

            if constexpr (HasCount)
                MakeState(ctx, state, dirs.data(), Count_->GetValue(ctx).Get<ui64>());
            else
                MakeState(ctx, state, dirs.data());

            auto& s = *static_cast<TState*>(state.AsBoxed().Get());
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            for (size_t i = 0; i < s.Values.size(); ++i) {
                fields[i] = &s.Values[i];
            }
            return s;
        }

        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    static TChunkedArrayIndex MakeChunkedArrayIndex(const arrow::Datum& datum) {
        TChunkedArrayIndex result;
        if (datum.is_array()) {
            result.push_back({datum.array().get(), 0});
        } else {
            auto chunks = datum.chunks();
            ui64 offset = 0;
            for (auto& chunk : chunks) {
                auto arrayData = chunk->data();
                result.push_back({arrayData.get(), offset});
                offset += arrayData->length;
            }
        }
        return result;
    }

    class TBlockLess {
    public:
        TBlockLess(const std::vector<ui32>& keyIndicies, const TState& state, const std::vector<TChunkedArrayIndex>& arrayIndicies)
            : KeyIndicies_(keyIndicies)
            , ArrayIndicies_(arrayIndicies)
            , State_(state)
        {}

        bool operator()(ui64 lhs, ui64 rhs) const {
            if (KeyIndicies_.size() == 1) {
                auto i = KeyIndicies_[0];
                auto& arrayIndex = ArrayIndicies_[i];
                if (arrayIndex.empty()) {
                    // scalar
                    return false;
                }

                auto leftItem = GetBlockItem(*State_.LeftReaders_[i], arrayIndex, lhs);
                auto rightItem = GetBlockItem(*State_.RightReaders_[i], arrayIndex, rhs);
                if (State_.Directions_[0]) {
                    return State_.Comparators_[0]->Less(leftItem, rightItem);
                } else {
                    return State_.Comparators_[0]->Greater(leftItem, rightItem);
                }
            } else {
                for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                    auto i = KeyIndicies_[k];
                    auto& arrayIndex = ArrayIndicies_[i];
                    if (arrayIndex.empty()) {
                        // scalar
                        continue;
                    }

                    auto leftItem = GetBlockItem(*State_.LeftReaders_[i], arrayIndex, lhs);
                    auto rightItem = GetBlockItem(*State_.RightReaders_[i], arrayIndex, rhs);
                    auto cmp = State_.Comparators_[k]->Compare(leftItem, rightItem);
                    if (cmp == 0) {
                        continue;
                    }

                    if (State_.Directions_[k]) {
                        return cmp < 0;
                    } else {
                        return cmp > 0;
                    }
                }

                return false;
            }
        }
    private:
        static TBlockItem GetBlockItem(IBlockReader& reader, const TChunkedArrayIndex& arrayIndex, ui64 idx) {
            Y_DEBUG_ABORT_UNLESS(!arrayIndex.empty());
            if (arrayIndex.size() == 1) {
                return reader.GetItem(*arrayIndex.front().Data, idx);
            }

            auto it = LookupArrayDataItem(arrayIndex.data(), arrayIndex.size(), idx);
            return reader.GetItem(*it->Data, idx);
        }

        const std::vector<ui32>& KeyIndicies_;
        const std::vector<TChunkedArrayIndex> ArrayIndicies_;
        const TState& State_;
    };

    IComputationWideFlowNode *const Flow_;
    IComputationNode *const Count_;
    const TComputationNodePtrVector Directions_;
    const std::vector<ui32> KeyIndicies_;
    std::vector<TBlockType*> Columns_;
    const size_t WideFieldsIndex_;
};

template <bool Sort, bool HasCount>
IComputationNode* WrapTopOrSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    constexpr ui32 offset = HasCount ? 0 : 1;
    const ui32 inputsWithCount = callable.GetInputsCount() + offset;
    MKQL_ENSURE(inputsWithCount > 2U && !(inputsWithCount % 2U), "Expected more arguments.");

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");

    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    IComputationNode* count = nullptr;
    if constexpr (HasCount) {
        const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
        MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");
        count = LocateNode(ctx.NodeLocator, callable, 1);
    }

    TComputationNodePtrVector directions;
    std::vector<ui32> keyIndicies;
    for (ui32 i = 2; i < inputsWithCount; i += 2) {
        ui32 keyIndex = AS_VALUE(TDataLiteral, callable.GetInput(i - offset))->AsValue().Get<ui32>();
        MKQL_ENSURE(keyIndex + 1 < wideComponents.size(), "Wrong key index");
        keyIndicies.push_back(keyIndex);
        directions.push_back(LocateNode(ctx.NodeLocator, callable, i + 1 - offset));
    }

    return new TTopOrSortBlocksWrapper<Sort, HasCount>(ctx.Mutables, wideFlow, wideComponents, count, std::move(directions), std::move(keyIndicies));
}

} //namespace

IComputationNode* WrapWideTopBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<false, true>(callable, ctx);
}

IComputationNode* WrapWideTopSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, true>(callable, ctx);
}

IComputationNode* WrapWideSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, false>(callable, ctx);
}

}
}
