#include "mkql_block_top.h"
#include "mkql_block_impl.h"
#include "mkql_block_reader.h"
#include "mkql_block_builder.h"

#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TTopBlocksWrapper : public TStatefulWideFlowBlockComputationNode<TTopBlocksWrapper> {
public:
    TTopBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TTupleType* tupleType, IComputationNode* count,
        TVector<IComputationNode*>&& directions, TVector<ui32>&& keyIndicies, bool sort)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, tupleType->GetElementsCount())
        , Flow_(flow)
        , Count_(count)
        , Directions_(std::move(directions))
        , KeyIndicies_(std::move(keyIndicies))
        , Sort_(sort)
    {
        for (ui32 i = 0; i < tupleType->GetElementsCount() - 1; ++i) {
            Columns_.push_back(AS_TYPE(TBlockType, tupleType->GetElementType(i)));
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY(output[Columns_.size()]);

        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        if (!s.PreparedCountAndDirections_) {
            s.Count_ = Count_->GetValue(ctx).Get<ui64>();
            for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                s.Directions_[k] = Directions_[k]->GetValue(ctx).Get<bool>();
            }

            s.PreparedCountAndDirections_ = true;
        }

        if (!s.Count_) {
            s.IsFinished_ = true;
            return EFetchResult::Finish;
        }

        if (!s.PreparedBuilders_) {
            s.AllocateBuilders(Columns_, ctx);
            s.PreparedBuilders_ = true;
        }

        for (;;) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result == EFetchResult::Yield) {
                return result;
            } else if (result == EFetchResult::One) {
                if (!s.ScalarsFilled_) {
                    for (ui32 i = 0; i < Columns_.size(); ++i) {
                        if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                            s.ScalarValues_[i] = s.Values_[i];
                        }
                    }

                    s.ScalarsFilled_ = true;
                }

                ui64 blockLen = TArrowBlock::From(s.Values_.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

                // shrink input block
                TMaybe<TVector<ui64>> blockIndicies;
                if (blockLen > s.Count_) {
                    blockIndicies.ConstructInPlace();
                    blockIndicies->reserve(blockLen);
                    for (ui64 row = 0; row < blockLen; ++row) {
                        blockIndicies->emplace_back(row);
                    }

                    TBlockLess cmp(KeyIndicies_, s, s.Values_);
                    std::nth_element(blockIndicies->begin(), blockIndicies->begin() + s.Count_, blockIndicies->end(), cmp);
                }
                
                // copy all to builders
                s.AddTop(Columns_, blockIndicies, blockLen);
                if (s.BuilderLength_ + s.Count_ > s.BuilderMaxLength_) {
                    s.CompressBuilders(false, Columns_, KeyIndicies_, ctx);
                }

            } else {
                s.IsFinished_ = true;
                if (!s.BuilderLength_) {
                    return EFetchResult::Finish;
                }

                if (s.BuilderLength_ > s.Count_ || Sort_) {
                    s.CompressBuilders(Sort_, Columns_, KeyIndicies_, ctx);
                }

                s.FillOutput(Columns_, output, ctx);
                return EFetchResult::One;
            }
        }
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            DependsOn(flow, Count_);

            for (auto dir : Directions_) {
                DependsOn(flow, dir);
            }
        }
    }

    class TState : public TComputationValue<TState> {
    public:
        bool IsFinished_ = false;
        bool PreparedCountAndDirections_ = false;
        ui64 Count_ = 0;
        TVector<bool> Directions_;
        bool ScalarsFilled_ = false;
        TVector<NUdf::TUnboxedValue> ScalarValues_;
        TVector<std::unique_ptr<IBlockReader>> LeftReaders_;
        TVector<std::unique_ptr<IBlockReader>> RightReaders_;
        bool PreparedBuilders_ = false;
        TVector<std::unique_ptr<IArrayBuilder>> Builders_;
        ui64 BuilderMaxLength_ = 0;
        ui64 BuilderLength_ = 0;
        TVector<std::unique_ptr<NUdf::IBlockItemComparator>> Comparators_; // by key columns only

        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;

        TVector<NUdf::TUnboxedValue> TmpValues_;

        TState(TMemoryUsageInfo* memInfo, const TVector<ui32>& keyIndicies, const TVector<TBlockType*>& columns)
            : TComputationValue(memInfo)
        {
            Directions_.resize(keyIndicies.size());
            LeftReaders_.resize(columns.size());
            RightReaders_.resize(columns.size());
            Builders_.resize(columns.size());
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                LeftReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
                RightReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
            }

            Values_.resize(columns.size() + 1);
            ValuePointers_.resize(columns.size() + 1);
            for (ui32 i = 0; i <= columns.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            TmpValues_.resize(columns.size());

            Comparators_.resize(keyIndicies.size());
            for (ui32 k = 0; k < keyIndicies.size(); ++k) {
                Comparators_[k] = NUdf::MakeBlockItemComparator(TTypeInfoHelper(), columns[keyIndicies[k]]->GetItemType());
            }
        }

        ui64 GetStorageLength() const {
            Y_VERIFY(PreparedCountAndDirections_);
            return 2 * Count_;
        }

        void AllocateBuilders(const TVector<TBlockType*>& columns, TComputationContext& ctx) {
            BuilderMaxLength_ = GetStorageLength();
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                BuilderMaxLength_ = Max(BuilderMaxLength_, CalcBlockLen(CalcMaxBlockItemSize(columns[i]->GetItemType())));
            };

            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), columns[i]->GetItemType(), ctx.ArrowMemoryPool, BuilderMaxLength_);
            }
        }

        void CompressBuilders(bool sort, const TVector<TBlockType*>& columns, const TVector<ui32>& keyIndicies, TComputationContext& ctx) {
            Y_VERIFY(ScalarsFilled_);
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    TmpValues_[i] = ScalarValues_[i];
                } else {
                    TmpValues_[i] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(false)));
                }
            }

            TVector<ui64> blockIndicies;
            blockIndicies.reserve(BuilderLength_);
            for (ui64 row = 0; row < BuilderLength_; ++row) {
                blockIndicies.push_back(row);
            }

            ui64 blockLen = Min(BuilderLength_, Count_);
            TBlockLess cmp(keyIndicies, *this, TmpValues_);
            if (BuilderLength_ <= Count_) {
                if (sort) {
                    std::sort(blockIndicies.begin(), blockIndicies.end(), cmp);
                }
            } else {
                if (sort) {
                    std::partial_sort(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                } else {
                    std::nth_element(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                }
            }

            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                const auto& datum = TArrowBlock::From(TmpValues_[i]).GetDatum();
                const auto& array = *datum.array();
                for (ui64 j = 0; j < blockLen; ++j) {
                    Builders_[i]->Add(LeftReaders_[i]->GetItem(array, blockIndicies[j]));
                }
            }

            BuilderLength_ = blockLen;

            for (ui32 i = 0; i < columns.size(); ++i) {
                TmpValues_[i] = {};
            }
        }

        void FillOutput(const TVector<TBlockType*>& columns, NUdf::TUnboxedValue*const* output, TComputationContext& ctx) {
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (!output[i]) {
                    continue;
                }

                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    *output[i] = ScalarValues_[i];
                } else {
                    *output[i] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(true)));
                }
            }

            *output[columns.size()] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BuilderLength_)));
        }

        void AddTop(const TVector<TBlockType*>& columns, const TMaybe<TVector<ui64>>& blockIndicies, ui64 blockLen) {
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                const auto& datum = TArrowBlock::From(Values_[i]).GetDatum();
                const auto& array = *datum.array();
                if (blockIndicies) {
                    for (ui64 j = 0; j < Count_; ++j) {
                        Builders_[i]->Add(LeftReaders_[i]->GetItem(array, (*blockIndicies)[j]));
                    }
                } else {
                    for (ui64 row = 0; row < blockLen; ++row) {
                        Builders_[i]->Add(LeftReaders_[i]->GetItem(array, row));
                    }
                }
            }

            if (blockIndicies) {
                BuilderLength_ += Count_;
            } else {
                BuilderLength_ += blockLen;
            }
        }
    };

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(KeyIndicies_, Columns_);
        }

        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    class TBlockLess {
    public:
        TBlockLess(const TVector<ui32>& keyIndicies, const TState& state, const TVector<NUdf::TUnboxedValue>& values)
            : KeyIndicies_(keyIndicies)
            , State_(state)
            , Values_(values)
        {}

        bool operator()(ui64 lhs, ui64 rhs) const {
            if (KeyIndicies_.size() == 1) {
                auto i = KeyIndicies_[0];
                const auto& datum = TArrowBlock::From(Values_[i]).GetDatum();
                if (datum.is_scalar()) {
                    return false;
                }

                const auto& array = *datum.array();
                auto leftItem = State_.LeftReaders_[i]->GetItem(array, lhs);
                auto rightItem = State_.RightReaders_[i]->GetItem(array, rhs);
                if (State_.Directions_[0]) {
                    return State_.Comparators_[0]->Less(leftItem, rightItem);
                } else {
                    return State_.Comparators_[0]->Greater(leftItem, rightItem);
                }
            } else {
                for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                    auto i = KeyIndicies_[k];
                    const auto& datum = TArrowBlock::From(Values_[i]).GetDatum();
                    if (datum.is_scalar()) {
                        continue;
                    }

                    const auto& array = *datum.array();
                    auto leftItem = State_.LeftReaders_[i]->GetItem(array, lhs);
                    auto rightItem = State_.RightReaders_[i]->GetItem(array, rhs);
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
        const TVector<ui32>& KeyIndicies_;
        const TState& State_;
        const TVector<NUdf::TUnboxedValue>& Values_;
    };

    IComputationWideFlowNode* Flow_;
    IComputationNode* Count_;
    const TVector<IComputationNode*> Directions_;
    const TVector<ui32> KeyIndicies_;
    const bool Sort_;
    TVector<TBlockType*> Columns_;
};

IComputationNode* WrapTop(TCallable& callable, const TComputationNodeFactoryContext& ctx, bool sort) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U && !(callable.GetInputsCount() % 2U), "Expected more arguments.");

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    MKQL_ENSURE(tupleType->GetElementsCount() > 0, "Expected at least one column");

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    TVector<IComputationNode*> directions;
    TVector<ui32> keyIndicies;
    for (ui32 i = 2; i < callable.GetInputsCount(); i += 2) {
        ui32 keyIndex = AS_VALUE(TDataLiteral, callable.GetInput(i))->AsValue().Get<ui32>();
        MKQL_ENSURE(keyIndex + 1 < tupleType->GetElementsCount(), "Wrong key index");
        keyIndicies.push_back(keyIndex);
        directions.push_back(LocateNode(ctx.NodeLocator, callable, i + 1));
    }

    return new TTopBlocksWrapper(ctx.Mutables, wideFlow, tupleType, count, std::move(directions), std::move(keyIndicies), sort);
}

} //namespace

IComputationNode* WrapWideTopBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTop(callable, ctx, false);
}

IComputationNode* WrapWideTopSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTop(callable, ctx, true);
}


}
}
