#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <optional>
#include <utility>

namespace NKikimr::NMiniKQL {

struct TJoinFilters {
    TSides<TComputationExternalNodePtrVector> Args;
    TSides<IComputationNode*> OneSide{};
    IComputationNode* BothSides = nullptr;

    explicit operator bool() const {
        return OneSide.Probe || OneSide.Build || BothSides;
    }

    void SwapSides() {
        std::swap(Args.Build, Args.Probe);
        std::swap(OneSide.Build, OneSide.Probe);
    }

    void RegisterDependencies(const std::function<void(IComputationNode*)>& dependsOn,
                              const std::function<void(IComputationExternalNode*)>& own) const {
        for (ESide side : EachSide) {
            for (IComputationExternalNode* arg : Args.SelectSide(side)) {
                own(arg);
            }
            if (IComputationNode* body = OneSide.SelectSide(side)) {
                dependsOn(body);
            }
        }
        if (BothSides) {
            dependsOn(BothSides);
        }
    }
};

inline constexpr ui32 JoinFilterInputs = 5;

inline TJoinFilters ParseJoinFilters(const TComputationNodeFactoryContext& ctx, TCallable& callable, ui32 firstInput) {
    if (callable.GetInputsCount() == firstInput) {
        return {};
    }
    MKQL_ENSURE(callable.GetInputsCount() == firstInput + JoinFilterInputs,
                "Expected " << firstInput << " or " << firstInput + JoinFilterInputs << " args, got "
                            << callable.GetInputsCount());

    const auto locateArgs = [&](ui32 input, TComputationExternalNodePtrVector& args) {
        const auto* tuple = AS_VALUE(TTupleLiteral, callable.GetInput(input));
        args.reserve(tuple->GetValuesCount());
        for (ui32 i = 0; i < tuple->GetValuesCount(); ++i) {
            auto* arg = dynamic_cast<IComputationExternalNode*>(
                LocateNode(ctx.NodeLocator, *tuple->GetValue(i).GetNode()));
            MKQL_ENSURE(arg, "Expected an external node as a join filter argument");
            args.push_back(arg);
        }
    };
    const auto locateBody = [&](ui32 input) -> IComputationNode* {
        const auto* tuple = AS_VALUE(TTupleLiteral, callable.GetInput(input));
        MKQL_ENSURE(tuple->GetValuesCount() <= 1, "Expected at most one join filter body per predicate");
        if (tuple->GetValuesCount() == 0) {
            return nullptr;
        }
        return LocateNode(ctx.NodeLocator, *tuple->GetValue(0).GetNode());
    };

    TJoinFilters filters;
    locateArgs(firstInput + 0, filters.Args.Probe);
    locateArgs(firstInput + 1, filters.Args.Build);
    filters.OneSide.Probe = locateBody(firstInput + 2);
    filters.OneSide.Build = locateBody(firstInput + 3);
    filters.BothSides = locateBody(firstInput + 4);
    return filters;
}

class TPackedTuplePairFilter {
  public:
    static std::optional<TPackedTuplePairFilter> TryCreate(TComputationContext& ctx, const TJoinFilters& filters,
                                                           const TSides<TVector<TType*>>& columnTypes,
                                                           const TSides<TVector<ui32>>& keyColumns,
                                                           const TSides<TVector<int>>& columnPermutation) {
        if (!filters) {
            return std::nullopt;
        }
        return TPackedTuplePairFilter(ctx, filters, columnTypes, keyColumns, columnPermutation);
    }

    TPackedTuplePairFilter(TComputationContext& ctx, const TJoinFilters& filters,
                           const TSides<TVector<TType*>>& columnTypes, const TSides<TVector<ui32>>& keyColumns,
                           const TSides<TVector<int>>& columnPermutation)
        : Ctx_(&ctx)
        , Filters_(filters)
    {
        TTypeInfoHelper helper;
        for (ESide side : EachSide) {
            const auto& types = columnTypes.SelectSide(side);
            const size_t args = Filters_.Args.SelectSide(side).size();
            if (args == 0) {
                MKQL_ENSURE(!Filters_.OneSide.SelectSide(side),
                            "A join filter of the " << AsString(side) << " side has no arguments to bind a row to");
                continue;
            }
            MKQL_ENSURE(args == types.size(), "Join filter takes " << args << " arguments but the "
                                                                  << AsString(side) << " side has " << types.size()
                                                                  << " columns");
            Decoders_.SelectSide(side).emplace(helper, types, keyColumns.SelectSide(side),
                                               columnPermutation.SelectSide(side), ctx.HolderFactory);
        }
    }

    void StartProbeRow(TSingleTuple probeRow) {
        ProbeRow_ = probeRow;
        ProbeChecked_ = false;
    }

    bool PairPasses(TSingleTuple buildRow) {
        if (!ProbeChecked_) {
            ProbeChecked_ = true;
            ProbePasses_ = BindAndCheck(ESide::Probe, ProbeRow_);
        }
        if (!ProbePasses_) {
            return false;
        }
        if (!BindAndCheck(ESide::Build, buildRow)) {
            return false;
        }
        return !Filters_.BothSides || Eval(Filters_.BothSides);
    }

  private:
    // Turns a single packed tuple back into unboxed values in the user's original column order.
    class TRowDecoder {
      public:
        TRowDecoder(const NUdf::ITypeInfoHelper& helper, const TVector<TType*>& columnTypes,
                    const TVector<ui32>& keyColumns, const TVector<int>& columnPermutation,
                    const THolderFactory& holderFactory)
            : Converter_(MakeScalarLayoutConverter(helper, columnTypes,
                                                   MakeColumnRoles(columnTypes.size(), keyColumns), holderFactory))
            , Permutation_(columnPermutation)
            , Packed_(columnTypes.size())
            , Row_(Permutation_.empty() ? 0 : columnTypes.size())
        {}

        const NUdf::TUnboxedValue* Decode(TSingleTuple tuple) {
            OneTuple_.Reset();
            OneTuple_.AppendTuple(tuple, Converter_->GetTupleLayout());
            Converter_->Unpack(OneTuple_, 0, Packed_.data());
            if (Permutation_.empty()) {
                return Packed_.data();
            }
            for (size_t i = 0; i < Packed_.size(); ++i) {
                Row_[Permutation_[i]] = Packed_[i];
            }
            return Row_.data();
        }

      private:
        IScalarLayoutConverter::TPtr Converter_;
        const TVector<int> Permutation_;
        TPackResult OneTuple_;
        TVector<NUdf::TUnboxedValue> Packed_; // packed ("keys first") column order
        TVector<NUdf::TUnboxedValue> Row_;    // user column order, unused when Permutation_ is empty
    };

    bool BindAndCheck(ESide side, TSingleTuple row) {
        auto& decoder = Decoders_.SelectSide(side);
        if (!decoder) {
            return true;
        }
        const NUdf::TUnboxedValue* values = decoder->Decode(row);
        const auto& args = Filters_.Args.SelectSide(side);
        for (size_t i = 0; i < args.size(); ++i) {
            args[i]->SetValue(*Ctx_, NUdf::TUnboxedValue(values[i]));
        }
        IComputationNode* body = Filters_.OneSide.SelectSide(side);
        return !body || Eval(body);
    }

    bool Eval(IComputationNode* body) const {
        const NUdf::TUnboxedValue result = body->GetValue(*Ctx_);
        return result && result.Get<bool>();
    }

    TComputationContext* Ctx_;
    TJoinFilters Filters_;
    TSides<std::optional<TRowDecoder>> Decoders_;
    TSingleTuple ProbeRow_{};
    bool ProbeChecked_ = false;
    bool ProbePasses_ = false;
};

} // namespace NKikimr::NMiniKQL
