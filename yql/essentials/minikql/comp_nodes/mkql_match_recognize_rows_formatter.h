#pragma once

#include "mkql_match_recognize_nfa.h"

#include <yql/essentials/core/sql_types/match_recognize.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

struct TOutputColumnEntry {
    size_t Index;
    NYql::NMatchRecognize::EOutputColumnSource SourceType;
};
using TOutputColumnOrder = std::vector<TOutputColumnEntry, TMKQLAllocator<TOutputColumnEntry>>;

class IRowsFormatter {
public:
    struct TState {
        std::unique_ptr<TContainerCacheOnContext> Cache;
        TOutputColumnOrder OutputColumnOrder;
        TComputationNodePtrVector Measures;
        NYql::NMatchRecognize::ERowsPerMatch RowsPerMatch;

        TState(
            const TComputationNodeFactoryContext& ctx,
            TOutputColumnOrder outputColumnOrder,
            TComputationNodePtrVector measures,
            NYql::NMatchRecognize::ERowsPerMatch rowsPerMatch)
        : Cache(std::make_unique<TContainerCacheOnContext>(ctx.Mutables))
        , OutputColumnOrder(std::move(outputColumnOrder))
        , Measures(std::move(measures))
        , RowsPerMatch(rowsPerMatch)
        {}
    };

    explicit IRowsFormatter(const TState& state);
    virtual ~IRowsFormatter() = default;

    virtual NUdf::TUnboxedValue GetFirstMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph,
        const TNfa::TMatch& match) = 0;

    virtual NUdf::TUnboxedValue GetOtherMatchRow(
        TComputationContext& ctx,
        const TSparseList& rows,
        const NUdf::TUnboxedValue& partitionKey,
        const TNfaTransitionGraph& graph) = 0;

    virtual void Load(TMrInputSerializer& serializer) = 0;
    virtual void Save(TMrOutputSerializer& serializer) const = 0;

    static TOutputColumnOrder GetOutputColumnOrder(TRuntimeNode outputColumnOrder);

    static std::unique_ptr<IRowsFormatter> Create(const TState& state);

protected:
    NUdf::TUnboxedValue DoGetMatchRow(TComputationContext& ctx, const TSparseList& rows, const NUdf::TUnboxedValue& partitionKey, const TNfaTransitionGraph& graph);

    inline void Clear() {
        Match_ = {};
        CurrentRowIndex_ = Max();
    }

    const TState& State_;
    TNfa::TMatch Match_ {};
    size_t CurrentRowIndex_ = Max();
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
