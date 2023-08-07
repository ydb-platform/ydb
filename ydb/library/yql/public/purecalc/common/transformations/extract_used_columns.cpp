#include "extract_used_columns.h"

#include <ydb/library/yql/public/purecalc/common/inspect_input.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TUsedColumnsExtractor : public TSyncTransformerBase {
    private:
        TVector<THashSet<TString>>* const Destination_;
        const TVector<THashSet<TString>>& AllColumns_;
        TString NodeName_;

        bool CalculatedUsedFields_ = false;

    public:
        TUsedColumnsExtractor(
            TVector<THashSet<TString>>* destination,
            const TVector<THashSet<TString>>& allColumns,
            TString nodeName
        )
            : Destination_(destination)
            , AllColumns_(allColumns)
            , NodeName_(std::move(nodeName))
        {
        }

        TUsedColumnsExtractor(TVector<THashSet<TString>>*, TVector<THashSet<TString>>&&, TString) = delete;

    public:
        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;

            if (CalculatedUsedFields_) {
                return IGraphTransformer::TStatus::Ok;
            }

            bool hasError = false;

            *Destination_ = AllColumns_;

            VisitExpr(input, [&](const TExprNode::TPtr& inputExpr) {
                NNodes::TExprBase node(inputExpr);
                if (auto maybeExtract = node.Maybe<NNodes::TCoExtractMembers>()) {
                    auto extract = maybeExtract.Cast();
                    const auto& arg = extract.Input().Ref();
                    if (arg.IsCallable(NodeName_)) {
                        ui32 inputIndex;
                        if (!TryFetchInputIndexFromSelf(arg, ctx, AllColumns_.size(), inputIndex)) {
                            hasError = true;
                            return false;
                        }

                        YQL_ENSURE(inputIndex < AllColumns_.size());

                        auto& destinationColumnsSet = (*Destination_)[inputIndex];
                        const auto& allColumnsSet = AllColumns_[inputIndex];

                        destinationColumnsSet.clear();
                        for (const auto& columnAtom : extract.Members()) {
                            TString name = TString(columnAtom.Value());
                            YQL_ENSURE(allColumnsSet.contains(name), "unexpected column in the input struct");
                            destinationColumnsSet.insert(name);
                        }
                    }
                }

                return true;
            });

            if (hasError) {
                return IGraphTransformer::TStatus::Error;
            }

            CalculatedUsedFields_ = true;

            return IGraphTransformer::TStatus::Ok;
        }

        void Rewind() final {
            CalculatedUsedFields_ = false;
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeUsedColumnsExtractor(
    TVector<THashSet<TString>>* destination,
    const TVector<THashSet<TString>>& allColumns,
    const TString& nodeName
) {
    return new TUsedColumnsExtractor(destination, allColumns, nodeName);
}
