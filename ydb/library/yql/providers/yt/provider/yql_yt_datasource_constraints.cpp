#include "yql_yt_provider_impl.h"
#include "yql_yt_table.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/ast/yql_constraint.h>

#include <util/generic/variant.h>


namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSourceConstraintTransformer : public TVisitorTransformerBase {
public:
    TYtDataSourceConstraintTransformer(TYtState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        AddHandler({TYtTable::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleTable));
        AddHandler({TYtPath::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandlePath));
        AddHandler({TYtSection::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleSection));
        AddHandler({TYtReadTable::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleReadTable));
        AddHandler({TYtTableContent::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleTableContent));

        AddHandler({TYtIsKeySwitch::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYqlRowSpec::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TEpoch::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtMeta::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtStat::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtRow::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtRowRange::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtKeyExact::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtKeyRange::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtReadTableScheme::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtLength::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtConfigure::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtTablePath::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtTableRecord::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtTableIndex::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtRowNumber::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
        AddHandler({TYtTableName::CallableName()}, Hndl(&TYtDataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleTable(TExprBase input, TExprContext& ctx) {
        const auto table = input.Cast<TYtTable>();
        const auto epoch = TEpochInfo::Parse(table.Epoch().Ref());
        const auto tableName = TString{TYtTableInfo::GetTableLabel(table)};
        TYtTableDescription& tableDesc = State_->TablesData->GetModifTable(TString{table.Cluster().Value()}, tableName, epoch);
        if (epoch) {
            if (!tableDesc.ConstraintsReady) {
                if (State_->Types->EvaluationInProgress) {
                    ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                        << "Table  " << tableName.Quote() << " is used before commit"));
                    return TStatus::Error;
                }
                return TStatus(TStatus::Repeat, true);
            }
            input.Ptr()->SetConstraints(tableDesc.Constraints);
        } else if (!table.RowSpec().Maybe<TCoVoid>()) {
            TYqlRowSpecInfo rowSpec(table.RowSpec(), false);
            auto set = rowSpec.GetSomeConstraints(State_->Configuration->ApplyStoredConstraints.Get().GetOrElse(DEFAULT_APPLY_STORED_CONSTRAINTS), ctx);

            if (!set.GetConstraint<TSortedConstraintNode>()) {
                if (const auto sorted = rowSpec.MakeSortConstraint(ctx))
                    set.AddConstraint(sorted);
            }

            if (!set.GetConstraint<TDistinctConstraintNode>()) {
                if (const auto distinct = rowSpec.MakeDistinctConstraint(ctx)) {
                    set.AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(TUniqueConstraintNode::TContentType(distinct->GetContent())));
                    set.AddConstraint(distinct);
                }
            }
            input.Ptr()->SetConstraints(set);
            if (!tableDesc.ConstraintsReady) {
                tableDesc.Constraints = set;
                tableDesc.SetConstraintsReady();
            }
        }
        if (!table.Stat().Maybe<TCoVoid>()) {
            if (TYtTableStatInfo(table.Stat()).IsEmpty() && !TYtTableMetaInfo(table.Meta()).IsDynamic) {
                input.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            }
        }
        return TStatus::Ok;
    }

    TStatus HandlePath(TExprBase input, TExprContext& ctx) {
        auto path = input.Cast<TYtPath>();
        const auto outItemType = path.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        const auto filter = [outItemType](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && outItemType->FindItem(path.front()); };

        if (const auto sort = path.Table().Ref().GetConstraint<TSortedConstraintNode>()) {
            if (const auto filtered = sort->FilterFields(ctx, filter)) {
                path.Ptr()->AddConstraint(filtered);
            }
        }

        if (const auto uniq = path.Table().Ref().GetConstraint<TUniqueConstraintNode>()) {
            if (const auto filtered = uniq->FilterFields(ctx, filter)) {
                path.Ptr()->AddConstraint(filtered);
            }
        }

        if (const auto dist = path.Table().Ref().GetConstraint<TDistinctConstraintNode>()) {
            if (const auto filtered = dist->FilterFields(ctx, filter)) {
                path.Ptr()->AddConstraint(filtered);
            }
        }

        if (auto empty = path.Table().Ref().GetConstraint<TEmptyConstraintNode>()) {
            path.Ptr()->AddConstraint(empty);
        } else if (path.Ranges().Maybe<TExprList>()) {
            auto rangeInfo = TYtRangesInfo(path.Ranges());
            if (rangeInfo.IsEmpty()) {
                path.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            }
        }

        return TStatus::Ok;
    }

    TStatus HandleSection(TExprBase input, TExprContext& ctx) {
        auto section = input.Cast<TYtSection>();
        if (section.Paths().Size() == 1) {
            auto path = section.Paths().Item(0);
            if (!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered)) {
                if (auto sorted = path.Ref().GetConstraint<TSortedConstraintNode>()) {
                    input.Ptr()->AddConstraint(sorted);
                }
            }
            if (!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::NonUnique)) {
                if (const auto unique = path.Ref().GetConstraint<TUniqueConstraintNode>()) {
                    input.Ptr()->AddConstraint(unique);
                }
                if (const auto distinct = path.Ref().GetConstraint<TDistinctConstraintNode>()) {
                    input.Ptr()->AddConstraint(distinct);
                }
            }
        }

        TVector<const TConstraintSet*> allConstraints;
        for (const auto& path : section.Paths()) {
            allConstraints.push_back(&path.Ref().GetConstraintSet());
        }

        if (auto empty = TEmptyConstraintNode::MakeCommon(allConstraints, ctx)) {
            input.Ptr()->AddConstraint(empty);
        }

        return TStatus::Ok;
    }

    TStatus HandleReadTable(TExprBase input, TExprContext& ctx) {
        auto read = input.Cast<TYtReadTable>();
        if (read.Input().Size() == 1) {
            auto section = read.Input().Item(0);
            input.Ptr()->CopyConstraints(section.Ref());
        } else {
            TMultiConstraintNode::TMapType multiItems;
            for (ui32 index = 0; index < read.Input().Size(); ++index) {
                auto section = read.Input().Item(index);
                if (!section.Ref().GetConstraint<TEmptyConstraintNode>()) {
                    multiItems.push_back(std::make_pair(index, section.Ref().GetConstraintSet()));
                }
            }
            input.Ptr()->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
        }
        return TStatus::Ok;
    }

    TStatus HandleTableContent(TExprBase input, TExprContext& /*ctx*/) {
        TYtTableContent tableContent = input.Cast<TYtTableContent>();
        input.Ptr()->CopyConstraints(tableContent.Input().Ref());
        return TStatus::Ok;
    }

    TStatus HandleDefault(TExprBase input, TExprContext& /*ctx*/) {
        return UpdateAllChildLambdasConstraints(input.Ref());
    }

private:
    const TYtState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYtDataSourceConstraintTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSourceConstraintTransformer(state));
}

}
