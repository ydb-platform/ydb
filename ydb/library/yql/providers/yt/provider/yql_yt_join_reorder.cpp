#include "yql_yt_join_impl.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/dq/opt/dq_opt_log.h>

namespace NYql {

namespace {

bool AreSimilarTrees(TYtJoinNode::TPtr node1, TYtJoinNode::TPtr node2) {
    if (node1 == node2) {
        return true;
    }
    if (node1 && !node2) {
        return false;
    }
    if (node2 && !node1) {
        return false;
    }
    if (node1->Scope != node2->Scope) {
        return false;
    }
    auto opLeft = dynamic_cast<TYtJoinNodeOp*>(node1.Get());
    auto opRight = dynamic_cast<TYtJoinNodeOp*>(node2.Get());
    if (opLeft && opRight) {
        return AreSimilarTrees(opLeft->Left, opRight->Left)
            && AreSimilarTrees(opLeft->Right, opRight->Right);
    } else if (!opLeft && !opRight) {
        return true;
    } else {
        return false;
    }
}

void DebugPrint(TYtJoinNode::TPtr node, TExprContext& ctx, int level) {
    auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get());
    auto printScope = [](const TVector<TString>& scope) -> TString {
        TStringBuilder b;
        for (auto& s : scope) {
            b << s << ",";
        }
        return b;
    };
    TString prefix;
    for (int i = 0; i < level; i++) {
        prefix += ' ';
    }
    if (op) {
        Cerr << prefix
            << "Op: "
            << "Type: " << NCommon::ExprToPrettyString(ctx, *op->JoinKind)
            << "Left: " << NCommon::ExprToPrettyString(ctx, *op->LeftLabel)
            << "Right: " << NCommon::ExprToPrettyString(ctx, *op->RightLabel)
            << "Scope: " << printScope(op->Scope) << "\n"
            << "\n";
        DebugPrint(op->Left, ctx, level+1);
        DebugPrint(op->Right, ctx, level+1);
    } else {
        auto* leaf = dynamic_cast<TYtJoinNodeLeaf*>(node.Get());
        Cerr << prefix
            << "Leaf: "
            << "Section: " << NCommon::ExprToPrettyString(ctx, *leaf->Section.Ptr())
            << "Label: " << NCommon::ExprToPrettyString(ctx, *leaf->Label)
            << "Scope: " << printScope(leaf->Scope) << "\n"
            << "\n";
    }
}

class TJoinReorderer {
public:
    TJoinReorderer(
        TYtJoinNodeOp::TPtr op,
        const TYtState::TPtr& state,
        TExprContext& ctx,
        ECostBasedOptimizer optimizerType,
        bool debug = false)
        : Root(op)
        , State(state)
        , Ctx(ctx)
        , OptimizerType(optimizerType)
        , Debug(debug)
    {
        Y_UNUSED(State);

        if (Debug) {
            DebugPrint(Root, Ctx, 0);
        }
    }

    TYtJoinNodeOp::TPtr Do() {
        CollectRels(Root);
        if (!CollectOps(Root)) {
            return Root;
        }

        IOptimizer::TInput input;
        input.EqClasses = std::move(EqClasses);
        input.Left = std::move(Left);
        input.Right = std::move(Right);
        input.Rels = std::move(Rels);
        input.Normalize();
        YQL_CLOG(INFO, ProviderYt) << "Input: " << input.ToString();

        std::function<void(const TString& str)> log;

        log = [](const TString& str) {
            YQL_CLOG(INFO, ProviderYt) << str;
        };

        std::unique_ptr<IOptimizer> opt;

        switch (OptimizerType) {
        case ECostBasedOptimizer::PG:
            opt = std::unique_ptr<IOptimizer>(MakePgOptimizer(input, log));
            break;
        case ECostBasedOptimizer::Native:
            opt = std::unique_ptr<IOptimizer>(NDq::MakeNativeOptimizer(input, log));
            break;
        default:
            YQL_CLOG(ERROR, ProviderYt) << "Unknown optimizer type";
            return Root;
            break;
        }

        try {
            Result = opt->JoinSearch();
        } catch (...) {
            YQL_CLOG(ERROR, ProviderYt) << "Cannot do join search " << CurrentExceptionMessage();
            return Root;
        }

        YQL_CLOG(INFO, ProviderYt) << "Result: " << Result.ToString();

        TVector<TString> scope;
        TYtJoinNodeOp::TPtr res = dynamic_cast<TYtJoinNodeOp*>(Convert(0, scope).Get());

        YQL_ENSURE(res);
        DebugPrint(res, Ctx, 0);

        if (Debug) {
            DebugPrint(res, Ctx, 0);
        }

        return res;
    }

private:
    int GetVarId(int relId, TStringBuf column) {
        int varId = 0;
        auto maybeVarId = VarIds[relId-1].find(column);
        if (maybeVarId != VarIds[relId-1].end()) {
            varId = maybeVarId->second;
        } else {
            varId = Rels[relId - 1].TargetVars.size() + 1;
            VarIds[relId - 1][column] = varId;
            Rels[relId - 1].TargetVars.emplace_back();
            Var2TableCol[relId - 1].emplace_back();
        }
        return varId;
    }

    void ExtractVars(auto& vars, TExprNode::TPtr labels) {
        for (ui32 i = 0; i < labels->ChildrenSize(); i += 2) {
            auto table = labels->Child(i)->Content();
            auto column = labels->Child(i + 1)->Content();

            const auto& relIds = Table2RelIds[table];
            YQL_ENSURE(!relIds.empty());

            for (int relId : relIds) {
                int varId = GetVarId(relId, column);

                vars.emplace_back(std::make_tuple(relId, varId, table, column));
            }
        }
    };

    std::vector<TStringBuf> GetTables(TExprNode::TPtr label)
    {
        if (label->ChildrenSize() == 0) {
            return {label->Content()};
        } else {
            std::vector<TStringBuf> tables;
            tables.reserve(label->ChildrenSize());
            for (ui32 i = 0; i < label->ChildrenSize(); i++) {
                tables.emplace_back(label->Child(i)->Content());
            }
            return tables;
        }
    }

    void OnLeaf(TYtJoinNodeLeaf* leaf) {
        int relId = Rels.size() + 1;
        Rels.emplace_back(IOptimizer::TRel{});
        Var2TableCol.emplace_back();
        // rel -> varIds
        VarIds.emplace_back(THashMap<TStringBuf, int>{});
        // rel -> tables
        RelTables.emplace_back(std::vector<TStringBuf>{});
        for (const auto& table : GetTables(leaf->Label)) {
            RelTables.back().emplace_back(table);
            Table2RelIds[table].emplace_back(relId);
        }
        auto& rel = Rels[relId - 1];

        TYtSection section{leaf->Section};
        if (Y_UNLIKELY(!section.Settings().Empty()) && Y_UNLIKELY(section.Settings().Item(0).Name() == "Test")) {
            // ut
            for (const auto& setting : section.Settings()) {
                if (setting.Name() == "Rows") {
                    rel.Rows += FromString<ui64>(setting.Value().Ref().Content());
                } else if (setting.Name() == "Size") {
                    rel.TotalCost += FromString<ui64>(setting.Value().Ref().Content());
                }
            }
        } else {
            for (auto path: section.Paths()) {
                auto stat = TYtTableBaseInfo::GetStat(path.Table());
                rel.TotalCost += stat->DataSize;
                rel.Rows += stat->RecordsCount;
            }
            if (!(rel.Rows > 0)) {
                YQL_CLOG(INFO, ProviderYt) << "Cannot read stats from: " << NCommon::ExprToPrettyString(Ctx, *section.Ptr());
            }
        }

        int leafIndex = relId - 1;
        if (leafIndex >= static_cast<int>(Leafs.size())) {
            Leafs.resize(leafIndex + 1);
        }
        Leafs[leafIndex] = leaf;
    };

    IOptimizer::TEq MakeEqClass(const auto& vars) {
        IOptimizer::TEq eqClass;

        for (auto& [relId, varId, table, column] : vars) {
            eqClass.Vars.emplace_back(std::make_tuple(relId, varId));
            Var2TableCol[relId - 1][varId - 1] = std::make_tuple(table, column);
        }

        return eqClass;
    }

    void MakeEqClasses(std::vector<IOptimizer::TEq>& res, const auto& leftVars, const auto& rightVars) {
        for (int i = 0; i < (int)leftVars.size(); i++) {
            auto& [lrelId, lvarId, ltable, lcolumn] = leftVars[i];
            auto& [rrelId, rvarId, rtable, rcolumn] = rightVars[i];

            IOptimizer::TEq eqClass; eqClass.Vars.reserve(2);
            eqClass.Vars.emplace_back(std::make_tuple(lrelId, lvarId));
            eqClass.Vars.emplace_back(std::make_tuple(rrelId, rvarId));

            Var2TableCol[lrelId - 1][lvarId - 1] = std::make_tuple(ltable, lcolumn);
            Var2TableCol[rrelId - 1][rvarId - 1] = std::make_tuple(rtable, rcolumn);

            res.emplace_back(std::move(eqClass));
        }
    }

    bool OnOp(TYtJoinNodeOp* op) {
#define CHECK(A, B) \
        if (Y_UNLIKELY(!(A))) { \
            TIssues issues; \
            issues.AddIssue(TIssue(B).SetCode(0, NYql::TSeverityIds::S_INFO)); \
            Ctx.IssueManager.AddIssues(issues); \
            return false; \
        }

        CHECK(!op->Output, "Non empty output");
        CHECK(op->StarOptions.empty(), "Non empty StarOptions");

        const auto& joinKind = op->JoinKind->Content();

        if (joinKind == "Inner") {
            // relId, varId, table, column
            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> leftVars;
            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> rightVars;
 
            ExtractVars(leftVars, op->LeftLabel);
            ExtractVars(rightVars, op->RightLabel);

            CHECK(leftVars.size() == rightVars.size(), "Left and right labels must have the same size");

            MakeEqClasses(EqClasses, leftVars, rightVars);
        } else if (joinKind == "Left" || joinKind == "Right") {
            CHECK(op->LeftLabel->ChildrenSize() == 2, "Only 1 var per join supported");
            CHECK(op->RightLabel->ChildrenSize() == 2, "Only 1 var per join supported");

            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> leftVars, rightVars;
            ExtractVars(leftVars, op->LeftLabel);
            ExtractVars(rightVars, op->RightLabel);

            IOptimizer::TEq leftEqClass = MakeEqClass(leftVars);
            IOptimizer::TEq rightEqClass = MakeEqClass(rightVars);
            IOptimizer::TEq eqClass = leftEqClass;
            eqClass.Vars.insert(eqClass.Vars.end(), rightEqClass.Vars.begin(), rightEqClass.Vars.end());

            CHECK(eqClass.Vars.size() == 2, "Only a=b left|right join supported yet");

            EqClasses.emplace_back(std::move(leftEqClass));
            EqClasses.emplace_back(std::move(rightEqClass));
            if (joinKind == "Left") {
                Left.emplace_back(eqClass);
            } else {
                Right.emplace_back(eqClass);
            }
        } else {
            CHECK(false, "Unsupported join type");
        }

#undef CHECK
        return true;
    }

    bool CollectOps(TYtJoinNode::TPtr node)
    {
        if (auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get())) {
            return OnOp(op)
                && CollectOps(op->Left)
                && CollectOps(op->Right);
        }
        return true;
    }

    void CollectRels(TYtJoinNode::TPtr node)
    {
        if (auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get())) {
            CollectRels(op->Left);
            CollectRels(op->Right);
        } else if (auto* leaf = dynamic_cast<TYtJoinNodeLeaf*>(node.Get())) {
            OnLeaf(leaf);
        }
    }

    TExprNode::TPtr MakeLabel(const std::vector<IOptimizer::TVarId>& vars) const {
        TVector<TExprNodePtr> label; label.reserve(vars.size() * 2);
 
        for (auto [relId, varId] : vars) {
            auto [table, column] = Var2TableCol[relId - 1][varId - 1];

            label.emplace_back(Ctx.NewAtom(Root->JoinKind->Pos(), table));
            label.emplace_back(Ctx.NewAtom(Root->JoinKind->Pos(), column));
        }

        return Build<TCoAtomList>(Ctx, Root->JoinKind->Pos())
                .Add(label)
                .Done()
            .Ptr();
    }

    TYtJoinNode::TPtr Convert(int nodeId, TVector<TString>& scope) const
    {
        const IOptimizer::TJoinNode* node = &Result.Nodes[nodeId];
        if (node->Outer == -1 && node->Inner == -1) {
            YQL_ENSURE(node->Rels.size() == 1);
            auto leaf = Leafs[node->Rels[0]-1];
            YQL_ENSURE(leaf);
            YQL_ENSURE(!leaf->Scope.empty());
            scope.insert(scope.end(), leaf->Scope.begin(), leaf->Scope.end());
            return leaf;
        } else if (node->Outer != -1 && node->Inner != -1) {
            auto ret = MakeIntrusive<TYtJoinNodeOp>();
            TString joinKind;
            switch (node->Mode) {
            case IOptimizer::EJoinType::Inner:
                joinKind = "Inner";
                break;
            case IOptimizer::EJoinType::Left:
                joinKind = "Left";
                break;
            case IOptimizer::EJoinType::Right:
                joinKind = "Right";
                break;
            default:
                YQL_ENSURE(false, "Unsupported join type");
                break;
            }
            ret->JoinKind = Ctx.NewAtom(Root->JoinKind->Pos(), joinKind);
            ret->LeftLabel = MakeLabel(node->LeftVars);
            ret->RightLabel = MakeLabel(node->RightVars);
            int index = scope.size();
            ret->Left = Convert(node->Outer, scope);
            ret->Right = Convert(node->Inner, scope);
            ret->Scope.insert(ret->Scope.end(), scope.begin() + index, scope.end());
            return ret;
        } else {
            YQL_ENSURE(false, "Wrong CBO node");
        }
    }

    TYtJoinNodeOp::TPtr Root;
    const TYtState::TPtr& State;
    TExprContext& Ctx;
    ECostBasedOptimizer OptimizerType;
    bool Debug;

    THashMap<TStringBuf, std::vector<int>> Table2RelIds;
    std::vector<IOptimizer::TRel> Rels;
    std::vector<std::vector<TStringBuf>> RelTables;
    std::vector<TYtJoinNodeLeaf*> Leafs;
    std::vector<std::vector<std::tuple<TStringBuf, TStringBuf>>> Var2TableCol;

    std::vector<THashMap<TStringBuf, int>> VarIds;

    std::vector<IOptimizer::TEq> EqClasses;
    std::vector<IOptimizer::TEq> Left;
    std::vector<IOptimizer::TEq> Right;

    IOptimizer::TOutput Result;
};

} // namespace

TYtJoinNodeOp::TPtr OrderJoins(TYtJoinNodeOp::TPtr op, const TYtState::TPtr& state, TExprContext& ctx, bool debug)
{
    auto optimizerType = state->Configuration->CostBasedOptimizer.Get().GetOrElse(ECostBasedOptimizer::Disable);
    if (optimizerType == ECostBasedOptimizer::Disable) {
        return op;
    }

    auto result = TJoinReorderer(op, state, ctx, optimizerType, debug).Do();
    if (!debug && AreSimilarTrees(result, op)) {
        return op;
    }
    return result;
}

} // namespace NYql
