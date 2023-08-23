#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_table.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

void ScanPlanDependencies(const TExprNode::TPtr& input, TExprNode::TListType& children) {
    VisitExpr(input, [&children](const TExprNode::TPtr& node) {
        if (TMaybeNode<TYtReadTable>(node)) {
            children.push_back(node);
            return false;
        }
        if (const auto maybeOutput = TMaybeNode<TYtOutput>(node)) {
            const auto& output = maybeOutput.Cast();
            if (const auto& maybeTryFirst = output.Operation().Maybe<TYtTryFirst>()) {
                const auto& tryFirst = maybeTryFirst.Cast();
                children.emplace_back(tryFirst.Second().Ptr());
                children.emplace_back(tryFirst.First().Ptr());
            } else
                children.emplace_back(GetOutputOp(output).Ptr());
            return false;
        }
        if (node->IsCallable("DqCnResult")) { // For TYtDqProcessWrite.
            children.emplace_back(node->HeadPtr());
            return false;
        }
        return true;
    });
}

void ScanForUsedOutputTables(const TExprNode& input, TVector<TString>& usedNodeIds)
{
    VisitExpr(input, [&usedNodeIds](const TExprNode& node) {
        if (auto maybeYtOutput = TMaybeNode<TYtOutput>(&node)) {

            auto ytOutput = maybeYtOutput.Cast();

            TString cluster = TString{GetOutputOp(ytOutput).DataSink().Cluster().Value()};
            TString table = TString{GetOutTable(ytOutput).Cast<TYtOutTable>().Name().Value()};

            if (!cluster.empty() && !table.empty()) {
                usedNodeIds.push_back(MakeUsedNodeId(cluster, table));
            }
            return false;
        }
        return true;
    });

}

TString MakeUsedNodeId(const TString& cluster, const TString& table)
{
    YQL_ENSURE(!cluster.empty());
    YQL_ENSURE(!table.empty());

    return cluster + "." + table;
}

TString MakeTableDisplayName(NNodes::TExprBase table, bool isOutput) {
    TStringBuilder name;
    if (table.Maybe<TYtTable>()) {
        auto ytTable = table.Cast<TYtTable>();
        name << ytTable.Cluster().Value() << ".";
        if (NYql::HasSetting(ytTable.Settings().Ref(), EYtSettingType::Anonymous)) {
            name << ytTable.Name().Value();
        }
        else {
            name << '`' << ytTable.Name().Value() << '`';
        }
        auto epoch = isOutput ? ytTable.CommitEpoch() : ytTable.Epoch();
        if (auto epochVal = TEpochInfo::Parse(epoch.Ref()).GetOrElse(0)) {
            name << " #" << epochVal;
        }
    }
    else {
        name << "(tmp)";
    }
    return name;
}


} // NYql
