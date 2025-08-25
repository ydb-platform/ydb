#include "yql_yt_ytflow_integration.h"
#include "yql_yt_provider.h"
#include "yql_yt_table.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

#include <util/generic/string.h>


namespace NYql {

using namespace NNodes;


class TYtYtflowIntegration: public IYtflowIntegration {
public:
    TYtYtflowIntegration(TYtState* state)
        : State_(state)
    {
    }

    TMaybe<bool> CanRead(const TExprNode& node, TExprContext& ctx) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(&node);
        if (!maybeReadTable) {
            return Nothing();
        }

        if (maybeReadTable.Cast().Input().Size() != 1) {
            AddMessage(ctx, "multiple path groups");
            return false;
        }

        for (auto section: maybeReadTable.Cast().Input()) {
            if (section.Paths().Size() != 1) {
                AddMessage(ctx, "multiple paths");
                return false;
            }

            for (auto path: section.Paths()) {
                if (!path.Table().Maybe<TYtTable>()) {
                    AddMessage(ctx, "non-table path");
                    return false;
                }

                auto pathInfo = TYtPathInfo(path);
                auto tableInfo = pathInfo.Table;

                if (!tableInfo->Meta->IsDynamic) {
                    AddMessage(ctx, "static table");
                    return false;
                }
            }
        }

        return true;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(read);
        YQL_ENSURE(maybeReadTable);

        return Build<TYtflowReadWrap>(ctx, read->Pos())
            .Input(maybeReadTable.Cast())
            .Done().Ptr();
    }

    TMaybe<bool> CanWrite(const TExprNode& node, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(&node);
        if (!maybeWriteTable) {
            return Nothing();
        }

        auto cluster = TString(maybeWriteTable.Cast().DataSink().Cluster().Value());
        auto tableName = TString(TYtTableInfo::GetTableLabel(maybeWriteTable.Cast().Table()));
        auto commitEpoch = TEpochInfo::Parse(maybeWriteTable.Cast().Table().CommitEpoch().Ref());

        auto tableDesc = State_->TablesData->GetTable(
            cluster, tableName, 0);

        auto commitTableDesc = State_->TablesData->GetTable(
            cluster, tableName, commitEpoch);

        if (!tableDesc.Meta->IsDynamic
            && tableDesc.Meta->DoesExist
            && !(commitTableDesc.Intents & TYtTableIntent::Override)
        ) {
            AddMessage(ctx, "write to static table");
            return false;
        }

        return true;
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        YQL_ENSURE(maybeWriteTable);

        return Build<TYtflowWriteWrap>(ctx, write->Pos())
            .Input(maybeWriteTable.Cast())
            .Done().Ptr();
    }

    TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& /*ctx*/) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(&read);
        YQL_ENSURE(maybeReadTable);
        return maybeReadTable.Cast().World().Ptr();
    }

    TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(&write);
        YQL_ENSURE(maybeWriteTable);
        return maybeWriteTable.Cast().World().Ptr();
    }

    TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(&write);
        YQL_ENSURE(maybeWriteTable);
        return maybeWriteTable.Cast().Content().Ptr();
    }

    void FillSourceSettings(
        const TExprNode& source, ::google::protobuf::Any& settings, TExprContext& /*ctx*/
    ) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(&source);
        YQL_ENSURE(maybeReadTable);

        YQL_ENSURE(maybeReadTable.Cast().Input().Size() == 1);
        auto section = maybeReadTable.Cast().Input().Item(0);

        YQL_ENSURE(section.Paths().Size() == 1);
        auto table = section.Paths().Item(0).Table().Cast<TYtTable>();

        auto* rowType = TYqlRowSpecInfo(table.RowSpec()).GetType();

        NYtflow::NProto::TQYTSourceMessage sourceSettings;
        sourceSettings.SetCluster(table.Cluster().StringValue());
        sourceSettings.SetPath(table.Name().StringValue());
        sourceSettings.SetRowType(NCommon::WriteTypeToYson(rowType));

        settings.PackFrom(sourceSettings);
    }

    void FillSinkSettings(
        const TExprNode& sink, ::google::protobuf::Any& settings, TExprContext& /*ctx*/
    ) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(&sink);
        YQL_ENSURE(maybeWriteTable);

        NYtflow::NProto::TQYTSinkMessage sinkSettings;

        {
            auto table = maybeWriteTable.Cast().Table().Cast<TYtTable>();

            sinkSettings.SetCluster(table.Cluster().StringValue());
            sinkSettings.SetPath(table.Name().StringValue());

            auto* rowType = maybeWriteTable.Cast().Content().Ref().GetTypeAnn()
                ->Cast<TListExprType>()->GetItemType();

            sinkSettings.SetRowType(NCommon::WriteTypeToYson(rowType));
        }

        {
            auto cluster = TString(maybeWriteTable.Cast().DataSink().Cluster().Value());
            auto tableName = TString(TYtTableInfo::GetTableLabel(maybeWriteTable.Cast().Table()));

            auto tableDesc = State_->TablesData->GetTable(
                cluster, tableName, 0);

            sinkSettings.SetDoesExist(tableDesc.Meta->DoesExist);
            sinkSettings.SetTruncate(tableDesc.Intents & TYtTableIntent::Override);
        }

        settings.PackFrom(sinkSettings);
    }

private:
    void AddMessage(TExprContext& ctx, const TString& message, bool error = true) {
        TIssue issue(message);

        if (error) {
            YQL_CLOG(ERROR, ProviderYtflow) << message;
            issue.Severity = TSeverityIds::S_ERROR;
        } else {
            YQL_CLOG(INFO, ProviderYtflow) << message;
            issue.Severity = TSeverityIds::S_INFO;
        }

        ctx.IssueManager.RaiseIssue(issue);
    }

private:
    TYtState* State_;
};

THolder<IYtflowIntegration> CreateYtYtflowIntegration(TYtState* state) {
    Y_ABORT_UNLESS(state);
    return MakeHolder<TYtYtflowIntegration>(state);
}

} // namespace NYql
