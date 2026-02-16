#include "yql_yt_ytflow_integration.h"
#include "yql_yt_provider.h"
#include "yql_yt_table.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

#include <util/generic/string.h>


namespace NYql {

using namespace NNodes;


class TYtYtflowIntegration: public TEmptyYtflowIntegration {
public:
    TYtYtflowIntegration(TYtState::TWeakPtr state)
        : State_(state)
    {
    }

    TMaybe<bool> CanRead(const TExprNode& node, TExprContext& ctx) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(&node);
        if (!maybeReadTable) {
            return Nothing();
        }

        if (maybeReadTable.Cast().Input().Size() != 1) {
            AddIssue(ctx, TIssue("multiple path groups"));
            return false;
        }

        for (auto section: maybeReadTable.Cast().Input()) {
            if (section.Paths().Size() != 1) {
                AddIssue(ctx, TIssue("multiple paths"));
                return false;
            }

            for (auto path: section.Paths()) {
                if (!path.Table().Maybe<TYtTable>()) {
                    AddIssue(ctx, TIssue("non-table path"));
                    return false;
                }

                auto pathInfo = TYtPathInfo(path);
                auto tableInfo = pathInfo.Table;

                if (!tableInfo->Meta->IsDynamic) {
                    AddIssue(ctx, TIssue("static table"));
                    return false;
                }
            }
        }

        return true;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(read);
        YQL_ENSURE(maybeReadTable);

        auto cluster = TString(maybeReadTable.Cast().DataSource().Cluster().Value());
        TString token = TStringBuilder() << "cluster:default_" << cluster;

        return Build<TYtflowReadWrap>(ctx, read->Pos())
            .Input(maybeReadTable.Cast())
            .Token()
                .Name()
                    .Value(std::move(token))
                    .Build()
                .Build()
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

        auto ytState = State_.lock();
        YQL_ENSURE(ytState);

        auto tableDesc = ytState->TablesData->GetTable(
            cluster, tableName, 0);

        auto commitTableDesc = ytState->TablesData->GetTable(
            cluster, tableName, commitEpoch);

        if (!tableDesc.Meta->IsDynamic
            && tableDesc.Meta->DoesExist
            && !(commitTableDesc.Intents & TYtTableIntent::Override)
        ) {
            AddIssue(ctx, TIssue("write to static table"));
            return false;
        }

        return true;
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        YQL_ENSURE(maybeWriteTable);

        auto cluster = TString(maybeWriteTable.Cast().DataSink().Cluster().Value());
        TString token = TStringBuilder() << "cluster:default_" << cluster;

        return Build<TYtflowWriteWrap>(ctx, write->Pos())
            .Input(maybeWriteTable.Cast())
            .Token()
                .Name()
                    .Value(std::move(token))
                    .Build()
                .Build()
            .Done().Ptr();
    }

    TMaybe<bool> CanLookupRead(
        const TExprNode& node,
        const TVector<TStringBuf>& keys,
        ERowSelectionMode /*rowSelectionMode*/,
        TExprContext& ctx
    ) override {
        auto canRead = CanRead(node, ctx);
        if (!canRead || !*canRead) {
            return Nothing();
        }

        auto maybeReadTable = TMaybeNode<TYtReadTable>(&node);
        YQL_ENSURE(maybeReadTable);

        bool uniqueKeys = false;
        bool unexpectedSortKeys = false;

        for (auto section : maybeReadTable.Cast().Input()) {
            for (auto path: section.Paths()) {
                auto pathInfo = TYtPathInfo(path);
                auto rowSpecInfo = pathInfo.Table->RowSpec;

                if (!rowSpecInfo->UniqueKeys) {
                    continue;
                }

                uniqueKeys = true;

                TVector<TStringBuf> sortKeys;
                for (const auto& [key, _] : pathInfo.Table->RowSpec->GetForeignSort()) {
                    sortKeys.push_back(key);
                }

                if (keys != sortKeys) {
                    AddIssue(ctx, TIssue(
                        ctx.GetPosition(node.Pos()),
                        TStringBuilder()
                            << "Got unexpected lookup key columns, expected: "
                            << JoinSeq(", ", sortKeys) << ", but got: "
                            << JoinSeq(", ", keys)));

                    unexpectedSortKeys = true;
                    continue;
                }
            }
        }

        if (!uniqueKeys) {
            return Nothing();
        }

        if (unexpectedSortKeys) {
            return false;
        }

        return true;
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

    TExprNode::TPtr UpdateWriteWorld(const TExprNode::TPtr& write, const TExprNode::TPtr& world, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        YQL_ENSURE(maybeWriteTable);
        return Build<TYtWriteTable>(ctx, write->Pos())
            .InitFrom(maybeWriteTable.Cast())
            .World(world)
            .Done().Ptr();
    }

    TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(&write);
        YQL_ENSURE(maybeWriteTable);
        return maybeWriteTable.Cast().Content().Ptr();
    }

    TExprNode::TPtr UpdateWriteContent(
        const TExprNode::TPtr& write,
        const TExprNode::TPtr& content,
        TExprContext& ctx
    ) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        YQL_ENSURE(maybeWriteTable);
        return Build<TYtWriteTable>(ctx, write->Pos())
            .InitFrom(maybeWriteTable.Cast())
            .Content(content)
            .Done().Ptr();
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

            auto ytState = State_.lock();
            YQL_ENSURE(ytState);

            auto tableDesc = ytState->TablesData->GetTable(
                cluster, tableName, 0);

            sinkSettings.SetDoesExist(tableDesc.Meta->DoesExist);
            sinkSettings.SetTruncate(tableDesc.Intents & TYtTableIntent::Override);
        }

        settings.PackFrom(sinkSettings);
    }

    NKikimr::NMiniKQL::TRuntimeNode BuildLookupSourceArgs(
        const TExprNode& read, NCommon::TMkqlBuildContext& ctx
    ) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(&read);
        YQL_ENSURE(maybeReadTable);

        YQL_ENSURE(maybeReadTable.Cast().Input().Size() == 1);
        auto section = maybeReadTable.Cast().Input().Item(0);

        YQL_ENSURE(section.Paths().Size() == 1);
        auto table = section.Paths().Item(0).Table().Cast<TYtTable>();

        auto tableName = TString(table.Name().StringValue());
        if (!tableName.StartsWith("//")) {
            tableName = NYT::TConfig::Get()->Prefix + tableName;
            if (!tableName.StartsWith("//")) {
                tableName = "//" + tableName;
            }
        }

        auto tablePathData = ctx.ProgramBuilder.NewDataLiteral<
            NUdf::EDataSlot::String>(tableName);

        auto cluster = maybeReadTable.Cast().DataSource().Cluster().StringValue();
        auto clusterData = ctx.ProgramBuilder.NewDataLiteral<
            NUdf::EDataSlot::String>(cluster);

        TString token = TStringBuilder() << "cluster:default_" << cluster;
        auto tokenData = ctx.ProgramBuilder.NewDataLiteral<
            NUdf::EDataSlot::String>(token);

        return ctx.ProgramBuilder.NewTuple(TVector<NKikimr::NMiniKQL::TRuntimeNode>{
            std::move(clusterData), std::move(tablePathData), std::move(tokenData)
        });
    }

private:
    void AddIssue(TExprContext& ctx, TIssue issue) {
        switch (issue.Severity) {
        case TSeverityIds::S_FATAL:
        case TSeverityIds::S_ERROR:
            YQL_CLOG(ERROR, ProviderYtflow) << issue.ToString(/*oneLine*/ true);
            break;

        case TSeverityIds::S_WARNING:
        case TSeverityIds::S_INFO:
            YQL_CLOG(INFO, ProviderYtflow) << issue.ToString(/*oneLine*/ true);
            break;

        default:
            break;
        }

        ctx.IssueManager.RaiseIssue(issue);
    }

private:
    TYtState::TWeakPtr State_;
};

THolder<IYtflowIntegration> CreateYtYtflowIntegration(TYtState::TWeakPtr state) {
    YQL_ENSURE(!state.expired());
    return MakeHolder<TYtYtflowIntegration>(state);
}

} // namespace NYql
