#include "yql_clickhouse_provider_impl.h"
#include "yql_clickhouse_util.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <library/cpp/json/json_reader.h>

namespace NYql {

using namespace NNodes;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

class TClickHouseLoadTableMetadataTransformer : public TGraphTransformerBase {
    using TMapType = std::unordered_map<std::pair<TString, TString>, std::shared_ptr<std::optional<IHTTPGateway::TResult>>, THash<std::pair<TString, TString>>>;
public:
    TClickHouseLoadTableMetadataTransformer(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway)
        : State_(std::move(state)), Gateway_(std::move(gateway))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        std::unordered_set<TMapType::key_type, TMapType::hasher> pendingTables;
        if (const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            if (const auto maybeRead = TMaybeNode<TClRead>(node)) {
                return maybeRead.Cast().DataSource().Category().Value() == ClickHouseProviderName;
            }
            return false;
        }); !reads.empty()) {
            for (const auto& r : reads) {
                const TClRead read(r);
                if (!read.FreeArgs().Get(2).Ref().IsCallable("MrTableConcat")) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), TStringBuilder() << "Expected Key"));
                    return TStatus::Error;
                }

                const auto maybeKey = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Maybe<TCoKey>();
                if (!maybeKey) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), TStringBuilder() << "Expected Key"));
                    return TStatus::Error;
                }

                const auto& keyArg = maybeKey.Cast().Ref().Head();
                if (!keyArg.IsList() || keyArg.ChildrenSize() != 2U
                    || !keyArg.Head().IsAtom("table") || !keyArg.Tail().IsCallable(TCoString::CallableName())) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyArg.Pos()), TStringBuilder() << "Expected single table name"));
                    return TStatus::Error;
                }

                const auto cluster = read.DataSource().Cluster().StringValue();
                const auto tableName = TString(keyArg.Tail().Head().Content());
                if (pendingTables.insert(std::make_pair(cluster, tableName)).second) {
                    YQL_CLOG(INFO, ProviderClickHouse) << "Load table meta for: `" << cluster << "`.`" << tableName << "`";
                }
            }
        }

        std::vector<NThreading::TFuture<void>> handles;
        handles.reserve(pendingTables.size());
        Results_.reserve(pendingTables.size());

        for (const auto& item : pendingTables) {
            const auto& cluster = item.first;
            const auto it = State_->Configuration->Urls.find(cluster);
            YQL_ENSURE(State_->Configuration->Urls.cend() != it, "Cluster not found:" << cluster);

            TString token;
            if (const auto cred = State_->Types->Credentials->FindCredential("default_" + cluster)) {
                token = cred->Content;
            } else {
                token = State_->Configuration->Tokens[cluster];
            }

            const auto one = token.find('#'), two = token.rfind('#');
            YQL_ENSURE(one != TString::npos && two != TString::npos && one < two, "Bad token format:" << token);

            TStringBuilder url;
            switch (std::get<EHostScheme>(it->second)) {
                case HS_HTTP: url << "http://"; break;
                case HS_HTTPS: url << "https://"; break;
            }
            url << token.substr(one + 1U, two - one - 1U) << ':' << token.substr(two + 1U) << '@' << std::get<TString>(it->second) << ':' << std::get<ui16>(it->second) << "/?default_format=JSONCompactEachRow";

            const auto& table  = item.second;
            TStringBuf db, dbTable;
            if (!TStringBuf(table).TrySplit('.', db, dbTable)) {
                db = "default";
                dbTable = table;
            }

            TStringBuilder sql;
            sql << "select timezone(),groupArray((name,type)) from system.columns where database = " << EscapeChString(db) << " and table = " << EscapeChString(dbTable) << ';';

            auto promise = NThreading::NewPromise();
            handles.emplace_back(promise.GetFuture());

            auto result = Results_.emplace( item, std::make_shared<TMapType::mapped_type::element_type>()).first->second;

            Gateway_->Upload(
                std::move(url),
                {},
                std::move(sql),
                std::bind(
                    &TClickHouseLoadTableMetadataTransformer::OnDiscovery,
                    std::move(result),
                    std::placeholders::_1,
                    std::move(promise))
                );
        }

        if (handles.empty()) {
            return TStatus::Ok;
        }

        AsyncFuture_ = NThreading::WaitExceptionOrAll(handles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_ENSURE(AsyncFuture_.HasValue());

        const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            if (const auto maybeRead = TMaybeNode<TClRead>(node)) {
                return maybeRead.Cast().DataSource().Category().Value() == ClickHouseProviderName;
            }
            return false;
        });

        TNodeOnNodeOwnedMap replaces(reads.size());
        bool bad = false;
        for (const auto& r : reads) {
            const TClRead read(r);
            const auto cluster = read.DataSource().Cluster().StringValue();
            const auto& keyArg = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Cast<TCoKey>().Ref().Head();
            const auto table = TString(keyArg.Tail().Head().Content());

            const auto it = Results_.find(std::make_pair(cluster, table));
            if (Results_.cend() != it && it->second && *it->second) {
                if (!(*it->second)->Issues) {
                    TClickHouseState::TTableMeta meta;
                    if (const auto& parse = ParseTableMeta(std::move((*it->second)->Content.Extract()), cluster, table, ctx, meta.ColumnOrder); parse.first) {
                        meta.ItemType = parse.first;
                        State_->Timezones[read.DataSource().Cluster().Value()] = ctx.AppendString(parse.second);
                        if (const auto ins = replaces.emplace(read.Raw(), TExprNode::TPtr()); ins.second)
                            ins.first->second = Build<TClReadTable>(ctx, read.Pos())
                                .World(read.World())
                                .DataSource(read.DataSource())
                                .Table().Value(table).Build()
                                .Columns<TCoVoid>().Build()
                                .Timezone().Value(parse.second).Build()
                            .Done().Ptr();
                        State_->Tables.emplace(it->first, meta);
                    } else
                        bad = true;
                } else {
                    const auto issues = std::move((*it->second)->Issues);
                    std::for_each(issues.begin(), issues.end(), std::bind(&TExprContext::AddError, std::ref(ctx), std::placeholders::_1));
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Error on load meta for " << cluster << '.' << table));
                    bad = true;
                }
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Not found result for " << cluster << '.' << table));
                bad = true;
            }
        }


        if (bad)
            return TStatus::Error;

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
    }

    void Rewind() final {
        Results_.clear();
        AsyncFuture_ = {};
    }
private:
    static void OnDiscovery(const TMapType::mapped_type& res, IHTTPGateway::TResult&& result, NThreading::TPromise<void>& promise) {
        *res = std::move(result);
        return promise.SetValue();
    }

    std::pair<const TStructExprType*, TString> ParseTableMeta(const std::string_view& blob, const std::string_view& cluster, const std::string_view& table, TExprContext& ctx, TVector<TString>& columnOrder) try {
        columnOrder.clear();
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(blob, &json, false)) {
            ctx.AddError(TIssue({}, TStringBuilder() << "ClickHouse response: " << blob));
            return {nullptr, {}};
        }

        const auto& row = json.GetArray();
        auto timezone = row.front().GetString();
        if (!FindTimezoneId(timezone)) {
            ctx.AddWarning(TIssue({}, TStringBuilder() << "Unsupported timezone: " << timezone));
            timezone.clear();
        }

        const auto& cols = row.back().GetArray();
        if (cols.empty()) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Table " << cluster << '.' << table << " doesn't exist."));
            return {nullptr, {}};
        }

        TSmallVec<std::array<TString, 2U>> rawMeta;
        rawMeta.reserve(cols.size());

        std::transform(cols.cbegin(), cols.cend(), std::back_inserter(rawMeta),[](const NJson::TJsonValue& v) -> std::array<TString, 2U> {
            return {{v.GetArray().front().GetString(), v.GetArray().back().GetString()}};
        });

        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pb(env, *State_->FunctionRegistry);

        TRuntimeNode::TList types;
        types.reserve(rawMeta.size());
        std::transform(rawMeta.cbegin(), rawMeta.cend(), std::back_inserter(types), [&](const std::array<TString, 2U>& c) { return pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(c.back()); });

        const auto strType = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        const auto list = pb.NewList(strType, types);
        const auto zone = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(timezone);
        const auto root = pb.Map(list, [&](TRuntimeNode type) { return pb.Apply(pb.Udf("ClickHouseClient.ToYqlType"), {type, zone}); });

        auto randomProvider = CreateDefaultRandomProvider();
        auto timeProvider = CreateDefaultTimeProvider();

        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), env);
        TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
            State_->FunctionRegistry, NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);

        std::vector<TNode*> entryPoints(1, root.GetNode());
        auto pattern = MakeComputationPattern(explorer, root, entryPoints, opts);
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        TBindTerminator bind(graph->GetTerminator());

        TVector<const TItemExprType*> items;
        items.reserve(rawMeta.size());
        const auto output = graph->GetValue();
        YQL_ENSURE(output.GetListLength() == rawMeta.size(), "Enexpected list size " << output.GetListLength() << " != " << rawMeta.size());

        for (auto i = 0U; i < rawMeta.size(); ++i) {
            if (const auto& e = output.GetElement(i)) {
                TMemoryPool pool(4096);
                TIssues issues;
                const auto parsedType = ParseType(e.AsStringRef(), pool, issues, {});
                YQL_ENSURE(parsedType, "Failed to parse type" << issues.ToString());

                const auto astRoot = TAstNode::NewList({}, pool,
                    TAstNode::NewList({}, pool, TAstNode::NewLiteralAtom({}, TStringBuf("return"), pool), parsedType));
                TExprNode::TPtr exprRoot;
                YQL_ENSURE(CompileExpr(*astRoot, exprRoot, ctx, nullptr, nullptr), "Failed to compile.");

                // TODO: Collect type annotation directly from AST.
                const auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(*State_->Types);
                const auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, *State_->Types);
                YQL_ENSURE(InstantTransform(*typeTransformer, exprRoot, ctx) == IGraphTransformer::TStatus::Ok, "Failed to type check.");

                const auto type = exprRoot->GetTypeAnn()->Cast<NYql::TTypeExprType>()->GetType();
                items.emplace_back(ctx.MakeType<TItemExprType>(rawMeta[i].front(), type));
                columnOrder.emplace_back(rawMeta[i].front());
            }
        }

        return std::make_pair(ctx.MakeType<TStructExprType>(items), std::move(timezone));
    } catch (std::exception&) {
        ctx.AddError(TIssue({}, TStringBuilder() << "Failed to parse table metadata: " << CurrentExceptionMessage()));
        return {nullptr, {}};
    }
private:
    const TClickHouseState::TPtr State_;
    const IHTTPGateway::TPtr Gateway_;

    TMapType Results_;
    NThreading::TFuture<void> AsyncFuture_;
};

THolder<IGraphTransformer> CreateClickHouseLoadTableMetadataTransformer(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway) {
    return MakeHolder<TClickHouseLoadTableMetadataTransformer>(std::move(state), std::move(gateway));
}

} // namespace NYql
