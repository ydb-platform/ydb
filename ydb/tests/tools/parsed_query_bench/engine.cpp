#include "engine.h"
#include "kql_codec.h"

#include <ydb/core/kqp/common/simple/query_ref.h>
#include <ydb/core/kqp/common/simple/user_request_context.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/json_reader.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/system/file.h>

#include <set>
#include <thread>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

using namespace NYql;

class TCatalog : public IKqpGateway::IKqpTableMetadataLoader {
public:
    TString Database = "/Root";

    void Load(const TString& path, const NScheme::TTypeRegistry& registry, TString& cluster, TString& prefix) {
        TFile file(path, OpenExisting | RdOnly);
        const i64 size = file.GetLength();
        Y_ENSURE(size > 0 && size <= static_cast<i64>(MaxRequestBytes), "Invalid catalog size");
        TString bytes(size, '\0');
        file.Load(bytes.Detach(), bytes.size());
        NJson::TJsonValue catalog;
        Y_ENSURE(NJson::ReadJsonTree(bytes, &catalog) && catalog["format_version"].GetUIntegerSafe() == 1,
            "Invalid offline catalog");
        cluster = catalog["cluster"].GetStringSafe();
        Database = catalog["database"].GetStringSafe();
        prefix = catalog["table_path_prefix"].GetStringSafe();
        Y_ENSURE(cluster && Database.StartsWith('/') && (prefix == Database || prefix.StartsWith(Database + "/")),
            "Invalid catalog cluster, database or table path prefix");

        std::set<std::pair<ui64, ui64>> ids;
        for (const auto& table : catalog["tables"].GetArraySafe()) {
            const auto& name = table["path"].GetStringSafe();
            const auto owner = table["owner_id"].GetUIntegerSafe();
            const auto id = table["table_id"].GetUIntegerSafe();
            const auto version = table["schema_version"].GetUIntegerSafe();
            Y_ENSURE(name.StartsWith(Database + "/") && owner && id && version && ids.emplace(owner, id).second,
                "Invalid catalog table identity: " << name);
            auto metadata = MakeIntrusive<TKikimrTableMetadata>(cluster, name);
            metadata->PathId = TKikimrPathId(owner, id);
            metadata->SchemaVersion = version;
            metadata->DoesExist = true;
            const auto& storage = table["storage"].GetStringSafe();
            Y_ENSURE(storage == "row" || storage == "column", "Unsupported catalog storage: " << storage);
            metadata->Kind = storage == "row" ? EKikimrTableKind::Datashard : EKikimrTableKind::Olap;
            metadata->StoreType = storage == "row" ? EStoreType::Row : EStoreType::Column;
            THashSet<ui32> columnIds;
            for (const auto& col : table["columns"].GetArraySafe()) {
                const auto& colName = col["name"].GetStringSafe();
                const auto colId = col["id"].GetUIntegerSafe();
                const auto& type = col["type"].GetStringSafe();
                Y_ENSURE(colName && colId && colId <= Max<ui32>() && columnIds.insert(colId).second,
                    "Invalid catalog column: " << colName);
                NScheme::TTypeInfo info;
                TString error;
                Y_ENSURE(NScheme::GetTypeInfo(registry.GetType(type), {}, type, colName, info, error), error);
                Y_ENSURE(metadata->Columns.emplace(colName,
                    TKikimrColumnMetadata(colName, colId, type, col["not_null"].GetBooleanSafe(), info)).second,
                    "Duplicate catalog column: " << colName);
                metadata->ColumnOrder.push_back(colName);
            }
            THashSet<TString> keys;
            for (const auto& key : table["key_columns"].GetArraySafe()) {
                const auto& keyName = key.GetStringSafe();
                Y_ENSURE(metadata->Columns.contains(keyName) && keys.insert(keyName).second, "Invalid catalog key: " << keyName);
                metadata->KeyColumnNames.push_back(keyName);
            }
            Y_ENSURE(!metadata->Columns.empty() && Tables.emplace(name, std::move(metadata)).second,
                "Empty or duplicate catalog table: " << name);
        }
    }

    NThreading::TFuture<IKqpGateway::TTableMetadataResult> LoadTableMetadata(const TString& cluster, const TString& table,
        const IKqpGateway::TLoadTableMetadataSettings&, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>&) override
    {
        const auto* entry = Tables.FindPtr(table);
        Y_ENSURE(entry && (*entry)->Cluster == cluster && database == Database, "Table outside benchmark catalog: " << table);
        IKqpGateway::TTableMetadataResult result;
        result.Metadata = *entry;
        result.SetSuccess();
        return NThreading::MakeFuture(std::move(result));
    }

    void BindTables(const TExprNode::TPtr& query, TKikimrSessionContext& session, TExprContext& ctx) const {
        VisitExpr(query, [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("KqpTable")) {
                Y_ENSURE(node->ChildrenSize() == 4 && node->Child(0)->IsAtom() && node->Child(1)->IsAtom()
                    && node->Child(2)->IsAtom() && node->Child(3)->IsAtom(), "Invalid KQL table reference");
                const NNodes::TKqpTable table(node);
                const auto path = table.Path().StringValue();
                const auto* entry = Tables.FindPtr(path);
                Y_ENSURE(entry && table.PathId().Value() == (*entry)->PathId.ToString() && table.SysView().Value().empty()
                    && table.Version().Value() == ToString((*entry)->SchemaVersion), "Catalog identity mismatch: " << path);
                auto& desc = session.Tables().GetOrAddTable((*entry)->Cluster, Database, path);
                if (!desc.Metadata) {
                    desc.Metadata = *entry;
                    Y_ENSURE(desc.Load(ctx, session.Config().SystemColumnsEnabled()), "Invalid catalog table: " << path);
                }
            }
            return true;
        });
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
        return {};
    }

private:
    THashMap<TString, TKikimrTableMetadataPtr> Tables;
};

void CapturePlan(TLogicalResult& result, const TVector<TIntrusivePtr<TOpRoot>>& roots, TExprContext& ctx) {
    for (const auto& root : roots) {
        auto& signature = result.Roots.emplace_back();
        signature.Plan = root->PlanToString(ctx);
        signature.TypedExpression = ConvertToAst(*root->Node, ctx, TExprAnnotationFlags::Types, true).Root->ToString();
        signature.QueryColumns = root->GetQueryColumns();
        signature.ResultIndex = root->PlanProps.ResultIndex.value();
        signature.PhysicalTxGroup = root->PlanProps.PhysicalTxGroup;
        signature.UnorderedResult = root->PlanProps.UnorderedResult;
        const auto type = root->Node->GetTypeAnn();
        Y_ENSURE(type && type->GetKind() == ETypeAnnotationKind::List, "Missing RBO result list type");
        const auto* fields = type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto& [iu, name] : root->GetColumns().Items()) {
            signature.ColumnIds.push_back(iu);
            const auto* field = fields->FindItemType(name);
            Y_ENSURE(field, "Missing RBO result field: " << name);
            signature.Columns.emplace_back(name, FormatType(field));
        }
    }
}

} // namespace

struct TEngine::TImpl {
    NActors::TTestActorRuntime Runtime;
    TModuleResolverState Modules;
    std::shared_ptr<TCatalog> Catalog = std::make_shared<TCatalog>();
    TKqpSettings Defaults;
    TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<IKqpHost> Host;
    std::thread::id Caller;
    TIntrusivePtr<IKqpGateway> Gateway;

    TKikimrConfiguration::TPtr MakeConfig(const TString& cluster, const TString& prefix) const {
        auto config = MakeIntrusive<TKikimrConfiguration>();
        config->Init(Defaults.DefaultSettings.GetDefaultSettings(), cluster, Defaults.Settings, false);
        config->_KqpTablePathPrefix = prefix;
        config->SetEnableNewRBO(true);
        config->DisableBlockExecution = true;
        config->FreezeDefaults();
        return config;
    }
};

TEngine::TEngine(const TString& catalogPath)
    : Impl(std::make_unique<TImpl>())
{
    NYql::NLog::TComponentHelpers::ForEach([](auto component) {
        NYql::NLog::YqlLogger().SetComponentLevel(component, NYql::NLog::ELevel::WARN);
    });
    Impl->Runtime.Initialize(TAppPrepare(TAppPrepare::TLightweightTag{}).Unwrap());
    if (catalogPath) {
        Impl->Catalog->Load(catalogPath, *Impl->Runtime.GetAppData().TypeRegistry, Cluster, PathPrefix);
    }
    Y_ENSURE(GetYqlDefaultModuleResolver(Impl->Modules.ExprCtx, Impl->Modules.ModuleResolver),
        Impl->Modules.ExprCtx.IssueManager.GetIssues().ToString());
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    Impl->Config = Impl->MakeConfig(Cluster, PathPrefix);
    const auto dynamic = Impl->Runtime.GetDynamicCounters();
    counters->Counters = new TKqpCounters(dynamic);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(dynamic);
    Impl->Gateway = CreateKikimrIcGateway(Cluster, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY,
        Impl->Catalog->Database, Impl->Catalog->Database, Impl->Catalog, Impl->Runtime.GetActorSystem(0),
        Impl->Runtime.GetNodeId(), counters);
}

TEngine::~TEngine() = default;

const TKikimrConfiguration& TEngine::TranslationConfig() const {
    return *Impl->Config;
}

void TEngine::WithSession(std::function<void()> run) const {
    Impl->Caller = std::this_thread::get_id();
    Impl->Runtime.RunCall([&] {
        Y_ENSURE(Impl->Caller == std::this_thread::get_id() && !Impl->Host, "Invalid benchmark actor session");
        Impl->Host = CreateKqpHost(Impl->Gateway, Cluster, Impl->Catalog->Database, Impl->MakeConfig(Cluster, PathPrefix), Impl->Modules.ModuleResolver,
            std::nullopt, nullptr, std::make_shared<TGUCSettings>(), {}, Nothing(), Impl->Runtime.GetAppData().FunctionRegistry);
        Y_DEFER { Impl->Host.Reset(); };
        run();
        return true;
    });
}

namespace {

IKqpHost::TPrepareSettings CaptureRoots(TLogicalResult& result, bool verify, std::thread::id caller) {
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
    settings.CompileTrace = MakeIntrusive<TCompileTrace>(NWilson::TTraceId{}, nullptr);
    settings.CompileTrace->CaptureRboRoots = [&, verify, caller](const auto& roots, const TExprNode&, TExprContext& ctx) {
        result.Done = TMeasurementTime::Now();
        Y_ENSURE(std::this_thread::get_id() == caller, "Compiler moved between thread CPU clocks");
        Y_ENSURE(!roots.empty(), "Expected at least one RBO root");
        for (const auto& root : roots) {
            Y_ENSURE(!root->PlanProps.WithEffects, "Offline benchmark requires read-only queries");
        }
        if (verify) {
            CapturePlan(result, roots, ctx);
        }
        result.RootCount = roots.size();
    };
    return settings;
}

void RunUntil(const IKqpHost::IAsyncQueryResultPtr& compilation, const std::function<bool()>& reached, TStringBuf boundary) {
    while (!compilation->HasResult() && !reached()) {
        const auto ready = compilation->Continue();
        Y_ENSURE(ready.IsReady(), "Compilation requested an unavailable offline service");
        if (ready.GetValue()) {
            break;
        }
    }
    if (!reached()) {
        const auto failed = compilation->GetResult();
        ythrow yexception() << "Compilation did not reach " << boundary << ": " << failed.Issues().ToString();
    }
}

IKqpHost::IAsyncQueryResultPtr Compile(IKqpHost& host, const TString& text, const TAstParseResult* ast,
    const TParameterMap& parameters, const IKqpHost::TPrepareSettings& settings)
{
    TKqpQueryRef query(text);
    query.TranslatedAst = ast;
    if (!parameters.empty()) {
        query.ParameterTypes = std::make_shared<std::map<TString, Ydb::Type>>();
        for (const auto& [name, value] : parameters) {
            Y_ENSURE(value.has_type(), "Missing type for " << name);
            query.ParameterTypes->emplace(name, value.type());
        }
    }
    return host.PrepareGenericQuery(query, settings);
}

void CheckParameterTypes(const TExprNode::TPtr& query, const TParameterMap& parameters, TExprContext& ctx) {
    VisitExpr(query, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("Parameter")) {
            Y_ENSURE(node->ChildrenSize() == 2 && node->Head().IsAtom(), "Invalid KQL parameter");
            const TString name(node->Head().Content());
            auto it = parameters.find(name);
            if (it == parameters.end() && name.StartsWith('$')) {
                it = parameters.find(name.substr(1));
            }
            if (it != parameters.end()) {
                const auto* type = ParseTypeFromYdbType(it->second.type(), ctx);
                Y_ENSURE(type, "Invalid request parameter type: " << name);
                const auto canonical = ExpandType(node->Pos(), *type, ctx);
                const TExprNode* expected = canonical.Get();
                const TExprNode* actual = node->Child(1);
                Y_ENSURE(CompareExprTrees(expected, actual), "KQL parameter type mismatch: " << name);
            }
        }
        return true;
    });
}

} // namespace

TLogicalResult TEngine::LowerText(const TString& text, const TParameterMap& parameters, bool verify, TMeasurementTime* imported) const {
    TLogicalResult result;
    auto settings = CaptureRoots(result, verify, Impl->Caller);
    if (imported) {
        settings.CompileTrace->OnSqlTranslated = [imported] { *imported = TMeasurementTime::Now(); };
    }
    RunUntil(Compile(*Impl->Host, text, nullptr, parameters, settings), [&] { return result.RootCount != 0; }, "RBO root conversion");
    return result;
}

TLogicalResult TEngine::Lower(TAstParseResult ast, const TParameterMap& parameters, bool verify) const {
    TLogicalResult result;
    auto settings = CaptureRoots(result, verify, Impl->Caller);
    RunUntil(Compile(*Impl->Host, {}, &ast, parameters, settings), [&] { return result.RootCount != 0; }, "RBO root conversion");
    return result;
}

void TEngine::ExportKql(const TString& text, const TParameterMap& parameters, Ydb::Query::Benchmark::KqlQueryList& out) const {
    bool captured = false;
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
    settings.CompileTrace = MakeIntrusive<TCompileTrace>(NWilson::TTraceId{}, nullptr);
    settings.CompileTrace->CaptureKqlQuery = [&](const TExprNode& query, TExprContext& ctx) {
        ExportKqlQueryList(query, ctx, out);
        captured = true;
    };
    RunUntil(Compile(*Impl->Host, text, nullptr, parameters, settings), [&] { return captured; }, "BuildKqlQuery");
}

TLogicalResult TEngine::LowerKql(const Ydb::Query::Benchmark::KqlQueryList& wire, const TParameterMap& parameters,
    bool verify, TMeasurementTime* imported) const
{
    TLogicalResult result;
    auto settings = CaptureRoots(result, verify, Impl->Caller);
    auto compilation = Impl->Host->InspectKqlQuery(settings, [&](TExprContext& ctx, TKikimrSessionContext& session) {
        auto query = ImportKqlQueryList(wire, ctx);
        Impl->Catalog->BindTables(query, session, ctx);
        CheckParameterTypes(query, parameters, ctx);
        if (imported) {
            *imported = TMeasurementTime::Now();
        }
        return query;
    });
    RunUntil(compilation, [&] { return result.RootCount != 0; }, "RBO root conversion");
    return result;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
