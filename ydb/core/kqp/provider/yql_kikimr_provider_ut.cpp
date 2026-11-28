#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_provider.h"
#include "yql_kikimr_settings.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/common/simple/user_request_context.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/testlib/helpers.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/sql/v1/translation/context.h>
#include <yql/essentials/sql/v1/translation/source.h>

namespace NYql {

namespace {

using namespace NSQLTranslationV1;

TContext CreateDefaultParserContext(NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DefaultCluster = "/Cluster";
    settings.AssumeYdbOnClusterWithSlash = true;

    NSQLTranslationV1::TLexers lexers;
    NSQLTranslationV1::TParsers parsers;
    static const NSQLTranslation::TParsedTokenList emptyTokens;
    return TContext(lexers, parsers, settings, { /* hints */ }, emptyTokens, issues);
}

TAstNode* CreateAlterTable(TContext& parserContext, const TString& tableName, const TAlterTableParameters& params) {
    TTableRef tableRef(tableName, parserContext.Scoped->CurrService, parserContext.Scoped->CurrCluster, {});
    {
        TDeferredAtom tableAtom(parserContext.Pos(), tableName);
        tableRef.Keys = BuildTableKey(parserContext.Pos(), tableRef.Service, tableRef.Cluster, tableAtom, {});
    }

    auto alterTableNode = BuildAlterTable(parserContext.Pos(), tableRef, params, parserContext.Scoped);
    UNIT_ASSERT_C(alterTableNode, parserContext.Issues.ToString());
    UNIT_ASSERT_C(alterTableNode->Init(parserContext, nullptr), parserContext.Issues.ToString());
    TAstNode* alterTableAst = alterTableNode->Translate(parserContext);
    UNIT_ASSERT_C(alterTableAst, parserContext.Issues.ToString());

    UNIT_ASSERT_C(alterTableAst->IsList()
        && alterTableAst->GetChildrenCount() > 1
        && alterTableAst->GetChild(1)->IsList()
        && alterTableAst->GetChild(1)->GetChildrenCount() > 1
        && alterTableAst->GetChild(1)->GetChild(1),
        alterTableAst->ToString()
    );
    // this child represents the world
    return alterTableAst->GetChild(1)->GetChild(1);
}

void Find(const TAstNode* node, const std::function<bool(const TAstNode*)>& predicate) {
    if (predicate(node)) {
        return;
    }
    if (node->IsList()) {
        for (auto* child : node->GetChildren()) {
            Find(child, predicate);
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KikimrProvider) {
    Y_UNIT_TEST_TWIN(CreateGeneratedColumnMetadataOutlivesContext, Stored) {
        for (bool newRbo : {false, true}) {
            auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
            auto config = MakeIntrusive<TKikimrConfiguration>();
            config->SetEnableNewRBO(newRbo);
            config->SetEnableIndexStreamWrite(true);
            config->FeatureFlags.SetEnableGeneratedStored(true);
            config->FeatureFlags.SetEnableGeneratedVirtual(true);

            // CREATE's asynchronous gateway callback can retain both of these
            // after publishing completion and allowing the compiler to tear down.
            TKikimrTableMetadataPtr metadata;
            TIntrusivePtr<TKikimrSessionContext> session;
            {
                TExprContext ctx;
                auto types = MakeIntrusive<TTypeAnnotationContext>();
                session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), config,
                    CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
                session->Query().Type = EKikimrQueryType::Query;
                auto& table = session->Tables().GetOrAddTable("kikimr", "/Root", "/Root/Generated");
                table.Metadata = MakeIntrusive<TKikimrTableMetadata>("kikimr", "/Root/Generated");
                table.Metadata->DoesExist = false;

                const TPositionHandle pos;
                const auto atom = [&](TStringBuf value) { return ctx.NewAtom(pos, value); };
                const auto list = [&](TExprNode::TListType items = {}) { return ctx.NewList(pos, std::move(items)); };
                auto type = ctx.NewCallable(pos, "OptionalType", {ctx.NewCallable(pos, "DataType", {atom("Int32")})});
                type->SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int32))));
                auto generated = list({atom("generated"), list({atom("expression"), atom("k + 1"), atom(Stored ? "stored" : "virtual")})});
                auto columns = list({
                    list({atom("k"), type}),
                    list({atom("g"), type, list({atom("constraints"), list({generated})})})});
                auto world = ctx.NewWorld(pos);
                world->SetTypeAnn(ctx.MakeType<TWorldExprType>());
                auto create = ctx.NewCallable(pos, "KiCreateTable!", {
                    world, ctx.NewCallable(pos, "DataSink", {atom("kikimr"), atom("kikimr")}),
                    atom("/Root/Generated"), columns, list({atom("k")}),
                    list(), list(), list(), list(), list(), list(),
                    atom("table"), atom("false"), atom("0"), atom("0"), list()});

                auto transformer = CreateKiSinkTypeAnnotationTransformer(nullptr, session, *types);
                TExprNode::TPtr output;
                UNIT_ASSERT_VALUES_EQUAL_C(transformer->Transform(create, output, ctx), IGraphTransformer::TStatus::Ok,
                    ctx.IssueManager.GetIssues().ToString());
                metadata = session->Tables().GetTable("kikimr", "/Root/Generated").Metadata;
            }

            // Metadata remains useful without keeping any validation AST alive.
            const auto& generated = metadata->Columns.at("g").DefaultExpression;
            UNIT_ASSERT(generated);
            UNIT_ASSERT_VALUES_EQUAL(generated->ExprText, "k + 1");
            UNIT_ASSERT_VALUES_EQUAL(generated->Stored, Stored);
            UNIT_ASSERT_VALUES_EQUAL(generated->Dependencies.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(generated->Dependencies.front(), "k");
            UNIT_ASSERT(!generated->Expr);
            session.Reset();
            metadata.Reset();
        }
    }

    Y_UNIT_TEST(LegacySyntaxVersionIsNormalized) {
        NYql::NProto::TTranslationSettings serializedSettings;
        serializedSettings.SetSyntaxVersion(0);

        NSQLTranslation::TTranslationSettings settings;
        NSQLTranslation::Deserialize(serializedSettings, settings);

        UNIT_ASSERT_VALUES_EQUAL(settings.SyntaxVersion, 1);
    }

    Y_UNIT_TEST(SystemColumnsMatchCoreScheme) {
        const auto& schemeColumns = NKikimr::GetSystemColumns();
        const auto& kikimrColumns = KikimrSystemColumns();

        UNIT_ASSERT_VALUES_EQUAL(kikimrColumns.size(), schemeColumns.size());
        for (const auto& [name, schemeColumn] : schemeColumns) {
            const auto* kikimrType = kikimrColumns.FindPtr(name);
            UNIT_ASSERT_C(kikimrType, "Missing KQP system column: " << name);
            UNIT_ASSERT_VALUES_EQUAL(*kikimrType, NKikimr::NUdf::GetDataSlot(schemeColumn.TypeId));
        }

        const auto* partitionColumn = schemeColumns.FindPtr(NKikimr::YqlPartitionColumnName);
        UNIT_ASSERT(partitionColumn);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(partitionColumn->ColumnId),
            static_cast<ui32>(NKikimr::TKeyDesc::EColumnIdDataShard));
    }

    Y_UNIT_TEST(TestFillAuthPropertiesNone) {
        THashMap<TString, TString> properties;
        NKikimrSchemeOp::TAuth auth;
        auth.MutableNone();
        TExternalSourceAuth source(auth);
        properties = source.BuildAuthProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 1);
        auto it = properties.find("authMethod");
        UNIT_ASSERT(it != properties.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, "NONE");
    }

    Y_UNIT_TEST(TestFillAuthPropertiesServiceAccount) {
        THashMap<TString, TString> properties;
        NKikimrSchemeOp::TAuth auth;
        auto* sa = auth.MutableServiceAccount();
        sa->SetId("saId");
        sa->SetSecretName("secretName");
        TExternalSourceAuth source(auth);
        source.InitSecretValues({"saSignature"});
        properties = source.BuildAuthProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 4);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "SERVICE_ACCOUNT");
        }
        {
            auto it = properties.find("serviceAccountId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saId");
        }
        {
            auto it = properties.find("serviceAccountIdSignature");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saSignature");
        }
        {
            auto it = properties.find("serviceAccountIdSignatureReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "secretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesBasic) {
        THashMap<TString, TString> properties;
        NKikimrSchemeOp::TAuth auth;
        auto* basic = auth.MutableBasic();
        basic->SetLogin("login");
        basic->SetPasswordSecretName("passwordSecretName");
        TExternalSourceAuth source(auth);
        source.InitSecretValues({"password"});
        properties = source.BuildAuthProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 4);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "BASIC");
        }
        {
            auto it = properties.find("login");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "login");
        }
        {
            auto it = properties.find("password");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "password");
        }
        {
            auto it = properties.find("passwordReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "passwordSecretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesMdbBasic) {
        THashMap<TString, TString> properties;
        NKikimrSchemeOp::TAuth auth;
        auto* mdb = auth.MutableMdbBasic();
        mdb->SetServiceAccountId("saId");
        mdb->SetServiceAccountSecretName("secretName");
        mdb->SetLogin("login");
        mdb->SetPasswordSecretName("passwordSecretName");
        TExternalSourceAuth source(auth);
        source.InitSecretValues({"saSignature", "password"});
        properties = source.BuildAuthProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 7);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "MDB_BASIC");
        }
        {
            auto it = properties.find("login");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "login");
        }
        {
            auto it = properties.find("password");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "password");
        }
        {
            auto it = properties.find("passwordReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "passwordSecretName");
        }
        {
            auto it = properties.find("serviceAccountId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saId");
        }
        {
            auto it = properties.find("serviceAccountIdSignature");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saSignature");
        }
        {
            auto it = properties.find("serviceAccountIdSignatureReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "secretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesAws) {
        THashMap<TString, TString> properties;
        NKikimrSchemeOp::TAuth auth;
        auto* aws = auth.MutableAws();
        aws->SetAwsAccessKeyIdSecretName("accessIdName");
        aws->SetAwsSecretAccessKeySecretName("accessSecretName");
        aws->SetAwsRegion("region");
        TExternalSourceAuth source(auth);
        source.InitSecretValues({"accessId", "accessSecret"});
        properties = source.BuildAuthProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 6);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "AWS");
        }
        {
            auto it = properties.find("awsAccessKeyId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessId");
        }
        {
            auto it = properties.find("awsSecretAccessKey");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessSecret");
        }
        {
            auto it = properties.find("awsAccessKeyIdReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessIdName");
        }
        {
            auto it = properties.find("awsSecretAccessKeyReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessSecretName");
        }
        {
            auto it = properties.find("awsRegion");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "region");
        }
    }

    // test the YQL pipeline from the moment of the alter table AST node creation to the TKiAlterTable TExprNode
    Y_UNIT_TEST(AlterTableAddIndexWithTableSettings) {
        NYql::TIssues issues;
        auto parserContext = CreateDefaultParserContext(issues);

        TAlterTableParameters params;
        {
            NSQLTranslationV1::TIndexDescription indexDescription(TIdentifier(parserContext.Pos(), "index"));
            indexDescription.TableSettings.MinPartitions = new TLiteralNumberNode<i32>(parserContext.Pos(), "Int32", "12345");
            indexDescription.TableSettings.MaxPartitions = new TLiteralNumberNode<i32>(parserContext.Pos(), "Int32", "54321");
            params.AddIndexes.emplace_back(std::move(indexDescription));
        }

        TString tableName = "table";
        auto* alterTableAst = CreateAlterTable(parserContext, tableName, params);

        TString tableSettingsAst;
        Find(alterTableAst, [&tableSettingsAst](const TAstNode* node) {
            if (node->IsList()
                && node->GetChildrenCount() == 2
                && node->GetChild(0)->ToString() == "'tableSettings"
            ) {
                tableSettingsAst = node->GetChild(1)->ToString();
                return true;
            }
            return false;
        });
        UNIT_ASSERT_STRINGS_EQUAL_C(
            tableSettingsAst, R"('('('minPartitions (Int32 '"12345")) '('maxPartitions (Int32 '"54321"))))",
            alterTableAst->ToString()
        );

        TExprContext exprContext;
        TExprNode::TPtr alterTableExpr;
        UNIT_ASSERT_C(CompileExpr(*alterTableAst, alterTableExpr, exprContext, nullptr, nullptr),
            exprContext.IssueManager.GetIssues().ToString()
        );

        UNIT_ASSERT_GT_C(alterTableExpr->ChildrenSize(), 4, alterTableExpr->Dump());
        const auto* writeSettingsNode = alterTableExpr->Child(4);
        std::optional<NCommon::TWriteTableSettings> writeSettings;
        try {
            writeSettings = NCommon::ParseWriteTableSettings(NNodes::TExprList(writeSettingsNode), exprContext);
        } catch (...) {
            UNIT_FAIL(CurrentExceptionMessage());
        }

        TKikimrKey key(exprContext);
        UNIT_ASSERT_C(key.Extract(*alterTableExpr->Child(2)), alterTableExpr->Child(2)->Dump());
        UNIT_ASSERT_STRINGS_EQUAL(key.GetTablePath(), tableName);
        UNIT_ASSERT(writeSettings->AlterActions);
        UNIT_ASSERT(!writeSettings->AlterActions.Cast().Empty());

        auto alterActions = TExprNode::TPtr(writeSettings->AlterActions.MutableRaw());
        TString tableSettingsExpr;
        VisitExpr(alterActions, [&](const TExprNode::TPtr& node) {
            if (node->IsList()
                && node->ChildrenSize() == 2
                && node->Child(0)->IsAtom("tableSettings")
            ) {
                tableSettingsExpr = NCommon::SerializeExpr(exprContext, *node->Child(1));
                return false;
            }
            return true;
        });
        UNIT_ASSERT_STRING_CONTAINS_C(
            tableSettingsExpr, R"('('('minPartitions (Int32 '"12345")) '('maxPartitions (Int32 '"54321"))))",
            NCommon::SerializeExpr(exprContext, *alterActions)
        );

        // the main result of the test is the TKiAlterTable TExprNode built from the initial alter table AST node
        UNIT_ASSERT_C(NNodes::Build<NNodes::TKiAlterTable>(exprContext, alterTableExpr->Pos())
            .World(alterTableExpr->Child(0))
            .DataSink(alterTableExpr->Child(1))
            .Table().Build(key.GetTablePath())
            .Actions(writeSettings->AlterActions.Cast())
            .TableType().Build("table")
            .Done()
            .Ptr(),
            alterTableExpr->Dump()
        );
    }
}

} // namespace NYql
