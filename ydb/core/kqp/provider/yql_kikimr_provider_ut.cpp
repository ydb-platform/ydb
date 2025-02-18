#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/v1/source.h>
#include <yql/essentials/sql/v1/context.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

using namespace NSQLTranslationV1;

TContext CreateDefaultParserContext(NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DefaultCluster = "/Cluster";
    settings.AssumeYdbOnClusterWithSlash = true;

    return TContext(settings, { /* hints */ }, issues, { /* query */ });
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

}

Y_UNIT_TEST_SUITE(KikimrProvider) {
    Y_UNIT_TEST(TestFillAuthPropertiesNone) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        source.DataSourceAuth.MutableNone();
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 1);
        auto it = properties.find("authMethod");
        UNIT_ASSERT(it != properties.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, "NONE");
    }

    Y_UNIT_TEST(TestFillAuthPropertiesServiceAccount) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableServiceAccount();
        sa.SetId("saId");
        sa.SetSecretName("secretName");
        source.ServiceAccountIdSignature = "saSignature";
        FillAuthProperties(properties, source);
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
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableBasic();
        sa.SetLogin("login");
        sa.SetPasswordSecretName("passwordSecretName");
        source.Password = "password";
        FillAuthProperties(properties, source);
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
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableMdbBasic();
        sa.SetServiceAccountId("saId");
        sa.SetServiceAccountSecretName("secretName");
        source.ServiceAccountIdSignature = "saSignature";
        sa.SetLogin("login");
        sa.SetPasswordSecretName("passwordSecretName");
        source.Password = "password";
        FillAuthProperties(properties, source);
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
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableAws();
        sa.SetAwsAccessKeyIdSecretName("accessIdName");
        sa.SetAwsSecretAccessKeySecretName("accessSecretName");
        sa.SetAwsRegion("region");
        source.AwsAccessKeyId = "accessId";
        source.AwsSecretAccessKey = "accessSecret";
        FillAuthProperties(properties, source);
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
