#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/host/new_parser/kqp_pragma.h>
#include <ydb/core/kqp/host/new_parser/kqp_type_annotation.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/config/yql_config_provider.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;
using namespace NYql;

struct TFixture {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    std::unique_ptr<TScript> Ast;

    explicit TFixture(TAllowSettingPolicy policy = {}) {
        auto provider = CreateConfigProvider(Types, nullptr, {}, policy);
        UNIT_ASSERT(provider->Initialize(Ctx));
        Types.AddDataSource("config", provider);
    }

    bool Apply(const TString& sql) {
        auto parsed = ParseQueryWithNewParser(sql, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
        auto references = CollectTableReferences(*Ast, {});
        UNIT_ASSERT_C(references.IsOk(), references.Issues.ToString());
        return ApplyProviderPragmas(*Ast, references.ProviderPragmas, Ctx, Types);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlPragma) {
    Y_UNIT_TEST(ConfigFlagsAffectScalarTypes) {
        TFixture f;
        f.Types.UpdateDecimalConversionMode(EDecimalConversionMode::WithoutCommonTypeFixup);
        UNIT_ASSERT(f.Apply(R"(
            PRAGMA config.flags("DecimalCommonTypeConversionMode", "with_common_type_fixup");
            SELECT CAST('1' AS Decimal(12,2)) AS value
            UNION ALL SELECT CAST('2' AS Decimal(15,2)) AS value;
        )"));
        UNIT_ASSERT(f.Types.GetDecimalConversionMode() == EDecimalConversionMode::WithCommonTypeFixup);
        auto names = ResolveNames(*f.Ast, {}, {});
        UNIT_ASSERT_C(names.IsOk(), names.Issues.ToString());
        const auto annotated = AnnotateAst(*f.Ast, *names.Names, f.Ctx, f.Types);
        UNIT_ASSERT_C(annotated, f.Ctx.IssueManager.GetIssues().ToString());
        const auto& query = *std::get<TQueryStatement>(f.Ast->Statements[1].Value).Query;
        UNIT_ASSERT_VALUES_EQUAL(FormatType(annotated->QueryOutputs.at(&query).at(0)), "Optional<Decimal(15,2)>");
    }

    Y_UNIT_TEST(ConfigFlagsKeepOrderAndArgumentCase) {
        TFixture f;
        UNIT_ASSERT(f.Apply("PRAGMA CoNfIg.Fl_Ags('UseBlocks'); PRAGMA config.flags('BlockEngine', 'force'); "
            "PRAGMA config.flags('BlockEngine', 'disable'); "
            "PRAGMA config.flags('OptimizerFlags', 'First', 'SECOND'); SELECT 1;"));
        UNIT_ASSERT(f.Types.UseBlocks);
        UNIT_ASSERT(f.Types.BlockEngineMode == EBlockEngineMode::Disable);
        UNIT_ASSERT(f.Types.OptimizerFlags.contains("first"));
        UNIT_ASSERT(f.Types.OptimizerFlags.contains("second"));
    }

    Y_UNIT_TEST(ConfigFlagsUseProviderValidationAndPolicy) {
        for (const TString args : {"'UnknownFlag'", "'DecimalCommonTypeConversionMode'",
            "'DecimalCommonTypeConversionMode', 'invalid'", "'_SystemFlag'", "'useblocks'"}) {
            TFixture f;
            UNIT_ASSERT_C(!f.Apply("PRAGMA config.flags(" + args + "); SELECT 1;"), args);
            UNIT_ASSERT(!f.Ctx.IssueManager.GetIssues().Empty());
        }
        TFixture restricted([](TStringBuf name) { return name == "UseBlocks"; });
        UNIT_ASSERT(!restricted.Apply("PRAGMA config.flags('OptimizerFlags', 'flag'); SELECT 1;"));
        UNIT_ASSERT_STRING_CONTAINS(restricted.Ctx.IssueManager.GetIssues().ToString(), "is not allowed");
        UNIT_ASSERT(restricted.Types.OptimizerFlags.empty());
    }
}

} // namespace NKikimr::NKqp
