#include <library/cpp/testing/unittest/registar.h>

#include "check_format.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <yql/essentials/utils/string/trim_indent.h>

#include <google/protobuf/arena.h>

Y_UNIT_TEST_SUITE(CheckedFormat) {

Y_UNIT_TEST(PgSyntax) {
    google::protobuf::Arena arena;

    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;

    TString query = NYql::TrimIndent(R"sql(
        --!syntax_pg
        SELECT
            convert_from(a, 'UTF8')
        FROM
            plato.x
        WHERE
            convert_from(b, 'UTF8') !~ '^[0-9]+$';
    )sql");

    NYql::TIssues issues;
    auto formatted = NSQLFormat::CheckedFormat(
        query, Nothing(), settings, issues, NSQLFormat::EConvergenceRequirement::Double);

    UNIT_ASSERT_C(formatted.Defined(), issues.ToString());
    UNIT_ASSERT(!formatted->empty());
}

Y_UNIT_TEST(QueryText) {
    TString query = NYql::TrimIndent(R"sql(
        USE plato;

        CREATE VIEW x AS DO BEGIN
            SELECT 1;
        END DO;
    )sql");

    google::protobuf::Arena arena;

    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.ClusterMapping["plato"] = NYql::YtProviderName;
    settings.LangVer = NYql::GetMaxLangVersion();

    NYql::TIssues issues;
    auto formatted = NSQLFormat::CheckedFormat(
        query, Nothing(), settings, issues, NSQLFormat::EConvergenceRequirement::Double);

    UNIT_ASSERT_C(formatted.Defined(), issues.ToString());
}

Y_UNIT_TEST(EffectiveSettings) {
    TString query = NYql::TrimIndent(R"sql(
        --!ansi_lexer
        /* /* */ */ select 1 /* /* */ */
    )sql");

    google::protobuf::Arena arena;

    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;

    NYql::TIssues issues;
    auto formatted = NSQLFormat::CheckedFormat(
        query, Nothing(), settings, issues, NSQLFormat::EConvergenceRequirement::Double);

    UNIT_ASSERT_C(formatted.Defined(), issues.ToString());
}

} // Y_UNIT_TEST_SUITE(CheckedFormat)
