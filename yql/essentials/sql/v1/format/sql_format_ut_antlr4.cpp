#include <library/cpp/testing/unittest/registar.h>

#include "sql_format.h"

#include <google/protobuf/arena.h>
#include <util/string/subst.h>
#include <util/string/join.h>

namespace {

using TCases = TVector<std::pair<TString, TString>>;

struct TSetup {
    TSetup(bool ansiLexer = false) {
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &Arena;
        settings.Antlr4Parser = true;
        settings.AnsiLexer = ansiLexer;
        Formatter = NSQLFormat::MakeSqlFormatter(settings);
    }

    void Run(const TCases& cases, NSQLFormat::EFormatMode mode = NSQLFormat::EFormatMode::Pretty) {
        for (const auto& c : cases) {
            NYql::TIssues issues;
            TString formatted;
            auto res = Formatter->Format(c.first, formatted, issues, mode);
            UNIT_ASSERT_C(res, issues.ToString());
            auto expected = c.second;
            SubstGlobal(expected, "\t", TString(NSQLFormat::OneIndent, ' '));
            UNIT_ASSERT_NO_DIFF(formatted, expected);

            TString formatted2;
            auto res2 = Formatter->Format(formatted, formatted2, issues);
            UNIT_ASSERT_C(res2, issues.ToString());
            UNIT_ASSERT_NO_DIFF(formatted, formatted2);

            if (mode == NSQLFormat::EFormatMode::Pretty) {
                auto mutatedQuery = NSQLFormat::MutateQuery(c.first);
                auto res3 = Formatter->Format(mutatedQuery, formatted, issues);
                UNIT_ASSERT_C(res3, issues.ToString());
            }
        }
    }

    google::protobuf::Arena Arena;
    NSQLFormat::ISqlFormatter::TPtr Formatter;
};

}

Y_UNIT_TEST_SUITE(CheckSqlFormatter) {
    #include "sql_format_ut.h"
}
