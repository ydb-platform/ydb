#include <yql/essentials/fuzz_targets/sql_feature_parse_common.h>
#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/lexer_common/parse_hints_impl.h>

using namespace NYql::NFuzzSql;

namespace {

TString BuildHintComment(FuzzedDataProvider& provider) {
    const TString name = Identifier(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 24)), "Hint");
    const TString value = QuoteSqlString(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 64)));
    TStringBuilder out;
    if (provider.ConsumeBool()) {
        out << "/*+ " << name << "(" << value << " token another_token) */";
    } else {
        out << "--+ " << name << "(" << value << " token another_token)\n";
    }
    return Limit(out, 1024);
}

void ExerciseHintComment(TStringBuf comment, bool utf8Aware) {
    try {
        auto hints = NSQLTranslation::NDetail::ParseSqlHints({}, comment, utf8Aware);
        for (const auto& hint : hints) {
            (void)hint.ToString();
        }
    } catch (...) {
    }
}

void ExerciseCollectHints(const TString& query, const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    google::protobuf::Arena arena;
    auto settings = MakeSettings(provider, arena);
    try {
        NYql::TIssues issues;
        ui16 actualSyntaxVersion = 0;
        auto lexer = NSQLTranslation::SqlLexer(GetTranslators(), query, issues, settings, &actualSyntaxVersion);
        if (!lexer) {
            return;
        }
        NSQLTranslation::TSQLHints hints;
        (void)NSQLTranslation::CollectSqlHints(
            *lexer,
            query,
            "fuzz.sql",
            settings.File,
            hints,
            issues,
            settings.MaxErrors,
            provider.ConsumeBool());
        for (const auto& [pos, items] : hints) {
            (void)pos;
            for (const auto& hint : items) {
                (void)hint.ToString();
            }
        }
        (void)actualSyntaxVersion;
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseHintComment(raw, false);
    ExerciseHintComment(raw, true);

    FuzzedDataProvider provider(data, size);
    const TString hint = BuildHintComment(provider);
    ExerciseHintComment(hint, provider.ConsumeBool());

    const TString query = hint + " SELECT key FROM plato.Input WHERE value = " +
        QuoteSqlString(provider.ConsumeRemainingBytesAsString()) + ";";
    ExerciseCollectHints(query, data, size);
    ExerciseSqlWithFreshSettings(query, data, size);
    return 0;
}
