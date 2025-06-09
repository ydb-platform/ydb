#include "yql_completer.h"

#include "ydb_schema.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/cached/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <util/charset/utf8.h>

namespace NYdb::NConsoleClient {

    class TYQLCompleter: public IYQLCompleter {
    public:
        using TPtr = THolder<IYQLCompleter>;

        TYQLCompleter(
            NSQLComplete::ISqlCompletionEngine::TPtr heavyEngine,
            NSQLComplete::ISqlCompletionEngine::TPtr lightEngine,
            TColorSchema color)
            : HeavyEngine(std::move(heavyEngine))
            , LightEngine(std::move(lightEngine))
            , Color(std::move(color))
        {
        }

        TCompletions ApplyHeavy(TStringBuf text, const std::string& prefix, int& contextLen) override {
            return Apply(text, prefix, contextLen, /* light = */ false);
        }

        THints ApplyLight(TStringBuf text, const std::string& prefix, int& contextLen) override {
            replxx::Replxx::hints_t hints;
            for (auto& candidate : Apply(text, prefix, contextLen, /* light = */ true)) {
                hints.emplace_back(std::move(candidate.text()));
            }
            return hints;
        }

    private:
        TCompletions Apply(TStringBuf text, const std::string& prefix, int& contextLen, bool light) {
            NSQLComplete::TCompletionInput input = {
                .Text = text,
                .CursorPosition = prefix.length(),
            };

            auto completion = GetEngine(light)->CompleteAsync(input).ExtractValueSync();

            contextLen = GetNumberOfUTF8Chars(completion.CompletedToken.Content);

            return ReplxxCompletionsOf(std::move(completion.Candidates));
        }

        NSQLComplete::ISqlCompletionEngine::TPtr& GetEngine(bool light) {
            if (light) {
                return LightEngine;
            }
            return HeavyEngine;
        }

        replxx::Replxx::completions_t ReplxxCompletionsOf(TVector<NSQLComplete::TCandidate> candidates) const {
            replxx::Replxx::completions_t entries;
            entries.reserve(candidates.size());
            for (auto& candidate : candidates) {
                if (candidate.Kind == NSQLComplete::ECandidateKind::FolderName &&
                    candidate.Content.EndsWith('`')) {
                    candidate.Content.pop_back();
                }
                entries.emplace_back(ReplxxCompletionOf(std::move(candidate)));
            }
            return entries;
        }

        replxx::Replxx::Completion ReplxxCompletionOf(NSQLComplete::TCandidate candidate) const {
            const auto back = candidate.Content.back();
            if (
                !(candidate.Kind == NSQLComplete::ECandidateKind::FolderName ||
                  (candidate.Kind == NSQLComplete::ECandidateKind::TableName) &&
                      (!IsLeftPunct(back) && back != '<' || IsQuotation(back)))) {
                candidate.Content += ' ';
            }

            return {
                std::move(candidate.Content),
                ReplxxColorOf(candidate.Kind),
            };
        }

        replxx::Replxx::Color ReplxxColorOf(NSQLComplete::ECandidateKind kind) const {
            switch (kind) {
                case NSQLComplete::ECandidateKind::Keyword:
                    return Color.keyword;
                case NSQLComplete::ECandidateKind::TypeName:
                    return Color.identifier.type;
                case NSQLComplete::ECandidateKind::FunctionName:
                    return Color.identifier.function;
                case NSQLComplete::ECandidateKind::FolderName:
                case NSQLComplete::ECandidateKind::TableName:
                    return Color.identifier.quoted;
                case NSQLComplete::ECandidateKind::BindingName:
                    return Color.identifier.variable;
                default:
                    return replxx::Replxx::Color::DEFAULT;
            }
        }

        NSQLComplete::ISqlCompletionEngine::TPtr HeavyEngine;
        NSQLComplete::ISqlCompletionEngine::TPtr LightEngine;
        TColorSchema Color;
    };

    NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
        return [lexers = std::move(lexers)](bool ansi) {
            return NSQLTranslationV1::MakeLexer(
                lexers, ansi, /* antlr4 = */ true,
                NSQLTranslationV1::ELexerFlavor::Pure);
        };
    }

    NSQLComplete::ISchemaListCache::TPtr MakeSchemaCache() {
        using TKey = NSQLComplete::TSchemaListCacheKey;
        using TValue = TVector<NSQLComplete::TFolderEntry>;

        return NSQLComplete::MakeLocalCache<TKey, TValue>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(),
            /* config = */ {});
    }

    IYQLCompleter::TPtr MakeYQLCompleter(
        TColorSchema color, TDriver driver, TString database, bool isVerbose) {
        NSQLComplete::TLexerSupplier lexer = MakePureLexerSupplier();

        auto ranking = NSQLComplete::MakeDefaultRanking(NSQLComplete::LoadFrequencyData());

        TVector<NSQLComplete::INameService::TPtr> services = {
            NSQLComplete::MakeStaticNameService(
                NSQLComplete::LoadDefaultNameSet(), ranking),

            NSQLComplete::MakeSchemaNameService(
                NSQLComplete::MakeSimpleSchema(
                    NSQLComplete::MakeCachedSimpleSchema(
                        MakeSchemaCache(),
                        /* zone = */ "",
                        MakeYDBSchema(std::move(driver), std::move(database), isVerbose)))),
        };

        return IYQLCompleter::TPtr(new TYQLCompleter(
            NSQLComplete::MakeSqlCompletionEngine(
                lexer,
                NSQLComplete::MakeUnionNameService(services, ranking)),
            NSQLComplete::MakeSqlCompletionEngine(
                lexer,
                NSQLComplete::MakeUnionNameService(services, ranking)),
            std::move(color)));
    }

} // namespace NYdb::NConsoleClient
