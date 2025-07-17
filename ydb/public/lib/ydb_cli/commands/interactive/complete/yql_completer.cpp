#include "yql_completer.h"

#include "ydb_schema.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/cached/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/impatient/name_service.h>
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

            if (light &&
                completion.Candidates.size() == 1 &&
                !completion.Candidates[0].Content.StartsWith(completion.CompletedToken.Content)) {
                completion.Candidates.push_back({
                    // Disable inline hint
                    .Kind = NSQLComplete::ECandidateKind::Keyword,
                    .Content = " ",
                });
            }

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
                entries.emplace_back(ReplxxCompletionOf(std::move(candidate)));
            }
            return entries;
        }

        replxx::Replxx::Completion ReplxxCompletionOf(NSQLComplete::TCandidate candidate) const {
            Y_ENSURE(candidate.CursorShift <= candidate.Content.length());
            const size_t prefixLen = candidate.Content.length() - candidate.CursorShift;
            candidate.Content.resize(prefixLen);

            Y_ENSURE(!candidate.Content.empty());
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
                case NSQLComplete::ECandidateKind::ColumnName:
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

    NSQLComplete::TSchemaCaches MakeSchemaCaches() {
        using TKey = NSQLComplete::TSchemaDescribeCacheKey;

        auto time = NMonotonic::CreateDefaultMonotonicTimeProvider();

        NSQLComplete::TLocalCacheConfig config = {
            .ByteCapacity = 1 * 1024 * 1024,
            .TTL = TDuration::Seconds(8),
        };

        return {
            .List = NSQLComplete::MakeLocalCache<
                TKey, TVector<NSQLComplete::TFolderEntry>>(time, config),
            .DescribeTable = NSQLComplete::MakeLocalCache<
                TKey, TMaybe<NSQLComplete::TTableDetails>>(time, config),
        };
    }

    IYQLCompleter::TPtr MakeYQLCompleter(
        TColorSchema color, TDriver driver, TString database, bool isVerbose) {
        NSQLComplete::TLexerSupplier lexer = MakePureLexerSupplier();

        auto ranking = NSQLComplete::MakeDefaultRanking(NSQLComplete::LoadFrequencyData());

        auto statics = NSQLComplete::MakeStaticNameService(NSQLComplete::LoadDefaultNameSet(), ranking);

        auto schema =
            NSQLComplete::MakeSchemaNameService(
                NSQLComplete::MakeSimpleSchema(
                    NSQLComplete::MakeCachedSimpleSchema(
                        MakeSchemaCaches(),
                        /* zone = */ "",
                        MakeYDBSchema(std::move(driver), std::move(database), isVerbose))));

        auto heavy = NSQLComplete::MakeUnionNameService(
            {
                statics,
                schema,
            }, ranking);

        auto light = NSQLComplete::MakeUnionNameService(
            {
                statics,
                NSQLComplete::MakeImpatientNameService(schema),
            }, ranking);

        auto config = NSQLComplete::MakeYDBConfiguration();

        return IYQLCompleter::TPtr(new TYQLCompleter(
            /* heavyEngine = */ NSQLComplete::MakeSqlCompletionEngine(lexer, heavy, config),
            /* lightEngine = */ NSQLComplete::MakeSqlCompletionEngine(lexer, light, config),
            std::move(color)));
    }

} // namespace NYdb::NConsoleClient
