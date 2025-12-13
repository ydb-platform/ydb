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

namespace {

    class TYQLCompleter final : public IYQLCompleter {
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

    class TCompositeCommandCompleter final : public IYQLCompleter {
    public:
        TCompositeCommandCompleter(const std::vector<TString>& commands, const std::optional<TYQLCompleterConfig>& yqlCompleterConfig)
            : Commands(commands)
            , YQLCompleter(yqlCompleterConfig ? MakeYQLCompleter(*yqlCompleterConfig) : nullptr)
        {}

        TCompletions ApplyHeavy(TStringBuf text, const std::string& prefix, int& contextLen) final {
            if (RunCommandCompletion(text)) {
                auto completions = GetCommandCompletions(text, contextLen);

                TCompletions result;
                result.reserve(completions.size());
                for (auto& completion : completions) {
                    result.emplace_back(std::move(completion));
                }

                return result;
            }

            return YQLCompleter ? YQLCompleter->ApplyHeavy(text, prefix, contextLen) : TCompletions{};
        }

        THints ApplyLight(TStringBuf text, const std::string& prefix, int& contextLen) final {
            if (RunCommandCompletion(text)) {
                return GetCommandCompletions(text, contextLen);
            }

            return YQLCompleter ? YQLCompleter->ApplyLight(text, prefix, contextLen) : THints{};
        }

    private:
        bool RunCommandCompletion(TStringBuf text) const {
            return text.StartsWith('/');
        }

        THints GetCommandCompletions(TStringBuf text, int& contextLen) const {
            THints result;
            result.reserve(Commands.size());

            for (const auto& command : Commands) {
                if (command.StartsWith(text)) {
                    result.push_back(command);
                }
            }

            contextLen = text.size();
            return result;
        }

    private:
        const std::vector<TString> Commands;
        const IYQLCompleter::TPtr YQLCompleter;
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

} // anonymous namespace

    IYQLCompleter::TPtr MakeYQLCompleter(const TYQLCompleterConfig& settings) {
        NSQLComplete::TLexerSupplier lexer = MakePureLexerSupplier();

        auto ranking = NSQLComplete::MakeDefaultRanking(NSQLComplete::LoadFrequencyData());

        auto statics = NSQLComplete::MakeStaticNameService(NSQLComplete::LoadDefaultNameSet(), ranking);

        auto schema =
            NSQLComplete::MakeSchemaNameService(
                NSQLComplete::MakeSimpleSchema(
                    NSQLComplete::MakeCachedSimpleSchema(
                        MakeSchemaCaches(),
                        /* zone = */ "",
                        MakeYDBSchema(std::move(settings.Driver), std::move(settings.Database), settings.IsVerbose))));

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

        return MakeHolder<TYQLCompleter>(
            /* heavyEngine = */ NSQLComplete::MakeSqlCompletionEngine(lexer, heavy, config),
            /* lightEngine = */ NSQLComplete::MakeSqlCompletionEngine(lexer, light, config),
            std::move(settings.Color));
    }

    IYQLCompleter::TPtr MakeYQLCompositeCompleter(const std::vector<TString>& commands, const std::optional<TYQLCompleterConfig>& yqlCompleterConfig) {
        return MakeHolder<TCompositeCommandCompleter>(commands, yqlCompleterConfig);
    }

} // namespace NYdb::NConsoleClient
