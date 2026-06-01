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
#include <util/generic/hash_set.h>

#include <cctype>
#include <vector>

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

    // TCL keywords that are allowed to appear as the very first token of an
    // interactive transaction control line.
    static const std::vector<TString>& TclKeywords() {
        static const std::vector<TString> kKeywords = {
            "BEGIN", "START", "COMMIT", "END", "ROLLBACK",
        };
        return kKeywords;
    }

    // Splits text into whitespace-separated tokens. The last "incomplete" token
    // (without trailing whitespace) is returned as `partial`; everything before
    // it as `complete`. The token offset (byte position) is preserved so the
    // caller can compute contextLen as text.size() - partialOffset.
    struct TTokenization {
        std::vector<TStringBuf> Complete;
        TStringBuf Partial;
        size_t PartialOffset = 0;  // byte offset of the partial in the source
    };

    static TTokenization TokenizeForCompletion(TStringBuf text) {
        TTokenization out;
        out.PartialOffset = text.size();
        size_t i = 0;
        while (i < text.size()) {
            while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) {
                ++i;
            }
            if (i == text.size()) {
                break;
            }
            const size_t s = i;
            while (i < text.size() && !std::isspace(static_cast<unsigned char>(text[i]))) {
                ++i;
            }
            TStringBuf token = text.SubStr(s, i - s);
            if (i == text.size()) {
                out.Partial = token;
                out.PartialOffset = s;
            } else {
                out.Complete.push_back(token);
            }
        }
        return out;
    }

    static bool IEquals(TStringBuf a, TStringBuf b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (size_t i = 0; i < a.size(); ++i) {
            if (std::toupper(static_cast<unsigned char>(a[i]))
                != std::toupper(static_cast<unsigned char>(b[i]))) {
                return false;
            }
        }
        return true;
    }

    static bool IStartsWith(TStringBuf s, TStringBuf prefix) {
        if (s.size() < prefix.size()) {
            return false;
        }
        for (size_t i = 0; i < prefix.size(); ++i) {
            if (std::toupper(static_cast<unsigned char>(s[i]))
                != std::toupper(static_cast<unsigned char>(prefix[i]))) {
                return false;
            }
        }
        return true;
    }

    // Context-aware completer for TCL command lines.
    //
    // Given the user's input split into completed tokens and a partial last
    // token, the completer scans a precomputed list of "full TCL commands"
    // (e.g. "BEGIN TRANSACTION online-ro INCONSISTENT READS") and for every
    // match returns only the *next word* — never the whole tail. This produces
    // the same UX as the YQL completer (e.g. typing `BEGIN T<TAB>` proposes
    // `TRANSACTION`, not `TRANSACTION online-ro INCONSISTENT READS`).
    class TTclCompleter {
    public:
        explicit TTclCompleter(const std::vector<TString>& commands) {
            FullCommands.reserve(commands.size());
            for (const auto& cmd : commands) {
                std::vector<TString> tokens;
                size_t i = 0;
                while (i < cmd.size()) {
                    while (i < cmd.size() && std::isspace(static_cast<unsigned char>(cmd[i]))) {
                        ++i;
                    }
                    if (i == cmd.size()) {
                        break;
                    }
                    const size_t s = i;
                    while (i < cmd.size() && !std::isspace(static_cast<unsigned char>(cmd[i]))) {
                        ++i;
                    }
                    tokens.emplace_back(cmd.substr(s, i - s));
                }
                if (!tokens.empty()) {
                    FullCommands.push_back(std::move(tokens));
                }
            }
        }

        // Returns true if the input may be a TCL command (so that the YQL
        // completer should not be queried in addition). The check is permissive
        // by design: any input whose first token is a prefix of a TCL keyword
        // is treated as a TCL context.
        bool IsContext(const TTokenization& t) const {
            if (FullCommands.empty()) {
                return false;
            }
            const TStringBuf firstToken = t.Complete.empty() ? t.Partial : t.Complete.front();
            if (firstToken.empty()) {
                return false;
            }
            for (const auto& kw : TclKeywords()) {
                if (t.Complete.empty()) {
                    if (IStartsWith(kw, firstToken)) {
                        return true;
                    }
                } else if (IEquals(firstToken, kw)) {
                    return true;
                }
            }
            return false;
        }

        // Returns next-word candidates for the given tokenization. If no
        // candidates apply, returns an empty list and leaves contextLen
        // untouched. Otherwise contextLen is the length (in bytes) of the
        // partial token that should be replaced.
        std::vector<TString> Complete(const TTokenization& t, int& contextLen) const {
            std::vector<TString> result;
            THashSet<TString> seen;
            for (const auto& full : FullCommands) {
                if (full.size() <= t.Complete.size()) {
                    continue;
                }
                bool matched = true;
                for (size_t i = 0; i < t.Complete.size(); ++i) {
                    if (!IEquals(t.Complete[i], full[i])) {
                        matched = false;
                        break;
                    }
                }
                if (!matched) {
                    continue;
                }
                const TString& nextWord = full[t.Complete.size()];
                if (!IStartsWith(nextWord, t.Partial)) {
                    continue;
                }
                if (seen.insert(nextWord).second) {
                    result.push_back(nextWord);
                }
            }
            if (!result.empty()) {
                contextLen = t.Partial.size();
            }
            return result;
        }

    private:
        std::vector<std::vector<TString>> FullCommands;
    };

    class TCompositeCommandCompleter final : public IYQLCompleter {
    public:
        explicit TCompositeCommandCompleter(const TCompositeCompleterConfig& config)
            : SlashCommands(config.SlashCommands)
            , TclCompleter(config.TclCommands)
            , HasTclCompleter(!config.TclCommands.empty())
            , YQLCompleter(config.YqlCompleterConfig
                ? MakeYQLCompleter(*config.YqlCompleterConfig)
                : nullptr)
        {}

        TCompletions ApplyHeavy(TStringBuf text, const std::string& prefix, int& contextLen) final {
            const TStringBuf prefixView(prefix.data(), prefix.size());
            if (prefixView.StartsWith('/')) {
                return ToCompletions(MatchSlashCommands(prefixView, contextLen));
            }
            if (HasTclCompleter) {
                // Replxx may pass only the current word as `prefix`; TCL completion
                // needs the full input line (e.g. "BEGIN s", not just "s").
                const TStringBuf lineView = !text.empty() ? text : prefixView;
                const auto tokenization = TokenizeForCompletion(lineView);
                if (TclCompleter.IsContext(tokenization)) {
                    int tclCtx = 0;
                    auto tclWords = TclCompleter.Complete(tokenization, tclCtx);
                    contextLen = tclCtx;
                    return ToCompletions(THints(tclWords.begin(), tclWords.end()));
                }
            }
            return YQLCompleter ? YQLCompleter->ApplyHeavy(text, prefix, contextLen) : TCompletions{};
        }

        THints ApplyLight(TStringBuf text, const std::string& prefix, int& contextLen) final {
            const TStringBuf prefixView(prefix.data(), prefix.size());
            if (prefixView.StartsWith('/')) {
                return MatchSlashCommands(prefixView, contextLen);
            }
            if (HasTclCompleter) {
                const TStringBuf lineView = !text.empty() ? text : prefixView;
                const auto tokenization = TokenizeForCompletion(lineView);
                if (TclCompleter.IsContext(tokenization)) {
                    int tclCtx = 0;
                    auto tclWords = TclCompleter.Complete(tokenization, tclCtx);
                    contextLen = tclCtx;
                    return THints(tclWords.begin(), tclWords.end());
                }
            }
            return YQLCompleter ? YQLCompleter->ApplyLight(text, prefix, contextLen) : THints{};
        }

    private:
        THints MatchSlashCommands(TStringBuf prefix, int& contextLen) const {
            THints result;
            for (const auto& cmd : SlashCommands) {
                if (cmd.StartsWith(prefix)) {
                    result.push_back(cmd);
                }
            }
            contextLen = prefix.size();
            return result;
        }

        static TCompletions ToCompletions(THints hints) {
            TCompletions out;
            out.reserve(hints.size());
            for (auto& h : hints) {
                out.emplace_back(std::move(h));
            }
            return out;
        }

    private:
        const std::vector<TString> SlashCommands;
        const TTclCompleter TclCompleter;
        const bool HasTclCompleter;
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
                        MakeYDBSchema(settings.LazyDriver, settings.Database, settings.IsVerbose))));

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

    IYQLCompleter::TPtr MakeYQLCompositeCompleter(const TCompositeCompleterConfig& config) {
        return MakeHolder<TCompositeCommandCompleter>(config);
    }

} // namespace NYdb::NConsoleClient
