#include "yql_completer.h"

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/static/name_service.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

namespace NYdb::NConsoleClient {

    class TYQLCompleter: public IYQLCompleter {
    public:
        using TPtr = THolder<IYQLCompleter>;

        explicit TYQLCompleter(NSQLComplete::ISqlCompletionEngine::TPtr engine)
            : Engine(std::move(engine))
        {
        }

        TCompletions Apply(const std::string& prefix, int& /* contextLen */) override {
            auto completion = Engine->Complete({
                .Text = prefix,
                .CursorPosition = prefix.length(),
            });

            replxx::Replxx::completions_t entries;
            for (auto& candidate : completion.Candidates) {
                candidate.Content += ' ';
                entries.emplace_back(
                    std::move(candidate.Content),
                    ReplxxColorOf(candidate.Kind));
            }
            return entries;
        }

    private:
        static replxx::Replxx::Color ReplxxColorOf(NSQLComplete::ECandidateKind /* kind */) {
            return replxx::Replxx::Color::DEFAULT;
        }

        NSQLComplete::ISqlCompletionEngine::TPtr Engine;
    };

    IYQLCompleter::TPtr MakeYQLCompleter() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();

        auto names = NSQLComplete::MakeDefaultNameSet();
        NSQLComplete::INameService::TPtr service = MakeStaticNameService(std::move(names));

        return IYQLCompleter::TPtr(new TYQLCompleter(
            NSQLComplete::MakeSqlCompletionEngine([lexers = std::move(lexers)](bool ansi) {
                return NSQLTranslationV1::MakeLexer(
                    lexers, ansi, /* antlr4 = */ true, 
                    NSQLTranslationV1::ELexerFlavor::Pure);
            }, std::move(service))));
    }

} // namespace NYdb::NConsoleClient
