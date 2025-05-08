#include "yql_completer.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/complete/ydb_schema.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
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

        explicit TYQLCompleter(NSQLComplete::ISqlCompletionEngine::TPtr engine, TColorSchema color)
            : Engine(std::move(engine))
            , Color(std::move(color))
        {
        }

        TCompletions Apply(TStringBuf text, const std::string& prefix, int& contextLen) override {
            auto completion = Engine->Complete({
                .Text = text,
                .CursorPosition = prefix.length(),
            });

            contextLen = GetNumberOfUTF8Chars(completion.CompletedToken.Content);

            contextLen = GetNumberOfUTF8Chars(completion.CompletedToken.Content);

            replxx::Replxx::completions_t entries;
            for (auto& candidate : completion.Candidates) {
                const auto back = candidate.Content.back();
                if (
                    !(candidate.Kind == NSQLComplete::ECandidateKind::FolderName || candidate.Kind == NSQLComplete::ECandidateKind::TableName) && (!IsLeftPunct(back) && back != '<' || IsQuotation(back))) {
                    candidate.Content += ' ';
                }

                entries.emplace_back(
                    std::move(candidate.Content),
                    ReplxxColorOf(candidate.Kind));
            }
            return entries;
        }

    private:
        replxx::Replxx::Color ReplxxColorOf(NSQLComplete::ECandidateKind kind) {
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
                default:
                    return replxx::Replxx::Color::DEFAULT;
            }
        }

        NSQLComplete::ISqlCompletionEngine::TPtr Engine;
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

    IYQLCompleter::TPtr MakeYQLCompleter(TColorSchema color, TDriver driver, TString database) {
        NSQLComplete::TLexerSupplier lexer = MakePureLexerSupplier();

        NSQLComplete::NameSet names = NSQLComplete::MakeDefaultNameSet();

        NSQLComplete::IRanking::TPtr ranking = NSQLComplete::MakeDefaultRanking();

        NSQLComplete::INameService::TPtr service = NSQLComplete::MakeUnionNameService({
                                                                                          NSQLComplete::MakeStaticNameService(std::move(names), ranking),
                                                                                          NSQLComplete::MakeSchemaNameService(
                                                                                              MakeYDBSchema(std::move(driver), std::move(database))),
                                                                                      }, ranking);

        return IYQLCompleter::TPtr(new TYQLCompleter(
            NSQLComplete::MakeSqlCompletionEngine(std::move(lexer), std::move(service)),
            std::move(color)));
    }

} // namespace NYdb::NConsoleClient
