#include "completion.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>
#include <yql/essentials/tools/yql_language_server/lsp/support/position.h>

#include <yql/essentials/sql/v1/ide/completion/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

namespace NLsp::NYql {

namespace {

NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers, ansi, /*antlr4=*/true,
            NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

} // namespace

TCompletionService::TCompletionService(NSQLComplete::ISqlCompletionEngine::TPtr engine)
    : Radix_(TRadix::SimpleAlphabet())
    , Engine_(std::move(engine))
{
}

TCompletionList TCompletionService::Completion(TStringBuf text, const TCompletionParams& params) const {
    size_t position = ToBytes(params.Position, text);

    const NSQLComplete::TCompletionInput input = {
        .Text = text,
        .CursorPosition = position,
    };

    auto completion = Engine_->CompleteAsync(input).ExtractValueSync();
    return ToMessage(completion.Candidates);
}

TString TCompletionService::SortText(size_t index, size_t length) const {
    return Radix_.Encode(index, length);
}

ECompletionItemKind TCompletionService::ToMessage(NSQLComplete::ECandidateKind kind) {
    using NSQLComplete::ECandidateKind;
    switch (kind) {
        case ECandidateKind::Keyword:
            return ECompletionItemKind::Keyword;
        case ECandidateKind::TypeName:
            return ECompletionItemKind::TypeParameter;
        case ECandidateKind::FunctionName:
            return ECompletionItemKind::Function;
        case ECandidateKind::PragmaName:
            return ECompletionItemKind::Property;
        case ECandidateKind::HintName:
            return ECompletionItemKind::EnumMember;
        case ECandidateKind::FolderName:
            return ECompletionItemKind::Folder;
        case ECandidateKind::TableName:
            return ECompletionItemKind::File;
        case ECandidateKind::ClusterName:
            return ECompletionItemKind::Module;
        case ECandidateKind::ColumnName:
            return ECompletionItemKind::Field;
        case ECandidateKind::BindingName:
            return ECompletionItemKind::Variable;
        case ECandidateKind::UnknownName:
            return ECompletionItemKind::EnumMember;
    }
}

TCompletionItem TCompletionService::ToMessage(
    NSQLComplete::TCandidate candidate, size_t index, size_t sortTextLen) const {
    TString label = candidate.Content;

    TString sortText = SortText(index, sortTextLen);

    const bool isSnippet = (candidate.CursorShift > 0);

    TMaybe<TString> insertText;
    if (isSnippet) {
        size_t position = label.size() - candidate.CursorShift;
        insertText = label.substr(0, position) + "$0" + label.substr(position);
    }

    EInsertTextFormat insertTextFormat =
        isSnippet
            ? EInsertTextFormat::Snippet
            : EInsertTextFormat::PlainText;

    TMaybe<TString> detail;
    if (candidate.Documentation) {
        detail = "See details";
    }

    TMaybe<TMarkupContent> documentation;
    if (candidate.Documentation) {
        documentation = TMarkupContent{
            .Kind = EMarkupKind::Markdown,
            .Value = std::move(*candidate.Documentation),
        };
    }

    return {
        .Label = std::move(label),
        .Kind = ToMessage(candidate.Kind),
        .Detail = std::move(detail),
        .Documentation = std::move(documentation),
        .SortText = std::move(sortText),
        .FilterText = candidate.FilterText(),
        .InsertText = std::move(insertText),
        .InsertTextFormat = insertTextFormat,
    };
}

TCompletionList TCompletionService::ToMessage(TVector<NSQLComplete::TCandidate> candidates) const {
    const size_t sortTextLen =
        candidates.empty() ? 0 : Radix_.Encode(candidates.size() - 1).size();

    TVector<TCompletionItem> items(Reserve(candidates.size()));
    for (size_t i = 0; i < candidates.size(); ++i) {
        items.emplace_back(ToMessage(std::move(candidates[i]), i, sortTextLen));
    }

    return {
        .IsIncomplete = false,
        .Items = std::move(items),
    };
}

TCompletionService::TPtr MakeCompletionService() {
    NSQLComplete::TLexerSupplier lexer = MakePureLexerSupplier();

    auto ranking = NSQLComplete::MakeDefaultRanking(NSQLComplete::LoadFrequencyData());
    auto statics = NSQLComplete::MakeStaticNameService(NSQLComplete::LoadDefaultNameSet(), ranking);

    auto config = NSQLComplete::MakeYQLConfiguration();

    auto engine = NSQLComplete::MakeSqlCompletionEngine(
        std::move(lexer), std::move(statics), std::move(config), std::move(ranking));

    return new TCompletionService(std::move(engine));
}

} // namespace NLsp::NYql
