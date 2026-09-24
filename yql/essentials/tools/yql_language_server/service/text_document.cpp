#include "text_document.h"

namespace NLsp::NYql {

TTextDocument::TTextDocument(TTextDocumentItem item, IParser::TPtr parser)
    : Parser_(std::move(parser))
    , Item_(std::move(item))
    , ParseTree_(Parser_->Parse(Item_.Text))
{
}

void TTextDocument::Change(TTextDocumentVersion version, TMaybe<TString> text) {
    IParseTree::TPtr parseTree;

    Y_ENSURE(Item_.Version <= version);
    if (text) {
        parseTree = Parser_->Parse(*text);
    }

    Item_.Version = version;
    if (text) {
        Item_.Text = std::move(*text);
        ParseTree_ = std::move(parseTree);
    }
}

TTextDocumentVersion TTextDocument::Version() const {
    return Item_.Version;
}

TDocumentUri TTextDocument::Uri() const {
    return Item_.Uri;
}

TStringBuf TTextDocument::Text() const {
    return ParseTree_->Text();
}

const antlr4::CommonTokenStream& TTextDocument::Tokens() const {
    return ParseTree_->Tokens();
}

const SQLv1& TTextDocument::Parser() const {
    return ParseTree_->Parser();
}

SQLv1::Sql_queryContext* TTextDocument::Root() {
    return ParseTree_->Root();
}

TTextDocuments::TPtr MakeTextDocuments(IParser::TPtr parser) {
    return new TTextDocuments([parser = std::move(parser)](TTextDocumentItem item) {
        return TTextDocument::TPtr(new TTextDocument(std::move(item), parser));
    });
}

} // namespace NLsp::NYql
