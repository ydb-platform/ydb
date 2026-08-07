#include "synchronization.h"

namespace NLsp {

bool TTextDocumentContentChangeEvent::IsIncremental() const {
    return Range.Defined();
}

} // namespace NLsp

namespace NYql::NJson {

JSON_DEFINE_FROM(NLsp::TVersionedTextDocumentIdentifier, json) {
    NLsp::TVersionedTextDocumentIdentifier x;
    JSON_MOVE_FROM(json, "uri", x.Uri);
    JSON_MOVE_FROM(json, "version", x.Version);
    return x;
}

JSON_DEFINE_FROM(NLsp::TDidOpenTextDocumentParams, json) {
    NLsp::TDidOpenTextDocumentParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    return x;
}

JSON_DEFINE_FROM(NLsp::TTextDocumentContentChangeEvent, json) {
    NLsp::TTextDocumentContentChangeEvent x;
    JSON_MOVE_FROM(json, "range", x.Range);
    JSON_MOVE_FROM(json, "text", x.Text);
    return x;
}

JSON_DEFINE_FROM(NLsp::TDidChangeTextDocumentParams, json) {
    NLsp::TDidChangeTextDocumentParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    JSON_MOVE_FROM(json, "contentChanges", x.ContentChanges);
    return x;
}

JSON_DEFINE_FROM(NLsp::TDidCloseTextDocumentParams, json) {
    NLsp::TDidCloseTextDocumentParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    return x;
}

} // namespace NYql::NJson
