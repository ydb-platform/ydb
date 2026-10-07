#include "synchronization.h"

#include <yql/essentials/utils/json/reflection.h>

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

YQL_DERIVE_JSON_FROM(NLsp::TDidOpenTextDocumentParams);
YQL_DERIVE_JSON_FROM(NLsp::TTextDocumentContentChangeEvent);
YQL_DERIVE_JSON_FROM(NLsp::TDidChangeTextDocumentParams);
YQL_DERIVE_JSON_FROM(NLsp::TDidCloseTextDocumentParams);

} // namespace NYql::NJson
