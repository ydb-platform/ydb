#include "text_document.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NYql::NJson {

YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TPosition);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TRange);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TTextDocumentIdentifier);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TTextDocumentItem);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TTextDocumentPositionParams);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TTextEdit);
YQL_DERIVE_JSON_BIDIRECTIONAL(NLsp::TLocation);

} // namespace NYql::NJson
