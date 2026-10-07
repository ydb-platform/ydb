#include "formatting.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NYql::NJson {

YQL_DERIVE_JSON_FROM(NLsp::TFormattingOptions);
YQL_DERIVE_JSON_FROM(NLsp::TDocumentFormattingParams);

} // namespace NYql::NJson
