#include "result_parser.h"

namespace NYql {

void ParseResult(TStringBuf yson, IResultVisitor& visitor, const TResultParseOptions& options) {
    visitor.OnLabel("TODO");
    Y_UNUSED(yson, visitor, options);
}

}
