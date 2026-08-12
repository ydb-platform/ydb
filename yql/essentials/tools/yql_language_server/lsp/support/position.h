#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/text_document.h>

namespace NLsp {

size_t ToBytes(TPosition position, TStringBuf text);

TPosition FromBytes(size_t bytes, TStringBuf text);

} // namespace NLsp
