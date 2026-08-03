#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/formatting.h>

namespace NLsp {

class IFormattingApi {
public:
    virtual ~IFormattingApi() = default;

    virtual TVector<TTextEdit> Formatting(const TDocumentFormattingParams& params) const = 0;
};

} // namespace NLsp
