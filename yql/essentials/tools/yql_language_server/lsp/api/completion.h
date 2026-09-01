#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/completion.h>

namespace NLsp {

class ICompletionApi {
public:
    virtual ~ICompletionApi() = default;

    virtual TCompletionList Completion(const TCompletionParams& params) const = 0;
};

} // namespace NLsp
