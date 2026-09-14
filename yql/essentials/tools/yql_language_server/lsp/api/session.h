#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/session.h>

#include <library/cpp/threading/future/future.h>

namespace NLsp {

class ISessionApi {
public:
    virtual ~ISessionApi() = default;

    virtual TInitializeResult Initialize(TInitializeParams params) = 0;

    virtual void Initialized(TInitializedParams params) = 0;

    virtual void SetTrace(TSetTraceParams params) = 0;
};

} // namespace NLsp
