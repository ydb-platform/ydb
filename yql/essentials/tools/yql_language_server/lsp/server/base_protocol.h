#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/consumer/base.h>
#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>

#include <util/generic/string.h>

#include <expected>

namespace NLsp {

class TLspBaseProtocolException: public TLspException {
public:
    TLspBaseProtocolException();
};

void LspBaseProtocolReader(IInputStream& in, IConsumer<TString>::TPtr consumer);

IConsumer<TString>::TPtr LspBaseProtocolWriter(IOutputStream& out);

} // namespace NLsp
