#include "symbols.h"

#include <yql/essentials/public/abi_impl/message.h>
#include <yql/essentials/public/sql_inspect/abi/interface/symbols.h>
#include <yql/essentials/public/sql_inspect/abi/serialization/serialization.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>

namespace NYql::NSqlInspect {

TSymbolsResult GetSymbols(const TString& sql, const TSymbolSettings& settings) {
    const TString request = NYql::NAbi::SerializeMessage(NAbi::EncodeRequest(sql, settings));
    auto* handle = YqlSqlInspectV1(request.data(), request.size());
    Y_DEFER {
        YqlSqlInspectFreeV1(handle);
    };
    size_t size = 0;
    const char* data = YqlSqlInspectErrorV1(handle, &size);
    Y_ENSURE(!size, TStringBuf(data, size));
    data = YqlSqlInspectResultV1(handle, &size);
    return NAbi::DecodeResult(NYql::NAbi::ParseMessage<NAbi::NProto::TSymbolsResponse>(data, size));
}

} // namespace NYql::NSqlInspect
