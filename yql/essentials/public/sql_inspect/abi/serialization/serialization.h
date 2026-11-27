#pragma once

#include <yql/essentials/public/sql_inspect/abi/protos/sql_inspect.pb.h>
#include <yql/essentials/public/sql_inspect/symbols.h>

namespace NYql::NSqlInspect::NAbi {

NProto::TSymbolsRequest EncodeRequest(const TString& sql, const TSymbolSettings& settings);
TSymbolSettings DecodeSettings(const NProto::TSymbolsRequest& request);
NProto::TSymbolsResponse EncodeResult(const TSymbolsResult& result);
TSymbolsResult DecodeResult(const NProto::TSymbolsResponse& response);

} // namespace NYql::NSqlInspect::NAbi
