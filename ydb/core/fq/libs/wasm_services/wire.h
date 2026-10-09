#pragma once

#include <ydb/udfs/wasm/sdk/services/transport.h>

namespace NFq::NWasmServices {

using NYdb::NWasm::NServices::Decode;
using NYdb::NWasm::NServices::EClientError;
using NYdb::NWasm::NServices::Encode;
using NYdb::NWasm::NServices::TArgumentsHeader;
using NYdb::NWasm::NServices::TRequestHeader;
using NYdb::NWasm::NServices::TResponseHeader;
using NYdb::NWasm::NServices::TResultHeader;
using NYdb::NWasm::NServices::WireVersion;

} // namespace NFq::NWasmServices
