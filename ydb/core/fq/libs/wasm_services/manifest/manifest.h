#pragma once

#include <ydb/public/lib/udf/service_manifest/manifest.h>

namespace NFq::NWasmServices {
using NYdb::NWasm::NServices::ParseServiceManifest;
using NYdb::NWasm::NServices::TServiceField;
using NYdb::NWasm::NServices::TServiceManifest;
using NYdb::NWasm::NServices::TServiceMethod;
}   // namespace NFq::NWasmServices
