#pragma once

#include "wasm_udf_registry.hpp"

#include <yql/essentials/public/udf/udf_helpers.h>

#include <ydb/library/wasm/api/compartment.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <memory>
#include <mutex>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

//! Per-`.so` metadata: which function descriptors belong here and which wasm exports
//! this `.so` actually provides. We use the export set to validate the descriptor at load time
//! and to scope a YQL module's `GetAllFunctions` to functions from this .so only.
struct TWasmSoLocator
{
    TString ModuleName;
    TString ModulePath;
    TVector<TString> DescriptorOrder;
    THashSet<TString> Exports;
};

//! One compartment per env-registry directory. The compartment hosts the (optional) SDK and
//! every UDF `.so` from that directory; UDFs dynamically link against the shared SDK
//! ("сложный код" with libc/libcxx works only in this layout).
//! Invocations are serialized by `InvocationMutex` (Phase 1 thread safety).
struct TWasmDirectoryState
{
    std::mutex InvocationMutex;
    std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> Compartment;
    THashMap<TString, TWasmUdfDescriptor> Functions; // key = wasm export name
    THashMap<TString, TWasmSoLocator> Locators;       // key = YQL module name (CamelCase)
};

using TWasmDirectoryStatePtr = std::shared_ptr<TWasmDirectoryState>;

//! Translate a subdir name to a CamelCase YQL module name: `base64` -> `Base64`,
//! `local_udf` -> `LocalUdf`, `yql_like_string` -> `YqlLikeString`.
//! Separators recognised: `-`, `_`, `.`. Leading `lib` and `common-` prefixes are dropped.
TString DeriveModuleName(TStringBuf subdirName);

//! Scan the env-registry directory in YT subdir-style layout and build a YQL module per UDF subdir.
//! On any per-subdir error we log to stderr and skip that subdir rather than aborting the whole
//! Register() — an unrelated broken UDF must not take down the rest of libwasm_udf.
struct TBuiltSoModule
{
    TString ModuleName;
    NYql::NUdf::TUniquePtr<IUdfModule> Module;
};

TVector<TBuiltSoModule> BuildSoModulesFromRegistry(TStringBuf directory);

} // namespace NWasm::NYQL
