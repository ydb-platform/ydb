#pragma once

#include <ydb/core/fq/libs/config/protos/wasm_services.pb.h>

#include <ydb/services/udf_store/wasm/module_catalog.h>

namespace NFq::NWasmServices {

// Sources only supply modules. Both use the same transform/runtime/transport.
class IServiceModuleSource {
public:
    using TPtr = std::shared_ptr<IServiceModuleSource>;
    virtual ~IServiceModuleSource() = default;
    virtual TVector<NYdb::NWasm::NServices::TServiceManifest> Describe() const = 0;
    virtual NKikimr::NUdfStore::NWasm::TWasmModuleArtifactPtr Load(const TString& name) = 0;
};

IServiceModuleSource::TPtr CreateServiceModuleSource(const NConfig::TWasmServicesConfig& config);

}   // namespace NFq::NWasmServices
