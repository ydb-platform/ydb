#include "module_source.h"

#include <ydb/services/udf_store/wasm/compile.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>

#include <mutex>

namespace NFq::NWasmServices {
namespace {
using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm::NServices;

class TStoreModuleSource final: public IServiceModuleSource {
public:
    TVector<TServiceManifest> Describe() const override {
        TVector<TServiceManifest> result;
        auto& catalog = GetWasmModuleCatalog();
        for (const auto& name : catalog.ListModuleNames()) {
            const auto module = catalog.FindByModuleName(name);
            if (module && module->Manifest.Services) {
                result.push_back(*module->Manifest.Services);
            }
        }
        return result;
    }

    TWasmModuleArtifactPtr Load(const TString& name) override {
        auto module = GetWasmModuleCatalog().FindByModuleName(name);
        Y_ENSURE(module && module->Manifest.Services, "WASM service module is not loaded in UDF Store");
        return module;
    }
};

class TLocalModuleSource final: public IServiceModuleSource {
    struct TEntry {
        TString Path;
        TWasmManifest Manifest;
        TWasmModuleArtifactPtr Loaded;
    };

    THashMap<TString, TEntry> Modules;
    std::mutex Mutex;

public:
    explicit TLocalModuleSource(const NConfig::TWasmServicesConfig& config) {
        Y_ENSURE(config.GetModulePath().empty() || !config.ModulesSize(), "Use either ModulePath or Modules for WASM services");
        auto add = [&](const TString& path, const TString& manifestPath) {
            Y_ENSURE(!path.empty() && !manifestPath.empty(), "WASM service module and manifest paths are required");
            const TFile file(manifestPath, RdOnly);
            Y_ENSURE(file.GetLength() <= 65536, "WASM service manifest exceeds byte limit");
            auto manifest = ParseManifest(TFileInput(file).ReadAll());
            Y_ENSURE(manifest.Services, "Expected a WASM service module manifest");
            Y_ENSURE(manifest.RequiredLibraries.empty(), "Local WASM services require self-contained modules");
            const auto name = manifest.ModuleName;
            Y_ENSURE(Modules.emplace(name, TEntry{ path, std::move(manifest), {} }).second, "Duplicate WASM service module name");
        };
        if (!config.GetModulePath().empty()) {
            add(config.GetModulePath(), config.GetModulePath() + ".manifest.json");
        }
        for (const auto& module : config.GetModules()) {
            add(module.GetModulePath(), module.GetManifestPath());
        }
        Y_ENSURE(!Modules.empty(), "WASM service modules are required");
    }

    TVector<TServiceManifest> Describe() const override {
        TVector<TServiceManifest> result;
        for (const auto& [_, entry] : Modules) {
            result.push_back(*entry.Manifest.Services);
        }
        return result;
    }

    TWasmModuleArtifactPtr Load(const TString& name) override {
        std::lock_guard guard(Mutex);
        const auto it = Modules.find(name);
        Y_ENSURE(it != Modules.end(), "Unknown local WASM service module");
        auto& entry = it->second;
        if (!entry.Loaded) {
            auto artifact = std::make_shared<TWasmModuleArtifact>();
            artifact->ModuleName = name;
            artifact->Manifest = entry.Manifest;
            const auto bytes = TFileInput(entry.Path).ReadAll();
            const auto object = CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary);
            artifact->ModuleBytecode = MakeModuleBytecode(bytes, object, NYdb::NWasm::EBytecodeFormat::Binary);
            entry.Loaded = std::move(artifact);
        }
        return entry.Loaded;
    }
};
}   // namespace

IServiceModuleSource::TPtr CreateServiceModuleSource(const NConfig::TWasmServicesConfig& config) {
    Y_ENSURE(config.GetEnabled(), "WASM services are disabled");
    Y_ENSURE(config.GetMaxBatchRows() <= MaxServiceBatchRows, "WASM service batch row limit exceeds 64");
    const auto bytes = config.GetMaxBatchBytes();
    Y_ENSURE(!bytes || (bytes >= sizeof(TServiceRequest) && bytes <= MaxServiceBatchBytes), "Invalid WASM service batch byte limit");
    if (config.GetModuleSource() == NConfig::TWasmServicesConfig::LOCAL_FILES) {
        return std::make_shared<TLocalModuleSource>(config);
    }
    Y_ENSURE(config.GetModuleSource() == NConfig::TWasmServicesConfig::UDF_STORE, "Unknown WASM service module source");
    Y_ENSURE(config.GetModulePath().empty() && !config.ModulesSize(), "UDF Store source does not accept local WASM paths");
    return std::make_shared<TStoreModuleSource>();
}
}   // namespace NFq::NWasmServices
