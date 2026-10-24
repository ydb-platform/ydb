#include "module_source.h"

#include <ydb/public/lib/udf/manifest/manifest.h>
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
        Y_ENSURE(module && (module->Manifest.Services || module->Manifest.Auth), "WASM service/auth module is not loaded in UDF Store");
        return module;
    }
};

class TLocalModuleSource final: public IServiceModuleSource {
    struct TEntry {
        TString Path;
        TWasmManifest Manifest;
        TWasmModuleArtifactPtr Loaded;
        bool IsLibrary = false;
        std::optional<NYdb::NWasm::TModuleBytecode> Bytecode;
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
            const auto json = TFileInput(file).ReadAll();
            const auto common = NYdb::NUdfManifest::Parse(json);
            Y_ENSURE(common.Kind == NYdb::NUdfManifest::EModuleKind::Wasm, "Expected a WASM artifact manifest");
            const bool library = common.Type == NYdb::NUdfManifest::EModuleType::Library;
            TWasmManifest manifest;
            if (!library) {
                manifest = ParseManifest(json);
                Y_ENSURE(manifest.Services || manifest.Auth, "Expected a WASM service/auth module manifest");
            }
            manifest.ModuleName = common.Name;
            Y_ENSURE(Modules.emplace(common.Name, TEntry{ path, std::move(manifest), {}, library, {} }).second, "Duplicate WASM artifact name");
        };
        if (!config.GetModulePath().empty()) {
            add(config.GetModulePath(), config.GetModulePath() + ".manifest.json");
        }
        for (const auto& module : config.GetModules()) {
            add(module.GetModulePath(), module.GetManifestPath());
        }
        Y_ENSURE(!Modules.empty(), "WASM service modules are required");
        for (const auto& [_, entry] : Modules) {
            THashSet<TString> unique;
            for (const auto& name : entry.Manifest.RequiredLibraries) {
                const auto library = Modules.find(name);
                Y_ENSURE(unique.insert(name).second && library != Modules.end() && library->second.IsLibrary,
                    "Required local WASM library is missing or invalid");
            }
        }
    }

    TVector<TServiceManifest> Describe() const override {
        TVector<TServiceManifest> result;
        for (const auto& [_, entry] : Modules) {
            if (entry.Manifest.Services) {
                result.push_back(*entry.Manifest.Services);
            }
        }
        return result;
    }

    TWasmModuleArtifactPtr Load(const TString& name) override {
        std::lock_guard guard(Mutex);
        const auto it = Modules.find(name);
        Y_ENSURE(it != Modules.end(), "Unknown local WASM service module");
        auto& entry = it->second;
        Y_ENSURE(!entry.IsLibrary, "A WASM library is not a service/auth module");
        auto compile = [](TEntry& item) -> const NYdb::NWasm::TModuleBytecode& {
            if (!item.Bytecode) {
                const auto bytes = TFileInput(item.Path).ReadAll();
                const auto object = CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary);
                item.Bytecode = MakeModuleBytecode(bytes, object, NYdb::NWasm::EBytecodeFormat::Binary);
            }
            return *item.Bytecode;
        };
        if (!entry.Loaded) {
            auto artifact = std::make_shared<TWasmModuleArtifact>();
            artifact->ModuleName = name;
            artifact->Manifest = entry.Manifest;
            for (const auto& name : entry.Manifest.RequiredLibraries) {
                artifact->Libraries.push_back({ name, compile(Modules.at(name)) });
            }
            artifact->ModuleBytecode = compile(entry);
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
