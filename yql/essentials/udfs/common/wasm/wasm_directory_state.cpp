#include "wasm_directory_state.hpp"

#include "wasm_state.hpp"
#include "wasm_udf_function.hpp"
#include "wasm_udf_registry_helpers.hpp"

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/ascii.h>

namespace NWasm::NYQL {

namespace {

bool IsDirectorySeparator(char ch)
{
    return ch == '-' || ch == '_' || ch == '.';
}

TString StripPrefix(TString name, TStringBuf prefix)
{
    if (name.size() > prefix.size() && name.StartsWith(prefix)) {
        return name.substr(prefix.size());
    }
    return name;
}

TString FindUdfModulePath(const TFsPath& subdir)
{
    TVector<TFsPath> children;
    subdir.List(children);

    TVector<TFsPath> candidates;
    for (const auto& child : children) {
        if (!child.IsFile()) {
            continue;
        }
        const TString name = child.GetName();
        if (name == "sdk.so" || name == "sdk.wasm" || name == "sdk.wat" || name == "sdk.wast") {
            continue;
        }
        if (name.EndsWith(".so") || name.EndsWith(".wasm")
            || name.EndsWith(".wat") || name.EndsWith(".wast"))
        {
            candidates.push_back(child);
        }
    }

    if (candidates.empty()) {
        ythrow yexception()
            << "No wasm module (*.so / *.wasm / *.wat / *.wast) found in directory: "
            << subdir.GetPath();
    }
    if (candidates.size() > 1) {
        TStringBuilder names;
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (i > 0) {
                names << ", ";
            }
            names << candidates[i].GetName();
        }
        ythrow yexception()
            << "Multiple wasm module candidates in directory " << subdir.GetPath()
            << ": " << names << "; expected exactly one";
    }

    return candidates.front().GetPath();
}

} // namespace

TString DeriveModuleName(TStringBuf subdirName)
{
    TString name = TString(subdirName);
    name = StripPrefix(name, "lib");
    name = StripPrefix(name, "common-");

    TString result;
    result.reserve(name.size());
    bool nextUpper = true;
    for (char ch : name) {
        if (IsDirectorySeparator(ch)) {
            nextUpper = true;
            continue;
        }
        if (nextUpper) {
            result.push_back(AsciiToUpper(ch));
            nextUpper = false;
        } else {
            result.push_back(ch);
        }
    }

    if (result.empty()) {
        ythrow yexception() << "Cannot derive wasm UDF module name from subdir \"" << subdirName << "\"";
    }
    return result;
}

TVector<TBuiltSoModule> BuildSoModulesFromRegistry(TStringBuf directory)
{
    TVector<TBuiltSoModule> result;

    const TFsPath registryPath{TString(directory)};
    if (!registryPath.Exists()) {
        Cerr << "Wasm UDF registry path does not exist, skipping: " << directory << Endl;
        return result;
    }
    if (!registryPath.IsDirectory()) {
        Cerr << "Wasm UDF registry path is not a directory, skipping: " << directory << Endl;
        return result;
    }
    const TString registryRoot = registryPath.GetPath();
    Cerr << "[wasm-udf] BuildSoModulesFromRegistry: scanning " << registryRoot << Endl;

    if (TFsPath(JoinPath(registryRoot, "function_descriptor.yson")).Exists()) {
        Cerr << "Wasm UDF env registry: " << registryRoot
             << " contains a top-level function_descriptor.yson; expected subdir-per-UDF layout. "
             << "Move the descriptor into a named subdir." << Endl;
        return result;
    }

    const auto sdkPath = FindOptionalSdkPath(registryRoot);
    Cerr << "[wasm-udf] sdk path: \"" << sdkPath << "\"" << Endl;

    auto state = std::make_shared<TWasmDirectoryState>();
    try {
        Cerr << "[wasm-udf] creating registry compartment (sdk loading may take a while)..." << Endl;
        state->Compartment = CreateRegistryCompartment(registryRoot, sdkPath);
        Cerr << "[wasm-udf] registry compartment ready" << Endl;
    } catch (const std::exception& ex) {
        Cerr << "Failed to create wasm UDF compartment for registry " << registryRoot
             << ": " << ex.what() << Endl;
        return result;
    }

    TVector<TFsPath> children;
    registryPath.List(children);
    Sort(children.begin(), children.end(), [](const TFsPath& a, const TFsPath& b) {
        return a.GetName() < b.GetName();
    });

    for (const auto& child : children) {
        if (!child.IsDirectory()) {
            continue;
        }

        const TString subdirName = child.GetName();
        const TFsPath aggregateDescriptor = child / "aggregate_descriptor.yson";
        const TFsPath functionDescriptor = child / "function_descriptor.yson";

        if (aggregateDescriptor.Exists()) {
            Cerr << "Wasm UDF registry: aggregate UDFs are not supported (Phase 1); "
                 << "ignoring " << aggregateDescriptor.GetPath() << Endl;
            continue;
        }

        if (!functionDescriptor.Exists()) {
            continue;
        }

        try {
            Cerr << "[wasm-udf] processing subdir: " << child.GetPath() << Endl;
            const TString modulePath = FindUdfModulePath(child);
            Cerr << "[wasm-udf]   module path: " << modulePath << Endl;
            const TString moduleBytecode = ReadFileContent(modulePath);
            Cerr << "[wasm-udf]   read " << moduleBytecode.size() << " bytes from " << modulePath << Endl;

            auto exportsByName = ExtractWasmExportsFromPath(modulePath, moduleBytecode);
            Cerr << "[wasm-udf]   extracted " << exportsByName.size() << " exports" << Endl;
            for (const auto& exportEntry : exportsByName) {
                Cerr << "[wasm-udf]     export: " << exportEntry.first << Endl;
            }

            TWasmSoLocator locator;
            locator.ModuleName = DeriveModuleName(subdirName);
            locator.ModulePath = modulePath;

            const auto descriptors = ParseFunctionDescriptors(ReadFileContent(functionDescriptor.GetPath()));
            if (descriptors.empty()) {
                Cerr << "Wasm UDF registry: descriptor " << functionDescriptor.GetPath()
                     << " declares no functions; skipping" << Endl;
                continue;
            }

            for (const auto& descriptor : descriptors) {
                if (!exportsByName.contains(descriptor.Name)) {
                    ythrow yexception()
                        << "Wasm UDF \"" << descriptor.Name
                        << "\" declared in " << functionDescriptor.GetPath()
                        << " is not exported by " << modulePath;
                }
                if (state->Functions.contains(descriptor.Name)) {
                    ythrow yexception()
                        << "Duplicate wasm UDF export \"" << descriptor.Name
                        << "\" already provided by another .so in the same registry";
                }
                locator.DescriptorOrder.push_back(descriptor.Name);
                locator.Exports.insert(descriptor.Name);
                state->Functions.emplace(descriptor.Name, descriptor);
            }

            if (state->Locators.contains(locator.ModuleName)) {
                ythrow yexception()
                    << "Wasm UDF registry: duplicate module name \"" << locator.ModuleName
                    << "\" derived from subdir \"" << subdirName << "\"";
            }

            Cerr << "[wasm-udf]   adding module \"" << locator.ModuleName << "\" into compartment..." << Endl;
            AddModuleFromFile(state->Compartment.get(), modulePath);
            Cerr << "[wasm-udf]   module \"" << locator.ModuleName << "\" added" << Endl;

            const TString moduleName = locator.ModuleName;
            state->Locators.emplace(moduleName, std::move(locator));
        } catch (const std::exception& ex) {
            Cerr << "Failed to register wasm UDF subdir " << child.GetPath()
                 << ": " << ex.what() << Endl;
            continue;
        }
    }

    for (const auto& [moduleName, locator] : state->Locators) {
        TBuiltSoModule built;
        built.ModuleName = moduleName;
        built.Module = NYql::NUdf::TUniquePtr<IUdfModule>(new TWasmSoModule(state, moduleName));
        result.push_back(std::move(built));
    }

    Sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.ModuleName < b.ModuleName;
    });
    return result;
}

} // namespace NWasm::NYQL
