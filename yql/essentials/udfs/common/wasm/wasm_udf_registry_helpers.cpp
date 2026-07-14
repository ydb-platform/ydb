#include "wasm_udf_registry_helpers.hpp"

#include <ydb/library/wasm/api/bytecode.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/yt/memory/range.h>
#include <library/cpp/yt/memory/ref.h>

#include <util/digest/city.h>
#include <util/folder/path.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/ascii.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

#include <unistd.h>

#include <cstdio>
#include <cstdlib>

namespace NWasm::NYQL {

using namespace NYT;
using namespace NYdb::NWasm;

namespace {

size_t FindMatchingClose(TStringBuf text, size_t openPos, char open, char close)
{
    if (openPos >= text.size() || text[openPos] != open) {
        ythrow yexception() << "Malformed function_descriptor.yson: expected '" << open << "'";
    }

    int depth = 0;
    for (size_t i = openPos; i < text.size(); ++i) {
        if (text[i] == open) {
            ++depth;
        } else if (text[i] == close) {
            --depth;
            if (depth == 0) {
                return i;
            }
        }
    }

    ythrow yexception() << "Malformed function_descriptor.yson: unclosed '" << open << "'";
}

TString ExtractRequiredValue(TStringBuf block, TStringBuf key)
{
    const TString needle = TString(key) + "=";
    const auto keyPos = block.find(needle);
    if (keyPos == TStringBuf::npos) {
        ythrow yexception() << "Missing key \"" << key << "\" in function_descriptor.yson block";
    }

    auto valueStart = keyPos + needle.size();
    while (valueStart < block.size() && IsAsciiSpace(block[valueStart])) {
        ++valueStart;
    }

    auto valueEnd = valueStart;
    while (valueEnd < block.size()
        && block[valueEnd] != ';'
        && block[valueEnd] != ']'
        && !IsAsciiSpace(block[valueEnd]))
    {
        ++valueEnd;
    }

    return StripString(TString(block.substr(valueStart, valueEnd - valueStart)));
}

TString ExtractOptionalValue(TStringBuf block, TStringBuf key)
{
    const TString needle = TString(key) + "=";
    const auto keyPos = block.find(needle);
    if (keyPos == TStringBuf::npos) {
        return {};
    }

    auto valueStart = keyPos + needle.size();
    while (valueStart < block.size() && IsAsciiSpace(block[valueStart])) {
        ++valueStart;
    }

    auto valueEnd = valueStart;
    while (valueEnd < block.size()
        && block[valueEnd] != ';'
        && block[valueEnd] != '}'
        && block[valueEnd] != ']'
        && !IsAsciiSpace(block[valueEnd]))
    {
        ++valueEnd;
    }

    return StripString(TString(block.substr(valueStart, valueEnd - valueStart)));
}

void ValidateConcreteTag(TStringBuf block, TStringBuf where)
{
    // Only "tag=concrete_type" is supported. Repeated / type_parameter etc. are not.
    const TString tag = ExtractOptionalValue(block, "tag");
    if (!tag.empty() && tag != "concrete_type") {
        ythrow yexception()
            << "Only tag=concrete_type is supported in function_descriptor.yson, got tag="
            << tag << " in " << where;
    }
}

TVector<EUdfValueType> ExtractArgumentTypes(TStringBuf block)
{
    const TString argsKey = "argument_types=[";
    const auto argsPos = block.find(argsKey);
    if (argsPos == TStringBuf::npos) {
        return {};
    }

    const auto listOpen = argsPos + argsKey.size() - 1;
    const auto argsEnd = FindMatchingClose(block, listOpen, '[', ']');

    const auto argsStart = listOpen + 1;
    TStringBuf argsBlock = block.substr(argsStart, argsEnd - argsStart);
    TVector<EUdfValueType> result;
    size_t pos = 0;
    while (true) {
        const auto entryOpen = argsBlock.find('{', pos);
        if (entryOpen == TStringBuf::npos) {
            break;
        }
        const auto entryClose = FindMatchingClose(argsBlock, entryOpen, '{', '}');
        const TStringBuf entry = argsBlock.substr(entryOpen, entryClose - entryOpen + 1);

        ValidateConcreteTag(entry, "argument_types");
        const TString value = ExtractRequiredValue(entry, "value");
        result.push_back(ParseValueType(value));

        pos = entryClose + 1;
    }

    return result;
}

EUdfValueType ExtractResultType(TStringBuf block)
{
    const auto resultPos = block.find("result_type=");
    if (resultPos == TStringBuf::npos) {
        ythrow yexception() << "Missing result_type in function_descriptor.yson";
    }

    const auto tail = block.substr(resultPos);
    const auto open = tail.find('{');
    if (open == TStringBuf::npos) {
        ythrow yexception() << "Malformed result_type in function_descriptor.yson";
    }
    const auto close = FindMatchingClose(tail, open, '{', '}');
    const TStringBuf entry = tail.substr(open, close - open + 1);

    ValidateConcreteTag(entry, "result_type");
    return ParseValueType(ExtractRequiredValue(entry, "value"));
}

} // namespace

bool EndsWith(TStringBuf value, TStringBuf suffix)
{
    return value.size() >= suffix.size() && value.substr(value.size() - suffix.size()) == suffix;
}

TString ReadFileContent(const TString& path)
{
    TFileInput input(path);
    return input.ReadAll();
}

TString JoinPath(const TString& directory, const TString& name)
{
    return (TFsPath(directory) / name).GetPath();
}

EUdfValueType ParseValueType(TStringBuf type)
{
    if (type == "int64") {
        return EUdfValueType::Int64;
    }
    if (type == "uint64") {
        return EUdfValueType::Uint64;
    }
    if (type == "double") {
        return EUdfValueType::Double;
    }
    if (type == "boolean" || type == "bool") {
        return EUdfValueType::Boolean;
    }
    if (type == "string") {
        return EUdfValueType::String;
    }
    if (type == "null") {
        return EUdfValueType::Null;
    }
    ythrow yexception() << "Unsupported wasm UDF descriptor type: " << type;
}

const char* ValueTypeToString(EUdfValueType type)
{
    switch (type) {
        case EUdfValueType::Null:
            return "null";
        case EUdfValueType::Int64:
            return "int64";
        case EUdfValueType::Uint64:
            return "uint64";
        case EUdfValueType::Double:
            return "double";
        case EUdfValueType::Boolean:
            return "boolean";
        case EUdfValueType::String:
            return "string";
    }
    return "unknown";
}

TVector<TWasmUdfDescriptor> ParseFunctionDescriptors(const TString& content)
{
    TVector<TWasmUdfDescriptor> result;
    size_t pos = 0;
    while (true) {
        const auto blockStart = content.find('{', pos);
        if (blockStart == TString::npos) {
            break;
        }
        const auto blockEnd = FindMatchingClose(content, blockStart, '{', '}');
        if (blockEnd + 1 >= content.size() || content[blockEnd + 1] != ';') {
            ythrow yexception() << "Malformed function_descriptor.yson";
        }

        const TStringBuf block(content.data() + blockStart, blockEnd - blockStart + 1);
        const auto convention = ExtractRequiredValue(block, "calling_convention");
        if (convention != "unversioned_value") {
            ythrow yexception()
                << "Only calling_convention=unversioned_value is supported, got " << convention;
        }

        TWasmUdfDescriptor descriptor;
        descriptor.Name = ExtractRequiredValue(block, "name");
        descriptor.Args = ExtractArgumentTypes(block);
        descriptor.Result = ExtractResultType(block);
        result.push_back(std::move(descriptor));

        pos = blockEnd + 2;
    }

    return result;
}

TString DescriptorPathToModulePath(const TString& descriptorPath)
{
    const TString suffix = ".function_descriptor.yson";
    if (EndsWith(descriptorPath, suffix)) {
        const auto base = descriptorPath.substr(0, descriptorPath.size() - suffix.size());
        for (const auto& extension : {TString(".so"), TString(".wasm"), TString(".wat"), TString(".wast")}) {
            const auto candidate = base + extension;
            if (TFsPath(candidate).Exists()) {
                return candidate;
            }
        }
    }

    // YT-experiments layout: directory contains literal function_descriptor.yson plus a single .so.
    const TFsPath descriptorFsPath(descriptorPath);
    const TFsPath parent = descriptorFsPath.Parent();
    if (parent.IsDirectory()) {
        TVector<TFsPath> children;
        parent.List(children);
        TVector<TFsPath> candidates;
        for (const auto& child : children) {
            const TString name = child.GetName();
            if (EndsWith(name, ".so")
                || EndsWith(name, ".wasm")
                || EndsWith(name, ".wat")
                || EndsWith(name, ".wast"))
            {
                if (name != "sdk.so" && name != "sdk.wasm" && name != "sdk.wat" && name != "sdk.wast") {
                    candidates.push_back(child);
                }
            }
        }
        if (candidates.size() == 1) {
            return candidates.front().GetPath();
        }
        if (candidates.size() > 1) {
            ythrow yexception()
                << "Multiple wasm module candidates next to descriptor " << descriptorPath
                << "; expected exactly one .so/.wasm/.wat alongside the function_descriptor.yson";
        }
    }

    ythrow yexception() << "Cannot find UDF module for descriptor: " << descriptorPath;
}

TString FindOptionalSdkPath(const TString& directory)
{
    for (const auto& name : {TString("sdk.so"), TString("sdk.wasm"), TString("sdk.wat"), TString("sdk.wast")}) {
        const auto path = JoinPath(directory, name);
        if (TFsPath(path).Exists()) {
            return path;
        }
    }
    return {};
}

void AddModuleFromFile(IWebAssemblyCompartment* compartment, const TString& path)
{
    const auto content = ReadFileContent(path);
    if (path.EndsWith(".wat") || path.EndsWith(".wast")) {
        compartment->AddModule(content, path);
    } else {
        compartment->AddModule(NYT::TRef::FromString(content), path);
    }
}

namespace {

TString GetSdkObjectCodeCacheDir()
{
    if (const char* override = std::getenv("YQL_WASM_UDF_SDK_OBJECT_CACHE_DIR");
        override && *override)
    {
        return TString(override);
    }
    if (const char* xdg = std::getenv("XDG_CACHE_HOME"); xdg && *xdg) {
        return TString(xdg) + "/yql-wasm-udf-sdk";
    }
    if (const char* home = std::getenv("HOME"); home && *home) {
        return TString(home) + "/.cache/yql-wasm-udf-sdk";
    }
    return "/tmp/yql-wasm-udf-sdk";
}

TString GetSdkObjectCodeCachePath(TStringBuf sdkBytes)
{
    const ui64 hash = CityHash64(sdkBytes.data(), sdkBytes.size());
    return GetSdkObjectCodeCacheDir()
        + '/' + Sprintf("sdk-%016lx-%lu.objcode", hash, sdkBytes.size());
}

TString TryReadSdkObjectCodeCache(const TString& cachePath)
{
    if (!TFsPath(cachePath).Exists()) {
        return {};
    }
    try {
        TFileInput input(cachePath);
        return input.ReadAll();
    } catch (const std::exception& ex) {
        Cerr << "[wasm-udf] failed to read SDK object code cache " << cachePath
             << ": " << ex.what() << "; will recompile" << Endl;
        return {};
    }
}

void WriteSdkObjectCodeCacheAtomically(const TString& cachePath, TStringBuf objectCode)
{
    try {
        TFsPath(cachePath).Parent().MkDirs();
        const TString tmpPath = cachePath + ".tmp." + Sprintf("%d", static_cast<int>(::getpid()));
        {
            TUnbufferedFileOutput out(tmpPath);
            out.Write(objectCode.data(), objectCode.size());
        }
        if (std::rename(tmpPath.c_str(), cachePath.c_str()) != 0) {
            std::remove(tmpPath.c_str());
            Cerr << "[wasm-udf] failed to install SDK object code cache " << cachePath
                 << ": rename() failed" << Endl;
        }
    } catch (const std::exception& ex) {
        Cerr << "[wasm-udf] failed to write SDK object code cache " << cachePath
             << ": " << ex.what() << Endl;
    }
}

TString CompileSdkObjectCode(TStringBuf sdkBytes)
{
    auto featureSpec = WAVM::IR::FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.table64 = true;
    featureSpec.exceptionHandling = true;

    WAVM::IR::Module irModule(featureSpec);
    auto loadError = WAVM::WASM::LoadError();
    const bool parsed = WAVM::WASM::loadBinaryModule(
        std::bit_cast<const WAVM::U8*>(sdkBytes.data()),
        sdkBytes.size(),
        irModule,
        &loadError);
    if (!parsed) {
        ythrow yexception() << "Failed to parse SDK wasm binary for object-code caching: "
                            << loadError.message;
    }

    auto compiled = WAVM::Runtime::compileModule(irModule);
    auto objectCode = WAVM::Runtime::getObjectCode(compiled);
    return TString(
        std::bit_cast<const char*>(objectCode.data()),
        objectCode.size());
}

} // namespace

std::unique_ptr<IWebAssemblyCompartment> CreateRegistryCompartment(
    const TString& directory,
    const TString& sdkPath)
{
    if (!sdkPath.empty()) {
        Cerr << "[wasm-udf] CreateRegistryCompartment: reading SDK bytes from " << sdkPath << Endl;
        auto sdk = NYdb::NWasm::TModuleBytecode{};
        sdk.Format = NYdb::NWasm::EBytecodeFormat::Binary;
        auto bytes = ReadFileContent(sdkPath);
        Cerr << "[wasm-udf] CreateRegistryCompartment: SDK is " << bytes.size() << " bytes" << Endl;

        const auto cachePath = GetSdkObjectCodeCachePath(bytes);
        auto cachedObjectCode = TryReadSdkObjectCodeCache(cachePath);
        if (!cachedObjectCode.empty()) {
            Cerr << "[wasm-udf] using precompiled SDK object code from " << cachePath
                 << " (" << cachedObjectCode.size() << " bytes, fast path)" << Endl;
            sdk.ObjectCode = NYT::TSharedRef::FromString(std::move(cachedObjectCode));
        } else {
            Cerr << "[wasm-udf] no SDK object-code cache at " << cachePath
                 << "; compiling once (this can take several minutes)..." << Endl;
            try {
                auto compiled = CompileSdkObjectCode(bytes);
                Cerr << "[wasm-udf] compiled SDK object code: " << compiled.size()
                     << " bytes; saving cache" << Endl;
                WriteSdkObjectCodeCacheAtomically(cachePath, compiled);
                sdk.ObjectCode = NYT::TSharedRef::FromString(std::move(compiled));
            } catch (const std::exception& ex) {
                Cerr << "[wasm-udf] SDK precompile failed (" << ex.what()
                     << "); falling back to YT in-process compilation" << Endl;
            }
        }

        sdk.Data = NYT::TSharedRef::FromString(std::move(bytes));
        Cerr << "[wasm-udf] CreateRegistryCompartment: calling CreateImageFromSdk..." << Endl;
        auto image = CreateImageFromSdk(sdk);
        Cerr << "[wasm-udf] CreateRegistryCompartment: CreateImageFromSdk returned" << Endl;
        return image;
    }

    Y_UNUSED(directory);
    Cerr << "[wasm-udf] CreateRegistryCompartment: no SDK, using minimal runtime" << Endl;
    return CreateMinimalRuntimeImage();
}

TUnversionedValue MakeEmptyValue()
{
    TUnversionedValue value{};
    value.Type = EAbiValueType::Null;
    value.Flags = EAbiValueFlags::None;
    return value;
}

void StoreValue(IWebAssemblyCompartment* compartment, uintptr_t offset, const TUnversionedValue& value)
{
    auto* destination = PtrFromVM(compartment, std::bit_cast<TUnversionedValue*>(offset));
    *destination = value;
}

TCurrentCompartmentGuard::TCurrentCompartmentGuard(IWebAssemblyCompartment* compartment)
    : Previous_(GetCurrentCompartment())
{
    SetCurrentCompartment(compartment);
}

TCurrentCompartmentGuard::~TCurrentCompartmentGuard()
{
    SetCurrentCompartment(Previous_);
}

void InvokeUdfExport(
    IWebAssemblyCompartment* compartment,
    const TString& functionName,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args)
{
    auto* runtimeFunction = compartment->GetFunction(std::string(functionName));
    if (runtimeFunction == nullptr) {
        ythrow yexception() << "Unknown wasm export: " << functionName;
    }

    // YT calling convention: void udf(TExpressionContext*, TUnversionedValue* result, TUnversionedValue* args...).
    // All arguments are passed as wasm i64 (memory64 mode). We dynamically build the runtime function
    // type so there is no compile-time arity limit.
    constexpr size_t kMaxArgs = 32;
    const size_t totalArgs = 2 + args.size();
    if (totalArgs > kMaxArgs) {
        ythrow yexception() << "Too many wasm UDF arguments: " << args.size();
    }

    std::array<EWebAssemblyValueType, kMaxArgs> argumentTypes;
    for (size_t i = 0; i < totalArgs; ++i) {
        argumentTypes[i] = EWebAssemblyValueType::UintPtr;
    }
    const auto runtimeType = GetTypeId(
        /*intrinsic*/ false,
        EWebAssemblyValueType::Void,
        TRange(argumentTypes.data(), totalArgs));

    std::array<TWavmPodValue, kMaxArgs> wavmArgs{};
    wavmArgs[0].Data = context;
    wavmArgs[1].Data = result;
    for (size_t i = 0; i < args.size(); ++i) {
        wavmArgs[2 + i].Data = args[i];
    }

    try {
        NYdb::NWasm::NDetail::WavmInvoke(
            compartment,
            runtimeType,
            runtimeFunction,
            /*result*/ nullptr,
            TRange(wavmArgs.data(), totalArgs));
    } catch (WAVM::Runtime::Exception* exception) {
        const auto message = WAVM::Runtime::describeException(exception);
        WAVM::Runtime::destroyException(exception);
        ythrow yexception() << "WAVM runtime exception while calling \""
            << functionName << "\": " << message;
    }
}

} // namespace NWasm::NYQL
