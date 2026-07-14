#include "wasm_udf_registry.hpp"

#include "wasm_invocation_context.hpp"
#include "wasm_state.hpp"
#include "wasm_udf_registry_helpers.hpp"

#include <yql/essentials/udfs/common/wasm/abi/udf_cpp_abi.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/scope.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;
using namespace NYT::NQueryClient::NUdf;
using namespace NYdb::NWasm;

namespace {

TPreparedUdfArg PrepareArgFromJson(
    IWebAssemblyCompartment* compartment,
    const NJson::TJsonValue& json,
    EUdfValueType type)
{
    TPreparedUdfArg prepared;
    auto value = MakeEmptyValue();

    if (json.IsNull()) {
        value.Type = EAbiValueType::Null;
    } else {
        switch (type) {
            case EUdfValueType::Int64:
                if (!json.IsInteger()) {
                    ythrow yexception() << "Expected JSON integer argument";
                }
                value.Type = EAbiValueType::Int64;
                value.Data.Int64 = static_cast<i64>(json.GetInteger());
                break;
            case EUdfValueType::Uint64:
                if (!json.IsUInteger() && !json.IsInteger()) {
                    ythrow yexception() << "Expected JSON unsigned integer argument";
                }
                value.Type = EAbiValueType::Uint64;
                value.Data.Uint64 = json.IsUInteger()
                    ? static_cast<ui64>(json.GetUInteger())
                    : static_cast<ui64>(json.GetInteger());
                break;
            case EUdfValueType::Double:
                if (json.IsInteger()) {
                    value.Data.Double = static_cast<double>(json.GetInteger());
                } else if (json.IsUInteger()) {
                    value.Data.Double = static_cast<double>(json.GetUInteger());
                } else if (json.IsDouble()) {
                    value.Data.Double = json.GetDouble();
                } else {
                    ythrow yexception() << "Expected JSON numeric argument";
                }
                value.Type = EAbiValueType::Double;
                break;
            case EUdfValueType::Boolean:
                if (!json.IsBoolean()) {
                    ythrow yexception() << "Expected JSON boolean argument";
                }
                value.Type = EAbiValueType::Boolean;
                value.Data.Boolean = json.GetBoolean() ? 1 : 0;
                break;
            case EUdfValueType::String: {
                if (!json.IsString()) {
                    ythrow yexception() << "Expected JSON string argument";
                }
                const auto& string = json.GetString();
                prepared.StringGuard = CopyIntoCompartment(TStringBuf(string), compartment);
                value.Type = EAbiValueType::String;
                value.Length = string.size();
                value.Data.String = std::bit_cast<char*>(prepared.StringGuard.GetCopiedOffset());
                break;
            }
            case EUdfValueType::Null:
                value.Type = EAbiValueType::Null;
                break;
        }
    }

    const auto offset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    prepared.ValueGuard = TCopyGuard(compartment, offset);
    prepared.Offset = offset;
    StoreValue(compartment, offset, value);
    return prepared;
}

NJson::TJsonValue ReadResultJson(IWebAssemblyCompartment* compartment, uintptr_t resultOffset)
{
    const auto result = *PtrFromVM(compartment, std::bit_cast<TUnversionedValue*>(resultOffset));
    switch (result.Type) {
        case EAbiValueType::Null:
            return NJson::TJsonValue(NJson::JSON_NULL);
        case EAbiValueType::Int64:
            return NJson::TJsonValue(result.Data.Int64);
        case EAbiValueType::Uint64:
            return NJson::TJsonValue(result.Data.Uint64);
        case EAbiValueType::Double:
            return NJson::TJsonValue(result.Data.Double);
        case EAbiValueType::Boolean:
            return NJson::TJsonValue(static_cast<bool>(result.Data.Boolean));
        case EAbiValueType::String: {
            const auto* hostData = PtrFromVM(compartment, result.Data.String, result.Length);
            return NJson::TJsonValue(TString(hostData, result.Length));
        }
        default:
            ythrow yexception() << "Unsupported wasm UDF result value type: " << static_cast<int>(result.Type);
    }
}

} // namespace

TWasmUdfRegistryStatePtr LoadWasmUdfRegistry(const TString& path)
{
    const TFsPath inputPath(path);
    if (!inputPath.Exists()) {
        ythrow yexception() << "Wasm UDF registry path does not exist: " << path;
    }

    const bool singleDescriptor = inputPath.IsFile() && EndsWith(path, ".function_descriptor.yson");
    const TFsPath directory = singleDescriptor ? inputPath.Parent() : inputPath;
    if (!directory.IsDirectory()) {
        ythrow yexception() << "Wasm UDF registry path is not a directory or descriptor file: " << path;
    }
    const TString directoryPath = directory.GetPath();

    auto state = std::make_shared<TWasmUdfRegistryState>();
    const auto sdkPath = FindOptionalSdkPath(directoryPath);
    state->Compartment = CreateRegistryCompartment(directoryPath, sdkPath);

    TVector<TString> modulePaths;
    TVector<TString> descriptorPaths;
    if (singleDescriptor) {
        descriptorPaths.push_back(path);
    } else {
        TFileEntitiesList listing{TFileEntitiesList::EM_FILES};
        listing.Fill(directoryPath);
        const char* filename = nullptr;
        while ((filename = listing.Next()) != nullptr) {
            const TString name(filename);
            if (EndsWith(name, ".function_descriptor.yson")) {
                descriptorPaths.push_back(JoinPath(directoryPath, name));
            }
        }
    }

    for (const auto& descriptorPath : descriptorPaths) {
        const auto modulePath = DescriptorPathToModulePath(descriptorPath);
        modulePaths.push_back(modulePath);

        for (auto descriptor : ParseFunctionDescriptors(ReadFileContent(descriptorPath))) {
            if (state->Functions.contains(descriptor.Name)) {
                ythrow yexception() << "Duplicate wasm UDF descriptor: " << descriptor.Name;
            }
            state->Functions.emplace(descriptor.Name, std::move(descriptor));
        }
    }

    for (const auto& modulePath : modulePaths) {
        AddModuleFromFile(state->Compartment.get(), modulePath);
    }

    return state;
}

TVector<TWasmUdfDescriptor> ListWasmUdfDescriptors(const TWasmUdfRegistryStatePtr& state)
{
    TVector<TWasmUdfDescriptor> result;
    result.reserve(state->Functions.size());
    for (const auto& item : state->Functions) {
        result.push_back(item.second);
    }
    Sort(result.begin(), result.end(), [] (const auto& left, const auto& right) {
        return left.Name < right.Name;
    });
    return result;
}

TString InvokeWasmUdfJson(
    const TWasmUdfRegistryStatePtr& state,
    const TString& functionName,
    TStringBuf argsJson)
{
    const auto* descriptor = state->Functions.FindPtr(functionName);
    if (!descriptor) {
        ythrow yexception() << "Unknown wasm UDF function: " << functionName;
    }

    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(argsJson, &root) || !root.IsArray()) {
        ythrow yexception() << "Wasm UDF arguments must be a JSON array";
    }

    const auto& jsonArgs = root.GetArray();
    if (jsonArgs.size() != descriptor->Args.size()) {
        ythrow yexception()
            << "Wasm UDF \"" << functionName << "\" expects " << descriptor->Args.size()
            << " argument(s), got " << jsonArgs.size();
    }

    auto* compartment = state->Compartment.get();
    TCurrentCompartmentGuard currentCompartment(compartment);
    TWasmUdfInvocationContext context(compartment);
    Y_DEFER {
        context.WebAssemblyPool.Clear();
    };

    TVector<TPreparedUdfArg> preparedArgs;
    preparedArgs.reserve(jsonArgs.size());
    TVector<uintptr_t> argOffsets;
    argOffsets.reserve(jsonArgs.size());
    for (size_t index = 0; index < jsonArgs.size(); ++index) {
        preparedArgs.push_back(PrepareArgFromJson(compartment, jsonArgs[index], descriptor->Args[index]));
        argOffsets.push_back(preparedArgs.back().Offset);
    }

    const auto resultOffset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    auto resultGuard = TCopyGuard(compartment, resultOffset);
    StoreValue(compartment, resultOffset, MakeEmptyValue());

    InvokeUdfExport(
        compartment,
        functionName,
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        argOffsets);

    return NJson::WriteJson(ReadResultJson(compartment, resultOffset), false);
}

TStringRef TUdfRegistryDescribe::Name()
{
    static auto name = TStringRef::Of("Describe");
    return name;
}

TType* TUdfRegistryDescribe::BuildFunctionType(IFunctionTypeInfoBuilder& builder)
{
    const auto stringType = builder.SimpleType<char*>();
    const auto argsListType = builder.List()->Item(stringType).Build();

    ui32 nameIdx = 0;
    ui32 argsIdx = 0;
    ui32 returnIdx = 0;
    const auto describeStructType = builder.Struct(3U)
        ->AddField("Name", stringType, &nameIdx)
        .AddField("Args", argsListType, &argsIdx)
        .AddField("Return", stringType, &returnIdx)
        .Build();
    const auto resultListType = builder.List()->Item(describeStructType).Build();
    return builder.Callable()
        ->Returns(resultListType)
        .Build();
}

TUdfRegistryDescribe::TUdfRegistryDescribe(TWasmUdfRegistryStatePtr state)
    : State_(std::move(state))
{
}

TUnboxedValue TUdfRegistryDescribe::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod*) const
{
    try {
        TVector<TUnboxedValue> items;
        for (const auto& descriptor : ListWasmUdfDescriptors(State_)) {
            TVector<TUnboxedValue> args;
            args.reserve(descriptor.Args.size());
            for (const auto arg : descriptor.Args) {
                args.push_back(valueBuilder->NewString(TStringRef(TString(ValueTypeToString(arg)))));
            }

            TUnboxedValue* structItems = nullptr;
            const auto describeStruct = valueBuilder->NewArray(3U, structItems);
            structItems[0] = valueBuilder->NewString(TStringRef(descriptor.Name));
            structItems[1] = valueBuilder->NewList(args.data(), args.size());
            structItems[2] = valueBuilder->NewString(TStringRef(TString(ValueTypeToString(descriptor.Result))));
            items.push_back(describeStruct);
        }
        return valueBuilder->NewList(items.data(), items.size());
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

TStringRef TUdfRegistryRun::Name()
{
    static auto name = TStringRef::Of("Run");
    return name;
}

TType* TUdfRegistryRun::BuildFunctionType(IFunctionTypeInfoBuilder& builder)
{
    return builder.Callable()
        ->Returns<char*>()
        .Arg<char*>()
        .Name("FunctionName")
        .Arg<char*>()
        .Name("ArgsJson")
        .Build();
}

TUdfRegistryRun::TUdfRegistryRun(TWasmUdfRegistryStatePtr state)
    : State_(std::move(state))
{
}

TUnboxedValue TUdfRegistryRun::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    try {
        const TString functionName(args[0].AsStringRef());
        const TString argsJson(args[1].AsStringRef());
        const auto resultJson = InvokeWasmUdfJson(State_, functionName, argsJson);
        return valueBuilder->NewString(TStringRef(resultJson));
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

} // namespace NWasm::NYQL
