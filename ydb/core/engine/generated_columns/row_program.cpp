#include "row_program.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NGeneratedColumns {
namespace {
using namespace NMiniKQL;

const Ydb::Type& PhysicalType(const Ydb::Type& type) {
    return type.has_optional_type() ? type.optional_type().item() : type;
}

NScheme::TTypeInfoMod CellType(const Ydb::Type& type) {
    NScheme::TTypeInfoMod result;
    TString error;
    Y_ENSURE(NScheme::TypeInfoFromProto(PhysicalType(type), result, error), error);
    return result;
}

void CheckType(TType* type, const Ydb::Type& expected) {
    Ydb::Type actual;
    ExportTypeToProto(type, actual);
    Y_ENSURE(actual.has_optional_type() == expected.has_optional_type()
        && CellType(actual).TypeInfo == CellType(expected).TypeInfo,
        "Generated column program type does not match its bound schema");
}

NUdf::TUnboxedValue ReadCell(const TCell& cell, NScheme::TTypeInfo type) {
    if (cell.IsNull()) {
        return {};
    }
    if (type.GetTypeId() == NScheme::NTypeIds::Pg) {
        return NYql::NCommon::PgValueFromNativeBinary(cell.AsBuf(), NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc()));
    }
    switch (type.GetTypeId()) {
#define READ_FIXED_CELL(dataType, layout) \
    case NUdf::TDataType<dataType>::Id: \
        Y_ENSURE(cell.Size() == sizeof(layout), "Invalid generated column input cell size"); \
        return NUdf::TUnboxedValuePod(cell.AsValue<layout>());
        KNOWN_FIXED_VALUE_TYPES(READ_FIXED_CELL)
#undef READ_FIXED_CELL
    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        Y_ENSURE(cell.Size() == sizeof(NYql::NDecimal::TInt128), "Invalid decimal cell size");
        NYql::NDecimal::TInt128 value;
        std::memcpy(&value, cell.Data(), sizeof(value));
        return NUdf::TUnboxedValuePod(value);
    }
    default:
        Y_ENSURE(NScheme::NTypeIds::IsYqlType(type.GetTypeId()), "Unsupported generated input type");
        return MakeString(NUdf::TStringRef(cell.Data(), cell.Size()));
    }
}

TSerializedCellVec WriteCell(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type, i32 typmod, ui64 memoryLimit) {
    const auto own = [memoryLimit](const TCell& cell) {
        Y_ENSURE(cell.Size() <= memoryLimit, "Generated column result exceeds program memory limit");
        return TSerializedCellVec(TConstArrayRef<TCell>(&cell, 1));
    };
    if (!value) {
        return own(TCell());
    }
    switch (type.GetTypeId()) {
#define WRITE_FIXED_CELL(dataType, layout) \
    case NUdf::TDataType<dataType>::Id: return own(TCell::Make(value.Get<layout>()));
        KNOWN_FIXED_VALUE_TYPES(WRITE_FIXED_CELL)
#undef WRITE_FIXED_CELL
    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        const auto decimal = value.GetInt128();
        return own(TCell(reinterpret_cast<const char*>(&decimal), sizeof(decimal)));
    }
    case NScheme::NTypeIds::Pg: {
        const auto oid = NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc());
        TMaybe<TString> error;
        const TString binary = typmod != -1 && NPg::TypeDescNeedsCoercion(type.GetPgTypeDesc())
            ? NYql::NCommon::PgValueCoerce(value, oid, typmod, &error)
            : NYql::NCommon::PgValueToNativeBinary(value, oid);
        Y_ENSURE(!error, error.GetOrElse(""));
        return own(TCell(binary.data(), binary.size()));
    }
    default: {
        const auto text = value.AsStringRef();
        return own(TCell(text.Data(), text.Size()));
    }
    }
}

class TRowProgram final : public IRowProgram {
public:
    TRowProgram(const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
        const IFunctionRegistry& registry, ui64 memoryLimitBytes)
        : Descriptor(descriptor)
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , Random(CreateDefaultRandomProvider())
        , Time(CreateDefaultTimeProvider())
        , MemoryLimit(memoryLimitBytes)
    {
        Y_ENSURE(memoryLimitBytes, "Generated column program memory limit must be positive");
        Alloc.SetLimit(memoryLimitBytes);
        THashSet<TString> dependencyNames;
        THashSet<ui32> dependencyIds;
        for (const auto& dependency : descriptor.GetDependencies()) {
            Y_ENSURE(!dependency.GetName().empty() && dependency.GetColumnId() && dependency.HasType(),
                "Incomplete generated column dependency binding");
            Y_ENSURE(dependencyNames.insert(dependency.GetName()).second && dependencyIds.insert(dependency.GetColumnId()).second,
                "Duplicate generated column dependency binding");
            const auto type = CellType(dependency.GetType());
            if (type.TypeInfo.GetTypeId() == NScheme::NTypeIds::Pg && !type.TypeMod.empty()) {
                const auto modifier = NPg::BinaryTypeModFromTextTypeMod(type.TypeMod, type.TypeInfo.GetPgTypeDesc());
                Y_ENSURE(!modifier.Error, modifier.Error.GetOrElse(""));
            }
        }
        Y_ENSURE(descriptor.GetProgramFormatVersion() == 1, "Unsupported generated column program format");
        Y_ENSURE(descriptor.GetMiniKqlRuntimeVersion() == RuntimeVersion.Value(), "Incompatible generated column MiniKQL runtime version");
        Y_ENSURE(descriptor.HasLanguageVersion() && (descriptor.GetLanguageVersion() == NYql::UnknownLangVersion
            || NYql::IsValidLangVersion(descriptor.GetLanguageVersion())), "Unsupported generated column language version");
        Y_ENSURE(descriptor.HasRuntimeSettings(), "Missing generated column runtime settings");
        Settings = NYql::CreateRuntimeSettingsFromString(descriptor.GetRuntimeSettings());
        Y_ENSURE(!descriptor.GetProgram().empty(), "Empty generated column program");
        auto root = DeserializeRuntimeNode(descriptor.GetProgram(), Env);
        CheckType(root.GetStaticType(), descriptor.GetOutputType());
        OutputType = CellType(descriptor.GetOutputType());
        if (OutputType.TypeInfo.GetTypeId() == NScheme::NTypeIds::Pg && !OutputType.TypeMod.empty()) {
            const auto typmod = NPg::BinaryTypeModFromTextTypeMod(OutputType.TypeMod, OutputType.TypeInfo.GetPgTypeDesc());
            Y_ENSURE(!typmod.Error, typmod.Error.GetOrElse(""));
            OutputTypmod = typmod.Typmod;
        }
        Explorer.Walk(root.GetNode(), Env.GetNodeStack());
        THashSet<TString> requiredUdfs;
        for (auto* node : Explorer.GetNodes()) {
            if (node->GetType()->GetKind() != TType::EKind::Callable) {
                continue;
            }
            auto* callable = static_cast<TCallable*>(node);
            const auto name = callable->GetType()->GetName();
            Y_ENSURE(name != "ScriptUdf", "ScriptUdf is unsupported in generated column backfills");
            if (name == "Udf") {
                Y_ENSURE(callable->GetInputsCount() > 0 && callable->GetInput(0).HasValue(), "Malformed generated column UDF");
                const TString function(AS_VALUE(TDataLiteral, callable->GetInput(0))->AsValue().AsStringRef());
                const TString module = function.substr(0, function.find('.'));
                Y_ENSURE(!NUdfStore::NWasm::GetWasmModuleCatalog().FindByModuleName(module),
                    "WASM UDF is unsupported in generated column backfills: " << function);
                Y_ENSURE(registry.IsLoadedUdfModule(module), "Missing generated column native UDF: " << function);
                requiredUdfs.insert(function);
            }
        }
        THashSet<TString> boundUdfs(descriptor.GetRequiredUdfNames().begin(), descriptor.GetRequiredUdfNames().end());
        Y_ENSURE(requiredUdfs == boundUdfs, "Generated column required UDF binding does not match the program");
        auto composite = GetCompositeWithBuiltinFactory({GetYqlFactory(), NYql::GetPgFactory()});
        auto factory = [this, composite](TCallable& callable, const TComputationNodeFactoryContext& context) -> IComputationNode* {
            if (callable.GetType()->GetName() == "GeneratedColumnInput") {
                Y_ENSURE(!InputNode && callable.GetInputsCount() == 0, "Invalid generated row input");
                auto* row = callable.GetType()->GetReturnType();
                Y_ENSURE(row->IsStruct(), "Generated column input must be a struct");
                InputType = static_cast<TStructType*>(row);
                Y_ENSURE(InputType->GetMembersCount() == Descriptor.DependenciesSize(), "Generated column dependency count mismatch");
                for (const auto& dependency : Descriptor.GetDependencies()) {
                    auto index = InputType->FindMemberIndex(dependency.GetName());
                    Y_ENSURE(index, "Generated column dependency is missing: " << dependency.GetName());
                    CheckType(InputType->GetMemberType(*index), dependency.GetType());
                    InputIndices.push_back(*index);
                    InputTypes.push_back(CellType(dependency.GetType()).TypeInfo);
                }
                return InputNode = new TExternalComputationNode(context.Mutables);
            }
            return composite(callable, context);
        };
        TComputationPatternOpts options(Alloc.Ref(), Env, factory, &registry,
            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "", EGraphPerProcess::Multi,
            nullptr, nullptr, nullptr, nullptr, descriptor.GetLanguageVersion(), Settings);
        Pattern = MakeComputationPattern(Explorer, root, {root.GetNode()}, options);
        Options = MakeHolder<TComputationOptsFull>(options.ToComputationOptions(*Random, *Time));
        // Preparation validates callables and native modules but never evaluates the expression.
        auto graph = Pattern->Clone(*Options);
        TBindTerminator terminator(graph->GetTerminator());
        graph->Prepare();
        Y_ENSURE(Alloc.GetUsed() <= MemoryLimit, "Generated column preparation exceeds program memory limit");
        graph.Reset();
        Alloc.Release();
    }

    ~TRowProgram() override {
        Alloc.Acquire();
    }

    bool Evaluate(TConstArrayRef<TCell> inputs, TSerializedCellVec& result, TString& error) override {
        error.clear();
        auto guard = Guard(Alloc);
        try {
            Y_ENSURE(inputs.size() == Descriptor.DependenciesSize(), "Generated column input count mismatch");
            ui64 inputBytes = 0;
            for (const auto& cell : inputs) {
                inputBytes += cell.Size();
            }
            Y_ENSURE(inputBytes <= MemoryLimit, "Generated column input exceeds program memory limit");
            // Clone a clean context for each row. Invalidate(input) alone misses zero-input
            // nondeterministic nodes and state captured by nested lambdas/native UDFs.
            // Native UDFs can build type information during evaluation: keep those types
            // in a row arena, with a lifetime longer than every value in the cloned graph.
            TTypeEnvironment rowEnv(Alloc);
            TComputationOptsFull rowOptions(Options->Stats, Alloc.Ref(), rowEnv, *Random, *Time,
                Options->ValidatePolicy, Options->SecureParamsProvider, Options->CountersProvider,
                Options->LogProvider, Options->LangVer, Options->RuntimeSettings,
                Options->BridgeMode, Options->BridgeBinaryPath);
            auto graph = Pattern->Clone(rowOptions);
            TBindTerminator terminator(graph->GetTerminator());
            graph->Prepare();
            if (InputNode) {
                NUdf::TUnboxedValue* members = nullptr;
                NUdf::TUnboxedValue row = graph->GetHolderFactory().CreateDirectArrayHolder(inputs.size(), members);
                for (size_t i = 0; i < inputs.size(); ++i) {
                    Y_ENSURE(!inputs[i].IsNull() || Descriptor.GetDependencies(i).GetType().has_optional_type()
                        || PhysicalType(Descriptor.GetDependencies(i).GetType()).has_pg_type(),
                        "NULL in NOT NULL generated column dependency " << Descriptor.GetDependencies(i).GetName());
                    members[InputIndices[i]] = ReadCell(inputs[i], InputTypes[i]);
                }
                InputNode->SetValue(graph->GetContext(), std::move(row));
            }
            auto value = graph->GetValue();
            Y_ENSURE(value || Descriptor.GetOutputType().has_optional_type() || PhysicalType(Descriptor.GetOutputType()).has_pg_type(),
                "Generated column expression returned NULL for a NOT NULL column");
            result = WriteCell(value, OutputType.TypeInfo, OutputTypmod, MemoryLimit);
            return true;
        } catch (...) {
            error = TStringBuilder() << "Generated column evaluation failed: " << CurrentExceptionMessage();
            return false;
        }
    }

private:
    NKikimrIndexBuilder::TGeneratedColumnBuild Descriptor;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TIntrusivePtr<IRandomProvider> Random;
    TIntrusivePtr<ITimeProvider> Time;
    const ui64 MemoryLimit;
    NYql::TRuntimeSettings::TConstPtr Settings;
    NScheme::TTypeInfoMod OutputType;
    i32 OutputTypmod = -1;
    TStructType* InputType = nullptr;
    IComputationExternalNode* InputNode = nullptr;
    TVector<ui32> InputIndices;
    TVector<NScheme::TTypeInfo> InputTypes;
    TExploringNodeVisitor Explorer;
    IComputationPattern::TPtr Pattern;
    THolder<TComputationOptsFull> Options;
};
} // namespace

THolder<IRowProgram> CreateRowProgram(const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
    const NMiniKQL::IFunctionRegistry& registry, ui64 memoryLimitBytes, TString& error)
{
    error.clear();
    try {
        return MakeHolder<TRowProgram>(descriptor, registry, memoryLimitBytes);
    } catch (...) {
        error = TStringBuilder() << "Cannot prepare generated column program: " << CurrentExceptionMessage();
        return {};
    }
}
} // namespace NKikimr::NGeneratedColumns
