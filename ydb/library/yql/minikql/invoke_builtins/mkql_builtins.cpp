#include "mkql_builtins.h"
#include "mkql_builtins_impl.h"  // Y_IGNORE
#include "mkql_builtins_compare.h"
#include "mkql_builtins_string_kernels.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <util/digest/murmur.h>
#include <util/generic/yexception.h>
#include <util/generic/maybe.h>

#include <algorithm>

#include <arrow/compute/registry.h>
#include <arrow/compute/registry_internal.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

void RegisterDefaultOperations(IBuiltinFunctionRegistry& registry, TKernelFamilyMap& kernelFamilyMap) {
    RegisterAdd(registry);
    RegisterAdd(kernelFamilyMap);
    RegisterAggrAdd(registry);
    RegisterSub(registry);
    RegisterSub(kernelFamilyMap);
    RegisterMul(registry);
    RegisterMul(kernelFamilyMap);
    RegisterDiv(registry);
    RegisterDiv(kernelFamilyMap);
    RegisterMod(registry);
    RegisterMod(kernelFamilyMap);
    RegisterIncrement(registry);
    RegisterDecrement(registry);
    RegisterBitAnd(registry);
    RegisterBitOr(registry);
    RegisterBitXor(registry);
    RegisterShiftLeft(registry);
    RegisterShiftRight(registry);
    RegisterRotLeft(registry);
    RegisterRotRight(registry);
    RegisterPlus(registry);
    RegisterMinus(registry);
    RegisterMinus(kernelFamilyMap);
    RegisterBitNot(registry);
    RegisterCountBits(registry);
    RegisterAbs(registry);
    RegisterAbs(kernelFamilyMap);
    RegisterConvert(registry);
    RegisterConcat(registry);
    RegisterSubstring(registry);
    RegisterFind(registry);
    RegisterWith(registry);
    RegisterWith(kernelFamilyMap);
    RegisterInversePresortString(registry);
    RegisterInverseString(registry);
    RegisterNanvl(registry);
    RegisterByteAt(registry);
    RegisterMax(registry);
    RegisterMin(registry);
    RegisterAggrMax(registry);
    RegisterAggrMin(registry);
    RegisterEquals(registry);
    RegisterEquals(kernelFamilyMap);
    RegisterNotEquals(registry);
    RegisterNotEquals(kernelFamilyMap);
    RegisterLess(registry);
    RegisterLess(kernelFamilyMap);
    RegisterLessOrEqual(registry);
    RegisterLessOrEqual(kernelFamilyMap);
    RegisterGreater(registry);
    RegisterGreater(kernelFamilyMap);
    RegisterGreaterOrEqual(registry);
    RegisterGreaterOrEqual(kernelFamilyMap);
    // Size is missing in registry
    RegisterSizeBuiltin(kernelFamilyMap);
}

void PrintType(NUdf::TDataTypeId schemeType, bool isOptional, IOutputStream& out)
{
    const auto slot = NUdf::FindDataSlot(schemeType);
    out << (slot ? NUdf::GetDataTypeInfo(*slot).Name : "unknown");

    if (isOptional) {
        out << '?';
    }
}

void PrintFunctionSignature(
        const std::string_view& funcName,
        const TFunctionDescriptor& desc,
        IOutputStream& out)
{
    const auto* param = desc.ResultAndArgs;
    out << '\t';

    // print results type
    PrintType(param->SchemeType, param->IsNullable(), out);
    ++param;

    // print function name and args types
    out << ' ' << funcName << '(';
    while (param->SchemeType != 0) {
        PrintType(param->SchemeType, param->IsNullable(), out);
        ++param;
        if (param->SchemeType != 0) {
            out << ", ";
        }
    }
    out << ')';
}

bool IsArgumentsMatch(
        const TFunctionParamMetadata* paramsMetadata,
        const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount)
{
    size_t index = 0;

    while (paramsMetadata->SchemeType) {
        if (index >= argTypesCount) {
            return false;
        }

        if (argTypes[index].first != paramsMetadata->SchemeType) {
            return false;
        }

        if (argTypes[index].second != paramsMetadata->IsNullable()) {
            return false;
        }

        ++paramsMetadata;
        ++index;
    }

    return index == argTypesCount;
}

//////////////////////////////////////////////////////////////////////////////
// TBuiltinFunctionRegistry
//////////////////////////////////////////////////////////////////////////////
class TBuiltinFunctionRegistry: public IBuiltinFunctionRegistry
{
public:
    TBuiltinFunctionRegistry();

private:
    TFunctionDescriptor GetBuiltin(const std::string_view& name,
            const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount) const final;

    bool HasBuiltin(const std::string_view& name) const final;

    ui64 GetMetadataEtag() const final;

    void PrintInfoTo(IOutputStream& out) const final;

    void Register(const std::string_view& name, const TFunctionDescriptor& description) final;

    void RegisterAll(TFunctionsMap&& functions, TFunctionParamMetadataList&& arguments) final;

    const TFunctionsMap& GetFunctions() const final;

    void CalculateMetadataEtag();

    std::optional<TFunctionDescriptor> FindBuiltin(const std::string_view& name, const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount) const;

    const TDescriptionList& FindCandidates(const std::string_view& name) const;

    const TKernel* FindKernel(const std::string_view& name, const NUdf::TDataTypeId* argTypes, size_t argTypesCount, NUdf::TDataTypeId returnType) const final;

    void RegisterKernelFamily(const std::string_view& name, std::unique_ptr<TKernelFamily>&& family) final;

    TVector<std::pair<TString, const TKernelFamily*>> GetAllKernelFamilies() const final;

    TFunctionsMap Functions;
    TFunctionParamMetadataList ArgumentsMetadata;
    std::optional<ui64> MetadataEtag;
    TKernelFamilyMap KernelFamilyMap;
};

TBuiltinFunctionRegistry::TBuiltinFunctionRegistry()
{
    RegisterDefaultOperations(*this, KernelFamilyMap);
    CalculateMetadataEtag();
}

void TBuiltinFunctionRegistry::Register(const std::string_view& name, const TFunctionDescriptor& description)
{
    Functions[TString(name)].push_back(description);
}

void TBuiltinFunctionRegistry::RegisterAll(TFunctionsMap&& functions, TFunctionParamMetadataList&& arguments)
{
    Functions = std::move(functions);
    ArgumentsMetadata = std::move(arguments);
    CalculateMetadataEtag();
}

const TFunctionsMap& TBuiltinFunctionRegistry::GetFunctions() const
{
    return Functions;
}

const TDescriptionList& TBuiltinFunctionRegistry::FindCandidates(const std::string_view& name) const {
    if (const auto it = Functions.find(TString(name)); it != Functions.cend())
        return it->second;

    ythrow yexception() << "Not found builtin function: '" << name << "' in " << Functions.size() << " total.";
}

std::optional<TFunctionDescriptor> TBuiltinFunctionRegistry::FindBuiltin(const std::string_view& name, const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount) const
{
    for (const auto& desc: FindCandidates(name)) {
        if (IsArgumentsMatch(desc.ResultAndArgs, argTypes, argTypesCount)) {
            return desc;
        }
    }

    return std::nullopt;
}

TFunctionDescriptor TBuiltinFunctionRegistry::GetBuiltin(const std::string_view& name,
        const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount) const
{
    if (const auto desc = FindBuiltin(name, argTypes, argTypesCount)) {
        return *desc;
    }

    TStringStream ss;
    PrintType(argTypes[0].first, argTypes[0].second, ss);
    ss << ' ' << name << '(';
    for (size_t i = 1U; i < argTypesCount; i++) {
        if (i > 1U) {
            ss << ", ";
        }
        PrintType(argTypes[i].first, argTypes[i].second, ss);
    }
    ss << ").\nCandidates are: [\n";
    ui32 i = 0;
    for (const TFunctionDescriptor& desc: FindCandidates(name)) {
        PrintFunctionSignature(name, desc, ss);
        ss << '\n';
        if (++i > 32) {
            ss << "\t...\n";
            break;
        }
    }
    ss << ']';

    ythrow yexception() << "Unsupported builtin function: " << ss.Str();
}

bool TBuiltinFunctionRegistry::HasBuiltin(const std::string_view& name) const
{
    return Functions.find(TString(name)) != Functions.cend();
}

void TBuiltinFunctionRegistry::CalculateMetadataEtag() {
    using TFunctionPair = std::pair<std::string_view, const TDescriptionList*>;

    std::vector<TFunctionPair> operations;
    for (const auto& func : Functions) {
        operations.emplace_back(func.first, &func.second);
    }

    std::sort(operations.begin(), operations.end(), [](const TFunctionPair& x, const TFunctionPair& y) {
        return x.first < y.first;
    });

    ui64 hash = 0;

    for (const auto& op : operations) {
        const ui64 nameLength = op.first.size();
        hash = MurmurHash<ui64>(&nameLength, sizeof(nameLength), hash);
        hash = MurmurHash<ui64>(op.first.data(), op.first.size(), hash);
        const auto& descriptions = *op.second;
        const ui64 descriptionCount = descriptions.size();
        hash = MurmurHash<ui64>(&descriptionCount, sizeof(descriptionCount), hash);
        for (const auto& description : descriptions) {
            for (const auto* args = description.ResultAndArgs; args->SchemeType; ++args) {
                hash = MurmurHash<ui64>(args, sizeof(*args), hash);
            }
        }
    }

    MetadataEtag = hash;
}

ui64 TBuiltinFunctionRegistry::GetMetadataEtag() const
{
    return *MetadataEtag;
}

void TBuiltinFunctionRegistry::PrintInfoTo(IOutputStream& out) const
{
    for (const auto& f: Functions) {
         out << f.first << ": [\n";

         for (const TFunctionDescriptor& desc: f.second) {
            PrintFunctionSignature(f.first, desc, out);
            out << '\n';
         }

         out << "]\n\n";
    }
}

const TKernel* TBuiltinFunctionRegistry::FindKernel(const std::string_view& name, const NUdf::TDataTypeId* argTypes, size_t argTypesCount, NUdf::TDataTypeId returnType) const {
    auto fit = KernelFamilyMap.find(TString(name));
    if (fit == KernelFamilyMap.end()) {
        return nullptr;
    }

    return fit->second->FindKernel(argTypes, argTypesCount, returnType);
}

void TBuiltinFunctionRegistry::RegisterKernelFamily(const std::string_view& name, std::unique_ptr<TKernelFamily>&& family) {
    Y_ENSURE(KernelFamilyMap.emplace(TString(name), std::move(family)).second);
}

TVector<std::pair<TString, const TKernelFamily*>> TBuiltinFunctionRegistry::GetAllKernelFamilies() const {
    TVector<std::pair<TString, const TKernelFamily*>> ret;
    for (const auto& f : KernelFamilyMap) {
        ret.emplace_back(std::make_pair(f.first, f.second.get()));
    }

    return ret;
}

} // namespace

IBuiltinFunctionRegistry::TPtr CreateBuiltinRegistry() {
    return MakeIntrusive<TBuiltinFunctionRegistry>();
}

} // namespace NMiniKQL
} // namespace NKikimr
