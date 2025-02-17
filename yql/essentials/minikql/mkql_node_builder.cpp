#include "mkql_node_builder.h"
#include "mkql_node_printer.h"

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NDetail;

namespace {
    TNode* BuildCallableTypePayload(const TVector<TStringBuf>& argNames,
        const TVector<ui64>& argFlags, TStringBuf payload, const TTypeEnvironment& env) {
        TStructTypeBuilder itemTypeBuilder(env);
        itemTypeBuilder.Add("Name", TDataType::Create(NUdf::TDataType<char*>::Id, env));
        itemTypeBuilder.Add("Flags", TDataType::Create(NUdf::TDataType<ui64>::Id, env));
        TType* itemType = itemTypeBuilder.Build();

        bool startedNames = false;
        THashSet<TStringBuf> usedNames;
        TListLiteralBuilder argsListBuilder(env, itemType);
        for (ui32 i = 0; i < argNames.size(); ++i) {
            TStructLiteralBuilder itemBuilder(env);
            auto name = argNames[i];
            auto flags = argFlags[i];
            bool hasName = name.size() > 0;
            if (startedNames) {
                MKQL_ENSURE(hasName, "Named arguments already started");
            } else {
                startedNames = hasName;
            }

            if (hasName) {
                MKQL_ENSURE(usedNames.insert(name).second, "Duplication of argument name: " << name);
            }

            itemBuilder.Add("Name", TRuntimeNode(BuildDataLiteral(name, NUdf::EDataSlot::String, env), true));
            itemBuilder.Add("Flags", TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(flags), NUdf::EDataSlot::Uint64, env), true));
            argsListBuilder.Add(TRuntimeNode(itemBuilder.Build(), true));
        }

        TRuntimeNode argsList = TRuntimeNode(argsListBuilder.Build(), true);
        TStructLiteralBuilder payloadBuilder(env);
        payloadBuilder.Add("Args", argsList);
        payloadBuilder.Add("Payload", TRuntimeNode(BuildDataLiteral(payload, NUdf::EDataSlot::String, env), true));
        return payloadBuilder.Build();
    }
}

TDataLiteral* BuildDataLiteral(const NUdf::TUnboxedValuePod& value, NUdf::TDataTypeId type, const TTypeEnvironment& env) {
    return TDataLiteral::Create(value, TDataType::Create(type, env), env);
}

TDataLiteral* BuildDataLiteral(const NUdf::TStringRef& data, NUdf::TDataTypeId type, const TTypeEnvironment& env) {
    return BuildDataLiteral(env.NewStringValue(data), type, env);
}

TOptionalLiteral* BuildOptionalLiteral(TRuntimeNode value, const TTypeEnvironment& env) {
    auto type = TOptionalType::Create(value.GetStaticType(), env);
    return TOptionalLiteral::Create(value, type, env);
}

TOptionalLiteral* BuildEmptyOptionalLiteral(TType *itemType, const TTypeEnvironment& env) {
    auto type = TOptionalType::Create(itemType, env);
    return TOptionalLiteral::Create(type, env);
}

TOptionalLiteral* BuildEmptyOptionalDataLiteral(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env) {
    return BuildEmptyOptionalLiteral(TDataType::Create(schemeType, env), env);
}

TType* UnpackOptional(TRuntimeNode data, bool& isOptional) {
    return UnpackOptional(data.GetStaticType(), isOptional);
}

TType* UnpackOptional(TType* type, bool& isOptional) {
    isOptional = type->IsOptional();
    if (!isOptional)
        return type;

    auto optType = static_cast<TOptionalType*>(type);
    return optType->GetItemType();
}

TDataType* UnpackOptionalData(TRuntimeNode data, bool& isOptional) {
    return UnpackOptionalData(data.GetStaticType(), isOptional);
}

TDataType* UnpackOptionalData(TType* type, bool& isOptional) {
    auto unpackedType = UnpackOptional(type, isOptional);
    MKQL_ENSURE(unpackedType->IsData(),
        "Expected data or optional of data, actual: " << PrintNode(type, true));

    return static_cast<TDataType*>(unpackedType);
}

TBlockType::EShape GetResultShape(const TVector<TType*>& types) {
    TBlockType::EShape result = TBlockType::EShape::Scalar;
    for (auto& type : types) {
        MKQL_ENSURE(type->IsBlock(),
                    "Expected block, actual: " << PrintNode(type, true));
        if (static_cast<TBlockType*>(type)->GetShape() == TBlockType::EShape::Many) {
            result = TBlockType::EShape::Many;
        }
    }
    return result;
}

TTupleLiteralBuilder::TTupleLiteralBuilder(const TTypeEnvironment& env) : Env(env)
{}

void TTupleLiteralBuilder::Reserve(ui32 size) {
    Types.reserve(size);
    Values.reserve(size);
}

TTupleLiteralBuilder& TTupleLiteralBuilder::Add(TRuntimeNode value) {
    Types.push_back(value.GetRuntimeType());
    Values.push_back(value);
    return *this;
}

TTupleLiteral* TTupleLiteralBuilder::Build() {
    const auto& type = TTupleType::Create(Types.size(), Types.data(), Env);
    return TTupleLiteral::Create(Values.size(), Values.data(), type, Env);
}

void TTupleLiteralBuilder::Clear() {
    Values.clear();
    Types.clear();
}

TStructTypeBuilder::TStructTypeBuilder(const TTypeEnvironment& env)
    : Env(&env)
{
}

void TStructTypeBuilder::Reserve(ui32 size) {
    Members.reserve(size);
}

TStructTypeBuilder& TStructTypeBuilder::Add(const TStringBuf& name, TType* type, ui32* index) {
    Members.push_back(TStructMember(Env->InternName(name).Str(), type, index));
    return *this;
}

TStructType* TStructTypeBuilder::Build() {
    if (Members.empty())
        return Env->GetEmptyStructLazy()->GetType();

    Sort(Members.begin(), Members.end());
    return TStructType::Create(Members.size(), Members.data(), *Env);
}

void TStructTypeBuilder::FillIndexes() {
    ui32 index = 0;
    for (const TStructMember& member: Members) {
        if (member.Index) {
            *(member.Index) = index++;
        }
    }
}

void TStructTypeBuilder::Clear() {
    Members.clear();
}

TStructLiteralBuilder::TStructLiteralBuilder(const TTypeEnvironment& env)
    : Env(&env)
{
}

void TStructLiteralBuilder::Reserve(ui32 size) {
    Members.reserve(size);
    Values.reserve(size);
}

TStructLiteralBuilder& TStructLiteralBuilder::Add(const TStringBuf& name, TRuntimeNode value) {
    TType* valueType = value.GetStaticType();
    Members.push_back(TStructMember(Env->InternName(name).Str(), valueType));
    Values.push_back(value);
    return *this;
}

TStructLiteral* TStructLiteralBuilder::Build() {
    Y_DEBUG_ABORT_UNLESS(Members.size() == Values.size());
    if (Members.empty())
        return Env->GetEmptyStructLazy();

    TVector<std::pair<TStringBuf, ui32>> sortedIndicies(Members.size());
    for (ui32 i = 0, e = Members.size(); i < e; ++i) {
        sortedIndicies[i] = std::make_pair(Members[i].Name, i);
    }

    Sort(sortedIndicies.begin(), sortedIndicies.end(),
        [](const std::pair<TStringBuf, ui32>& x, const std::pair<TStringBuf, ui32>& y) {
            return x.first < y.first;
    });

    TVector<TStructMember> sortedMembers(Members.size());
    TVector<TRuntimeNode> sortedValues(Members.size());

    for (ui32 i = 0, e = Members.size(); i < e; ++i) {
        sortedMembers[i] = Members[sortedIndicies[i].second];
        sortedValues[i] = Values[sortedIndicies[i].second];
    }

    auto type = TStructType::Create(sortedMembers.size(), sortedMembers.data(), *Env);
    return TStructLiteral::Create(sortedValues.size(), sortedValues.data(), type, *Env);
}

void TStructLiteralBuilder::Clear() {
    Members.clear();
}

TListLiteralBuilder::TListLiteralBuilder(const TTypeEnvironment& env, TType* type)
    : Env(&env)
    , Type(type)
{}

TListLiteralBuilder& TListLiteralBuilder::Add(TRuntimeNode item) {
    Items.push_back(item);
    return *this;
}

TListLiteral* TListLiteralBuilder::Build() {
    auto type = TListType::Create(Type, *Env);
    return TListLiteral::Create(Items.data(), Items.size(), type, *Env);
}

void TListLiteralBuilder::Clear() {
    Items.clear();
}

TDictLiteralBuilder::TDictLiteralBuilder(const TTypeEnvironment& env, TType* keyType, TType* payloadType)
    : Env(&env)
    , KeyType(keyType)
    , PayloadType(payloadType)
{
}

void TDictLiteralBuilder::Reserve(ui32 size) {
    Items.reserve(size);
}

TDictLiteralBuilder& TDictLiteralBuilder::Add(TRuntimeNode key, TRuntimeNode payload) {
    Items.push_back(std::make_pair(key, payload));
    return *this;
}

TDictLiteral* TDictLiteralBuilder::Build() {
    auto type = TDictType::Create(KeyType, PayloadType, *Env);
    return TDictLiteral::Create(Items.size(), Items.data(), type, *Env);
}

void TDictLiteralBuilder::Clear() {
    Items.clear();
}

TCallableTypeBuilder::TCallableTypeBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType)
    : Env(&env)
    , Name(Env->InternName(name))
    , ReturnType(returnType)
    , OptionalArgsCount(0)
    , HasPayload(false)
{}

void TCallableTypeBuilder::Reserve(ui32 size) {
    Arguments.reserve(size);
    ArgNames.reserve(size);
    ArgFlags.reserve(size);
}

TCallableTypeBuilder& TCallableTypeBuilder::Add(TType *type) {
    Arguments.push_back(type);
    ArgNames.emplace_back();
    ArgFlags.emplace_back();
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetArgumentName(const TStringBuf& name) {
    MKQL_ENSURE(!ArgNames.empty(), "No arguments");
    HasPayload = true;
    ArgNames.back() = name;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetArgumentFlags(ui64 flags) {
    MKQL_ENSURE(!ArgFlags.empty(), "No arguments");
    HasPayload = true;
    ArgFlags.back() = flags;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetOptionalArgs(ui32 count) {
    OptionalArgsCount = count;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetPayload(const TStringBuf& data) {
    HasPayload = true;
    FuncPayload = data;
    return *this;
}

TCallableType* TCallableTypeBuilder::Build() {
    TNode* payload = nullptr;
    if (HasPayload) {
        payload = BuildCallableTypePayload(ArgNames, ArgFlags, FuncPayload, *Env);
    }

    auto ret = TCallableType::Create(ReturnType, Name.Str(), Arguments.size(), Arguments.data(), payload, *Env);
    ret->SetOptionalArgumentsCount(OptionalArgsCount);
    return ret;
}

void TCallableTypeBuilder::Clear() {
    Arguments.clear();
    ArgNames.clear();
    ArgFlags.clear();
    OptionalArgsCount = 0;
    HasPayload = false;
    FuncPayload = TStringBuf();
}

TCallableBuilder::TCallableBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType, bool disableMerge)
    : Env(&env)
    , Name(Env->InternName(name))
    , ReturnType(returnType)
    , DisableMerge(disableMerge)
    , OptionalArgsCount(0)
    , HasPayload(false)
{}

void TCallableBuilder::Reserve(ui32 size) {
    Arguments.reserve(size);
    Inputs.reserve(size);
    ArgNames.reserve(size);
    ArgFlags.reserve(size);
}

TCallableBuilder& TCallableBuilder::Add(TRuntimeNode input) {
    TType* inputType = input.GetStaticType();
    Arguments.push_back(inputType);
    Inputs.push_back(input);
    ArgNames.emplace_back();
    ArgFlags.emplace_back();
    return *this;
}

TCallableBuilder& TCallableBuilder::SetOptionalArgs(ui32 count) {
    OptionalArgsCount = count;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetTypePayload(const TStringBuf& data) {
    HasPayload = true;
    FuncPayload = data;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetArgumentName(const TStringBuf& name) {
    MKQL_ENSURE(!ArgNames.empty(), "No arguments");
    HasPayload = true;
    ArgNames.back() = name;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetArgumentFlags(ui64 flags) {
    MKQL_ENSURE(!ArgFlags.empty(), "No arguments");
    HasPayload = true;
    ArgFlags.back() = flags;
    return *this;
}

TCallable* TCallableBuilder::Build() {
    TNode* payload = nullptr;
    if (HasPayload) {
        payload = BuildCallableTypePayload(ArgNames, ArgFlags, FuncPayload, *Env);
    }

    auto type = TCallableType::Create(ReturnType, Name.Str(), Arguments.size(), Arguments.data(), payload, *Env);
    type->SetOptionalArgumentsCount(OptionalArgsCount);
    if (DisableMerge)
        type->DisableMerge();
    return TCallable::Create(Inputs.size(), Inputs.data(), type, *Env);
}

void TCallableBuilder::Clear() {
    Arguments.clear();
    Inputs.clear();
    ArgNames.clear();
    ArgFlags.clear();
    OptionalArgsCount = 0;
    FuncPayload = TStringBuf();
}

}
}
