#include "mkql_node_builder.h"
#include "mkql_node_printer.h"

#include <util/generic/algorithm.h>

namespace NKikimr::NMiniKQL {

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
} // namespace

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

TOptionalLiteral* BuildEmptyOptionalLiteral(TType* itemType, const TTypeEnvironment& env) {
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
    if (!isOptional) {
        return type;
    }

    auto optType = static_cast<TOptionalType*>(type);
    return optType->GetItemType();
}

TDataType* UnpackOptionalData(TRuntimeNode data, bool& isOptional) {
    return UnpackOptionalData(data.GetStaticType(), isOptional);
}

TType* UnpackOptionalBlockItemType(TBlockType* type, const TTypeEnvironment& env) {
    MKQL_ENSURE(type->IsBlock(), "Expected block type.");
    bool isOptional;
    auto* unpackedType = UnpackOptional(type->GetItemType(), isOptional);
    MKQL_ENSURE(isOptional, "Must be optional type.");

    return TBlockType::Create(unpackedType, type->GetShape(), env);
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

TTupleLiteralBuilder::TTupleLiteralBuilder(const TTypeEnvironment& env)
    : Env_(env)
{
}

void TTupleLiteralBuilder::Reserve(ui32 size) {
    Types_.reserve(size);
    Values_.reserve(size);
}

TTupleLiteralBuilder& TTupleLiteralBuilder::Add(TRuntimeNode value) {
    Types_.push_back(value.GetRuntimeType());
    Values_.push_back(value);
    return *this;
}

TTupleLiteral* TTupleLiteralBuilder::Build() {
    const auto& type = TTupleType::Create(Types_.size(), Types_.data(), Env_);
    return TTupleLiteral::Create(Values_.size(), Values_.data(), type, Env_);
}

void TTupleLiteralBuilder::Clear() {
    Values_.clear();
    Types_.clear();
}

TStructTypeBuilder::TStructTypeBuilder(const TTypeEnvironment& env)
    : Env_(&env)
{
}

void TStructTypeBuilder::Reserve(ui32 size) {
    Members_.reserve(size);
}

TStructTypeBuilder& TStructTypeBuilder::Add(const TStringBuf& name, TType* type, ui32* index) {
    Members_.push_back(TStructMember(Env_->InternName(name).Str(), type, index));
    return *this;
}

TStructType* TStructTypeBuilder::Build() {
    if (Members_.empty()) {
        return Env_->GetEmptyStructLazy()->GetType();
    }

    Sort(Members_.begin(), Members_.end());
    return TStructType::Create(Members_.size(), Members_.data(), *Env_);
}

void TStructTypeBuilder::FillIndexes() {
    ui32 index = 0;
    for (const TStructMember& member : Members_) {
        if (member.Index) {
            *(member.Index) = index++;
        }
    }
}

void TStructTypeBuilder::Clear() {
    Members_.clear();
}

TStructLiteralBuilder::TStructLiteralBuilder(const TTypeEnvironment& env)
    : Env_(&env)
{
}

void TStructLiteralBuilder::Reserve(ui32 size) {
    Members_.reserve(size);
    Values_.reserve(size);
}

TStructLiteralBuilder& TStructLiteralBuilder::Add(const TStringBuf& name, TRuntimeNode value) {
    TType* valueType = value.GetStaticType();
    Members_.push_back(TStructMember(Env_->InternName(name).Str(), valueType));
    Values_.push_back(value);
    return *this;
}

TStructLiteral* TStructLiteralBuilder::Build() {
    Y_DEBUG_ABORT_UNLESS(Members_.size() == Values_.size());
    if (Members_.empty()) {
        return Env_->GetEmptyStructLazy();
    }

    TVector<std::pair<TStringBuf, ui32>> sortedIndicies(Members_.size());
    for (ui32 i = 0, e = Members_.size(); i < e; ++i) {
        sortedIndicies[i] = std::make_pair(Members_[i].Name, i);
    }

    Sort(sortedIndicies.begin(), sortedIndicies.end(),
         [](const std::pair<TStringBuf, ui32>& x, const std::pair<TStringBuf, ui32>& y) {
             return x.first < y.first;
         });

    TVector<TStructMember> sortedMembers(Members_.size());
    TVector<TRuntimeNode> sortedValues(Members_.size());

    for (ui32 i = 0, e = Members_.size(); i < e; ++i) {
        sortedMembers[i] = Members_[sortedIndicies[i].second];
        sortedValues[i] = Values_[sortedIndicies[i].second];
    }

    auto type = TStructType::Create(sortedMembers.size(), sortedMembers.data(), *Env_);
    return TStructLiteral::Create(sortedValues.size(), sortedValues.data(), type, *Env_);
}

void TStructLiteralBuilder::Clear() {
    Members_.clear();
}

TListLiteralBuilder::TListLiteralBuilder(const TTypeEnvironment& env, TType* type)
    : Env_(&env)
    , Type_(type)
{
}

TListLiteralBuilder& TListLiteralBuilder::Add(TRuntimeNode item) {
    Items_.push_back(item);
    return *this;
}

TListLiteral* TListLiteralBuilder::Build() {
    auto type = TListType::Create(Type_, *Env_);
    return TListLiteral::Create(Items_.data(), Items_.size(), type, *Env_);
}

void TListLiteralBuilder::Clear() {
    Items_.clear();
}

TDictLiteralBuilder::TDictLiteralBuilder(const TTypeEnvironment& env, TType* keyType, TType* payloadType)
    : Env_(&env)
    , KeyType_(keyType)
    , PayloadType_(payloadType)
{
}

void TDictLiteralBuilder::Reserve(ui32 size) {
    Items_.reserve(size);
}

TDictLiteralBuilder& TDictLiteralBuilder::Add(TRuntimeNode key, TRuntimeNode payload) {
    Items_.push_back(std::make_pair(key, payload));
    return *this;
}

TDictLiteral* TDictLiteralBuilder::Build() {
    auto type = TDictType::Create(KeyType_, PayloadType_, *Env_);
    return TDictLiteral::Create(Items_.size(), Items_.data(), type, *Env_);
}

void TDictLiteralBuilder::Clear() {
    Items_.clear();
}

TCallableTypeBuilder::TCallableTypeBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType)
    : Env_(&env)
    , Name_(Env_->InternName(name))
    , ReturnType_(returnType)
    , OptionalArgsCount_(0)
    , HasPayload_(false)
{
}

void TCallableTypeBuilder::Reserve(ui32 size) {
    Arguments_.reserve(size);
    ArgNames_.reserve(size);
    ArgFlags_.reserve(size);
}

TCallableTypeBuilder& TCallableTypeBuilder::Add(TType* type) {
    Arguments_.push_back(type);
    ArgNames_.emplace_back();
    ArgFlags_.emplace_back();
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetArgumentName(const TStringBuf& name) {
    MKQL_ENSURE(!ArgNames_.empty(), "No arguments");
    HasPayload_ = true;
    ArgNames_.back() = name;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetArgumentFlags(ui64 flags) {
    MKQL_ENSURE(!ArgFlags_.empty(), "No arguments");
    HasPayload_ = true;
    ArgFlags_.back() = flags;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetOptionalArgs(ui32 count) {
    OptionalArgsCount_ = count;
    return *this;
}

TCallableTypeBuilder& TCallableTypeBuilder::SetPayload(const TStringBuf& data) {
    HasPayload_ = true;
    FuncPayload_ = data;
    return *this;
}

TCallableType* TCallableTypeBuilder::Build() {
    TNode* payload = nullptr;
    if (HasPayload_) {
        payload = BuildCallableTypePayload(ArgNames_, ArgFlags_, FuncPayload_, *Env_);
    }

    auto ret = TCallableType::Create(ReturnType_, Name_.Str(), Arguments_.size(), Arguments_.data(), payload, *Env_);
    ret->SetOptionalArgumentsCount(OptionalArgsCount_);
    return ret;
}

void TCallableTypeBuilder::Clear() {
    Arguments_.clear();
    ArgNames_.clear();
    ArgFlags_.clear();
    OptionalArgsCount_ = 0;
    HasPayload_ = false;
    FuncPayload_ = TStringBuf();
}

TCallableBuilder::TCallableBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType, bool disableMerge)
    : Env_(&env)
    , Name_(Env_->InternName(name))
    , ReturnType_(returnType)
    , DisableMerge_(disableMerge)
    , OptionalArgsCount_(0)
    , HasPayload_(false)
{
}

void TCallableBuilder::Reserve(ui32 size) {
    Arguments_.reserve(size);
    Inputs_.reserve(size);
    ArgNames_.reserve(size);
    ArgFlags_.reserve(size);
}

TCallableBuilder& TCallableBuilder::Add(TRuntimeNode input) {
    TType* inputType = input.GetStaticType();
    Arguments_.push_back(inputType);
    Inputs_.push_back(input);
    ArgNames_.emplace_back();
    ArgFlags_.emplace_back();
    return *this;
}

TCallableBuilder& TCallableBuilder::SetOptionalArgs(ui32 count) {
    OptionalArgsCount_ = count;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetTypePayload(const TStringBuf& data) {
    HasPayload_ = true;
    FuncPayload_ = data;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetArgumentName(const TStringBuf& name) {
    MKQL_ENSURE(!ArgNames_.empty(), "No arguments");
    HasPayload_ = true;
    ArgNames_.back() = name;
    return *this;
}

TCallableBuilder& TCallableBuilder::SetArgumentFlags(ui64 flags) {
    MKQL_ENSURE(!ArgFlags_.empty(), "No arguments");
    HasPayload_ = true;
    ArgFlags_.back() = flags;
    return *this;
}

TCallable* TCallableBuilder::Build() {
    TNode* payload = nullptr;
    if (HasPayload_) {
        payload = BuildCallableTypePayload(ArgNames_, ArgFlags_, FuncPayload_, *Env_);
    }

    auto type = TCallableType::Create(ReturnType_, Name_.Str(), Arguments_.size(), Arguments_.data(), payload, *Env_);
    type->SetOptionalArgumentsCount(OptionalArgsCount_);
    if (DisableMerge_) {
        type->DisableMerge();
    }
    return TCallable::Create(Inputs_.size(), Inputs_.data(), type, *Env_);
}

void TCallableBuilder::Clear() {
    Arguments_.clear();
    Inputs_.clear();
    ArgNames_.clear();
    ArgFlags_.clear();
    OptionalArgsCount_ = 0;
    FuncPayload_ = TStringBuf();
}

} // namespace NKikimr::NMiniKQL
