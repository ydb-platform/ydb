#include "mkql_node.h"
#include "mkql_node_builder.h"
#include "mkql_node_cast.h"
#include "mkql_node_visitor.h"
#include "mkql_node_printer.h"
#include "mkql_runtime_version.h"
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/stream/str.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NMiniKQL {

#define MKQL_SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val: return TStringBuf(#name);

using namespace NDetail;

TTypeEnvironment::TTypeEnvironment(TScopedAlloc& alloc)
    : Alloc(alloc)
    , Arena(&Alloc.Ref())
    , EmptyStruct(nullptr)
    , EmptyTuple(nullptr)
{
}

TTypeEnvironment::~TTypeEnvironment() {
}

void TTypeEnvironment::ClearCookies() const {
    if (TypeOfType) {
        TypeOfType->SetCookie(0);
    }
    if (TypeOfVoid) {
        TypeOfVoid->SetCookie(0);
    }
    if (Void) {
        Void->SetCookie(0);
    }
    if (TypeOfNull) {
        TypeOfNull->SetCookie(0);
    }
    if (Null) {
        Null->SetCookie(0);
    }
    if (TypeOfEmptyList) {
        TypeOfEmptyList->SetCookie(0);
    }
    if (EmptyList) {
        EmptyList->SetCookie(0);
    }
    if (TypeOfEmptyDict) {
        TypeOfEmptyDict->SetCookie(0);
    }
    if (EmptyDict) {
        EmptyDict->SetCookie(0);
    }
    if (EmptyStruct) {
        EmptyStruct->SetCookie(0);
    }
    if (ListOfVoid) {
        ListOfVoid->SetCookie(0);
    }
    if (AnyType) {
        AnyType->SetCookie(0);
    }
    if (EmptyTuple) {
        EmptyTuple->SetCookie(0);
    }
}

TTypeType* TTypeEnvironment::GetTypeOfTypeLazy() const {
    if (!TypeOfType) {
        TypeOfType = TTypeType::Create(*this);
        TypeOfType->Type = TypeOfType;
    }
    return TypeOfType;
}

TVoidType* TTypeEnvironment::GetTypeOfVoidLazy() const {
    if (!TypeOfVoid) {
        TypeOfVoid = TVoidType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfVoid;
}

TVoid* TTypeEnvironment::GetVoidLazy() const {
    if (!Void) {
        Void = TVoid::Create(*this);
    }
    return Void;
}

TNullType* TTypeEnvironment::GetTypeOfNullLazy() const {
    if (!TypeOfNull) {
        TypeOfNull = TNullType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfNull;
}

TNull* TTypeEnvironment::GetNullLazy() const {
    if (!Null) {
        Null = TNull::Create(*this);
    }
    return Null;
}

TEmptyListType* TTypeEnvironment::GetTypeOfEmptyListLazy() const {
    if (!TypeOfEmptyList) {
        TypeOfEmptyList = TEmptyListType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfEmptyList;
}

TEmptyList* TTypeEnvironment::GetEmptyListLazy() const {
    if (!EmptyList) {
        EmptyList = TEmptyList::Create(*this);
    }
    return EmptyList;
}

TEmptyDictType* TTypeEnvironment::GetTypeOfEmptyDictLazy() const {
    if (!TypeOfEmptyDict) {
        TypeOfEmptyDict = TEmptyDictType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfEmptyDict;
}

TEmptyDict* TTypeEnvironment::GetEmptyDictLazy() const {
    if (!EmptyDict) {
        EmptyDict = TEmptyDict::Create(*this);
    }
    return EmptyDict;
}

TStructLiteral* TTypeEnvironment::GetEmptyStructLazy() const {
    if (!EmptyStruct) {
        EmptyStruct = TStructLiteral::Create(
            0,
            nullptr,
            TStructType::Create(0, nullptr, *this), *this, false);
    }
    return EmptyStruct;
}

TListLiteral* TTypeEnvironment::GetListOfVoidLazy() const {
    if (!ListOfVoid) {
        ListOfVoid = TListLiteral::Create(nullptr, 0, TListType::Create(GetVoidLazy()->GetGenericType(), *this), *this);
    }
    return ListOfVoid;
}

TAnyType* TTypeEnvironment::GetAnyTypeLazy() const {
    if (!AnyType) {
        AnyType = TAnyType::Create(GetTypeOfTypeLazy(), *this);
    }
    return AnyType;
}

TTupleLiteral* TTypeEnvironment::GetEmptyTupleLazy() const {
    if (!EmptyTuple) {
        EmptyTuple = TTupleLiteral::Create(0, nullptr, TTupleType::Create(0, nullptr, *this), *this, false);
    }
    return EmptyTuple;
}

TDataType* TTypeEnvironment::GetUi32Lazy() const {
    if (!Ui32) {
        Ui32 = TDataType::Create(NUdf::TDataType<ui32>::Id, *this);
    }
    return Ui32;
}

TDataType* TTypeEnvironment::GetUi64Lazy() const {
    if (!Ui64) {
        Ui64 = TDataType::Create(NUdf::TDataType<ui64>::Id, *this);
    }
    return Ui64;
}

std::vector<TNode*>& TTypeEnvironment::GetNodeStack() const {
    return Stack;
}

TInternName TTypeEnvironment::InternName(const TStringBuf& name) const {
    if (NamesPool.empty()) {
        NamesPool.reserve(64);
    }

    auto it = NamesPool.find(name);
    if (it != NamesPool.end()) {
        return TInternName(*it);
    }

    // Copy to arena and null-terminate
    char* data = (char*)AllocateBuffer(name.size()+1);
    memcpy(data, name.data(), name.size());
    data[name.size()] = 0;

    return TInternName(*NamesPool.insert(TStringBuf(data, name.size())).first);
}

#define LITERALS_LIST(xx) \
    xx(Void, TVoid) \
    xx(Null, TNull) \
    xx(EmptyList, TEmptyList) \
    xx(EmptyDict, TEmptyDict) \
    xx(Data, TDataLiteral) \
    xx(Struct, TStructLiteral) \
    xx(List, TListLiteral) \
    xx(Optional, TOptionalLiteral) \
    xx(Dict, TDictLiteral) \
    xx(Callable, TCallable) \
    xx(Any, TAny) \
    xx(Tuple, TTupleLiteral) \
    xx(Variant, TVariantLiteral)

void TNode::Accept(INodeVisitor& visitor) {
    const auto kind = Type->GetKind();
    switch (kind) {
    case TType::EKind::Type:
        return static_cast<TType&>(*this).Accept(visitor);

#define APPLY(kind, type) \
    case TType::EKind::kind: \
        return visitor.Visit(static_cast<type&>(*this));

    LITERALS_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

bool TNode::Equals(const TNode& nodeToCompare) const {
    if (this == &nodeToCompare)
        return true;

    if (!Type->IsSameType(*nodeToCompare.Type))
        return false;

    const auto kind = Type->GetKind();
    switch (kind) {
    case TType::EKind::Type:
        return static_cast<const TType&>(*this).IsSameType(static_cast<const TType&>(nodeToCompare));

#define APPLY(kind, type) \
    case TType::EKind::kind: \
        return static_cast<const type&>(*this).Equals(static_cast<const type&>(nodeToCompare));

    LITERALS_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

void TNode::UpdateLinks(const THashMap<TNode*, TNode*>& links) {
    const auto kind = Type->GetKind();
    switch (kind) {
    case TType::EKind::Type:
        return static_cast<TType&>(*this).UpdateLinks(links);

#define APPLY(kind, type) \
    case TType::EKind::kind: \
        return static_cast<type&>(*this).DoUpdateLinks(links);

        LITERALS_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

TNode* TNode::CloneOnCallableWrite(const TTypeEnvironment& env) const {
    const auto kind = Type->GetKind();
    switch (kind) {
    case TType::EKind::Type:
        return static_cast<const TType&>(*this).CloneOnCallableWrite(env);

#define APPLY(kind, type) \
    case TType::EKind::kind: \
        return static_cast<const type&>(*this).DoCloneOnCallableWrite(env);

        LITERALS_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

void TNode::Freeze(const TTypeEnvironment& env) {
    const auto kind = Type->GetKind();
    switch (kind) {
    case TType::EKind::Type:
        return static_cast<TType&>(*this).Freeze(env);

#define APPLY(kind, type) \
    case TType::EKind::kind: \
        return static_cast<type&>(*this).DoFreeze(env);

        LITERALS_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

bool TNode::IsMergeable() const {
    if (!Type->IsCallable()) {
        return true;
    }

    return !static_cast<const TCallable*>(this)->GetType()->IsMergeDisabled();
}

TStringBuf TType::KindAsStr(EKind kind) {
    switch (static_cast<int>(kind)) {
        MKQL_TYPE_KINDS(MKQL_SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

TStringBuf TType::GetKindAsStr() const {
    return KindAsStr(Kind);
}

#define TYPES_LIST(xx) \
    xx(Type, TTypeType) \
    xx(Void, TVoidType) \
    xx(Data, TDataType) \
    xx(Struct, TStructType) \
    xx(List, TListType) \
    xx(Stream, TStreamType) \
    xx(Optional, TOptionalType) \
    xx(Dict, TDictType) \
    xx(Callable, TCallableType) \
    xx(Any, TAnyType) \
    xx(Tuple, TTupleType) \
    xx(Resource, TResourceType) \
    xx(Variant, TVariantType) \
    xx(Flow, TFlowType) \
    xx(Null, TNullType) \
    xx(EmptyList, TEmptyListType) \
    xx(EmptyDict, TEmptyDictType) \
    xx(Tagged, TTaggedType) \
    xx(Block, TBlockType) \
    xx(Pg, TPgType) \
    xx(Multi, TMultiType) \

void TType::Accept(INodeVisitor& visitor) {
    switch (Kind) {

#define APPLY(kind, type) \
    case EKind::kind: \
        return visitor.Visit(static_cast<type&>(*this));

        TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

void TType::UpdateLinks(const THashMap<TNode*, TNode*>& links) {
    switch (Kind) {

#define APPLY(kind, type) \
    case EKind::kind: \
        return static_cast<type&>(*this).DoUpdateLinks(links);

        TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

TNode* TType::CloneOnCallableWrite(const TTypeEnvironment& env) const {
    switch (Kind) {

#define APPLY(kind, type) \
    case EKind::kind: \
        return static_cast<const type&>(*this).DoCloneOnCallableWrite(env);

        TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

void TType::Freeze(const TTypeEnvironment& env) {
    switch (Kind) {

#define APPLY(kind, type) \
    case EKind::kind: \
        return static_cast<type&>(*this).DoFreeze(env);

        TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

bool TTypeBase::IsSameType(const TTypeBase& typeToCompare) const {
    if (Kind != typeToCompare.Kind) {
        return false;
    }

    switch (Kind) {
#define APPLY(kind, type) \
    case EKind::kind: \
        return static_cast<const type&>(*this).IsSameType(static_cast<const type&>(typeToCompare));

    TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

size_t TTypeBase::CalcHash() const {
    switch (Kind) {
#define APPLY(kind, type) \
    case EKind::kind: \
        /* combine hashes to aviod collision, for example, between Kind and SchemeType */ \
        return CombineHashes(IntHash((size_t)Kind), static_cast<const type&>(*this).CalcHash());

    TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

bool TType::IsConvertableTo(const TType& typeToCompare, bool ignoreTagged) const {
    const TType* self = this;
    const TType* other = &typeToCompare;
    if (ignoreTagged) {
        while (self->Kind == EKind::Tagged) {
            self = static_cast<const TTaggedType*>(self)->GetBaseType();
        }

        while (other->Kind == EKind::Tagged) {
            other = static_cast<const TTaggedType*>(other)->GetBaseType();
        }
    }

    if (self->Kind != other->Kind) {
        return false;
    }

    switch (self->Kind) {
#define APPLY(kind, type) \
    case EKind::kind: \
        return static_cast<const type&>(*self).IsConvertableTo(static_cast<const type&>(*other), ignoreTagged);

    TYPES_LIST(APPLY)

#undef APPLY
    default:
        Y_ABORT();
    }
}

TTypeType* TTypeType::Create(const TTypeEnvironment& env) {
    return ::new(env.Allocate<TTypeType>()) TTypeType();
}

bool TTypeType::IsSameType(const TTypeType& typeToCompare) const {
    Y_UNUSED(typeToCompare);
    return true;
}

size_t TTypeType::CalcHash() const {
    return 0;
}

bool TTypeType::IsConvertableTo(const TTypeType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

void TTypeType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    Y_UNUSED(links);
}

TNode* TTypeType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TTypeType*>(this);
}

void TTypeType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}


TDataType::TDataType(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env)
    : TType(EKind::Data, env.GetTypeOfTypeLazy(), true)
    , SchemeType(schemeType)
    , DataSlot(NUdf::FindDataSlot(schemeType))
{
}

TDataType* TDataType::Create(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env) {
    MKQL_ENSURE(schemeType, "Null type isn't allowed.");
    MKQL_ENSURE(schemeType != NUdf::TDataType<NUdf::TDecimal>::Id, "Can't' create Decimal.");
    MKQL_ENSURE(schemeType != NKikimr::NScheme::NTypeIds::Pg, "Can't create Pg.");
    MKQL_ENSURE(schemeType != 0, "0 type");
    return ::new(env.Allocate<TDataType>()) TDataType(schemeType, env);
}

bool TDataType::IsSameType(const TDataType& typeToCompare) const {
    if (SchemeType != typeToCompare.SchemeType)
        return false;
    if (SchemeType != NUdf::TDataType<NUdf::TDecimal>::Id)
        return true;
    return static_cast<const TDataDecimalType&>(*this).IsSameType(static_cast<const TDataDecimalType&>(typeToCompare));
}

size_t TDataType::CalcHash() const {
    size_t hash = IntHash((size_t)GetSchemeType());
    if (SchemeType == NUdf::TDataType<NUdf::TDecimal>::Id) {
        hash = CombineHashes(hash, static_cast<const TDataDecimalType&>(*this).CalcHash());
    }
    return hash;
}

bool TDataType::IsConvertableTo(const TDataType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

void TDataType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    Y_UNUSED(links);
}

TNode* TDataType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TDataType*>(this);
}

void TDataType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TDataDecimalType::TDataDecimalType(ui8 precision, ui8 scale, const TTypeEnvironment& env)
    : TDataType(NUdf::TDataType<NUdf::TDecimal>::Id, env), Precision(precision), Scale(scale)
{
    MKQL_ENSURE(Precision > 0, "Precision must be positive.");
    MKQL_ENSURE(Scale <= Precision, "Scale too large.");
}

TDataDecimalType* TDataDecimalType::Create(ui8 precision, ui8 scale, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDataDecimalType>()) TDataDecimalType(precision, scale, env);
}

bool TDataDecimalType::IsSameType(const TDataDecimalType& typeToCompare) const {
    return Precision == typeToCompare.Precision && Scale == typeToCompare.Scale;
}

size_t TDataDecimalType::CalcHash() const {
    return CombineHashes(IntHash((size_t)Precision), IntHash((size_t)Scale));
}

bool TDataDecimalType::IsConvertableTo(const TDataDecimalType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return Precision == typeToCompare.Precision && Scale == typeToCompare.Scale;
}

std::pair<ui8, ui8> TDataDecimalType::GetParams() const {
    return std::make_pair(Precision, Scale);
}

TDataLiteral::TDataLiteral(const TUnboxedValuePod& value, TDataType* type)
    : TNode(type), TUnboxedValuePod(value)
{}

TDataLiteral* TDataLiteral::Create(const NUdf::TUnboxedValuePod& value, TDataType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDataLiteral>()) TDataLiteral(value, type);
}

void TDataLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }
}

TNode* TDataLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TDataLiteral*>(this);
}

void TDataLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

bool TDataLiteral::Equals(const TDataLiteral& nodeToCompare) const {
    const auto& self = AsValue();
    const auto& that = nodeToCompare.AsValue();
    switch (GetType()->GetSchemeType()) {
        case NUdf::TDataType<bool>::Id:   return self.Get<bool>() == that.Get<bool>();
        case NUdf::TDataType<ui8>::Id:    return self.Get<ui8>() == that.Get<ui8>();
        case NUdf::TDataType<i8>::Id:     return self.Get<i8>() == that.Get<i8>();
        case NUdf::TDataType<NUdf::TDate>::Id:
        case NUdf::TDataType<ui16>::Id:   return self.Get<ui16>() == that.Get<ui16>();
        case NUdf::TDataType<i16>::Id:    return self.Get<i16>() == that.Get<i16>();
        case NUdf::TDataType<NUdf::TDate32>::Id:
        case NUdf::TDataType<i32>::Id:    return self.Get<i32>() == that.Get<i32>();
        case NUdf::TDataType<NUdf::TDatetime>::Id:
        case NUdf::TDataType<ui32>::Id:   return self.Get<ui32>() == that.Get<ui32>();
        case NUdf::TDataType<NUdf::TInterval>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<i64>::Id:    return self.Get<i64>() == that.Get<i64>();
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
        case NUdf::TDataType<ui64>::Id:   return self.Get<ui64>() == that.Get<ui64>();
        case NUdf::TDataType<float>::Id:  return self.Get<float>() == that.Get<float>();
        case NUdf::TDataType<double>::Id: return self.Get<double>() == that.Get<double>();
        case NUdf::TDataType<NUdf::TTzDate>::Id: return self.Get<ui16>() == that.Get<ui16>() && self.GetTimezoneId() == that.GetTimezoneId();
        case NUdf::TDataType<NUdf::TTzDatetime>::Id: return self.Get<ui32>() == that.Get<ui32>() && self.GetTimezoneId() == that.GetTimezoneId();
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: return self.Get<ui64>() == that.Get<ui64>() && self.GetTimezoneId() == that.GetTimezoneId();
        case NUdf::TDataType<NUdf::TDecimal>::Id: return self.GetInt128() == that.GetInt128();
        default: return self.AsStringRef() == that.AsStringRef();
    }
}
static const THashSet<TStringBuf> PG_SUPPORTED_PRESORT = {
    "bool",
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "bytea",
    "varchar",
    "text",
    "cstring"
};

TPgType::TPgType(ui32 typeId, const TTypeEnvironment& env)
    : TType(EKind::Pg, env.GetTypeOfTypeLazy(), PG_SUPPORTED_PRESORT.contains(NYql::NPg::LookupType(typeId).Name))
    , TypeId(typeId)
{
}

TPgType* TPgType::Create(ui32 typeId, const TTypeEnvironment& env) {
    MKQL_ENSURE(typeId != 0, "0 type");
    return ::new(env.Allocate<TPgType>()) TPgType(typeId, env);
}

bool TPgType::IsSameType(const TPgType& typeToCompare) const {
    return TypeId == typeToCompare.TypeId;
}

size_t TPgType::CalcHash() const {
    return IntHash((size_t)TypeId);
}

bool TPgType::IsConvertableTo(const TPgType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

void TPgType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    Y_UNUSED(links);
}

TNode* TPgType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TPgType*>(this);
}

void TPgType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

const TString& TPgType::GetName() const {
    return NYql::NPg::LookupType(TypeId).Name;
}

TStructType::TStructType(ui32 membersCount, std::pair<TInternName, TType*>* members, const TTypeEnvironment& env,
    bool validate)
    : TType(EKind::Struct, env.GetTypeOfTypeLazy(), CalculatePresortSupport(membersCount, members))
    , MembersCount(membersCount)
    , Members(members)
{
    if (!validate)
        return;

    TInternName lastMemberName;
    for (size_t index = 0; index < membersCount; ++index) {
        const auto& name = Members[index].first;
        MKQL_ENSURE(!name.Str().empty(), "Empty member name is not allowed");

        MKQL_ENSURE(name.Str() > lastMemberName.Str(), "Member names are not sorted: "
                    << name.Str() << " <= " << lastMemberName.Str());

        lastMemberName = name;
    }
}

TStructType* TStructType::Create(const std::pair<TString, TType*>* members, ui32 membersCount, const TTypeEnvironment& env) {
    std::pair<TInternName, TType*>* allocatedMembers = nullptr;
    if (membersCount) {
        allocatedMembers = static_cast<std::pair<TInternName, TType*>*>(env.AllocateBuffer(membersCount * sizeof(*allocatedMembers)));
        for (ui32 i = 0; i < membersCount; ++i) {
            allocatedMembers[i] = std::make_pair(env.InternName(members[i].first), members[i].second);
        }
    }

    return ::new(env.Allocate<TStructType>()) TStructType(membersCount, allocatedMembers, env);
}

TStructType* TStructType::Create(ui32 membersCount, const TStructMember* members, const TTypeEnvironment& env) {
    std::pair<TInternName, TType*>* allocatedMembers = nullptr;
    if (membersCount) {
        allocatedMembers = static_cast<std::pair<TInternName, TType*>*>(env.AllocateBuffer(membersCount * sizeof(*allocatedMembers)));
        for (ui32 i = 0; i < membersCount; ++i) {
            allocatedMembers[i] = std::make_pair(env.InternName(members[i].Name), members[i].Type);
        }
    }

    return ::new(env.Allocate<TStructType>()) TStructType(membersCount, allocatedMembers, env);
}

bool TStructType::IsSameType(const TStructType& typeToCompare) const {
    if (this == &typeToCompare)
        return true;

    if (MembersCount != typeToCompare.MembersCount)
        return false;

    for (size_t index = 0; index < MembersCount; ++index) {
        if (Members[index].first != typeToCompare.Members[index].first)
            return false;
        if (!Members[index].second->IsSameType(*typeToCompare.Members[index].second))
            return false;
    }

    return true;
}

size_t TStructType::CalcHash() const {
    size_t hash = 0;
    for (size_t i = 0; i < MembersCount; ++i) {
        hash = CombineHashes(hash, Members[i].first.Hash());
        hash = CombineHashes(hash, Members[i].second->CalcHash());
    }
    return hash;
}

bool TStructType::IsConvertableTo(const TStructType& typeToCompare, bool ignoreTagged) const {
    if (this == &typeToCompare)
        return true;

    if (MembersCount != typeToCompare.MembersCount)
        return false;

    for (size_t index = 0; index < MembersCount; ++index) {
        if (Members[index].first != typeToCompare.Members[index].first)
            return false;
        if (!Members[index].second->IsConvertableTo(*typeToCompare.Members[index].second, ignoreTagged))
            return false;
    }

    return true;
}

void TStructType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    for (ui32 i = 0; i < MembersCount; ++i) {
        auto& member = Members[i];
        auto memberIt = links.find(member.second);
        if (memberIt != links.end()) {
            TNode* newNode = memberIt->second;
            Y_DEBUG_ABORT_UNLESS(member.second->Equals(*newNode));
            member.second = static_cast<TType*>(newNode);
        }
    }
}

TNode* TStructType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    bool needClone = false;
    for (ui32 i = 0; i < MembersCount; ++i) {
        if (Members[i].second->GetCookie()) {
            needClone = true;
            break;
        }
    }

    if (!needClone)
        return const_cast<TStructType*>(this);

    std::pair<TInternName, TType*>* allocatedMembers = nullptr;
    if (MembersCount) {
        allocatedMembers = static_cast<std::pair<TInternName, TType*>*>(env.AllocateBuffer(MembersCount * sizeof(*allocatedMembers)));
        for (ui32 i = 0; i < MembersCount; ++i) {
            allocatedMembers[i].first = Members[i].first;
            auto newNode = (TNode*)Members[i].second->GetCookie();
            if (newNode) {
                allocatedMembers[i].second = static_cast<TType*>(newNode);
            } else {
                allocatedMembers[i].second = Members[i].second;
            }
        }
    }

    return ::new(env.Allocate<TStructType>()) TStructType(MembersCount, allocatedMembers, env, false);
}

void TStructType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

bool TStructType::CalculatePresortSupport(ui32 membersCount, std::pair<TInternName, TType*>* members) {
    for (ui32 i = 0; i < membersCount; ++i) {
        if (!members[i].second->IsPresortSupported()) {
            return false;
        }
    }

    return true;
}

ui32 TStructType::GetMemberIndex(const TStringBuf& name) const {
    auto index = FindMemberIndex(name);
    if (index) {
        return *index;
    }

    TStringStream ss;
    for (ui32 i = 0; i < MembersCount; ++i) {
        ss << " " << Members[i].first.Str();
    }
    THROW yexception() << "Member with name '" << name << "' not found; "
            << " known members: " << ss.Str() << ".";
}

TMaybe<ui32> TStructType::FindMemberIndex(const TStringBuf& name) const {
    for (ui32 i = 0; i < MembersCount; ++i) {
        if (Members[i].first == name)
            return i;
    }

    return {};
}

TStructLiteral::TStructLiteral(TRuntimeNode* values, TStructType* type, bool validate)
    : TNode(type)
    , Values(values)
{
    if (!validate) {
        for (size_t index = 0; index < GetValuesCount(); ++index) {
            auto& value = Values[index];
            value.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < GetValuesCount(); ++index) {
        MKQL_ENSURE(!type->GetMemberName(index).empty(), "Empty struct member name is not allowed");

        auto& value = Values[index];
        MKQL_ENSURE(value.GetStaticType()->IsSameType(*type->GetMemberType(index)), "Wrong type of member");

        value.Freeze();
    }
}

TStructLiteral* TStructLiteral::Create(ui32 valuesCount, const TRuntimeNode* values, TStructType* type, const TTypeEnvironment& env, bool useCachedEmptyStruct) {
    MKQL_ENSURE(valuesCount == type->GetMembersCount(), "Wrong count of members");
    TRuntimeNode* allocatedValues = nullptr;
    if (valuesCount) {
        allocatedValues = static_cast<TRuntimeNode*>(env.AllocateBuffer(valuesCount * sizeof(*allocatedValues)));
        for (ui32 i = 0; i < valuesCount; ++i) {
            allocatedValues[i] = values[i];
        }
    } else if (useCachedEmptyStruct) {
        return env.GetEmptyStructLazy();
    }

    return ::new(env.Allocate<TStructLiteral>()) TStructLiteral(allocatedValues, type);
}

void TStructLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        auto& value = Values[i];
        auto valueIt = links.find(value.GetNode());
        if (valueIt != links.end()) {
            TNode* newNode = valueIt->second;
            Y_DEBUG_ABORT_UNLESS(value.GetNode()->Equals(*newNode));
            value = TRuntimeNode(newNode, value.IsImmediate());
        }
    }
}

TNode* TStructLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto typeNewNode = (TNode*)Type->GetCookie();
    bool needClone = false;
    if (typeNewNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            if (Values[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TStructLiteral*>(this);

    TRuntimeNode* allocatedValues = nullptr;
    if (GetValuesCount()) {
        allocatedValues = static_cast<TRuntimeNode*>(env.AllocateBuffer(GetValuesCount() * sizeof(*allocatedValues)));
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            allocatedValues[i] = Values[i];
            auto newNode = (TNode*)Values[i].GetNode()->GetCookie();
            if (newNode) {
                allocatedValues[i] = TRuntimeNode(newNode, Values[i].IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TStructLiteral>()) TStructLiteral(allocatedValues,
        typeNewNode ? static_cast<TStructType*>(typeNewNode) : GetType(), false);
}

void TStructLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        Values[i].Freeze();
    }
}

bool TStructLiteral::Equals(const TStructLiteral& nodeToCompare) const {
    if (GetValuesCount() != nodeToCompare.GetValuesCount())
        return false;

    for (size_t i = 0; i < GetValuesCount(); ++i) {
        if (Values[i] != nodeToCompare.Values[i])
            return false;
    }

    return true;
}

TListType::TListType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::List, env.GetTypeOfTypeLazy(), itemType->IsPresortSupported())
    , Data(itemType)
    , IndexDictKey(env.GetUi64Lazy())
{
    Y_UNUSED(validate);
}

TListType* TListType::Create(TType* itemType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TListType>()) TListType(itemType, env);
}

bool TListType::IsSameType(const TListType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType());
}

size_t TListType::CalcHash() const {
    return CombineHashes(IndexDictKey->CalcHash(), Data->CalcHash());
}

bool TListType::IsConvertableTo(const TListType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TListType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data = static_cast<TType*>(newNode);
    }
}

TNode* TListType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetItemType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TListType*>(this);

    return ::new(env.Allocate<TListType>()) TListType(static_cast<TType*>(newTypeNode), env, false);
}

void TListType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TListLiteral::TListLiteral(TRuntimeNode* items, ui32 count, TListType* type, const TTypeEnvironment& env, bool validate)
    : TNode(type)
    , Items(items)
    , Count(count)
{
    for (ui32 i = 0; i < count; ++i) {
        Y_DEBUG_ABORT_UNLESS(items[i].GetNode());
    }

    if (!validate) {
        TListLiteral::DoFreeze(env);
        return;
    }

    for (ui32 i = 0; i < Count; ++i) {
        auto& item = Items[i];
        MKQL_ENSURE(item.GetStaticType()->IsSameType(*type->GetItemType()), "Wrong type of item");
    }

    TListLiteral::DoFreeze(env);
}

TListLiteral* TListLiteral::Create(TRuntimeNode* items, ui32 count, TListType* type, const TTypeEnvironment& env) {
    TRuntimeNode* allocatedItems = nullptr;
    if (count) {
        allocatedItems = static_cast<TRuntimeNode*>(env.AllocateBuffer(count * sizeof(*allocatedItems)));
        for (ui32 i = 0; i < count; ++i) {
            allocatedItems[i] = items[i];
        }
    }

    return ::new(env.Allocate<TListLiteral>()) TListLiteral(allocatedItems, count, type, env);
}

void TListLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < Count; ++i) {
        auto& item = Items[i];
        auto itemIt = links.find(item.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(item.GetNode()->Equals(*newNode));
            item = TRuntimeNode(newNode, item.IsImmediate());
        }
    }
}

TNode* TListLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < Count; ++i) {
            if (Items[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TListLiteral*>(this);

    TVector<TRuntimeNode> newList;
    newList.reserve(Count);
    for (ui32 i = 0; i < Count; ++i) {
        auto newNode = (TNode*)Items[i].GetNode()->GetCookie();
        if (newNode) {
            newList.push_back(TRuntimeNode(newNode, Items[i].IsImmediate()));
        } else {
            newList.push_back(Items[i]);
        }
    }

    TRuntimeNode* allocatedItems = nullptr;
    if (newList.size()) {
        allocatedItems = static_cast<TRuntimeNode*>(env.AllocateBuffer(newList.size() * sizeof(*allocatedItems)));
        for (ui32 i = 0; i < newList.size(); ++i) {
            allocatedItems[i] = newList[i];
        }
    }

    return ::new(env.Allocate<TListLiteral>()) TListLiteral(allocatedItems, newList.size(),
        newTypeNode ? static_cast<TListType*>(newTypeNode) : GetType(), env, false);
}

void TListLiteral::DoFreeze(const TTypeEnvironment&) {
    ui32 voidCount = 0;
    for (ui32 i = 0; i < Count; ++i) {
        auto& node = Items[i];
        node.Freeze();
        if (node.HasValue() && node.GetValue()->GetType()->IsVoid()) {
            ++voidCount;
        }
    }

    if (!voidCount)
        return;

    TRuntimeNode* newItems = Items;
    for (ui32 i = 0; i < Count; ++i) {
        auto node = Items[i];
        if (node.HasValue() && node.GetValue()->GetType()->IsVoid()) {
            continue;
        }

        *newItems++ = node;
    }

    Count = newItems - Items;
}

bool TListLiteral::Equals(const TListLiteral& nodeToCompare) const {
    if (Count != nodeToCompare.Count)
        return false;

    for (ui32 i = 0; i < Count; ++i) {
        if (Items[i] != nodeToCompare.Items[i])
            return false;
    }

    return true;
}

TStreamType::TStreamType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Stream, env.GetTypeOfTypeLazy(), false)
    , Data(itemType)
{
    Y_UNUSED(validate);
}

TStreamType* TStreamType::Create(TType* itemType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TStreamType>()) TStreamType(itemType, env);
}

bool TStreamType::IsSameType(const TStreamType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType());
}

size_t TStreamType::CalcHash() const {
    return Data->CalcHash();
}

bool TStreamType::IsConvertableTo(const TStreamType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TStreamType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data = static_cast<TType*>(newNode);
    }
}

TNode* TStreamType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetItemType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TStreamType*>(this);

    return ::new(env.Allocate<TStreamType>()) TStreamType(static_cast<TType*>(newTypeNode), env, false);
}

void TStreamType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TFlowType::TFlowType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Flow, env.GetTypeOfTypeLazy(), false)
    , Data(itemType)
{
    Y_UNUSED(validate);
}

TFlowType* TFlowType::Create(TType* itemType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TFlowType>()) TFlowType(itemType, env);
}

bool TFlowType::IsSameType(const TFlowType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType());
}

size_t TFlowType::CalcHash() const {
    return Data->CalcHash();
}

bool TFlowType::IsConvertableTo(const TFlowType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TFlowType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data = static_cast<TType*>(newNode);
    }
}

TNode* TFlowType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetItemType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TFlowType*>(this);

    return ::new(env.Allocate<TFlowType>()) TFlowType(static_cast<TType*>(newTypeNode), env, false);
}

void TFlowType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TOptionalType::TOptionalType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Optional, env.GetTypeOfTypeLazy(), itemType->IsPresortSupported())
    , Data(itemType)
{
    Y_UNUSED(validate);
}

TOptionalType* TOptionalType::Create(TType* itemType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TOptionalType>()) TOptionalType(itemType, env);
}

bool TOptionalType::IsSameType(const TOptionalType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType());
}

size_t TOptionalType::CalcHash() const {
    return Data->CalcHash();
}

bool TOptionalType::IsConvertableTo(const TOptionalType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TOptionalType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data = static_cast<TType*>(newNode);
    }
}

TNode* TOptionalType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetItemType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TOptionalType*>(this);

    return ::new(env.Allocate<TOptionalType>()) TOptionalType(static_cast<TType*>(newTypeNode), env, false);
}

void TOptionalType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TTaggedType::TTaggedType(TType* baseType, TInternName tag, const TTypeEnvironment& env)
    : TType(EKind::Tagged, env.GetTypeOfTypeLazy(), baseType->IsPresortSupported())
    , BaseType(baseType)
    , Tag(tag)
{
}

TTaggedType* TTaggedType::Create(TType* baseType, const TStringBuf& tag, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TTaggedType>()) TTaggedType(baseType, env.InternName(tag), env);
}

bool TTaggedType::IsSameType(const TTaggedType& typeToCompare) const {
    return Tag == typeToCompare.Tag && GetBaseType()->IsSameType(*typeToCompare.GetBaseType());
}

size_t TTaggedType::CalcHash() const {
    return CombineHashes(BaseType->CalcHash(), Tag.Hash());
}

bool TTaggedType::IsConvertableTo(const TTaggedType& typeToCompare, bool ignoreTagged) const {
    return Tag == typeToCompare.Tag && GetBaseType()->IsConvertableTo(*typeToCompare.GetBaseType(), ignoreTagged);
}

void TTaggedType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetBaseType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetBaseType()->Equals(*newNode));
        BaseType = static_cast<TType*>(newNode);
    }
}

TNode* TTaggedType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetBaseType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TTaggedType*>(this);

    return ::new(env.Allocate<TTaggedType>()) TTaggedType(static_cast<TType*>(newTypeNode), Tag, env);
}

void TTaggedType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TOptionalLiteral::TOptionalLiteral(TOptionalType* type, bool validate)
    : TNode(type)
{
    Y_UNUSED(validate);
}

TOptionalLiteral::TOptionalLiteral(TRuntimeNode item, TOptionalType* type, bool validate)
    : TNode(type)
    , Item(item)
{
    if (!validate) {
        Item.Freeze();
        return;
    }

    Y_DEBUG_ABORT_UNLESS(Item.GetNode());

    MKQL_ENSURE(Item.GetStaticType()->IsSameType(*type->GetItemType()), "Wrong type of item");

    Item.Freeze();
}

TOptionalLiteral* TOptionalLiteral::Create(TOptionalType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(type);
}

TOptionalLiteral* TOptionalLiteral::Create(TRuntimeNode item, TOptionalType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(item, type);
}

void TOptionalLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    if (Item.GetNode()) {
        auto itemIt = links.find(Item.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(Item.GetNode()->Equals(*newNode));
            Item = TRuntimeNode(newNode, Item.IsImmediate());
        }
    }
}

TNode* TOptionalLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    auto newItemNode = Item.GetNode() ? (TNode*)Item.GetNode()->GetCookie() : nullptr;
    if (!newTypeNode && !newItemNode) {
        return const_cast<TOptionalLiteral*>(this);
    }

    if (!Item.GetNode()) {
        return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(
            newTypeNode ? static_cast<TOptionalType*>(newTypeNode) : GetType(), false);
    } else {
        return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(
            newItemNode ? TRuntimeNode(newItemNode, Item.IsImmediate()) : Item,
            newTypeNode ? static_cast<TOptionalType*>(newTypeNode) : GetType(), false);
    }
}

void TOptionalLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    if (Item.GetNode()) {
        Item.Freeze();
    }
}

bool TOptionalLiteral::Equals(const TOptionalLiteral& nodeToCompare) const {
    if (!Item.GetNode() != !nodeToCompare.Item.GetNode())
        return false;

    return !Item.GetNode() || Item.GetNode()->Equals(*nodeToCompare.Item.GetNode());
}

TDictType* TDictType::Create(TType* keyType, TType* payloadType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDictType>()) TDictType(keyType, payloadType, env);
}

bool TDictType::IsSameType(const TDictType& typeToCompare) const {
    return KeyType->IsSameType(*typeToCompare.KeyType)
        && PayloadType->IsSameType(*typeToCompare.PayloadType);
}

size_t TDictType::CalcHash() const {
    return CombineHashes(KeyType->CalcHash(), PayloadType->CalcHash());
}

bool TDictType::IsConvertableTo(const TDictType& typeToCompare, bool ignoreTagged) const {
    return KeyType->IsConvertableTo(*typeToCompare.KeyType, ignoreTagged)
        && PayloadType->IsConvertableTo(*typeToCompare.PayloadType, ignoreTagged);
}

void TDictType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto keyTypeIt = links.find(KeyType);
    if (keyTypeIt != links.end()) {
        TNode* newNode = keyTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(KeyType->Equals(*newNode));
        KeyType = static_cast<TType*>(newNode);
    }

    auto payloadTypeIt = links.find(PayloadType);
    if (payloadTypeIt != links.end()) {
        TNode* newNode = payloadTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(PayloadType->Equals(*newNode));
        PayloadType = static_cast<TType*>(newNode);
    }
}

TNode* TDictType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newKeyType = (TNode*)KeyType->GetCookie();
    auto newPayloadType = (TNode*)PayloadType->GetCookie();
    if (!newKeyType && !newPayloadType) {
        return const_cast<TDictType*>(this);
    }

    return ::new(env.Allocate<TDictType>()) TDictType(
        newKeyType ? static_cast<TType*>(newKeyType) : KeyType,
        newPayloadType ? static_cast<TType*>(newPayloadType) : PayloadType, env, false);
}

void TDictType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}


TDictType::TDictType(TType* keyType, TType* payloadType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Dict, env.GetTypeOfTypeLazy(), keyType->IsPresortSupported() && payloadType->IsPresortSupported())
    , KeyType(keyType)
    , PayloadType(payloadType)
{
    if (!validate)
        return;

    EnsureValidDictKey(keyType);
}

void TDictType::EnsureValidDictKey(TType* keyType) {
    Y_UNUSED(keyType);
}

TDictLiteral::TDictLiteral(ui32 itemsCount, std::pair<TRuntimeNode, TRuntimeNode>* items, TDictType* type, bool validate)
    : TNode(type)
    , ItemsCount(itemsCount)
    , Items(items)
{
    if (!validate) {
        for (size_t index = 0; index < itemsCount; ++index) {
            auto& item = Items[index];
            item.first.Freeze();
            item.second.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < itemsCount; ++index) {
        auto& item = Items[index];
        MKQL_ENSURE(item.first.GetStaticType()->IsSameType(*type->GetKeyType()), "Wrong type of key");

        MKQL_ENSURE(item.second.GetStaticType()->IsSameType(*type->GetPayloadType()), "Wrong type of payload");

        item.first.Freeze();
        item.second.Freeze();
    }
}

TDictLiteral* TDictLiteral::Create(ui32 itemsCount, const std::pair<TRuntimeNode, TRuntimeNode>* items, TDictType* type,
    const TTypeEnvironment& env) {
    std::pair<TRuntimeNode, TRuntimeNode>* allocatedItems = nullptr;
    if (itemsCount) {
        allocatedItems = static_cast<std::pair<TRuntimeNode, TRuntimeNode>*>(env.AllocateBuffer(itemsCount * sizeof(*allocatedItems)));
        for (ui32 i = 0; i < itemsCount; ++i) {
            allocatedItems[i] = items[i];
        }
    }

    return ::new(env.Allocate<TDictLiteral>()) TDictLiteral(itemsCount, allocatedItems, type);
}

void TDictLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < ItemsCount; ++i) {
        auto& item = Items[i];
        auto itemKeyIt = links.find(item.first.GetNode());
        if (itemKeyIt != links.end()) {
            TNode* newNode = itemKeyIt->second;
            Y_DEBUG_ABORT_UNLESS(item.first.GetNode()->Equals(*newNode));
            item.first = TRuntimeNode(newNode, item.first.IsImmediate());
        }

        auto itemPayloadIt = links.find(item.second.GetNode());
        if (itemPayloadIt != links.end()) {
            TNode* newNode = itemPayloadIt->second;
            Y_DEBUG_ABORT_UNLESS(item.second.GetNode()->Equals(*newNode));
            item.second = TRuntimeNode(newNode, item.second.IsImmediate());
        }
    }
}

TNode* TDictLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < ItemsCount; ++i) {
            if (Items[i].first.GetNode()->GetCookie()) {
                needClone = true;
                break;
            }

            if (Items[i].second.GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TDictLiteral*>(this);

    std::pair<TRuntimeNode, TRuntimeNode>* allocatedItems = nullptr;
    if (ItemsCount) {
        allocatedItems = static_cast<std::pair<TRuntimeNode, TRuntimeNode>*>(env.AllocateBuffer(ItemsCount * sizeof(*allocatedItems)));
        for (ui32 i = 0; i < ItemsCount; ++i) {
            allocatedItems[i] = Items[i];
            auto newKeyNode = (TNode*)Items[i].first.GetNode()->GetCookie();
            if (newKeyNode) {
                allocatedItems[i].first = TRuntimeNode(newKeyNode, Items[i].first.IsImmediate());
            }

            auto newPayloadNode = (TNode*)Items[i].second.GetNode()->GetCookie();
            if (newPayloadNode) {
                allocatedItems[i].second = TRuntimeNode(newPayloadNode, Items[i].second.IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TDictLiteral>()) TDictLiteral(ItemsCount, allocatedItems,
        newTypeNode ? static_cast<TDictType*>(newTypeNode) : GetType(), false);
}

void TDictLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < ItemsCount; ++i) {
        Items[i].first.Freeze();
        Items[i].second.Freeze();
    }
}

bool TDictLiteral::Equals(const TDictLiteral& nodeToCompare) const {
    if (ItemsCount != nodeToCompare.ItemsCount)
        return false;

    for (size_t i = 0; i < ItemsCount; ++i) {
        if (Items[i] != nodeToCompare.Items[i])
            return false;
    }

    return true;
}

TCallableType::TCallableType(const TInternName &name, TType* returnType, ui32 argumentsCount,
        TType **arguments, TNode* payload, const TTypeEnvironment& env)
    : TType(EKind::Callable, env.GetTypeOfTypeLazy(), false)
    , IsMergeDisabled0(false)
    , ArgumentsCount(argumentsCount)
    , Name(name)
    , ReturnType(returnType)
    , Arguments(arguments)
    , Payload(payload)
    , OptionalArgs(0)
{
}

TCallableType* TCallableType::Create(const TString& name, TType* returnType,
    ui32 argumentsCount, TType** arguments, TNode* payload, const TTypeEnvironment& env) {
    auto internedName = env.InternName(name);
    TType** allocatedArguments = nullptr;
    if (argumentsCount) {
        allocatedArguments = static_cast<TType**>(env.AllocateBuffer(argumentsCount * sizeof(*allocatedArguments)));
        for (ui32 i = 0; i < argumentsCount; ++i) {
            allocatedArguments[i] = arguments[i];
        }
    }

    return ::new(env.Allocate<TCallableType>()) TCallableType(internedName, returnType, argumentsCount,
        allocatedArguments, payload, env);
}

TCallableType* TCallableType::Create(TType* returnType, const TStringBuf& name, ui32 argumentsCount,
    TType** arguments, TNode* payload, const TTypeEnvironment& env) {
    auto internedName = env.InternName(name);
    TType** allocatedArguments = nullptr;
    if (argumentsCount) {
        allocatedArguments = static_cast<TType**>(env.AllocateBuffer(argumentsCount * sizeof(*allocatedArguments)));
        for (ui32 i = 0; i < argumentsCount; ++i) {
            allocatedArguments[i] = arguments[i];
        }
    }

    return ::new(env.Allocate<TCallableType>()) TCallableType(internedName, returnType, argumentsCount,
        allocatedArguments, payload, env);
}

bool TCallableType::IsSameType(const TCallableType& typeToCompare) const {
    if (this == &typeToCompare)
        return true;

    if (Name != typeToCompare.Name || IsMergeDisabled0 != typeToCompare.IsMergeDisabled0)
        return false;

    if (ArgumentsCount != typeToCompare.ArgumentsCount)
        return false;

    if (OptionalArgs != typeToCompare.OptionalArgs)
        return false;

    for (size_t index = 0; index < ArgumentsCount; ++index) {
        const auto arg = Arguments[index];
        const auto otherArg = typeToCompare.Arguments[index];
        if (!arg->IsSameType(*otherArg))
            return false;
    }

    if (!ReturnType->IsSameType(*typeToCompare.ReturnType))
        return false;

    if (!Payload != !typeToCompare.Payload)
        return false;

    return !Payload || Payload->Equals(*typeToCompare.Payload);
}

size_t TCallableType::CalcHash() const {
    size_t hash = 0;
    hash = CombineHashes(hash, IntHash<size_t>(ArgumentsCount));
    hash = CombineHashes(hash, Name.Hash());
    hash = CombineHashes(hash, ReturnType->CalcHash());
    for (size_t index = 0; index < ArgumentsCount; ++index) {
        hash = CombineHashes(hash, Arguments[index]->CalcHash());
    }
    hash = CombineHashes(hash, IntHash<size_t>(OptionalArgs));
    return hash;
}

bool TCallableType::IsConvertableTo(const TCallableType& typeToCompare, bool ignoreTagged) const {
    // do not check callable name here

    if (this == &typeToCompare)
        return true;

    if (IsMergeDisabled0 != typeToCompare.IsMergeDisabled0)
        return false;

    if (ArgumentsCount < typeToCompare.ArgumentsCount)
        return false;

    // function with fewer optional args can't be converted to function
    // with more optional args
    if (ArgumentsCount - OptionalArgs > typeToCompare.ArgumentsCount - typeToCompare.OptionalArgs)
        return false;

    for (size_t index = 0; index < typeToCompare.ArgumentsCount; ++index) {
        const auto arg = Arguments[index];
        const auto otherArg = typeToCompare.Arguments[index];
        if (!arg->IsConvertableTo(*otherArg, ignoreTagged))
            return false;
    }

    if (!ReturnType->IsConvertableTo(*typeToCompare.ReturnType, ignoreTagged))
        return false;

    if (!Payload) {
        return true;
    }

    if (!typeToCompare.Payload) {
        return false;
    }

    TCallablePayload parsedPayload(Payload), parsedPayloadToCompare(typeToCompare.Payload);
    for (size_t index = 0; index < typeToCompare.ArgumentsCount; ++index) {
        if (parsedPayload.GetArgumentName(index) != parsedPayloadToCompare.GetArgumentName(index)) {
            return false;
        }
        if (parsedPayload.GetArgumentFlags(index) != parsedPayloadToCompare.GetArgumentFlags(index)) {
            return false;
        }
    }

    return true;
}

void TCallableType::SetOptionalArgumentsCount(ui32 count) {
    MKQL_ENSURE(count <= ArgumentsCount, "Wrong optional arguments count: " << count << ", function has only " <<
        ArgumentsCount << " arguments");
    OptionalArgs = count;
    for (ui32 index = ArgumentsCount - OptionalArgs; index < ArgumentsCount; ++index) {
        MKQL_ENSURE(Arguments[index]->IsOptional(), "Optional argument #" << (index + 1) << " must be an optional");
    }
}

void TCallableType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto returnTypeIt = links.find(ReturnType);
    if (returnTypeIt != links.end()) {
        TNode* newNode = returnTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(ReturnType->Equals(*newNode));
        ReturnType = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < ArgumentsCount; ++i) {
        auto& arg = Arguments[i];
        auto argIt = links.find(arg);
        if (argIt != links.end()) {
            TNode* newNode = argIt->second;
            Y_DEBUG_ABORT_UNLESS(arg->Equals(*newNode));
            arg = static_cast<TType*>(newNode);
        }
    }

    if (Payload) {
        auto payloadIt = links.find(Payload);
        if (payloadIt != links.end()) {
            TNode* newNode = payloadIt->second;
            Y_DEBUG_ABORT_UNLESS(Payload->Equals(*newNode));
            Payload = newNode;
        }
    }
}

TNode* TCallableType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newReturnTypeNode = (TNode*)ReturnType->GetCookie();
    auto newPayloadNode = Payload ? (TNode*)Payload->GetCookie() : nullptr;
    bool needClone = false;
    if (newReturnTypeNode || newPayloadNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < ArgumentsCount; ++i) {
            if (Arguments[i]->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TCallableType*>(this);

    TType** allocatedArguments = nullptr;
    if (ArgumentsCount) {
        allocatedArguments = static_cast<TType**>(env.AllocateBuffer(ArgumentsCount * sizeof(*allocatedArguments)));
        for (ui32 i = 0; i < ArgumentsCount; ++i) {
            allocatedArguments[i] = Arguments[i];
            auto newArgNode = (TNode*)Arguments[i]->GetCookie();
            if (newArgNode) {
                allocatedArguments[i] = static_cast<TType*>(newArgNode);
            }
        }
    }

    return ::new(env.Allocate<TCallableType>()) TCallableType(Name,
        newReturnTypeNode ? static_cast<TType*>(newReturnTypeNode) : ReturnType,
        ArgumentsCount, allocatedArguments, newPayloadNode ? newPayloadNode : Payload, env);
}

void TCallableType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TCallable::TCallable(ui32 inputsCount, TRuntimeNode* inputs, TCallableType* type, bool validate)
    : TNode(type)
    , InputsCount(inputsCount)
    , UniqueId(0)
    , Inputs(inputs)
{
    if (!validate) {
        for (size_t index = 0; index < inputsCount; ++index) {
            auto& node = Inputs[index];
            node.Freeze();
        }

        return;
    }

    MKQL_ENSURE(inputsCount == type->GetArgumentsCount(), "Wrong count of inputs");

    for (size_t index = 0; index < inputsCount; ++index) {
        auto& node = Inputs[index];
        const auto argType = type->GetArgumentType(index);
        MKQL_ENSURE(node.GetStaticType()->IsSameType(*argType), "Wrong type of input");

        node.Freeze();
    }
}

TCallable::TCallable(TRuntimeNode result, TCallableType* type, bool validate)
    : TNode(type)
    , InputsCount(0)
    , UniqueId(0)
    , Inputs(nullptr)
    , Result(result)
{
    if (!validate) {
        Result.Freeze();
        return;
    }

    MKQL_ENSURE(result.GetStaticType()->IsSameType(*type->GetReturnType()), "incorrect result type for callable: "
        << GetType()->GetName());
    Result.Freeze();
}

TCallable* TCallable::Create(ui32 inputsCount, const TRuntimeNode* inputs, TCallableType* type, const TTypeEnvironment& env) {
    TRuntimeNode* allocatedInputs = nullptr;
    if (inputsCount) {
        allocatedInputs = static_cast<TRuntimeNode*>(env.AllocateBuffer(inputsCount * sizeof(*allocatedInputs)));
        for (ui32 i = 0; i < inputsCount; ++i) {
            allocatedInputs[i] = inputs[i];
        }
    }

    return ::new(env.Allocate<TCallable>()) TCallable(inputsCount, allocatedInputs, type);
}

TCallable* TCallable::Create(TRuntimeNode result, TCallableType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TCallable>()) TCallable(result, type);
}

void TCallable::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < InputsCount; ++i) {
        auto& input = Inputs[i];
        auto inputIt = links.find(input.GetNode());
        if (inputIt != links.end()) {
            TNode* newNode = inputIt->second;
            Y_DEBUG_ABORT_UNLESS(input.GetNode()->Equals(*newNode));
            input = TRuntimeNode(newNode, input.IsImmediate());
        }
    }

    if (Result.GetNode()) {
        auto resultIt = links.find(Result.GetNode());
        if (resultIt != links.end()) {
            TNode* newNode = resultIt->second;
            Y_DEBUG_ABORT_UNLESS(Result.GetNode()->Equals(*newNode));
            Result = TRuntimeNode(newNode, Result.IsImmediate());
        }
    }
}

TNode* TCallable::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    auto newResultNode = Result.GetNode() ? (TNode*)Result.GetNode()->GetCookie() : nullptr;
    bool needClone = false;
    if (newTypeNode || newResultNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < InputsCount; ++i) {
            if (Inputs[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TCallable*>(this);

    TRuntimeNode* allocatedInputs = nullptr;
    if (!Result.GetNode()) {
        if (InputsCount) {
            allocatedInputs = static_cast<TRuntimeNode*>(env.AllocateBuffer(InputsCount * sizeof(*allocatedInputs)));
            for (ui32 i = 0; i < InputsCount; ++i) {
                allocatedInputs[i] = Inputs[i];
                auto newNode = (TNode*)Inputs[i].GetNode()->GetCookie();
                if (newNode) {
                    allocatedInputs[i] = TRuntimeNode(newNode, Inputs[i].IsImmediate());
                }
            }
        }
    }

    TCallable* newCallable;
    if (Result.GetNode()) {
        newCallable = ::new(env.Allocate<TCallable>()) TCallable(
            newResultNode ? TRuntimeNode(newResultNode, Result.IsImmediate()) : Result,
            newTypeNode ? static_cast<TCallableType*>(newTypeNode) : GetType(), false);
    } else {
        newCallable = ::new(env.Allocate<TCallable>()) TCallable(InputsCount, allocatedInputs,
            newTypeNode ? static_cast<TCallableType*>(newTypeNode) : GetType(), false);
    }

    newCallable->SetUniqueId(GetUniqueId());
    return newCallable;
}

void TCallable::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < InputsCount; ++i) {
        Inputs[i].Freeze();
    }
}

bool TCallable::Equals(const TCallable& nodeToCompare) const {
    if (GetType()->IsMergeDisabled() && (this != &nodeToCompare))
        return false;

    if (InputsCount != nodeToCompare.InputsCount)
        return false;

    if (!Result.GetNode() != !nodeToCompare.Result.GetNode())
        return false;

    for (size_t i = 0; i < InputsCount; ++i) {
        if (Inputs[i] != nodeToCompare.Inputs[i])
            return false;
    }

    if (Result.GetNode() && Result != nodeToCompare.Result)
        return false;

    return true;
}

void TCallable::SetResult(TRuntimeNode result, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(!Result.GetNode(), "result is already set");

    MKQL_ENSURE(result.GetStaticType()->IsSameType(*GetType()->GetReturnType()),
                "incorrect result type of function " << GetType()->GetName()
                << ", left: " << PrintNode(result.GetStaticType(), true)
                << ", right: " << PrintNode(GetType()->GetReturnType(), true));

    Result = result;
    InputsCount = 0;
    Inputs = nullptr;
    Result.Freeze();
}

TCallablePayload::TCallablePayload(NMiniKQL::TNode* node)
{
    auto structObj = AS_VALUE(NMiniKQL::TStructLiteral, NMiniKQL::TRuntimeNode(node, true));
    auto argsIndex = structObj->GetType()->GetMemberIndex("Args");
    auto payloadIndex = structObj->GetType()->GetMemberIndex("Payload");
    Payload_ = AS_VALUE(NMiniKQL::TDataLiteral, structObj->GetValue(payloadIndex))->AsValue().AsStringRef();
    auto args = structObj->GetValue(argsIndex);
    auto argsList = AS_VALUE(NMiniKQL::TListLiteral, args);
    auto itemType = AS_TYPE(NMiniKQL::TStructType, AS_TYPE(NMiniKQL::TListType, args)->GetItemType());
    auto nameIndex = itemType->GetMemberIndex("Name");
    auto flagsIndex = itemType->GetMemberIndex("Flags");
    ArgsNames_.reserve(argsList->GetItemsCount());
    ArgsFlags_.reserve(argsList->GetItemsCount());
    for (ui32 i = 0; i < argsList->GetItemsCount(); ++i) {
        auto arg = AS_VALUE(NMiniKQL::TStructLiteral, argsList->GetItems()[i]);
        ArgsNames_.push_back(AS_VALUE(NMiniKQL::TDataLiteral, arg->GetValue(nameIndex))->AsValue().AsStringRef());
        ArgsFlags_.push_back(AS_VALUE(NMiniKQL::TDataLiteral, arg->GetValue(flagsIndex))->AsValue().Get<ui64>());
    }
}

bool TRuntimeNode::HasValue() const {
    TRuntimeNode current = *this;
    for (;;) {
        if (current.IsImmediate())
            return true;

        MKQL_ENSURE(current.GetNode()->GetType()->IsCallable(), "Wrong type");

        const auto& callable = static_cast<const TCallable&>(*current.GetNode());
        if (!callable.HasResult())
            return false;

        current = callable.GetResult();
    }
}

TNode* TRuntimeNode::GetValue() const {
    TRuntimeNode current = *this;
    for (;;) {
        if (current.IsImmediate())
            return current.GetNode();

        MKQL_ENSURE(current.GetNode()->GetType()->IsCallable(), "Wrong type");

        const auto& callable = static_cast<const TCallable&>(*current.GetNode());
        current = callable.GetResult();
    }
}

void TRuntimeNode::Freeze() {
    while (!IsImmediate()) {
        MKQL_ENSURE(GetNode()->GetType()->IsCallable(), "Wrong type");

        const auto& callable = static_cast<const TCallable&>(*GetNode());
        if (!callable.HasResult())
            break;

        *this = callable.GetResult();
    }
}

bool TAnyType::IsSameType(const TAnyType& typeToCompare) const {
    Y_UNUSED(typeToCompare);
    return true;
}

size_t TAnyType::CalcHash() const {
    return 0;
}

bool TAnyType::IsConvertableTo(const TAnyType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

void TAnyType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    Y_UNUSED(links);
}

TNode* TAnyType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TAnyType*>(this);
}

void TAnyType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TAnyType* TAnyType::Create(TTypeType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TAnyType>()) TAnyType(type);
}

TAny* TAny::Create(const TTypeEnvironment& env) {
    return ::new(env.Allocate<TAny>()) TAny(env.GetAnyTypeLazy());
}

void TAny::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    if (Item.GetNode()) {
        auto itemIt = links.find(Item.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(Item.GetNode()->Equals(*newNode));
            Item = TRuntimeNode(newNode, Item.IsImmediate());
        }
    }
}

TNode* TAny::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    if (!Item.GetNode())
        return const_cast<TAny*>(this);

    auto newItemNode = (TNode*)Item.GetNode()->GetCookie();
    if (!newItemNode) {
        return const_cast<TAny*>(this);
    }

    auto any = TAny::Create(env);
    any->SetItem(TRuntimeNode(newItemNode, Item.IsImmediate()));
    return any;
}

void TAny::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    if (Item.GetNode()) {
        Item.Freeze();
    }
}

void TAny::SetItem(TRuntimeNode newItem) {
    MKQL_ENSURE(!Item.GetNode(), "item is already set");

    Item = newItem;
    Item.Freeze();
}

bool TAny::Equals(const TAny& nodeToCompare) const {
    if (!Item.GetNode() || !nodeToCompare.Item.GetNode())
        return false;

    if (!Item.IsImmediate() != !nodeToCompare.Item.IsImmediate())
        return false;

    return Item.GetNode()->Equals(*nodeToCompare.Item.GetNode());
}

TTupleLiteral::TTupleLiteral(TRuntimeNode* values, TTupleType* type, bool validate)
    : TNode(type)
    , Values(values)
{
    if (!validate) {
        for (size_t index = 0; index < GetValuesCount(); ++index) {
            auto& value = Values[index];
            value.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < GetValuesCount(); ++index) {
        auto& value = Values[index];
        MKQL_ENSURE(value.GetStaticType()->IsSameType(*type->GetElementType(index)), "Wrong type of element");

        value.Freeze();
    }
}

TTupleLiteral* TTupleLiteral::Create(ui32 valuesCount, const TRuntimeNode* values, TTupleType* type, const TTypeEnvironment& env, bool useCachedEmptyTuple) {
    MKQL_ENSURE(valuesCount == type->GetElementsCount(), "Wrong count of elements");
    TRuntimeNode* allocatedValues = nullptr;
    if (valuesCount) {
        allocatedValues = static_cast<TRuntimeNode*>(env.AllocateBuffer(valuesCount * sizeof(*allocatedValues)));
        for (ui32 i = 0; i < valuesCount; ++i) {
            allocatedValues[i] = values[i];
        }
    } else if (useCachedEmptyTuple) {
        return env.GetEmptyTupleLazy();
    }

    return ::new(env.Allocate<TTupleLiteral>()) TTupleLiteral(allocatedValues, type);
}

void TTupleLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        auto& value = Values[i];
        auto valueIt = links.find(value.GetNode());
        if (valueIt != links.end()) {
            TNode* newNode = valueIt->second;
            Y_DEBUG_ABORT_UNLESS(value.GetNode()->Equals(*newNode));
            value = TRuntimeNode(newNode, value.IsImmediate());
        }
    }
}

TNode* TTupleLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            if (Values[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TTupleLiteral*>(this);

    TRuntimeNode* allocatedValues = nullptr;
    if (GetValuesCount()) {
        allocatedValues = static_cast<TRuntimeNode*>(env.AllocateBuffer(GetValuesCount() * sizeof(*allocatedValues)));
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            allocatedValues[i] = Values[i];
            auto newNode = (TNode*)Values[i].GetNode()->GetCookie();
            if (newNode) {
                allocatedValues[i] = TRuntimeNode(newNode, Values[i].IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TTupleLiteral>()) TTupleLiteral(allocatedValues,
        newTypeNode ? static_cast<TTupleType*>(newTypeNode) : GetType(), false);
}

void TTupleLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        Values[i].Freeze();
    }
}

bool TTupleLiteral::Equals(const TTupleLiteral& nodeToCompare) const {
    if (GetValuesCount() != nodeToCompare.GetValuesCount())
        return false;

    for (size_t i = 0; i < GetValuesCount(); ++i) {
        if (Values[i] != nodeToCompare.Values[i])
            return false;
    }

    return true;
}

bool TResourceType::IsSameType(const TResourceType& typeToCompare) const {
    return Tag == typeToCompare.Tag;
}

size_t TResourceType::CalcHash() const {
    return Tag.Hash();
}

bool TResourceType::IsConvertableTo(const TResourceType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

void TResourceType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    Y_UNUSED(links);
}

TNode* TResourceType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TResourceType*>(this);
}

void TResourceType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TResourceType* TResourceType::Create(const TStringBuf& tag, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TResourceType>()) TResourceType(env.GetTypeOfTypeLazy(), env.InternName(tag));
}

TVariantType* TVariantType::Create(TType* underlyingType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TVariantType>()) TVariantType(underlyingType, env);
}

bool TVariantType::IsSameType(const TVariantType& typeToCompare) const {
    return GetUnderlyingType()->IsSameType(*typeToCompare.GetUnderlyingType());
}

size_t TVariantType::CalcHash() const {
    return Data->CalcHash();
}

bool TVariantType::IsConvertableTo(const TVariantType& typeToCompare, bool ignoreTagged) const {
    return GetUnderlyingType()->IsConvertableTo(*typeToCompare.GetUnderlyingType(), ignoreTagged);
}

TVariantType::TVariantType(TType* underlyingType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Variant, env.GetTypeOfTypeLazy(), underlyingType->IsPresortSupported())
    , Data(underlyingType)
{
    if (validate) {
        MKQL_ENSURE(underlyingType->IsTuple() || underlyingType->IsStruct(),
            "Expected struct or tuple, but got: " << PrintNode(underlyingType, true));

        if (underlyingType->IsTuple()) {
            MKQL_ENSURE(AS_TYPE(TTupleType, underlyingType)->GetElementsCount() > 0, "Empty tuple is not allowed as underlying type for variant");
        } else {
            MKQL_ENSURE(AS_TYPE(TStructType, underlyingType)->GetMembersCount() > 0, "Empty struct is not allowed as underlying type for variant");
        }
    }
}

void TVariantType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetUnderlyingType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetUnderlyingType()->Equals(*newNode));
        Data = static_cast<TType*>(newNode);
    }
}

TNode* TVariantType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetUnderlyingType()->GetCookie();
    if (!newTypeNode) {
        return const_cast<TVariantType*>(this);
    }

    return ::new(env.Allocate<TVariantType>()) TVariantType(static_cast<TType*>(newTypeNode), env, false);
}

void TVariantType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TVariantLiteral* TVariantLiteral::Create(TRuntimeNode item, ui32 index, TVariantType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TVariantLiteral>()) TVariantLiteral(item, index, type);
}

TVariantLiteral::TVariantLiteral(TRuntimeNode item, ui32 index, TVariantType* type, bool validate)
    : TNode(type)
    , Item(item)
    , Index(index)
{
    Item.Freeze();
    if (validate) {
        MKQL_ENSURE(index < type->GetAlternativesCount(),
            "Index out of range: " << index << " alt. count: " << type->GetAlternativesCount());

        auto underlyingType = type->GetUnderlyingType();
        if (underlyingType->IsTuple()) {
            auto expectedType = AS_TYPE(TTupleType, underlyingType)->GetElementType(index);
            MKQL_ENSURE(Item.GetStaticType()->IsSameType(*expectedType), "Wrong type of item");
        } else {
            auto expectedType = AS_TYPE(TStructType, underlyingType)->GetMemberType(index);
            MKQL_ENSURE(Item.GetStaticType()->IsSameType(*expectedType), "Wrong type of item");
        }
    }
}

bool TVariantLiteral::Equals(const TVariantLiteral& nodeToCompare) const {
    if (Index != nodeToCompare.GetIndex()) {
        return false;
    }

    return Item.GetNode()->Equals(*nodeToCompare.GetItem().GetNode());
}

void TVariantLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type->Equals(*newNode));
        Type = static_cast<TType*>(newNode);
    }

    auto itemIt = links.find(Item.GetNode());
    if (itemIt != links.end()) {
        TNode* newNode = itemIt->second;
        Y_DEBUG_ABORT_UNLESS(Item.GetNode()->Equals(*newNode));
        Item = TRuntimeNode(newNode, Item.IsImmediate());
    }
}

TNode* TVariantLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type->GetCookie();
    auto newItemNode = (TNode*)Item.GetNode()->GetCookie();
    if (!newTypeNode && !newItemNode) {
        return const_cast<TVariantLiteral*>(this);
    }

    return ::new(env.Allocate<TVariantLiteral>()) TVariantLiteral(
        newItemNode ? TRuntimeNode(newItemNode, Item.IsImmediate()) : Item, Index,
        newTypeNode ? static_cast<TVariantType*>(newTypeNode) : GetType(), false);
}

void TVariantLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    Item.Freeze();
}

TBlockType::TBlockType(TType* itemType, EShape shape, const TTypeEnvironment& env)
    : TType(EKind::Block, env.GetTypeOfTypeLazy(), false)
    , ItemType(itemType)
    , Shape(shape)
{
}

TBlockType* TBlockType::Create(TType* itemType, EShape shape, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TBlockType>()) TBlockType(itemType, shape, env);
}

bool TBlockType::IsSameType(const TBlockType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType()) && Shape == typeToCompare.Shape;
}

size_t TBlockType::CalcHash() const {
    return CombineHashes(ItemType->CalcHash(), IntHash((size_t)Shape));
}

bool TBlockType::IsConvertableTo(const TBlockType& typeToCompare, bool ignoreTagged) const {
    return Shape == typeToCompare.Shape &&
        GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TBlockType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    const auto itemTypeIt = links.find(ItemType);
    if (itemTypeIt != links.end()) {
        auto* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(ItemType->Equals(*newNode));
        ItemType = static_cast<TType*>(newNode);
    }
}

TNode* TBlockType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)ItemType->GetCookie();
    if (!newTypeNode) {
        return const_cast<TBlockType*>(this);
    }

    return ::new(env.Allocate<TBlockType>()) TBlockType(static_cast<TType*>(newTypeNode), Shape, env);
}

void TBlockType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

bool IsNumericType(NUdf::TDataTypeId typeId) {
    auto slot = NUdf::FindDataSlot(typeId);
    return slot && NUdf::GetDataTypeInfo(*slot).Features & NUdf::NumericType;
}

bool IsCommonStringType(NUdf::TDataTypeId typeId) {
    auto slot = NUdf::FindDataSlot(typeId);
    return slot && NUdf::GetDataTypeInfo(*slot).Features & NUdf::StringType;
}

bool IsDateType(NUdf::TDataTypeId typeId) {
    auto slot = NUdf::FindDataSlot(typeId);
    return slot && NUdf::GetDataTypeInfo(*slot).Features & NUdf::DateType && *slot != NUdf::EDataSlot::Interval;
}

bool IsTzDateType(NUdf::TDataTypeId typeId) {
    auto slot = NUdf::FindDataSlot(typeId);
    return slot && NUdf::GetDataTypeInfo(*slot).Features & NUdf::TzDateType && *slot != NUdf::EDataSlot::Interval;
}

bool IsIntervalType(NUdf::TDataTypeId typeId) {
    auto slot = NUdf::FindDataSlot(typeId);
    return slot && NUdf::GetDataTypeInfo(*slot).Features & NUdf::TimeIntervalType;
}

EValueRepresentation GetValueRepresentation(NUdf::TDataTypeId type) {
    switch (type) {
#define CASE_FOR(type, layout) \
        case NUdf::TDataType<type>::Id:
KNOWN_FIXED_VALUE_TYPES(CASE_FOR)
#undef CASE_FOR
        case NUdf::TDataType<NUdf::TDecimal>::Id:
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            return EValueRepresentation::Embedded;
        default:
            return EValueRepresentation::String;
    }
}

EValueRepresentation GetValueRepresentation(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Data:
            return GetValueRepresentation(static_cast<const TDataType*>(type)->GetSchemeType());
        case TType::EKind::Optional:
            return GetValueRepresentation(static_cast<const TOptionalType*>(type)->GetItemType());
        case TType::EKind::Flow:
            return GetValueRepresentation(static_cast<const TFlowType*>(type)->GetItemType());

        case TType::EKind::Stream:
        case TType::EKind::Struct:
        case TType::EKind::Tuple:
        case TType::EKind::Dict:
        case TType::EKind::List:
        case TType::EKind::Resource:
        case TType::EKind::Block:
        case TType::EKind::Callable:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
        case TType::EKind::Multi:
            return EValueRepresentation::Boxed;

        case TType::EKind::Variant:
        case TType::EKind::Void:
            return EValueRepresentation::Any;

        case TType::EKind::Null:
            return EValueRepresentation::Embedded;

        case TType::EKind::Pg:
            return EValueRepresentation::Any;

        case TType::EKind::Tagged:
            return GetValueRepresentation(static_cast<const TTaggedType*>(type)->GetBaseType());

        default:
            Y_ABORT("Unsupported type.");
    }
}

TArrayRef<TType* const> GetWideComponents(const TFlowType* type) {
    if (type->GetItemType()->IsMulti()) {
        return AS_TYPE(TMultiType, type->GetItemType())->GetElements();
    }
    return AS_TYPE(TTupleType, type->GetItemType())->GetElements();
}

TArrayRef<TType* const> GetWideComponents(const TStreamType* type) {
    MKQL_ENSURE(RuntimeVersion > 35, "Wide stream is not supported in runtime version " << RuntimeVersion);
    return AS_TYPE(TMultiType, type->GetItemType())->GetElements();
}

}
}
