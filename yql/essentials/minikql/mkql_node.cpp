#include "mkql_node.h"
#include "mkql_node_builder.h"
#include "mkql_node_cast.h"
#include "mkql_node_visitor.h"
#include "mkql_node_printer.h"
#include "mkql_runtime_version.h"
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <util/stream/str.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NMiniKQL {

#define MKQL_SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val: return TStringBuf(#name);

using namespace NDetail;

TTypeEnvironment::TTypeEnvironment(TScopedAlloc& alloc)
    : Alloc_(alloc)
    , Arena_(&Alloc_.Ref())
    , EmptyStruct_(nullptr)
    , EmptyTuple_(nullptr)
{
}

TTypeEnvironment::~TTypeEnvironment() {
}

void TTypeEnvironment::ClearCookies() const {
    if (TypeOfType_) {
        TypeOfType_->SetCookie(0);
    }
    if (TypeOfVoid_) {
        TypeOfVoid_->SetCookie(0);
    }
    if (Void_) {
        Void_->SetCookie(0);
    }
    if (TypeOfNull_) {
        TypeOfNull_->SetCookie(0);
    }
    if (Null_) {
        Null_->SetCookie(0);
    }
    if (TypeOfEmptyList_) {
        TypeOfEmptyList_->SetCookie(0);
    }
    if (EmptyList_) {
        EmptyList_->SetCookie(0);
    }
    if (TypeOfEmptyDict_) {
        TypeOfEmptyDict_->SetCookie(0);
    }
    if (EmptyDict_) {
        EmptyDict_->SetCookie(0);
    }
    if (EmptyStruct_) {
        EmptyStruct_->SetCookie(0);
    }
    if (ListOfVoid_) {
        ListOfVoid_->SetCookie(0);
    }
    if (AnyType_) {
        AnyType_->SetCookie(0);
    }
    if (EmptyTuple_) {
        EmptyTuple_->SetCookie(0);
    }
}

TTypeType* TTypeEnvironment::GetTypeOfTypeLazy() const {
    if (!TypeOfType_) {
        TypeOfType_ = TTypeType::Create(*this);
        TypeOfType_->Type_ = TypeOfType_;
    }
    return TypeOfType_;
}

TVoidType* TTypeEnvironment::GetTypeOfVoidLazy() const {
    if (!TypeOfVoid_) {
        TypeOfVoid_ = TVoidType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfVoid_;
}

TVoid* TTypeEnvironment::GetVoidLazy() const {
    if (!Void_) {
        Void_ = TVoid::Create(*this);
    }
    return Void_;
}

TNullType* TTypeEnvironment::GetTypeOfNullLazy() const {
    if (!TypeOfNull_) {
        TypeOfNull_ = TNullType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfNull_;
}

TNull* TTypeEnvironment::GetNullLazy() const {
    if (!Null_) {
        Null_ = TNull::Create(*this);
    }
    return Null_;
}

TEmptyListType* TTypeEnvironment::GetTypeOfEmptyListLazy() const {
    if (!TypeOfEmptyList_) {
        TypeOfEmptyList_ = TEmptyListType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfEmptyList_;
}

TEmptyList* TTypeEnvironment::GetEmptyListLazy() const {
    if (!EmptyList_) {
        EmptyList_ = TEmptyList::Create(*this);
    }
    return EmptyList_;
}

TEmptyDictType* TTypeEnvironment::GetTypeOfEmptyDictLazy() const {
    if (!TypeOfEmptyDict_) {
        TypeOfEmptyDict_ = TEmptyDictType::Create(GetTypeOfTypeLazy(), *this);
    }
    return TypeOfEmptyDict_;
}

TEmptyDict* TTypeEnvironment::GetEmptyDictLazy() const {
    if (!EmptyDict_) {
        EmptyDict_ = TEmptyDict::Create(*this);
    }
    return EmptyDict_;
}

TStructLiteral* TTypeEnvironment::GetEmptyStructLazy() const {
    if (!EmptyStruct_) {
        EmptyStruct_ = TStructLiteral::Create(
            0,
            nullptr,
            TStructType::Create(0, nullptr, *this), *this, false);
    }
    return EmptyStruct_;
}

TListLiteral* TTypeEnvironment::GetListOfVoidLazy() const {
    if (!ListOfVoid_) {
        ListOfVoid_ = TListLiteral::Create(nullptr, 0, TListType::Create(GetVoidLazy()->GetGenericType(), *this), *this);
    }
    return ListOfVoid_;
}

TAnyType* TTypeEnvironment::GetAnyTypeLazy() const {
    if (!AnyType_) {
        AnyType_ = TAnyType::Create(GetTypeOfTypeLazy(), *this);
    }
    return AnyType_;
}

TTupleLiteral* TTypeEnvironment::GetEmptyTupleLazy() const {
    if (!EmptyTuple_) {
        EmptyTuple_ = TTupleLiteral::Create(0, nullptr, TTupleType::Create(0, nullptr, *this), *this, false);
    }
    return EmptyTuple_;
}

TDataType* TTypeEnvironment::GetUi32Lazy() const {
    if (!Ui32_) {
        Ui32_ = TDataType::Create(NUdf::TDataType<ui32>::Id, *this);
    }
    return Ui32_;
}

TDataType* TTypeEnvironment::GetUi64Lazy() const {
    if (!Ui64_) {
        Ui64_ = TDataType::Create(NUdf::TDataType<ui64>::Id, *this);
    }
    return Ui64_;
}

std::vector<TNode*>& TTypeEnvironment::GetNodeStack() const {
    return Stack_;
}

TInternName TTypeEnvironment::InternName(const TStringBuf& name) const {
    if (NamesPool_.empty()) {
        NamesPool_.reserve(64);
    }

    auto it = NamesPool_.find(name);
    if (it != NamesPool_.end()) {
        return TInternName(*it);
    }

    // Copy to arena and null-terminate
    char* data = (char*)AllocateBuffer(name.size()+1);
    memcpy(data, name.data(), name.size());
    data[name.size()] = 0;

    return TInternName(*NamesPool_.insert(TStringBuf(data, name.size())).first);
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
    const auto kind = Type_->GetKind();
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

    if (!Type_->IsSameType(*nodeToCompare.Type_))
        return false;

    const auto kind = Type_->GetKind();
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
    const auto kind = Type_->GetKind();
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
    const auto kind = Type_->GetKind();
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
    const auto kind = Type_->GetKind();
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
    if (!Type_->IsCallable()) {
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
    , SchemeType_(schemeType)
    , DataSlot_(NUdf::FindDataSlot(schemeType))
{
}

TDataType* TDataType::Create(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env) {
    MKQL_ENSURE(schemeType, "Null type isn't allowed.");
    MKQL_ENSURE(schemeType != NUdf::TDataType<NUdf::TDecimal>::Id, "Can't' create Decimal.");
    MKQL_ENSURE(schemeType != 0, "0 type");
    return ::new(env.Allocate<TDataType>()) TDataType(schemeType, env);
}

bool TDataType::IsSameType(const TDataType& typeToCompare) const {
    if (SchemeType_ != typeToCompare.SchemeType_)
        return false;
    if (SchemeType_ != NUdf::TDataType<NUdf::TDecimal>::Id)
        return true;
    return static_cast<const TDataDecimalType&>(*this).IsSameType(static_cast<const TDataDecimalType&>(typeToCompare));
}

size_t TDataType::CalcHash() const {
    size_t hash = IntHash((size_t)GetSchemeType());
    if (SchemeType_ == NUdf::TDataType<NUdf::TDecimal>::Id) {
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
    : TDataType(NUdf::TDataType<NUdf::TDecimal>::Id, env), Precision_(precision), Scale_(scale)
{
    MKQL_ENSURE(Precision_ > 0, "Precision must be positive.");
    MKQL_ENSURE(Scale_ <= Precision_, "Scale too large.");
}

TDataDecimalType* TDataDecimalType::Create(ui8 precision, ui8 scale, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDataDecimalType>()) TDataDecimalType(precision, scale, env);
}

bool TDataDecimalType::IsSameType(const TDataDecimalType& typeToCompare) const {
    return Precision_ == typeToCompare.Precision_ && Scale_ == typeToCompare.Scale_;
}

size_t TDataDecimalType::CalcHash() const {
    return CombineHashes(IntHash((size_t)Precision_), IntHash((size_t)Scale_));
}

bool TDataDecimalType::IsConvertableTo(const TDataDecimalType& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return Precision_ == typeToCompare.Precision_ && Scale_ == typeToCompare.Scale_;
}

std::pair<ui8, ui8> TDataDecimalType::GetParams() const {
    return std::make_pair(Precision_, Scale_);
}

TDataLiteral::TDataLiteral(const TUnboxedValuePod& value, TDataType* type)
    : TNode(type), TUnboxedValuePod(value)
{}

TDataLiteral* TDataLiteral::Create(const NUdf::TUnboxedValuePod& value, TDataType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDataLiteral>()) TDataLiteral(value, type);
}

void TDataLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
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
    : TType(EKind::Pg, env.GetTypeOfTypeLazy(),
        NYql::NPg::HasType(typeId) && PG_SUPPORTED_PRESORT.contains(NYql::NPg::LookupType(typeId).Name))
    , TypeId_(typeId)
{
}

TPgType* TPgType::Create(ui32 typeId, const TTypeEnvironment& env) {
    MKQL_ENSURE(typeId != 0, "0 type");
    return ::new(env.Allocate<TPgType>()) TPgType(typeId, env);
}

bool TPgType::IsSameType(const TPgType& typeToCompare) const {
    return TypeId_ == typeToCompare.TypeId_;
}

size_t TPgType::CalcHash() const {
    return IntHash((size_t)TypeId_);
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
    return NYql::NPg::LookupType(TypeId_).Name;
}

TStructType::TStructType(ui32 membersCount, std::pair<TInternName, TType*>* members, const TTypeEnvironment& env,
    bool validate)
    : TType(EKind::Struct, env.GetTypeOfTypeLazy(), CalculatePresortSupport(membersCount, members))
    , MembersCount_(membersCount)
    , Members_(members)
{
    if (!validate)
        return;

    TInternName lastMemberName;
    for (size_t index = 0; index < membersCount; ++index) {
        const auto& name = Members_[index].first;
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

    if (MembersCount_ != typeToCompare.MembersCount_)
        return false;

    for (size_t index = 0; index < MembersCount_; ++index) {
        if (Members_[index].first != typeToCompare.Members_[index].first)
            return false;
        if (!Members_[index].second->IsSameType(*typeToCompare.Members_[index].second))
            return false;
    }

    return true;
}

size_t TStructType::CalcHash() const {
    size_t hash = 0;
    for (size_t i = 0; i < MembersCount_; ++i) {
        hash = CombineHashes(hash, Members_[i].first.Hash());
        hash = CombineHashes(hash, Members_[i].second->CalcHash());
    }
    return hash;
}

bool TStructType::IsConvertableTo(const TStructType& typeToCompare, bool ignoreTagged) const {
    if (this == &typeToCompare)
        return true;

    if (MembersCount_ != typeToCompare.MembersCount_)
        return false;

    for (size_t index = 0; index < MembersCount_; ++index) {
        if (Members_[index].first != typeToCompare.Members_[index].first)
            return false;
        if (!Members_[index].second->IsConvertableTo(*typeToCompare.Members_[index].second, ignoreTagged))
            return false;
    }

    return true;
}

void TStructType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    for (ui32 i = 0; i < MembersCount_; ++i) {
        auto& member = Members_[i];
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
    for (ui32 i = 0; i < MembersCount_; ++i) {
        if (Members_[i].second->GetCookie()) {
            needClone = true;
            break;
        }
    }

    if (!needClone)
        return const_cast<TStructType*>(this);

    std::pair<TInternName, TType*>* allocatedMembers = nullptr;
    if (MembersCount_) {
        allocatedMembers = static_cast<std::pair<TInternName, TType*>*>(env.AllocateBuffer(MembersCount_ * sizeof(*allocatedMembers)));
        for (ui32 i = 0; i < MembersCount_; ++i) {
            allocatedMembers[i].first = Members_[i].first;
            auto newNode = (TNode*)Members_[i].second->GetCookie();
            if (newNode) {
                allocatedMembers[i].second = static_cast<TType*>(newNode);
            } else {
                allocatedMembers[i].second = Members_[i].second;
            }
        }
    }

    return ::new(env.Allocate<TStructType>()) TStructType(MembersCount_, allocatedMembers, env, false);
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
    for (ui32 i = 0; i < MembersCount_; ++i) {
        ss << " " << Members_[i].first.Str();
    }
    THROW yexception() << "Member with name '" << name << "' not found; "
            << " known members: " << ss.Str() << ".";
}

TMaybe<ui32> TStructType::FindMemberIndex(const TStringBuf& name) const {
    for (ui32 i = 0; i < MembersCount_; ++i) {
        if (Members_[i].first == name)
            return i;
    }

    return {};
}

TStructLiteral::TStructLiteral(TRuntimeNode* values, TStructType* type, bool validate)
    : TNode(type)
    , Values_(values)
{
    if (!validate) {
        for (size_t index = 0; index < GetValuesCount(); ++index) {
            auto& value = Values_[index];
            value.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < GetValuesCount(); ++index) {
        MKQL_ENSURE(!type->GetMemberName(index).empty(), "Empty struct member name is not allowed");

        auto& value = Values_[index];
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
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        auto& value = Values_[i];
        auto valueIt = links.find(value.GetNode());
        if (valueIt != links.end()) {
            TNode* newNode = valueIt->second;
            Y_DEBUG_ABORT_UNLESS(value.GetNode()->Equals(*newNode));
            value = TRuntimeNode(newNode, value.IsImmediate());
        }
    }
}

TNode* TStructLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto typeNewNode = (TNode*)Type_->GetCookie();
    bool needClone = false;
    if (typeNewNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            if (Values_[i].GetNode()->GetCookie()) {
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
            allocatedValues[i] = Values_[i];
            auto newNode = (TNode*)Values_[i].GetNode()->GetCookie();
            if (newNode) {
                allocatedValues[i] = TRuntimeNode(newNode, Values_[i].IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TStructLiteral>()) TStructLiteral(allocatedValues,
        typeNewNode ? static_cast<TStructType*>(typeNewNode) : GetType(), false);
}

void TStructLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        Values_[i].Freeze();
    }
}

bool TStructLiteral::Equals(const TStructLiteral& nodeToCompare) const {
    if (GetValuesCount() != nodeToCompare.GetValuesCount())
        return false;

    for (size_t i = 0; i < GetValuesCount(); ++i) {
        if (Values_[i] != nodeToCompare.Values_[i])
            return false;
    }

    return true;
}

TListType::TListType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::List, env.GetTypeOfTypeLazy(), itemType->IsPresortSupported())
    , Data_(itemType)
    , IndexDictKey_(env.GetUi64Lazy())
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
    return CombineHashes(IndexDictKey_->CalcHash(), Data_->CalcHash());
}

bool TListType::IsConvertableTo(const TListType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TListType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data_ = static_cast<TType*>(newNode);
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
    , Items_(items)
    , Count_(count)
{
    for (ui32 i = 0; i < count; ++i) {
        Y_DEBUG_ABORT_UNLESS(items[i].GetNode());
    }

    if (!validate) {
        TListLiteral::DoFreeze(env);
        return;
    }

    for (ui32 i = 0; i < Count_; ++i) {
        auto& item = Items_[i];
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
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < Count_; ++i) {
        auto& item = Items_[i];
        auto itemIt = links.find(item.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(item.GetNode()->Equals(*newNode));
            item = TRuntimeNode(newNode, item.IsImmediate());
        }
    }
}

TNode* TListLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type_->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < Count_; ++i) {
            if (Items_[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TListLiteral*>(this);

    TVector<TRuntimeNode> newList;
    newList.reserve(Count_);
    for (ui32 i = 0; i < Count_; ++i) {
        auto newNode = (TNode*)Items_[i].GetNode()->GetCookie();
        if (newNode) {
            newList.push_back(TRuntimeNode(newNode, Items_[i].IsImmediate()));
        } else {
            newList.push_back(Items_[i]);
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
    for (ui32 i = 0; i < Count_; ++i) {
        auto& node = Items_[i];
        node.Freeze();
        if (node.HasValue() && node.GetValue()->GetType()->IsVoid()) {
            ++voidCount;
        }
    }

    if (!voidCount)
        return;

    TRuntimeNode* newItems = Items_;
    for (ui32 i = 0; i < Count_; ++i) {
        auto node = Items_[i];
        if (node.HasValue() && node.GetValue()->GetType()->IsVoid()) {
            continue;
        }

        *newItems++ = node;
    }

    Count_ = newItems - Items_;
}

bool TListLiteral::Equals(const TListLiteral& nodeToCompare) const {
    if (Count_ != nodeToCompare.Count_)
        return false;

    for (ui32 i = 0; i < Count_; ++i) {
        if (Items_[i] != nodeToCompare.Items_[i])
            return false;
    }

    return true;
}

TStreamType::TStreamType(TType* itemType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Stream, env.GetTypeOfTypeLazy(), false)
    , Data_(itemType)
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
    return Data_->CalcHash();
}

bool TStreamType::IsConvertableTo(const TStreamType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TStreamType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data_ = static_cast<TType*>(newNode);
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
    , Data_(itemType)
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
    return Data_->CalcHash();
}

bool TFlowType::IsConvertableTo(const TFlowType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TFlowType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data_ = static_cast<TType*>(newNode);
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
    , Data_(itemType)
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
    return Data_->CalcHash();
}

bool TOptionalType::IsConvertableTo(const TOptionalType& typeToCompare, bool ignoreTagged) const {
    return GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TOptionalType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetItemType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetItemType()->Equals(*newNode));
        Data_ = static_cast<TType*>(newNode);
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
    , BaseType_(baseType)
    , Tag_(tag)
{
}

TTaggedType* TTaggedType::Create(TType* baseType, const TStringBuf& tag, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TTaggedType>()) TTaggedType(baseType, env.InternName(tag), env);
}

bool TTaggedType::IsSameType(const TTaggedType& typeToCompare) const {
    return Tag_ == typeToCompare.Tag_ && GetBaseType()->IsSameType(*typeToCompare.GetBaseType());
}

size_t TTaggedType::CalcHash() const {
    return CombineHashes(BaseType_->CalcHash(), Tag_.Hash());
}

bool TTaggedType::IsConvertableTo(const TTaggedType& typeToCompare, bool ignoreTagged) const {
    return Tag_ == typeToCompare.Tag_ && GetBaseType()->IsConvertableTo(*typeToCompare.GetBaseType(), ignoreTagged);
}

void TTaggedType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto itemTypeIt = links.find(GetBaseType());
    if (itemTypeIt != links.end()) {
        TNode* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(GetBaseType()->Equals(*newNode));
        BaseType_ = static_cast<TType*>(newNode);
    }
}

TNode* TTaggedType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)GetBaseType()->GetCookie();
    if (!newTypeNode)
        return const_cast<TTaggedType*>(this);

    return ::new(env.Allocate<TTaggedType>()) TTaggedType(static_cast<TType*>(newTypeNode), Tag_, env);
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
    , Item_(item)
{
    if (!validate) {
        Item_.Freeze();
        return;
    }

    Y_DEBUG_ABORT_UNLESS(Item_.GetNode());

    MKQL_ENSURE(Item_.GetStaticType()->IsSameType(*type->GetItemType()), "Wrong type of item");

    Item_.Freeze();
}

TOptionalLiteral* TOptionalLiteral::Create(TOptionalType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(type);
}

TOptionalLiteral* TOptionalLiteral::Create(TRuntimeNode item, TOptionalType* type, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(item, type);
}

void TOptionalLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    if (Item_.GetNode()) {
        auto itemIt = links.find(Item_.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(Item_.GetNode()->Equals(*newNode));
            Item_ = TRuntimeNode(newNode, Item_.IsImmediate());
        }
    }
}

TNode* TOptionalLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type_->GetCookie();
    auto newItemNode = Item_.GetNode() ? (TNode*)Item_.GetNode()->GetCookie() : nullptr;
    if (!newTypeNode && !newItemNode) {
        return const_cast<TOptionalLiteral*>(this);
    }

    if (!Item_.GetNode()) {
        return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(
            newTypeNode ? static_cast<TOptionalType*>(newTypeNode) : GetType(), false);
    } else {
        return ::new(env.Allocate<TOptionalLiteral>()) TOptionalLiteral(
            newItemNode ? TRuntimeNode(newItemNode, Item_.IsImmediate()) : Item_,
            newTypeNode ? static_cast<TOptionalType*>(newTypeNode) : GetType(), false);
    }
}

void TOptionalLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    if (Item_.GetNode()) {
        Item_.Freeze();
    }
}

bool TOptionalLiteral::Equals(const TOptionalLiteral& nodeToCompare) const {
    if (!Item_.GetNode() != !nodeToCompare.Item_.GetNode())
        return false;

    return !Item_.GetNode() || Item_.GetNode()->Equals(*nodeToCompare.Item_.GetNode());
}

TDictType* TDictType::Create(TType* keyType, TType* payloadType, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TDictType>()) TDictType(keyType, payloadType, env);
}

bool TDictType::IsSameType(const TDictType& typeToCompare) const {
    return KeyType_->IsSameType(*typeToCompare.KeyType_)
        && PayloadType_->IsSameType(*typeToCompare.PayloadType_);
}

size_t TDictType::CalcHash() const {
    return CombineHashes(KeyType_->CalcHash(), PayloadType_->CalcHash());
}

bool TDictType::IsConvertableTo(const TDictType& typeToCompare, bool ignoreTagged) const {
    return KeyType_->IsConvertableTo(*typeToCompare.KeyType_, ignoreTagged)
        && PayloadType_->IsConvertableTo(*typeToCompare.PayloadType_, ignoreTagged);
}

void TDictType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto keyTypeIt = links.find(KeyType_);
    if (keyTypeIt != links.end()) {
        TNode* newNode = keyTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(KeyType_->Equals(*newNode));
        KeyType_ = static_cast<TType*>(newNode);
    }

    auto payloadTypeIt = links.find(PayloadType_);
    if (payloadTypeIt != links.end()) {
        TNode* newNode = payloadTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(PayloadType_->Equals(*newNode));
        PayloadType_ = static_cast<TType*>(newNode);
    }
}

TNode* TDictType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newKeyType = (TNode*)KeyType_->GetCookie();
    auto newPayloadType = (TNode*)PayloadType_->GetCookie();
    if (!newKeyType && !newPayloadType) {
        return const_cast<TDictType*>(this);
    }

    return ::new(env.Allocate<TDictType>()) TDictType(
        newKeyType ? static_cast<TType*>(newKeyType) : KeyType_,
        newPayloadType ? static_cast<TType*>(newPayloadType) : PayloadType_, env, false);
}

void TDictType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}


TDictType::TDictType(TType* keyType, TType* payloadType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Dict, env.GetTypeOfTypeLazy(), keyType->IsPresortSupported() && payloadType->IsPresortSupported())
    , KeyType_(keyType)
    , PayloadType_(payloadType)
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
    , ItemsCount_(itemsCount)
    , Items_(items)
{
    if (!validate) {
        for (size_t index = 0; index < itemsCount; ++index) {
            auto& item = Items_[index];
            item.first.Freeze();
            item.second.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < itemsCount; ++index) {
        auto& item = Items_[index];
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
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < ItemsCount_; ++i) {
        auto& item = Items_[i];
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
    auto newTypeNode = (TNode*)Type_->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < ItemsCount_; ++i) {
            if (Items_[i].first.GetNode()->GetCookie()) {
                needClone = true;
                break;
            }

            if (Items_[i].second.GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TDictLiteral*>(this);

    std::pair<TRuntimeNode, TRuntimeNode>* allocatedItems = nullptr;
    if (ItemsCount_) {
        allocatedItems = static_cast<std::pair<TRuntimeNode, TRuntimeNode>*>(env.AllocateBuffer(ItemsCount_ * sizeof(*allocatedItems)));
        for (ui32 i = 0; i < ItemsCount_; ++i) {
            allocatedItems[i] = Items_[i];
            auto newKeyNode = (TNode*)Items_[i].first.GetNode()->GetCookie();
            if (newKeyNode) {
                allocatedItems[i].first = TRuntimeNode(newKeyNode, Items_[i].first.IsImmediate());
            }

            auto newPayloadNode = (TNode*)Items_[i].second.GetNode()->GetCookie();
            if (newPayloadNode) {
                allocatedItems[i].second = TRuntimeNode(newPayloadNode, Items_[i].second.IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TDictLiteral>()) TDictLiteral(ItemsCount_, allocatedItems,
        newTypeNode ? static_cast<TDictType*>(newTypeNode) : GetType(), false);
}

void TDictLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < ItemsCount_; ++i) {
        Items_[i].first.Freeze();
        Items_[i].second.Freeze();
    }
}

bool TDictLiteral::Equals(const TDictLiteral& nodeToCompare) const {
    if (ItemsCount_ != nodeToCompare.ItemsCount_)
        return false;

    for (size_t i = 0; i < ItemsCount_; ++i) {
        if (Items_[i] != nodeToCompare.Items_[i])
            return false;
    }

    return true;
}

TCallableType::TCallableType(const TInternName &name, TType* returnType, ui32 argumentsCount,
        TType **arguments, TNode* payload, const TTypeEnvironment& env)
    : TType(EKind::Callable, env.GetTypeOfTypeLazy(), false)
    , IsMergeDisabled0_(false)
    , ArgumentsCount_(argumentsCount)
    , Name_(name)
    , ReturnType_(returnType)
    , Arguments_(arguments)
    , Payload_(payload)
    , OptionalArgs_(0)
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

    if (Name_ != typeToCompare.Name_ || IsMergeDisabled0_ != typeToCompare.IsMergeDisabled0_)
        return false;

    if (ArgumentsCount_ != typeToCompare.ArgumentsCount_)
        return false;

    if (OptionalArgs_ != typeToCompare.OptionalArgs_)
        return false;

    for (size_t index = 0; index < ArgumentsCount_; ++index) {
        const auto arg = Arguments_[index];
        const auto otherArg = typeToCompare.Arguments_[index];
        if (!arg->IsSameType(*otherArg))
            return false;
    }

    if (!ReturnType_->IsSameType(*typeToCompare.ReturnType_))
        return false;

    if (!Payload_ != !typeToCompare.Payload_)
        return false;

    return !Payload_ || Payload_->Equals(*typeToCompare.Payload_);
}

size_t TCallableType::CalcHash() const {
    size_t hash = 0;
    hash = CombineHashes(hash, IntHash<size_t>(ArgumentsCount_));
    hash = CombineHashes(hash, Name_.Hash());
    hash = CombineHashes(hash, ReturnType_->CalcHash());
    for (size_t index = 0; index < ArgumentsCount_; ++index) {
        hash = CombineHashes(hash, Arguments_[index]->CalcHash());
    }
    hash = CombineHashes(hash, IntHash<size_t>(OptionalArgs_));
    return hash;
}

bool TCallableType::IsConvertableTo(const TCallableType& typeToCompare, bool ignoreTagged) const {
    // do not check callable name here

    if (this == &typeToCompare)
        return true;

    if (IsMergeDisabled0_ != typeToCompare.IsMergeDisabled0_)
        return false;

    if (ArgumentsCount_ < typeToCompare.ArgumentsCount_)
        return false;

    // function with fewer optional args can't be converted to function
    // with more optional args
    if (ArgumentsCount_ - OptionalArgs_ > typeToCompare.ArgumentsCount_ - typeToCompare.OptionalArgs_)
        return false;

    for (size_t index = 0; index < typeToCompare.ArgumentsCount_; ++index) {
        const auto arg = Arguments_[index];
        const auto otherArg = typeToCompare.Arguments_[index];
        if (!arg->IsConvertableTo(*otherArg, ignoreTagged))
            return false;
    }

    if (!ReturnType_->IsConvertableTo(*typeToCompare.ReturnType_, ignoreTagged))
        return false;

    if (!Payload_) {
        return true;
    }

    if (!typeToCompare.Payload_) {
        return false;
    }

    TCallablePayload parsedPayload(Payload_), parsedPayloadToCompare(typeToCompare.Payload_);
    for (size_t index = 0; index < typeToCompare.ArgumentsCount_; ++index) {
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
    MKQL_ENSURE(count <= ArgumentsCount_, "Wrong optional arguments count: " << count << ", function has only " <<
        ArgumentsCount_ << " arguments");
    OptionalArgs_ = count;
    for (ui32 index = ArgumentsCount_ - OptionalArgs_; index < ArgumentsCount_; ++index) {
        MKQL_ENSURE(Arguments_[index]->IsOptional(), "Optional argument #" << (index + 1) << " must be an optional");
    }
}

void TCallableType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto returnTypeIt = links.find(ReturnType_);
    if (returnTypeIt != links.end()) {
        TNode* newNode = returnTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(ReturnType_->Equals(*newNode));
        ReturnType_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < ArgumentsCount_; ++i) {
        auto& arg = Arguments_[i];
        auto argIt = links.find(arg);
        if (argIt != links.end()) {
            TNode* newNode = argIt->second;
            Y_DEBUG_ABORT_UNLESS(arg->Equals(*newNode));
            arg = static_cast<TType*>(newNode);
        }
    }

    if (Payload_) {
        auto payloadIt = links.find(Payload_);
        if (payloadIt != links.end()) {
            TNode* newNode = payloadIt->second;
            Y_DEBUG_ABORT_UNLESS(Payload_->Equals(*newNode));
            Payload_ = newNode;
        }
    }
}

TNode* TCallableType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newReturnTypeNode = (TNode*)ReturnType_->GetCookie();
    auto newPayloadNode = Payload_ ? (TNode*)Payload_->GetCookie() : nullptr;
    bool needClone = false;
    if (newReturnTypeNode || newPayloadNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < ArgumentsCount_; ++i) {
            if (Arguments_[i]->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TCallableType*>(this);

    TType** allocatedArguments = nullptr;
    if (ArgumentsCount_) {
        allocatedArguments = static_cast<TType**>(env.AllocateBuffer(ArgumentsCount_ * sizeof(*allocatedArguments)));
        for (ui32 i = 0; i < ArgumentsCount_; ++i) {
            allocatedArguments[i] = Arguments_[i];
            auto newArgNode = (TNode*)Arguments_[i]->GetCookie();
            if (newArgNode) {
                allocatedArguments[i] = static_cast<TType*>(newArgNode);
            }
        }
    }

    return ::new(env.Allocate<TCallableType>()) TCallableType(Name_,
        newReturnTypeNode ? static_cast<TType*>(newReturnTypeNode) : ReturnType_,
        ArgumentsCount_, allocatedArguments, newPayloadNode ? newPayloadNode : Payload_, env);
}

void TCallableType::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

TCallable::TCallable(ui32 inputsCount, TRuntimeNode* inputs, TCallableType* type, bool validate)
    : TNode(type)
    , InputsCount_(inputsCount)
    , UniqueId_(0)
    , Inputs_(inputs)
{
    if (!validate) {
        for (size_t index = 0; index < inputsCount; ++index) {
            auto& node = Inputs_[index];
            node.Freeze();
        }

        return;
    }

    MKQL_ENSURE(inputsCount == type->GetArgumentsCount(), "Wrong count of inputs");

    for (size_t index = 0; index < inputsCount; ++index) {
        auto& node = Inputs_[index];
        const auto argType = type->GetArgumentType(index);
        MKQL_ENSURE(node.GetStaticType()->IsSameType(*argType), "Wrong type of input");

        node.Freeze();
    }
}

TCallable::TCallable(TRuntimeNode result, TCallableType* type, bool validate)
    : TNode(type)
    , InputsCount_(0)
    , UniqueId_(0)
    , Inputs_(nullptr)
    , Result_(result)
{
    if (!validate) {
        Result_.Freeze();
        return;
    }

    MKQL_ENSURE(result.GetStaticType()->IsSameType(*type->GetReturnType()), "incorrect result type for callable: "
        << GetType()->GetName());
    Result_.Freeze();
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
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < InputsCount_; ++i) {
        auto& input = Inputs_[i];
        auto inputIt = links.find(input.GetNode());
        if (inputIt != links.end()) {
            TNode* newNode = inputIt->second;
            Y_DEBUG_ABORT_UNLESS(input.GetNode()->Equals(*newNode));
            input = TRuntimeNode(newNode, input.IsImmediate());
        }
    }

    if (Result_.GetNode()) {
        auto resultIt = links.find(Result_.GetNode());
        if (resultIt != links.end()) {
            TNode* newNode = resultIt->second;
            Y_DEBUG_ABORT_UNLESS(Result_.GetNode()->Equals(*newNode));
            Result_ = TRuntimeNode(newNode, Result_.IsImmediate());
        }
    }
}

TNode* TCallable::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type_->GetCookie();
    auto newResultNode = Result_.GetNode() ? (TNode*)Result_.GetNode()->GetCookie() : nullptr;
    bool needClone = false;
    if (newTypeNode || newResultNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < InputsCount_; ++i) {
            if (Inputs_[i].GetNode()->GetCookie()) {
                needClone = true;
                break;
            }
        }
    }

    if (!needClone)
        return const_cast<TCallable*>(this);

    TRuntimeNode* allocatedInputs = nullptr;
    if (!Result_.GetNode()) {
        if (InputsCount_) {
            allocatedInputs = static_cast<TRuntimeNode*>(env.AllocateBuffer(InputsCount_ * sizeof(*allocatedInputs)));
            for (ui32 i = 0; i < InputsCount_; ++i) {
                allocatedInputs[i] = Inputs_[i];
                auto newNode = (TNode*)Inputs_[i].GetNode()->GetCookie();
                if (newNode) {
                    allocatedInputs[i] = TRuntimeNode(newNode, Inputs_[i].IsImmediate());
                }
            }
        }
    }

    TCallable* newCallable;
    if (Result_.GetNode()) {
        newCallable = ::new(env.Allocate<TCallable>()) TCallable(
            newResultNode ? TRuntimeNode(newResultNode, Result_.IsImmediate()) : Result_,
            newTypeNode ? static_cast<TCallableType*>(newTypeNode) : GetType(), false);
    } else {
        newCallable = ::new(env.Allocate<TCallable>()) TCallable(InputsCount_, allocatedInputs,
            newTypeNode ? static_cast<TCallableType*>(newTypeNode) : GetType(), false);
    }

    newCallable->SetUniqueId(GetUniqueId());
    return newCallable;
}

void TCallable::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < InputsCount_; ++i) {
        Inputs_[i].Freeze();
    }
}

bool TCallable::Equals(const TCallable& nodeToCompare) const {
    if (GetType()->IsMergeDisabled() && (this != &nodeToCompare))
        return false;

    if (InputsCount_ != nodeToCompare.InputsCount_)
        return false;

    if (!Result_.GetNode() != !nodeToCompare.Result_.GetNode())
        return false;

    for (size_t i = 0; i < InputsCount_; ++i) {
        if (Inputs_[i] != nodeToCompare.Inputs_[i])
            return false;
    }

    if (Result_.GetNode() && Result_ != nodeToCompare.Result_)
        return false;

    return true;
}

void TCallable::SetResult(TRuntimeNode result, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(!Result_.GetNode(), "result is already set");

    MKQL_ENSURE(result.GetStaticType()->IsSameType(*GetType()->GetReturnType()),
                "incorrect result type of function " << GetType()->GetName()
                << ", left: " << PrintNode(result.GetStaticType(), true)
                << ", right: " << PrintNode(GetType()->GetReturnType(), true));

    Result_ = result;
    InputsCount_ = 0;
    Inputs_ = nullptr;
    Result_.Freeze();
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
    if (Item_.GetNode()) {
        auto itemIt = links.find(Item_.GetNode());
        if (itemIt != links.end()) {
            TNode* newNode = itemIt->second;
            Y_DEBUG_ABORT_UNLESS(Item_.GetNode()->Equals(*newNode));
            Item_ = TRuntimeNode(newNode, Item_.IsImmediate());
        }
    }
}

TNode* TAny::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    if (!Item_.GetNode())
        return const_cast<TAny*>(this);

    auto newItemNode = (TNode*)Item_.GetNode()->GetCookie();
    if (!newItemNode) {
        return const_cast<TAny*>(this);
    }

    auto any = TAny::Create(env);
    any->SetItem(TRuntimeNode(newItemNode, Item_.IsImmediate()));
    return any;
}

void TAny::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    if (Item_.GetNode()) {
        Item_.Freeze();
    }
}

void TAny::SetItem(TRuntimeNode newItem) {
    MKQL_ENSURE(!Item_.GetNode(), "item is already set");

    Item_ = newItem;
    Item_.Freeze();
}

bool TAny::Equals(const TAny& nodeToCompare) const {
    if (!Item_.GetNode() || !nodeToCompare.Item_.GetNode())
        return false;

    if (!Item_.IsImmediate() != !nodeToCompare.Item_.IsImmediate())
        return false;

    return Item_.GetNode()->Equals(*nodeToCompare.Item_.GetNode());
}

TTupleLiteral::TTupleLiteral(TRuntimeNode* values, TTupleType* type, bool validate)
    : TNode(type)
    , Values_(values)
{
    if (!validate) {
        for (size_t index = 0; index < GetValuesCount(); ++index) {
            auto& value = Values_[index];
            value.Freeze();
        }

        return;
    }

    for (size_t index = 0; index < GetValuesCount(); ++index) {
        auto& value = Values_[index];
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
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        auto& value = Values_[i];
        auto valueIt = links.find(value.GetNode());
        if (valueIt != links.end()) {
            TNode* newNode = valueIt->second;
            Y_DEBUG_ABORT_UNLESS(value.GetNode()->Equals(*newNode));
            value = TRuntimeNode(newNode, value.IsImmediate());
        }
    }
}

TNode* TTupleLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type_->GetCookie();
    bool needClone = false;
    if (newTypeNode) {
        needClone = true;
    } else {
        for (ui32 i = 0; i < GetValuesCount(); ++i) {
            if (Values_[i].GetNode()->GetCookie()) {
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
            allocatedValues[i] = Values_[i];
            auto newNode = (TNode*)Values_[i].GetNode()->GetCookie();
            if (newNode) {
                allocatedValues[i] = TRuntimeNode(newNode, Values_[i].IsImmediate());
            }
        }
    }

    return ::new(env.Allocate<TTupleLiteral>()) TTupleLiteral(allocatedValues,
        newTypeNode ? static_cast<TTupleType*>(newTypeNode) : GetType(), false);
}

void TTupleLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    for (ui32 i = 0; i < GetValuesCount(); ++i) {
        Values_[i].Freeze();
    }
}

bool TTupleLiteral::Equals(const TTupleLiteral& nodeToCompare) const {
    if (GetValuesCount() != nodeToCompare.GetValuesCount())
        return false;

    for (size_t i = 0; i < GetValuesCount(); ++i) {
        if (Values_[i] != nodeToCompare.Values_[i])
            return false;
    }

    return true;
}

bool TResourceType::IsSameType(const TResourceType& typeToCompare) const {
    return Tag_ == typeToCompare.Tag_;
}

size_t TResourceType::CalcHash() const {
    return Tag_.Hash();
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
    return Data_->CalcHash();
}

bool TVariantType::IsConvertableTo(const TVariantType& typeToCompare, bool ignoreTagged) const {
    return GetUnderlyingType()->IsConvertableTo(*typeToCompare.GetUnderlyingType(), ignoreTagged);
}

TVariantType::TVariantType(TType* underlyingType, const TTypeEnvironment& env, bool validate)
    : TType(EKind::Variant, env.GetTypeOfTypeLazy(), underlyingType->IsPresortSupported())
    , Data_(underlyingType)
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
        Data_ = static_cast<TType*>(newNode);
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
    , Item_(item)
    , Index_(index)
{
    Item_.Freeze();
    if (validate) {
        MKQL_ENSURE(index < type->GetAlternativesCount(),
            "Index out of range: " << index << " alt. count: " << type->GetAlternativesCount());

        auto underlyingType = type->GetUnderlyingType();
        if (underlyingType->IsTuple()) {
            auto expectedType = AS_TYPE(TTupleType, underlyingType)->GetElementType(index);
            MKQL_ENSURE(Item_.GetStaticType()->IsSameType(*expectedType), "Wrong type of item");
        } else {
            auto expectedType = AS_TYPE(TStructType, underlyingType)->GetMemberType(index);
            MKQL_ENSURE(Item_.GetStaticType()->IsSameType(*expectedType), "Wrong type of item");
        }
    }
}

bool TVariantLiteral::Equals(const TVariantLiteral& nodeToCompare) const {
    if (Index_ != nodeToCompare.GetIndex()) {
        return false;
    }

    return Item_.GetNode()->Equals(*nodeToCompare.GetItem().GetNode());
}

void TVariantLiteral::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    auto typeIt = links.find(Type_);
    if (typeIt != links.end()) {
        TNode* newNode = typeIt->second;
        Y_DEBUG_ABORT_UNLESS(Type_->Equals(*newNode));
        Type_ = static_cast<TType*>(newNode);
    }

    auto itemIt = links.find(Item_.GetNode());
    if (itemIt != links.end()) {
        TNode* newNode = itemIt->second;
        Y_DEBUG_ABORT_UNLESS(Item_.GetNode()->Equals(*newNode));
        Item_ = TRuntimeNode(newNode, Item_.IsImmediate());
    }
}

TNode* TVariantLiteral::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)Type_->GetCookie();
    auto newItemNode = (TNode*)Item_.GetNode()->GetCookie();
    if (!newTypeNode && !newItemNode) {
        return const_cast<TVariantLiteral*>(this);
    }

    return ::new(env.Allocate<TVariantLiteral>()) TVariantLiteral(
        newItemNode ? TRuntimeNode(newItemNode, Item_.IsImmediate()) : Item_, Index_,
        newTypeNode ? static_cast<TVariantType*>(newTypeNode) : GetType(), false);
}

void TVariantLiteral::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
    Item_.Freeze();
}

TBlockType::TBlockType(TType* itemType, EShape shape, const TTypeEnvironment& env)
    : TType(EKind::Block, env.GetTypeOfTypeLazy(), false)
    , ItemType_(itemType)
    , Shape_(shape)
{
}

TBlockType* TBlockType::Create(TType* itemType, EShape shape, const TTypeEnvironment& env) {
    return ::new(env.Allocate<TBlockType>()) TBlockType(itemType, shape, env);
}

bool TBlockType::IsSameType(const TBlockType& typeToCompare) const {
    return GetItemType()->IsSameType(*typeToCompare.GetItemType()) && Shape_ == typeToCompare.Shape_;
}

size_t TBlockType::CalcHash() const {
    return CombineHashes(ItemType_->CalcHash(), IntHash((size_t)Shape_));
}

bool TBlockType::IsConvertableTo(const TBlockType& typeToCompare, bool ignoreTagged) const {
    return Shape_ == typeToCompare.Shape_ &&
        GetItemType()->IsConvertableTo(*typeToCompare.GetItemType(), ignoreTagged);
}

void TBlockType::DoUpdateLinks(const THashMap<TNode*, TNode*>& links) {
    const auto itemTypeIt = links.find(ItemType_);
    if (itemTypeIt != links.end()) {
        auto* newNode = itemTypeIt->second;
        Y_DEBUG_ABORT_UNLESS(ItemType_->Equals(*newNode));
        ItemType_ = static_cast<TType*>(newNode);
    }
}

TNode* TBlockType::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    auto newTypeNode = (TNode*)ItemType_->GetCookie();
    if (!newTypeNode) {
        return const_cast<TBlockType*>(this);
    }

    return ::new(env.Allocate<TBlockType>()) TBlockType(static_cast<TType*>(newTypeNode), Shape_, env);
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
    return AS_TYPE(TMultiType, type->GetItemType())->GetElements();
}

TArrayRef<TType *const> GetWideComponents(const TType* type) {
    if (type->IsFlow()) {
        const auto outputFlowType = AS_TYPE(TFlowType, type);
        return GetWideComponents(outputFlowType);
    }
    if (type->IsStream()) {
        const auto outputStreamType = AS_TYPE(TStreamType, type);
        return GetWideComponents(outputStreamType);
    }
    MKQL_ENSURE(false, "Expect either flow or stream");
}

}
}
