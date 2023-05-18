#pragma once
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NYql::NPg {

// copied from pg_class.h
enum class ERelPersistence : char
{
    Permanent = 'p',
    Unlogged = 'u',
    Temp = 't',
};

enum class EOperKind {
    Binary,
    LeftUnary,
    RightUnary
};

struct TOperDesc {
    ui32 OperId = 0;
    TString Name;
    EOperKind Kind = EOperKind::Binary;
    ui32 LeftType = 0;
    ui32 RightType = 0;
    ui32 ResultType = 0;
    ui32 ProcId = 0;
};

enum EProcKind {
    Function,
    Aggregate,
    Window
};

struct TProcDesc {
    ui32 ProcId = 0;
    TString Name;
    TString Src;
    TVector<ui32> ArgTypes;
    ui32 ResultType = 0;
    bool IsStrict = true;
    EProcKind Kind = EProcKind::Function;
    bool ReturnSet = false;
};

// Copied from pg_collation_d.h
constexpr ui32 InvalidCollationOid = 0;
constexpr ui32 DefaultCollationOid = 100;
constexpr ui32 C_CollationOid = 950;
constexpr ui32 PosixCollationOid = 951;

// Copied from pg_type_d.h, TYPTYPE_* constants
enum class ETypType : char {
    Base = 'b',
    Composite = 'c',
    Domain = 'd',
    Enum = 'e',
    Multirange = 'm',
    Pseudo = 'p',
    Range = 'r',
};

struct TTypeDesc {
    ui32 TypeId = 0;
    ui32 ArrayTypeId = 0;
    TString Name;
    ui32 ElementTypeId = 0;
    bool PassByValue = false;
    char Category = '\0';
    char TypeAlign = '\0';
    char TypeDelim = ',';

    /*
     * Collation: InvalidCollationOid if type cannot use collations, nonzero (typically
     * DefaultCollationOid) for collatable base types, possibly some other
     * OID for domains over collatable types
     */
    ui32 TypeCollation = InvalidCollationOid;

    ui32 InFuncId = 0;
    ui32 OutFuncId = 0;
    ui32 SendFuncId = 0;
    ui32 ReceiveFuncId = 0;
    ui32 TypeModInFuncId = 0;
    ui32 TypeModOutFuncId = 0;
    ui32 TypeSubscriptFuncId = 0;
    i32 TypeLen = 0;
    // from opclass
    ui32 LessProcId = 0;
    ui32 EqualProcId = 0;
    ui32 CompareProcId = 0;
    ui32 HashProcId = 0;

    // If TypType is 'c', typrelid is the OID of the class' entry in pg_class.
    ETypType TypType = ETypType::Base;
};

enum class ECastMethod {
    Function,
    InOut,
    Binary
};

enum class ECoercionCode : char {
    Unknown = '?',      // not specified
    Implicit = 'i',     // coercion in context of expression
    Assignment = 'a',   // coercion in context of assignment
    Explicit = 'e',     // explicit cast operation
};

struct TCastDesc {
    ui32 SourceId = 0;
    ui32 TargetId = 0;
    ECastMethod Method = ECastMethod::Function;
    ui32 FunctionId = 0;
    ECoercionCode CoercionCode = ECoercionCode::Unknown;
};

enum class EAggKind {
    Normal,
    OrderedSet,
    Hypothetical
};

struct TAggregateDesc {
    TString Name;
    TVector<ui32> ArgTypes;
    EAggKind Kind = EAggKind::Normal;
    ui32 TransTypeId = 0;
    ui32 TransFuncId = 0;
    ui32 FinalFuncId = 0;
    ui32 CombineFuncId = 0;
    ui32 SerializeFuncId = 0;
    ui32 DeserializeFuncId = 0;
    TString InitValue;
};

enum class EOpClassMethod {
    Btree,
    Hash
};

struct TOpClassDesc {
    EOpClassMethod Method = EOpClassMethod::Btree;
    ui32 TypeId = 0;
    TString Name;
    TString Family;
    ui32 FamilyId = 0;
};

struct TAmOpDesc {
    TString Family;
    ui32 FamilyId = 0;
    ui32 Strategy = 0;
    ui32 LeftType = 0;
    ui32 RightType = 0;
    ui32 OperId = 0;
};

enum class EBtreeAmStrategy {
    Less = 1,
    LessOrEqual = 2,
    Equal = 3,
    GreaterOrEqual = 4,
    Greater = 5
};

struct TAmProcDesc {
    TString Family;
    ui32 FamilyId = 0;
    ui32 ProcNum = 0;
    ui32 LeftType = 0;
    ui32 RightType = 0;
    ui32 ProcId = 0;
};

enum class EBtreeAmProcNum {
    Compare = 1
};

enum class EHashAmProcNum {
    Hash = 1
};

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId);
bool HasReturnSetProc(const TString& name);
void EnumProc(std::function<void(ui32, const TProcDesc&)> f);

bool HasType(const TString& name);
const TTypeDesc& LookupType(const TString& name);
const TTypeDesc& LookupType(ui32 typeId);
void EnumTypes(std::function<void(ui32, const TTypeDesc&)> f);

bool HasCast(ui32 sourceId, ui32 targetId);
const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId);

const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId);

bool HasAggregation(const TString& name);
const TAggregateDesc& LookupAggregation(const TString& name, const TVector<ui32>& argTypeIds);

bool HasOpClass(EOpClassMethod method, ui32 typeId);
const TOpClassDesc* LookupDefaultOpClass(EOpClassMethod method, ui32 typeId);

bool HasAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType);
const TAmOpDesc& LookupAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType);

bool HasAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType);
const TAmProcDesc& LookupAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType);

bool IsCompatibleTo(ui32 actualType, ui32 expectedType);

inline bool IsArrayType(const TTypeDesc& typeDesc) noexcept {
    return typeDesc.ArrayTypeId == typeDesc.TypeId;
}

}

template <>
inline void Out<NYql::NPg::ETypType>(IOutputStream& o, NYql::NPg::ETypType typType) {
    o.Write(static_cast<std::underlying_type<NYql::NPg::ETypType>::type>(typType));
}

template <>
inline void Out<NYql::NPg::ECoercionCode>(IOutputStream& o, NYql::NPg::ECoercionCode coercionCode) {
    o.Write(static_cast<std::underlying_type<NYql::NPg::ECoercionCode>::type>(coercionCode));
}
