#pragma once
#include "ydb/library/yql/public/issue/yql_issue.h"
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <variant>
#include <functional>

namespace NYql::NPg {

constexpr ui32 UnknownOid = 705;
constexpr ui32 AnyOid = 2276;
constexpr ui32 AnyArrayOid = 2277;
constexpr ui32 RecordOid = 2249;
constexpr ui32 VarcharOid = 1043;
constexpr ui32 TextOid = 25;

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
    TVector<TString> OutputArgNames;
    TVector<ui32> OutputArgTypes;
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

constexpr char InvalidCategory = '\0';

struct TTypeDesc {
    ui32 TypeId = 0;
    ui32 ArrayTypeId = 0;
    TString Name;
    ui32 ElementTypeId = 0;
    bool PassByValue = false;
    char Category = InvalidCategory;
    bool IsPreferred = false;
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
    ui32 InternalId = 0;
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

struct TConversionDesc {
    ui32 ConversionId = 0;
    TString From;
    TString To;
    ui32 ProcId = 0;
};

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId);
std::variant<const TProcDesc*, const TTypeDesc*> LookupProcWithCasts(const TString& name, const TVector<ui32>& argTypeIds);
bool HasReturnSetProc(const TString& name);
void EnumProc(std::function<void(ui32, const TProcDesc&)> f);

bool HasType(const TString& name);
const TTypeDesc& LookupType(const TString& name);
const TTypeDesc& LookupType(ui32 typeId);
TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc);
TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc, bool& castsNeeded);
void EnumTypes(std::function<void(ui32, const TTypeDesc&)> f);

bool HasCast(ui32 sourceId, ui32 targetId);
const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId);

const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId);

bool HasAggregation(const TString& name);
const TAggregateDesc& LookupAggregation(const TString& name, const TVector<ui32>& argTypeIds);
const TAggregateDesc& LookupAggregation(const TString& name, ui32 stateType, ui32 resultType);
void EnumAggregation(std::function<void(ui32, const TAggregateDesc&)> f);

bool HasOpClass(EOpClassMethod method, ui32 typeId);
const TOpClassDesc* LookupDefaultOpClass(EOpClassMethod method, ui32 typeId);

bool HasAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType);
const TAmOpDesc& LookupAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType);

bool HasAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType);
const TAmProcDesc& LookupAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType);

bool HasConversion(const TString& from, const TString& to);
const TConversionDesc& LookupConversion(const TString& from, const TString& to);

bool IsCompatibleTo(ui32 actualType, ui32 expectedType);
bool IsCoercible(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType);

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
