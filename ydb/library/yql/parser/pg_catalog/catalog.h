#pragma once
#include <ydb/library/yql/public/issue/yql_issue.h>
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
constexpr ui32 AnyNonArrayOid = 2776;
constexpr ui32 AnyElementOid = 2283;
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
    TString Descr;
    EOperKind Kind = EOperKind::Binary;
    ui32 LeftType = 0;
    ui32 RightType = 0;
    ui32 ResultType = 0;
    ui32 ProcId = 0;
    ui32 ComId = 0;
    ui32 NegateId = 0;
};

enum class EProcKind : char {
    Function = 'f',
    Aggregate = 'a',
    Window = 'w'
};

constexpr ui32 LangInternal = 12;
constexpr ui32 LangC = 13;
constexpr ui32 LangSQL = 14;

struct TProcDesc {
    ui32 ProcId = 0;
    TString Name;
    TString Src;
    TString Descr;
    TVector<ui32> ArgTypes;
    TVector<TString> InputArgNames;
    ui32 ResultType = 0;
    bool IsStrict = true;
    EProcKind Kind = EProcKind::Function;
    bool ReturnSet = false;
    TVector<TString> OutputArgNames;
    TVector<ui32> OutputArgTypes;
    ui32 Lang = LangInternal;
    ui32 VariadicType = 0;
    ui32 VariadicArgType = 0;
    TString VariadicArgName;
    TVector<TMaybe<TString>> DefaultArgs;
    ui32 ExtensionIndex = 0;
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
    TString Descr;
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

    ui32 ExtensionIndex = 0;
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

enum class EAggKind : char {
    Normal = 'n',
    OrderedSet = 'o',
    Hypothetical = 'h'
};

struct TAggregateDesc {
    ui32 AggId = 0;
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
    bool FinalExtra = false;
};

enum class EAmType {
    Table = 't',
    Index = 'i'
};

struct TAmDesc {
    ui32 Oid = 0;
    TString Descr;
    TString AmName;
    EAmType AmType = EAmType::Index;
};

struct TNamespaceDesc {
    ui32 Oid = 0;
    TString Name;
    TString Descr;
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
    TString Descr;
    ui32 ProcId = 0;
};

struct TLanguageDesc {
    ui32 LangId = 0;
    TString Name;
    TString Descr;
};

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId);
std::variant<const TProcDesc*, const TTypeDesc*> LookupProcWithCasts(const TString& name, const TVector<ui32>& argTypeIds);
bool HasReturnSetProc(const TString& name);
void EnumProc(std::function<void(ui32, const TProcDesc&)> f);

bool HasType(const TString& name);
bool HasType(ui32 typeId);
const TTypeDesc& LookupType(const TString& name);
const TTypeDesc& LookupType(ui32 typeId);
TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc);
TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc, bool& castsNeeded);
void EnumTypes(std::function<void(ui32, const TTypeDesc&)> f);

const TAmDesc& LookupAm(ui32 oid);
void EnumAm(std::function<void(ui32, const TAmDesc&)> f);

void EnumConversions(std::function<void(const TConversionDesc&)> f);

const TNamespaceDesc& LookupNamespace(ui32 oid);
void EnumNamespace(std::function<void(ui32, const TNamespaceDesc&)> f);

void EnumOperators(std::function<void(const TOperDesc&)> f);

bool HasCast(ui32 sourceId, ui32 targetId);
const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId);

const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId);

bool HasAggregation(const TString& name, EAggKind kind);
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

const TLanguageDesc& LookupLanguage(ui32 langId);
void EnumLanguages(std::function<void(ui32, const TLanguageDesc&)> f);

bool IsCompatibleTo(ui32 actualType, ui32 expectedType);
bool IsCoercible(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType);

inline bool IsArrayType(const TTypeDesc& typeDesc) noexcept {
    return typeDesc.ArrayTypeId == typeDesc.TypeId;
}

enum class ERelKind : char {
    Relation = 'r',
    View = 'v'
};
struct TTableInfoKey {
    TString Schema;
    TString Name;

    bool operator==(const TTableInfoKey& other) const {
        return Schema == other.Schema && Name == other.Name;
    }

    size_t Hash() const {
        auto stringHasher = THash<TString>();
        return CombineHashes(stringHasher(Schema), stringHasher(Name));
    }
};

constexpr ui32 TypeRelationOid = 1247;
constexpr ui32 DatabaseRelationOid = 1262;
constexpr ui32 TableSpaceRelationOid = 1213;
constexpr ui32 SharedDescriptionRelationOid = 2396;
constexpr ui32 TriggerRelationOid = 2620;
constexpr ui32 InheritsRelationOid = 2611;
constexpr ui32 DescriptionRelationOid = 2609;
constexpr ui32 AccessMethodRelationOid = 2601;
constexpr ui32 NamespaceRelationOid = 2615;
constexpr ui32 AuthMemRelationOid = 1261;
constexpr ui32 RelationRelationOid = 1259;

struct TTableInfo : public TTableInfoKey {
    ERelKind Kind;
    ui32 Oid;
    ui32 ExtensionIndex = 0;
};

struct TColumnInfo {
    TString Schema;
    TString TableName;
    TString Name;
    TString UdtType;
    ui32 ExtensionIndex = 0;
};

const TVector<TTableInfo>& GetStaticTables();
const TTableInfo& LookupStaticTable(const TTableInfoKey& tableKey);
const THashMap<TTableInfoKey, TVector<TColumnInfo>>& GetStaticColumns();
const TVector<TMaybe<TString>>* ReadTable(
    const TTableInfoKey& tableKey,
    const TVector<TString>& columnNames,
    size_t* columnsRemap, // should have the same length as columnNames
    size_t& rowStep);

bool AreAllFunctionsAllowed();

struct TExtensionDesc {
    TString Name;               // postgis
    TString InstallName;        // $libdir/postgis-3
    TVector<TString> SqlPaths;  // paths to SQL files with DDL (CREATE TYPE/CREATE FUNCTION/etc), DML (INSERT/VALUES)
    TString LibraryPath;        // file path
    bool TypesOnly = false;     // Can't be loaded if true
};

class IExtensionSqlBuilder {
public:
    virtual ~IExtensionSqlBuilder() = default;

    virtual void CreateProc(const TProcDesc& desc) = 0;

    virtual void PrepareType(ui32 extensionIndex,const TString& name) = 0;

    virtual void UpdateType(const TTypeDesc& desc) = 0;

    virtual void CreateTable(const TTableInfo& table, const TVector<TColumnInfo>& columns) = 0;

    virtual void InsertValues(const TTableInfoKey& table, const TVector<TString>& columns,
        const TVector<TMaybe<TString>>& data) = 0; // row based layout
};

class IExtensionSqlParser {
public:
    virtual ~IExtensionSqlParser() = default;
    virtual void Parse(ui32 extensionIndex, const TVector<TString>& sqls, IExtensionSqlBuilder& builder) = 0;
};

class IExtensionLoader {
public:
    virtual ~IExtensionLoader() = default;
    virtual void Load(ui32 extensionIndex, const TString& name, const TString& path) = 0;
};

// should be called at most once before other catalog functions
void RegisterExtensions(const TVector<TExtensionDesc>& extensions, bool typesOnly,
    IExtensionSqlParser& parser, IExtensionLoader* loader);

void EnumExtensions(std::function<void(ui32 extensionIndex, const TExtensionDesc&)> f);
const TExtensionDesc& LookupExtension(ui32 extensionIndex);
ui32 LookupExtensionByName(const TString& name);
ui32 LookupExtensionByInstallName(const TString& installName);

}

template <>
inline void Out<NYql::NPg::ETypType>(IOutputStream& o, NYql::NPg::ETypType typType) {
    o.Write(static_cast<std::underlying_type<NYql::NPg::ETypType>::type>(typType));
}

template <>
inline void Out<NYql::NPg::ECoercionCode>(IOutputStream& o, NYql::NPg::ECoercionCode coercionCode) {
    o.Write(static_cast<std::underlying_type<NYql::NPg::ECoercionCode>::type>(coercionCode));
}

template <>
struct THash<NYql::NPg::TTableInfoKey> {
    size_t operator ()(const NYql::NPg::TTableInfoKey& val) const {
        return val.Hash();
    }
};
