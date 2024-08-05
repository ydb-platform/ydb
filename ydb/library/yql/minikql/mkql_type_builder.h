#pragma once

#include "mkql_node.h"

#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_type_helper.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/compare.h>

#include <util/generic/size_literals.h>

#include <arrow/datum.h>

namespace NKikimr {
namespace NMiniKQL {

class TBlockTypeHelper : public NUdf::IBlockTypeHelper {
public:
    NUdf::IBlockItemComparator::TPtr MakeComparator(NUdf::TType* type) const final;
    NUdf::IBlockItemHasher::TPtr MakeHasher(NUdf::TType* type) const final;
};

constexpr size_t MaxBlockSizeInBytes = 240_KB;
static_assert(MaxBlockSizeInBytes < (size_t)std::numeric_limits<i32>::max());
static_assert(MaxBlockSizeInBytes % 64 == 0, "arrow buffers are allocated with buffer size aligned to next 64 byte boundary");

// maximum size of block item in bytes
size_t CalcMaxBlockItemSize(const TType* type);

inline size_t CalcBlockLen(size_t maxBlockItemSize) {
    return MaxBlockSizeInBytes / std::max<size_t>(maxBlockItemSize, 1);
}

bool ConvertArrowType(TType* itemType, std::shared_ptr<arrow::DataType>& type);
bool ConvertArrowType(NUdf::EDataSlot slot, std::shared_ptr<arrow::DataType>& type);

template<NUdf::EDataSlot slot>
std::shared_ptr<arrow::DataType> MakeTzLayoutArrowType() {
    static_assert(slot == NUdf::EDataSlot::TzDate || slot == NUdf::EDataSlot::TzDatetime || slot == NUdf::EDataSlot::TzTimestamp
        || slot == NUdf::EDataSlot::TzDate32 || slot == NUdf::EDataSlot::TzDatetime64 || slot == NUdf::EDataSlot::TzTimestamp64,
        "Expected tz date type slot");

    if constexpr (slot == NUdf::EDataSlot::TzDate) {
        return arrow::uint16();
    }
    if constexpr (slot == NUdf::EDataSlot::TzDatetime) {
        return arrow::uint32();
    }
    if constexpr (slot == NUdf::EDataSlot::TzTimestamp) {
        return arrow::uint64();
    }
    if constexpr (slot == NUdf::EDataSlot::TzDate32) {
        return arrow::int32();
    }
    if constexpr (slot == NUdf::EDataSlot::TzDatetime64) {
        return arrow::int64();
    }
    if constexpr (slot == NUdf::EDataSlot::TzTimestamp64) {
        return arrow::int64();
    }
}

template<NUdf::EDataSlot slot>
std::shared_ptr<arrow::StructType> MakeTzDateArrowType() {
    std::vector<std::shared_ptr<arrow::Field>> fields {
        std::make_shared<arrow::Field>("datetime", MakeTzLayoutArrowType<slot>(), false),
        std::make_shared<arrow::Field>("timezoneId", arrow::uint16(), false),
    };
    return std::make_shared<arrow::StructType>(fields);
}

class TArrowType : public NUdf::IArrowType {
public:
    TArrowType(const std::shared_ptr<arrow::DataType>& type)
        : Type(type)
    {}

    std::shared_ptr<arrow::DataType> GetType() const {
        return Type;
    }

    void Export(ArrowSchema* out) const final;

private:
    const std::shared_ptr<arrow::DataType> Type;
};

//////////////////////////////////////////////////////////////////////////////
// TFunctionTypeInfo
//////////////////////////////////////////////////////////////////////////////
struct TFunctionTypeInfo
{
    TCallableType* FunctionType = nullptr;
    const TType* RunConfigType = nullptr;
    const TType* UserType = nullptr;
    NUdf::TUniquePtr<NUdf::IBoxedValue> Implementation;
    TString ModuleIR;
    TString ModuleIRUniqID;
    TString IRFunctionName;
    bool Deterministic = true;
    bool SupportsBlocks = false;
    bool IsStrict = false;
};

//////////////////////////////////////////////////////////////////////////////
// TArgInfo
//////////////////////////////////////////////////////////////////////////////
struct TArgInfo {
    NMiniKQL::TType* Type_ = nullptr;
    TInternName Name_;
    ui64 Flags_;
};

//////////////////////////////////////////////////////////////////////////////
// TFunctionTypeInfoBuilder
//////////////////////////////////////////////////////////////////////////////
class TFunctionTypeInfoBuilder: public NUdf::IFunctionTypeInfoBuilder
{
public:
    TFunctionTypeInfoBuilder(
            const TTypeEnvironment& env,
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
            const TStringBuf& moduleName,
            NUdf::ICountersProvider* countersProvider,
            const NUdf::TSourcePosition& pos,
            const NUdf::ISecureParamsProvider* provider = nullptr);

    NUdf::IFunctionTypeInfoBuilder1& ImplementationImpl(
            NUdf::TUniquePtr<NUdf::IBoxedValue> impl) override;

    NUdf::IFunctionTypeInfoBuilder1& ReturnsImpl(NUdf::TDataTypeId typeId) override;
    NUdf::IFunctionTypeInfoBuilder1& ReturnsImpl(const NUdf::TType* type) override;
    NUdf::IFunctionTypeInfoBuilder1& ReturnsImpl(
            const NUdf::ITypeBuilder& typeBuilder) override;

    NUdf::IFunctionArgTypesBuilder::TPtr Args(ui32 expectedItem) override;
    NUdf::IFunctionTypeInfoBuilder1& OptionalArgsImpl(ui32 optionalArgs) override;
    NUdf::IFunctionTypeInfoBuilder1& PayloadImpl(const NUdf::TStringRef& payload) override;

    NUdf::IFunctionTypeInfoBuilder1& RunConfigImpl(NUdf::TDataTypeId typeId) override;
    NUdf::IFunctionTypeInfoBuilder1& RunConfigImpl(const NUdf::TType* type) override;
    NUdf::IFunctionTypeInfoBuilder1& RunConfigImpl(
            const NUdf::ITypeBuilder& typeBuilder) override;

    NUdf::IFunctionTypeInfoBuilder1& UserTypeImpl(NUdf::TDataTypeId typeId) override;
    NUdf::IFunctionTypeInfoBuilder1& UserTypeImpl(const NUdf::TType* type) override;
    NUdf::IFunctionTypeInfoBuilder1& UserTypeImpl(const NUdf::ITypeBuilder& typeBuilder) override;

    void SetError(const NUdf::TStringRef& error) override;
    inline bool HasError() const { return !Error_.empty(); }
    inline const TString& GetError() const { return Error_; }

    void Build(TFunctionTypeInfo* funcInfo);

    NUdf::TType* Primitive(NUdf::TDataTypeId typeId) const override;
    NUdf::IOptionalTypeBuilder::TPtr Optional() const override;
    NUdf::IListTypeBuilder::TPtr List() const override;
    NUdf::IDictTypeBuilder::TPtr Dict() const override;
    NUdf::IStructTypeBuilder::TPtr Struct(ui32 expectedItems) const override;
    NUdf::ITupleTypeBuilder::TPtr Tuple(ui32 expectedItems) const override;
    NUdf::ICallableTypeBuilder::TPtr Callable(ui32 expectedArgs) const override;

    NUdf::TType* Void() const override;
    NUdf::TType* Resource(const NUdf::TStringRef& tag) const override;
    NUdf::IVariantTypeBuilder::TPtr Variant() const override;

    NUdf::IStreamTypeBuilder::TPtr Stream() const override;

    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper() const override;

    const TTypeEnvironment& Env() const { return Env_; }

    NUdf::TCounter GetCounter(const NUdf::TStringRef& name, bool deriv) override;
    NUdf::TScopedProbe GetScopedProbe(const NUdf::TStringRef& name) override;
    NUdf::TSourcePosition GetSourcePosition() override;

    NUdf::IHash::TPtr MakeHash(const NUdf::TType* type) override;
    NUdf::IEquate::TPtr MakeEquate(const NUdf::TType* type) override;
    NUdf::ICompare::TPtr MakeCompare(const NUdf::TType* type) override;

    NUdf::TType* Decimal(ui8 precision, ui8 scale) const override;

    NUdf::IFunctionTypeInfoBuilder7& IRImplementationImpl(
        const NUdf::TStringRef& moduleIR,
        const NUdf::TStringRef& moduleIRUniqId,
        const NUdf::TStringRef& functionName
    ) override;

    NUdf::TType* Null() const override;
    NUdf::TType* EmptyList() const override;
    NUdf::TType* EmptyDict() const override;

    void Unused1() override;
    NUdf::ISetTypeBuilder::TPtr Set() const override;
    NUdf::IEnumTypeBuilder::TPtr Enum(ui32 expectedItems = 10) const override;
    NUdf::TType* Tagged(const NUdf::TType* baseType, const NUdf::TStringRef& tag) const override;
    NUdf::TType* Pg(ui32 typeId) const override;
    NUdf::IBlockTypeBuilder::TPtr Block(bool isScalar) const override;
    void Unused2() override;
    void Unused3() override;

    NUdf::IFunctionTypeInfoBuilder15& SupportsBlocksImpl() override;
    NUdf::IFunctionTypeInfoBuilder15& IsStrictImpl() override;
    const NUdf::IBlockTypeHelper& IBlockTypeHelper() const override;

    bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const override;

private:
    const TTypeEnvironment& Env_;
    NUdf::TUniquePtr<NUdf::IBoxedValue> Implementation_;
    const TType* ReturnType_;
    const TType* RunConfigType_;
    const TType* UserType_;
    TVector<TArgInfo> Args_;
    TString Error_;
    ui32 OptionalArgs_ = 0;
    TString Payload_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    TBlockTypeHelper BlockTypeHelper;
    TStringBuf ModuleName_;
    NUdf::ICountersProvider* CountersProvider_;
    NUdf::TSourcePosition Pos_;
    const NUdf::ISecureParamsProvider* SecureParamsProvider_;
    TString ModuleIR_;
    TString ModuleIRUniqID_;
    TString IRFunctionName_;
    bool SupportsBlocks_ = false;
    bool IsStrict_ = false;
};

class TTypeInfoHelper : public NUdf::ITypeInfoHelper
{
public:
    NUdf::ETypeKind GetTypeKind(const NUdf::TType* type) const override;
    void VisitType(const NUdf::TType* type, NUdf::ITypeVisitor* visitor) const override;
    bool IsSameType(const NUdf::TType* type1, const NUdf::TType* type2) const override;
    const NYql::NUdf::TPgTypeDescription* FindPgTypeDescription(ui32 typeId) const override;
    NUdf::IArrowType::TPtr MakeArrowType(const NUdf::TType* type) const override;
    NUdf::IArrowType::TPtr ImportArrowType(ArrowSchema* schema) const override;
    ui64 GetMaxBlockLength(const NUdf::TType* type) const override;
    ui64 GetMaxBlockBytes() const override;

private:
    static void DoData(const NMiniKQL::TDataType* dt, NUdf::ITypeVisitor* v);
    static void DoStruct(const NMiniKQL::TStructType* st, NUdf::ITypeVisitor* v);
    static void DoList(const NMiniKQL::TListType* lt, NUdf::ITypeVisitor* v);
    static void DoOptional(const NMiniKQL::TOptionalType* lt, NUdf::ITypeVisitor* v);
    static void DoTuple(const NMiniKQL::TTupleType* tt, NUdf::ITypeVisitor* v);
    static void DoDict(const NMiniKQL::TDictType* dt, NUdf::ITypeVisitor* v);
    static void DoCallable(const NMiniKQL::TCallableType* ct, NUdf::ITypeVisitor* v);
    static void DoVariant(const NMiniKQL::TVariantType* vt, NUdf::ITypeVisitor* v);
    static void DoStream(const NMiniKQL::TStreamType* st, NUdf::ITypeVisitor* v);
    static void DoResource(const NMiniKQL::TResourceType* rt, NUdf::ITypeVisitor* v);
    static void DoTagged(const NMiniKQL::TTaggedType* tt, NUdf::ITypeVisitor* v);
    static void DoPg(const NMiniKQL::TPgType* tt, NUdf::ITypeVisitor* v);
    static void DoBlock(const NMiniKQL::TBlockType* tt, NUdf::ITypeVisitor* v);
};

bool CanHash(const NMiniKQL::TType* type);
NUdf::IHash::TPtr MakeHashImpl(const NMiniKQL::TType* type);
NUdf::ICompare::TPtr MakeCompareImpl(const NMiniKQL::TType* type);
NUdf::IEquate::TPtr MakeEquateImpl(const NMiniKQL::TType* type);

template<typename T>
ui64 CalcMaxBlockLength(T beginIt, T endIt, const NUdf::ITypeInfoHelper& helper) {
    ui64 maxBlockLen = Max<ui64>();
    while (beginIt != endIt) {
        const TType* itemType = *beginIt++;
        if (itemType) {
            maxBlockLen = std::min(maxBlockLen, helper.GetMaxBlockLength(itemType));
        }
    }
    return (maxBlockLen == Max<ui64>()) ? 0 : maxBlockLen;
}

class TTypeBuilder : public TMoveOnly {
public:
    TTypeBuilder(const TTypeEnvironment& env) 
        : Env(env)
    {}

    const TTypeEnvironment& GetTypeEnvironment() const {
        return Env;
    }

    TType* NewVoidType() const;
    TType* NewNullType() const;

    TType* NewDataType(NUdf::TDataTypeId schemeType, bool optional = false) const;
    TType* NewDataType(NUdf::EDataSlot slot, bool optional = false) const {
        return NewDataType(NUdf::GetDataTypeInfo(slot).TypeId, optional);
    }

    TType* NewDecimalType(ui8 precision, ui8 scale) const;
    TType* NewPgType(ui32 typeId) const;

    TType* NewOptionalType(TType* itemType) const;

    TType* NewEmptyStructType() const;
    TType* NewStructType(TType* baseStructType, const std::string_view& memberName, TType* memberType) const;
    TType* NewStructType(const TArrayRef<const std::pair<std::string_view, TType*>>& memberTypes) const;
    TType* NewArrayType(const TArrayRef<const std::pair<std::string_view, TType*>>& memberTypes) const;

    TType* NewListType(TType* itemType) const;

    TType* NewDictType(TType* keyType, TType* payloadType, bool multi) const;

    TType* NewStreamType(TType* itemType) const;
    TType* NewFlowType(TType* itemType) const;
    TType* NewTaggedType(TType* baseType, const std::string_view& tag) const;
    TType* NewBlockType(TType* itemType, TBlockType::EShape shape) const;

    TType* NewEmptyTupleType() const;
    TType* NewTupleType(const TArrayRef<TType* const>& elements) const;
    TType* NewArrayType(const TArrayRef<TType* const>& elements) const;

    TType* NewEmptyMultiType() const;
    TType* NewMultiType(const TArrayRef<TType* const>& elements) const;

    TType* NewResourceType(const std::string_view& tag) const;
    TType* NewVariantType(TType* underlyingType) const;

protected:
    const TTypeEnvironment& Env;
    bool UseNullType = true;
};

void RebuildTypeIndex();

} // namespace NMiniKQL
} // namespace Nkikimr
