#pragma once

#include "udf_counter.h"
#include "udf_types.h"
#include "udf_ptr.h"
#include "udf_string_ref.h"
#include "udf_type_size_check.h"
#include "udf_value.h"

#include <type_traits>

namespace NYql {
namespace NUdf {

class TStringRef;
class IBoxedValue;

///////////////////////////////////////////////////////////////////////////////
// ISecureParamsProvider
///////////////////////////////////////////////////////////////////////////////

class ISecureParamsProvider {
public:
    virtual ~ISecureParamsProvider() = default;
    virtual bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const = 0;
};

template <typename T>
struct TAutoMap { using ItemType = T; };

template <typename T>
struct TOptional { using ItemType = T; };

template <typename T, const char* Name>
struct TNamedArg { using ItemType = T; };

template <typename T>
struct TListType { using ItemType = T; };

template <typename TKey, typename TValue>
struct TDict {
    using KeyType = TKey;
    using ValueType = TValue;
};

template <typename TKey>
struct TSetType {
    using KeyType = TKey;
};

template <typename... TArgs>
struct TTuple;

template <const char* Tag>
struct TResource {};

template <typename... TArgs>
struct TVariant;

template <typename T>
struct TStream { using ItemType = T; };

template <typename T, const char* Tag>
struct TTagged { using BaseType = T; };

template <ui32 TypeId>
struct TPg;

template <typename T>
struct TBlockType { using ItemType = T; };

template <typename T>
struct TScalarType { using ItemType = T; };

struct TVoid {};

//////////////////////////////////////////////////////////////////////////////
// ITypeBuilder
//////////////////////////////////////////////////////////////////////////////
class ITypeBuilder
{
public:
    virtual ~ITypeBuilder() = default;

    virtual TType* Build() const = 0;
};

UDF_ASSERT_TYPE_SIZE(ITypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IOptionalTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IOptionalTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IOptionalTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IOptionalTypeBuilder& Item() {
        return Item(TDataType<T>::Id);
    }

    virtual IOptionalTypeBuilder& Item(TDataTypeId type) = 0;
    virtual IOptionalTypeBuilder& Item(const TType* type) = 0;
    virtual IOptionalTypeBuilder& Item(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(IOptionalTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IListTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IListTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IListTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IListTypeBuilder& Item() {
        return Item(TDataType<T>::Id);
    }

    virtual IListTypeBuilder& Item(TDataTypeId type) = 0;
    virtual IListTypeBuilder& Item(const TType* type) = 0;
    virtual IListTypeBuilder& Item(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(IListTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IVariantTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IVariantTypeBuilder : public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IVariantTypeBuilder>;

public:
    // type must be either tuple or struct
    virtual IVariantTypeBuilder& Over(const TType* type) = 0;
    virtual IVariantTypeBuilder& Over(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(IVariantTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IStreamTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IStreamTypeBuilder : public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IStreamTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IStreamTypeBuilder& Item() {
        return Item(TDataType<T>::Id);
    }

    virtual IStreamTypeBuilder& Item(TDataTypeId type) = 0;
    virtual IStreamTypeBuilder& Item(const TType* type) = 0;
    virtual IStreamTypeBuilder& Item(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(IStreamTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IDictTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IDictTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IDictTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IDictTypeBuilder& Key() {
        return Key(TDataType<T>::Id);
    }

    virtual IDictTypeBuilder& Key(TDataTypeId type) = 0;
    virtual IDictTypeBuilder& Key(const TType* type) = 0;
    virtual IDictTypeBuilder& Key(const ITypeBuilder& type) = 0;

    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IDictTypeBuilder& Value() {
        return Value(TDataType<T>::Id);
    }

    virtual IDictTypeBuilder& Value(TDataTypeId type) = 0;
    virtual IDictTypeBuilder& Value(const TType* type) = 0;
    virtual IDictTypeBuilder& Value(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(IDictTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// ISetTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class ISetTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<ISetTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline ISetTypeBuilder& Key() {
        return Key(TDataType<T>::Id);
    }

    virtual ISetTypeBuilder& Key(TDataTypeId type) = 0;
    virtual ISetTypeBuilder& Key(const TType* type) = 0;
    virtual ISetTypeBuilder& Key(const ITypeBuilder& type) = 0;
};

UDF_ASSERT_TYPE_SIZE(ISetTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IStructTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IStructTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IStructTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IStructTypeBuilder& AddField(
            const TStringRef& name, ui32* index)
    {
        return AddField(name, TDataType<T>::Id, index);
    }

    virtual IStructTypeBuilder& AddField(
            const TStringRef& name, TDataTypeId type, ui32* index) = 0;

    virtual IStructTypeBuilder& AddField(
            const TStringRef& name, const TType* type, ui32* index) = 0;

    virtual IStructTypeBuilder& AddField(
            const TStringRef& name,
            const ITypeBuilder& typeBuilder,
            ui32* index) = 0;
};

UDF_ASSERT_TYPE_SIZE(IStructTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IEnumTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IEnumTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IEnumTypeBuilder>;

public:
    virtual IEnumTypeBuilder& AddField(
            const TStringRef& name, ui32* index) = 0;
};

UDF_ASSERT_TYPE_SIZE(IEnumTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// ITupleTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class ITupleTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<ITupleTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    ITupleTypeBuilder& Add() {
        return Add(TDataType<T>::Id);
    }

    virtual ITupleTypeBuilder& Add(TDataTypeId type) = 0;
    virtual ITupleTypeBuilder& Add(const TType* type) = 0;
    virtual ITupleTypeBuilder& Add(const ITypeBuilder& typeBuilder) = 0;
};

UDF_ASSERT_TYPE_SIZE(ITupleTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// ICallableTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class ICallableTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<ICallableTypeBuilder>;

public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    ICallableTypeBuilder& Returns() {
        return Returns(TDataType<T>::Id);
    }

    virtual ICallableTypeBuilder& Returns(TDataTypeId type) = 0;
    virtual ICallableTypeBuilder& Returns(const TType* type) = 0;
    virtual ICallableTypeBuilder& Returns(const ITypeBuilder& typeBuilder) = 0;

    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    ICallableTypeBuilder& Arg() {
        return Arg(TDataType<T>::Id);
    }

    virtual ICallableTypeBuilder& Arg(TDataTypeId type) = 0;
    virtual ICallableTypeBuilder& Arg(const TType* type) = 0;
    virtual ICallableTypeBuilder& Arg(const ITypeBuilder& typeBuilder) = 0;

    virtual ICallableTypeBuilder& OptionalArgs(ui32 optionalArgs) = 0;

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 38)
    // sets name for the last added argument
    virtual ICallableTypeBuilder& Name(const TStringRef& name) = 0;

    // sets flags for the last added argument, see ICallablePayload::TArgumentFlags
    virtual ICallableTypeBuilder& Flags(ui64 flags) = 0;
#endif
};

UDF_ASSERT_TYPE_SIZE(ICallableTypeBuilder, 8);

class IFunctionTypeInfoBuilder;

//////////////////////////////////////////////////////////////////////////////
// IFunctionArgTypesBuilder
//////////////////////////////////////////////////////////////////////////////
class IFunctionArgTypesBuilder
{
public:
    using TPtr = TUniquePtr<IFunctionArgTypesBuilder>;

public:
    IFunctionArgTypesBuilder(IFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    virtual ~IFunctionArgTypesBuilder() = default;

    template <typename T>
    inline IFunctionArgTypesBuilder& Add();

    virtual IFunctionArgTypesBuilder& Add(TDataTypeId type) = 0;
    virtual IFunctionArgTypesBuilder& Add(const TType* type) = 0;
    virtual IFunctionArgTypesBuilder& Add(const ITypeBuilder& typeBuilder) = 0;
    // sets name for the last added argument
    virtual IFunctionArgTypesBuilder& Name(const TStringRef& name) = 0;
    // sets flags for the last added argument, see ICallablePayload::TArgumentFlags
    virtual IFunctionArgTypesBuilder& Flags(ui64 flags) = 0;

    inline IFunctionTypeInfoBuilder& Done() const {
        return Parent_;
    }

    inline IFunctionTypeInfoBuilder& Parent() const {
        return Parent_;
    }

private:
    IFunctionTypeInfoBuilder& Parent_;
};

UDF_ASSERT_TYPE_SIZE(IFunctionArgTypesBuilder, 16);

//////////////////////////////////////////////////////////////////////////////
// IRefCounted
//////////////////////////////////////////////////////////////////////////////
class IRefCounted {
public:
    virtual ~IRefCounted() = default;

    inline void Ref() noexcept {
        Refs_++;
    }

    inline void UnRef() noexcept {
        Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
        if (--Refs_ == 0) {
            delete this;
        }
    }

private:
    ui32 Refs_ = 0;
    ui32 Reserved_ = 0;

    void Unused() {
        Y_UNUSED(Reserved_);
    }
};

UDF_ASSERT_TYPE_SIZE(IRefCounted, 16);

//////////////////////////////////////////////////////////////////////////////
// IHash
//////////////////////////////////////////////////////////////////////////////
class IHash : public IRefCounted {
public:
    using TPtr = TRefCountedPtr<IHash>;

    virtual ui64 Hash(TUnboxedValuePod value) const = 0;
};

UDF_ASSERT_TYPE_SIZE(IHash, 16);

//////////////////////////////////////////////////////////////////////////////
// IEquate
//////////////////////////////////////////////////////////////////////////////
class IEquate : public IRefCounted {
public:
    using TPtr = TRefCountedPtr<IEquate>;

    virtual bool Equals(TUnboxedValuePod lhs, TUnboxedValuePod rhs) const = 0;
};

UDF_ASSERT_TYPE_SIZE(IEquate, 16);

//////////////////////////////////////////////////////////////////////////////
// ICompare
//////////////////////////////////////////////////////////////////////////////
class ICompare : public IRefCounted {
public:
    using TPtr = TRefCountedPtr<ICompare>;

    virtual bool Less(TUnboxedValuePod lhs, TUnboxedValuePod rhs) const = 0;
    // signed difference, for strings may not be just -1/0/1
    virtual int Compare(TUnboxedValuePod lhs, TUnboxedValuePod rhs) const = 0;
};

UDF_ASSERT_TYPE_SIZE(ICompare, 16);

//////////////////////////////////////////////////////////////////////////////
// IBlockTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class IBlockTypeBuilder: public ITypeBuilder
{
public:
    using TPtr = TUniquePtr<IBlockTypeBuilder>;

    explicit IBlockTypeBuilder(bool isScalar)
        : IsScalar_(isScalar)
    {}
public:
    template <typename T, typename = std::enable_if_t<TKnownDataType<T>::Result>>
    inline IBlockTypeBuilder& Item() {
        return Item(TDataType<T>::Id);
    }

    virtual IBlockTypeBuilder& Item(TDataTypeId type) = 0;
    virtual IBlockTypeBuilder& Item(const TType* type) = 0;
    virtual IBlockTypeBuilder& Item(const ITypeBuilder& type) = 0;

protected:
    bool IsScalar_;
};

UDF_ASSERT_TYPE_SIZE(IListTypeBuilder, 8);

//////////////////////////////////////////////////////////////////////////////
// IFunctionTypeInfoBuilder
//////////////////////////////////////////////////////////////////////////////
namespace NImpl {

template <typename T> struct TSimpleSignatureHelper;
template <typename T> struct TSimpleSignatureTypeHelper;
template <typename T> struct TTypeBuilderHelper;
template <typename... TArgs> struct TArgsHelper;
template <typename... TArgs> struct TTupleHelper;
template <typename... TArgs> struct TCallableArgsHelper;

} // namspace NImpl

struct TSourcePosition {
    TSourcePosition(ui32 row = 0, ui32 column = 0, TStringRef file = {})
        : Row_(row)
        , Column_(column)
        , File_(file)
    {}

    ui32 Row_;
    ui32 Column_;
    TStringRef File_;
};

UDF_ASSERT_TYPE_SIZE(TSourcePosition, 24);

inline IOutputStream& operator<<(IOutputStream& os, const TSourcePosition& pos) {
    os << (pos.File_.Size() ? TStringBuf(pos.File_) : TStringBuf("<main>")) << ':' << pos.Row_ << ':' << pos.Column_ << ':';
    return os;
}

class IFunctionTypeInfoBuilder1
{
public:
    virtual ~IFunctionTypeInfoBuilder1() = default;

    // function implementation
    virtual IFunctionTypeInfoBuilder1& ImplementationImpl(
            TUniquePtr<IBoxedValue> impl) = 0;

    virtual IFunctionTypeInfoBuilder1& ReturnsImpl(TDataTypeId type) = 0;
    virtual IFunctionTypeInfoBuilder1& ReturnsImpl(const TType* type) = 0;
    virtual IFunctionTypeInfoBuilder1& ReturnsImpl(const ITypeBuilder& typeBuilder) = 0;

    // function argument types
    virtual TUniquePtr<IFunctionArgTypesBuilder> Args(ui32 expectedItem = 10) = 0;
    virtual IFunctionTypeInfoBuilder1& OptionalArgsImpl(ui32 optionalArgs) = 0;

    virtual IFunctionTypeInfoBuilder1& RunConfigImpl(TDataTypeId type) = 0;
    virtual IFunctionTypeInfoBuilder1& RunConfigImpl(const TType* type) = 0;
    virtual IFunctionTypeInfoBuilder1& RunConfigImpl(const ITypeBuilder& typeBuilder) = 0;

    // errors
    virtual void SetError(const TStringRef& error) = 0;

    // primitive types
    virtual TType* Primitive(TDataTypeId typeId) const = 0;

    // complex types builders
    virtual IOptionalTypeBuilder::TPtr Optional() const = 0;
    virtual IListTypeBuilder::TPtr List() const = 0;
    virtual IDictTypeBuilder::TPtr Dict() const = 0;
    virtual IStructTypeBuilder::TPtr Struct(ui32 expectedItems = 10) const = 0;
    virtual ITupleTypeBuilder::TPtr Tuple(ui32 expectedItems = 10) const = 0;
    virtual ICallableTypeBuilder::TPtr Callable(ui32 expectedArgs = 10) const = 0;

    // special types
    virtual TType* Void() const = 0;
    virtual TType* Resource(const TStringRef& tag) const = 0;

    // type info
    virtual ITypeInfoHelper::TPtr TypeInfoHelper() const = 0;

    // 1.2
    virtual IFunctionTypeInfoBuilder1& PayloadImpl(const TStringRef& payload) = 0;

    // Important: Always add new ABI VERSION methods to the end of the class definition.

    virtual IVariantTypeBuilder::TPtr Variant() const = 0;

    virtual IFunctionTypeInfoBuilder1& UserTypeImpl(TDataTypeId type) = 0;
    virtual IFunctionTypeInfoBuilder1& UserTypeImpl(const TType* type) = 0;
    virtual IFunctionTypeInfoBuilder1& UserTypeImpl(const ITypeBuilder& typeBuilder) = 0;

    virtual NUdf::IStreamTypeBuilder::TPtr Stream() const = 0;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 5)
class IFunctionTypeInfoBuilder2: public IFunctionTypeInfoBuilder1 {
public:
    virtual TCounter GetCounter(const TStringRef& name, bool deriv) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 6)
class IFunctionTypeInfoBuilder3: public IFunctionTypeInfoBuilder2 {
public:
    virtual TScopedProbe GetScopedProbe(const TStringRef& name) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 9)
class IFunctionTypeInfoBuilder4: public IFunctionTypeInfoBuilder3 {
public:
    virtual TSourcePosition GetSourcePosition() = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 10)
class IFunctionTypeInfoBuilder5: public IFunctionTypeInfoBuilder4 {
public:
    virtual IHash::TPtr MakeHash(const TType* type) = 0;
    virtual IEquate::TPtr MakeEquate(const TType* type) = 0;
    virtual ICompare::TPtr MakeCompare(const TType* type) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
class IFunctionTypeInfoBuilder6: public IFunctionTypeInfoBuilder5 {
public:
    virtual TType* Decimal(ui8 precision, ui8 scale) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 16)
class IFunctionTypeInfoBuilder7: public IFunctionTypeInfoBuilder6 {
public:
    virtual IFunctionTypeInfoBuilder7& IRImplementationImpl(
        const TStringRef& moduleIR,
        const TStringRef& moduleIRUniqId,
        const TStringRef& functionName
    ) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 18)
class IFunctionTypeInfoBuilder8: public IFunctionTypeInfoBuilder7 {
public:
    virtual TType* Null() const = 0;
    virtual TType* EmptyList() const = 0;
    virtual TType* EmptyDict() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IFunctionTypeInfoBuilder9: public IFunctionTypeInfoBuilder8 {
public:
    virtual void Unused1() = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 20)
class IFunctionTypeInfoBuilder10: public IFunctionTypeInfoBuilder9 {
public:
    virtual ISetTypeBuilder::TPtr Set() const = 0;
    virtual IEnumTypeBuilder::TPtr Enum(ui32 expectedItems = 10) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
class IFunctionTypeInfoBuilder11: public IFunctionTypeInfoBuilder10 {
public:
    virtual TType* Tagged(const TType* baseType, const TStringRef& tag) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 22)
class IFunctionTypeInfoBuilder12: public IFunctionTypeInfoBuilder11 {
public:
    virtual bool GetSecureParam(TStringRef key, TStringRef& value) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class IFunctionTypeInfoBuilder13: public IFunctionTypeInfoBuilder12 {
public:
    virtual TType* Pg(ui32 typeId) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class IFunctionTypeInfoBuilder14: public IFunctionTypeInfoBuilder13 {
public:
    virtual IBlockTypeBuilder::TPtr Block(bool isScalar) const = 0;
    virtual void Unused2() = 0;
    virtual void Unused3() = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 28)
class IFunctionTypeInfoBuilder15: public IFunctionTypeInfoBuilder14 {
public:
    virtual IFunctionTypeInfoBuilder15& SupportsBlocksImpl() = 0;
    virtual IFunctionTypeInfoBuilder15& IsStrictImpl() = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 32)
class IBlockTypeHelper;

class IFunctionTypeInfoBuilder16: public IFunctionTypeInfoBuilder15 {
public:
    virtual const IBlockTypeHelper& IBlockTypeHelper() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 32)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder16;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 28)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder15;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder14;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder13;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 22)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder12;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder11;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 20)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder10;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder9;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 18)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder8;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 16)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder7;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder6;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 10)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder5;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 9)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder4;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 6)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder3;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 5)
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder2;
#else
using IFunctionTypeInfoBuilderImpl = IFunctionTypeInfoBuilder1;
#endif

class IFunctionTypeInfoBuilder: public IFunctionTypeInfoBuilderImpl {
public:
    IFunctionTypeInfoBuilder();
    
    IFunctionTypeInfoBuilder& Implementation(
            TUniquePtr<IBoxedValue> impl) {
        ImplementationImpl(std::move(impl));
        return *this;
    }

    IFunctionTypeInfoBuilder& OptionalArgs(ui32 optionalArgs) {
        OptionalArgsImpl(optionalArgs);
        return *this;
    }

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 28)
    IFunctionTypeInfoBuilder& SupportsBlocks() {
        SupportsBlocksImpl();
        return *this;
    }

    IFunctionTypeInfoBuilder& IsStrict() {
        IsStrictImpl();
        return *this;
    }
#endif

    IFunctionTypeInfoBuilder& Returns(TDataTypeId type) {
        ReturnsImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& Returns(const TType* type) {
        ReturnsImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& Returns(const ITypeBuilder& typeBuilder) {
        ReturnsImpl(typeBuilder);
        return *this;
    }

    // function return type
    template <typename T>
    inline IFunctionTypeInfoBuilder& Returns() {
        return Returns(NImpl::TTypeBuilderHelper<T>::Build(*this));
    }

    // simplified signature registration
    template <typename T>
    IFunctionTypeInfoBuilder& SimpleSignature() {
        NImpl::TSimpleSignatureHelper<T>::Register(*this);
        return *this;
    }

    template <typename T>
    TType* SimpleSignatureType(ui32 optionalArgs = 0) const {
        return NImpl::TSimpleSignatureTypeHelper<T>::Build(*this, optionalArgs);
    }

    IFunctionTypeInfoBuilder& RunConfig(TDataTypeId type) {
        RunConfigImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& RunConfig(const TType* type) {
        RunConfigImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& RunConfig(const ITypeBuilder& typeBuilder) {
        RunConfigImpl(typeBuilder);
        return *this;
    }

    // run config type
    template <typename T>
    inline IFunctionTypeInfoBuilder& RunConfig() {
        return RunConfig(NImpl::TTypeBuilderHelper<T>::Build(*this));
    }

    // simplified type builder
    template <typename T>
    inline TType* SimpleType() const {
        return NImpl::TTypeBuilderHelper<T>::Build(*this);
    }

    IFunctionTypeInfoBuilder& Payload(const TStringRef& payload) {
        PayloadImpl(payload);
        return *this;
    }

    IFunctionTypeInfoBuilder& UserType(TDataTypeId type) {
        UserTypeImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& UserType(const TType* type) {
        UserTypeImpl(type);
        return *this;
    }

    IFunctionTypeInfoBuilder& UserType(const ITypeBuilder& typeBuilder) {
        UserTypeImpl(typeBuilder);
        return *this;
    }

    // user type
    template <typename T>
    inline IFunctionTypeInfoBuilder& UserType() {
        return UserType(NImpl::TTypeBuilderHelper<T>::Build(*this));
    }

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 16)
    IFunctionTypeInfoBuilder& IRImplementation(
        const TStringRef& moduleIR,
        const TStringRef& moduleIRUniqId,
        const TStringRef& functionName
    ) {
        IRImplementationImpl(moduleIR, moduleIRUniqId, functionName);
        return *this;
    }
#endif
};

UDF_ASSERT_TYPE_SIZE(IFunctionTypeInfoBuilder, 8);

using IFunctionTypeInfoBuilderPtr = TUniquePtr<IFunctionTypeInfoBuilder>;

namespace NImpl {

template <typename T>
struct TTypeBuilderHelper {
    static TType* Build(
            const IFunctionTypeInfoBuilder& builder,
            std::enable_if_t<TKnownDataType<T>::Result>* = nullptr)
    {
        return builder.Primitive(TDataType<T>::Id);
    }
};

template <>
struct TTypeBuilderHelper<void> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Void();
    }
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
template <ui8 Precision, ui8 Scale>
struct TTypeBuilderHelper<TDecimalDataType<Precision, Scale>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Decimal(Precision, Scale);
    }
};
#endif

template <const char* Tag>
struct TTypeBuilderHelper<TResource<Tag>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Resource(TStringRef(Tag, std::strlen(Tag)));
    }
};

template <typename T>
struct TTypeBuilderHelper<TListType<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.List()->
                Item(TTypeBuilderHelper<T>::Build(builder))
                .Build();
    }
};

template <typename T>
struct TTypeBuilderHelper<TOptional<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Optional()->
                Item(TTypeBuilderHelper<T>::Build(builder))
                .Build();
    }
};

template <typename T>
struct TTypeBuilderHelper<TAutoMap<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return TTypeBuilderHelper<T>::Build(builder);
    }
};

template <typename TKey, typename TValue>
struct TTypeBuilderHelper<TDict<TKey, TValue>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Dict()->
                Key(TTypeBuilderHelper<TKey>::Build(builder))
                .Value(TTypeBuilderHelper<TValue>::Build(builder))
                .Build();
    }
};

template <typename TKey>
struct TTypeBuilderHelper<TSetType<TKey>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Dict()->
                Key(TTypeBuilderHelper<TKey>::Build(builder))
                .Value(builder.Void())
                .Build();
    }
};

template <>
struct TTypeBuilderHelper<TVoid> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Void();
    }
};

template <typename T>
struct TTypeBuilderHelper<TStream<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Stream()->
                Item(TTypeBuilderHelper<T>::Build(builder))
                .Build();
    }
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
template <typename T, const char* Tag>
struct TTypeBuilderHelper<TTagged<T, Tag>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Tagged(TTypeBuilderHelper<T>::Build(builder),
            TStringRef(Tag, std::strlen(Tag)));
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
template <ui32 TypeId>
struct TTypeBuilderHelper<TPg<TypeId>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Pg(TypeId);
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
template <typename T>
struct TTypeBuilderHelper<TBlockType<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Block(false)->
                Item(TTypeBuilderHelper<T>::Build(builder))
                .Build();
    }
};

template <typename T>
struct TTypeBuilderHelper<TScalarType<T>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        return builder.Block(true)->
                Item(TTypeBuilderHelper<T>::Build(builder))
                .Build();
    }
};
#endif

template <>
struct TCallableArgsHelper<> {
    static void Arg(
            ICallableTypeBuilder& callableBuilder,
            const IFunctionTypeInfoBuilder& builder)
    {
        Y_UNUSED(callableBuilder); Y_UNUSED(builder);
    }
};

template <typename TArg, typename... TArgs>
struct TCallableArgsHelper<TArg, TArgs...> {
    static void Arg(
            ICallableTypeBuilder& callableBuilder,
            const IFunctionTypeInfoBuilder& builder)
    {
        callableBuilder.Arg(TTypeBuilderHelper<TArg>::Build(builder));
        TCallableArgsHelper<TArgs...>::Arg(callableBuilder, builder);
    }
};

template <typename TReturn, typename... TArgs>
struct TTypeBuilderHelper<TReturn(*)(TArgs...)> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        auto callableBuilder = builder.Callable(sizeof...(TArgs));
        callableBuilder->Returns(TTypeBuilderHelper<TReturn>::Build(builder));
        TCallableArgsHelper<TArgs...>::Arg(*callableBuilder, builder);
        return callableBuilder->Build();
    }
};

template <typename TArg, typename... TArgs>
struct TTupleHelper<TArg, TArgs...> {
    static void Add(
            ITupleTypeBuilder& tupleBuilder,
            const IFunctionTypeInfoBuilder& builder)
    {
        tupleBuilder.Add(TTypeBuilderHelper<TArg>::Build(builder));
        TTupleHelper<TArgs...>::Add(tupleBuilder, builder);
    }
};

template <>
struct TTupleHelper<> {
    static void Add(
            ITupleTypeBuilder& tupleBuilder,
            const IFunctionTypeInfoBuilder& builder)
    {
        Y_UNUSED(tupleBuilder); Y_UNUSED(builder);
    }
};

template <typename... TArgs>
struct TTypeBuilderHelper<TTuple<TArgs...>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        auto tupleBuilder = builder.Tuple(sizeof...(TArgs));
        TTupleHelper<TArgs...>::Add(*tupleBuilder, builder);
        return tupleBuilder->Build();
    }
};

template <typename... TArgs>
struct TTypeBuilderHelper<NUdf::TVariant<TArgs...>> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder) {
        auto tupleBuilder = builder.Tuple(sizeof...(TArgs));
        TTupleHelper<TArgs...>::Add(*tupleBuilder, builder);
        return builder.Variant()->Over(*tupleBuilder).Build();
    }
};

template <>
struct TArgsHelper<> {
    static void Add(IFunctionArgTypesBuilder& builder, const char* name = nullptr, ui64 flags = 0) {
        Y_UNUSED(builder);
        Y_UNUSED(name);
        Y_UNUSED(flags);
    }
};

template <typename TArg, typename... TArgs>
struct TArgsHelper<TAutoMap<TArg>, TArgs...> {
   static void Add(IFunctionArgTypesBuilder& builder, const char* name = nullptr, ui64 flags = 0) {
       TArgsHelper<TArg>::Add(builder, name, flags | ICallablePayload::TArgumentFlags::AutoMap);
       TArgsHelper<TArgs...>::Add(builder);
   }
};

template <const char* Name, typename TArg, typename... TArgs>
struct TArgsHelper<TNamedArg<TArg, Name>, TArgs...> {
    static void Add(IFunctionArgTypesBuilder& builder, const char* name = nullptr, ui64 flags = 0) {
        Y_UNUSED(name);
        TArgsHelper<TOptional<TArg>>::Add(builder, Name, flags);
        TArgsHelper<TArgs...>::Add(builder);
  }
};

template <typename TArg, typename... TArgs>
struct TArgsHelper<TArg, TArgs...> {
    static void Add(IFunctionArgTypesBuilder& builder, const char* name = nullptr, ui64 flags = 0) {
        builder.Add(TTypeBuilderHelper<TArg>::Build(builder.Parent())).Flags(flags);
        if (name) {
            builder.Name(TStringRef(name, std::strlen(name)));
        }
        TArgsHelper<TArgs...>::Add(builder);
    }
};

template <typename TReturn, typename... TArgs>
struct TSimpleSignatureHelper<TReturn(TArgs...)> {
    static TType* BuildReturnType(IFunctionTypeInfoBuilder& builder) {
        return TTypeBuilderHelper<TReturn>::Build(builder);
    }
    static void Register(IFunctionTypeInfoBuilder& builder) {
        builder.Returns(BuildReturnType(builder));
        TArgsHelper<TArgs...>::Add(*builder.Args());
    }
};

template <typename TReturn, typename... TArgs>
struct TSimpleSignatureTypeHelper<TReturn(TArgs...)> {
    static TType* Build(const IFunctionTypeInfoBuilder& builder, ui32 optionalArgc) {
        auto callableBuilder = builder.Callable(sizeof...(TArgs));
        callableBuilder->Returns(TTypeBuilderHelper<TReturn>::Build(builder));
        TCallableArgsHelper<TArgs...>::Arg(*callableBuilder, builder);
        callableBuilder->OptionalArgs(optionalArgc);
        return callableBuilder->Build();
    }
};

} // namspace NImpl

template <typename T>
inline IFunctionArgTypesBuilder& IFunctionArgTypesBuilder::Add()
{
    NImpl::TArgsHelper<T>::Add(*Parent_.Args());
    return *this;
}

} // namespace NUdf
} // namespace NYql
