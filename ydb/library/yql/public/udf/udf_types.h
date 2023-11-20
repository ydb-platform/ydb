#pragma once

#include "udf_ptr.h"
#include "udf_data_type.h"
#include "udf_pg_type_description.h"
#include "udf_type_size_check.h"
#include "udf_version.h"

struct ArrowSchema;

namespace NYql {
namespace NUdf {

// opaque type info
using TType = void;

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)

#define UDF_TYPE_KIND_MAP(XX) \
    XX(Unknown)               \
    XX(Data)                  \
    XX(Struct)                \
    XX(List)                  \
    XX(Optional)              \
    XX(Tuple)                 \
    XX(Dict)                  \
    XX(Callable)              \
    XX(Resource)              \
    XX(Void)                  \
    XX(Variant)               \
    XX(Stream)                \
    XX(Null)                  \
    XX(EmptyList)             \
    XX(EmptyDict)             \
    XX(Tagged)                \
    XX(Pg)                    \
    XX(Block)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)

#define UDF_TYPE_KIND_MAP(XX) \
    XX(Unknown)               \
    XX(Data)                  \
    XX(Struct)                \
    XX(List)                  \
    XX(Optional)              \
    XX(Tuple)                 \
    XX(Dict)                  \
    XX(Callable)              \
    XX(Resource)              \
    XX(Void)                  \
    XX(Variant)               \
    XX(Stream)                \
    XX(Null)                  \
    XX(EmptyList)             \
    XX(EmptyDict)             \
    XX(Tagged)                \
    XX(Pg)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)

#define UDF_TYPE_KIND_MAP(XX) \
    XX(Unknown)               \
    XX(Data)                  \
    XX(Struct)                \
    XX(List)                  \
    XX(Optional)              \
    XX(Tuple)                 \
    XX(Dict)                  \
    XX(Callable)              \
    XX(Resource)              \
    XX(Void)                  \
    XX(Variant)               \
    XX(Stream)                \
    XX(Null)                  \
    XX(EmptyList)             \
    XX(EmptyDict)             \
    XX(Tagged)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 18)

#define UDF_TYPE_KIND_MAP(XX) \
    XX(Unknown)               \
    XX(Data)                  \
    XX(Struct)                \
    XX(List)                  \
    XX(Optional)              \
    XX(Tuple)                 \
    XX(Dict)                  \
    XX(Callable)              \
    XX(Resource)              \
    XX(Void)                  \
    XX(Variant)               \
    XX(Stream)                \
    XX(Null)                  \
    XX(EmptyList)             \
    XX(EmptyDict)
#else

#define UDF_TYPE_KIND_MAP(XX) \
    XX(Unknown)               \
    XX(Data)                  \
    XX(Struct)                \
    XX(List)                  \
    XX(Optional)              \
    XX(Tuple)                 \
    XX(Dict)                  \
    XX(Callable)              \
    XX(Resource)              \
    XX(Void)                  \
    XX(Variant)               \
    XX(Stream)

#endif

enum ETypeKind
{
    UDF_TYPE_KIND_MAP(ENUM_VALUE_GEN_NO_VALUE)
};

ENUM_TO_STRING(ETypeKind, UDF_TYPE_KIND_MAP)

//////////////////////////////////////////////////////////////////////////////
// ICallablePayload
//////////////////////////////////////////////////////////////////////////////
class ICallablePayload
{
public:
    virtual ~ICallablePayload() = default;

    struct TArgumentFlags {
        enum {
            AutoMap = 0x01,
        };
    };

    virtual TStringRef GetPayload() const = 0;
    virtual TStringRef GetArgumentName(ui32 index) const = 0;
    virtual ui64 GetArgumentFlags(ui32 index) const = 0;
};

UDF_ASSERT_TYPE_SIZE(ICallablePayload, 8);

//////////////////////////////////////////////////////////////////////////////
// ITypeVisitor
//////////////////////////////////////////////////////////////////////////////
class ITypeVisitor1
{
public:
    inline bool IsCompatibleTo(ui16 compatibilityVersion) const {
        return AbiCompatibility_ >= compatibilityVersion;
    }

    virtual ~ITypeVisitor1() = default;

    virtual void OnDataType(TDataTypeId typeId) = 0;
    virtual void OnStruct(
            ui32 membersCount,
            TStringRef* membersNames,
            const TType** membersTypes) = 0;
    virtual void OnList(const TType* itemType) = 0;
    virtual void OnOptional(const TType* itemType) = 0;
    virtual void OnTuple(ui32 elementsCount, const TType** elementsTypes) = 0;
    virtual void OnDict(const TType* keyType, const TType* valueType) = 0;
    virtual void OnCallable(
            const TType* returnType,
            ui32 argsCount, const TType** argsTypes,
            ui32 optionalArgsCount, const ICallablePayload* payload) = 0;
    virtual void OnVariant(const TType* underlyingType) = 0;
    virtual void OnStream(const TType* itemType) = 0;
private:
    ui16 AbiCompatibility_ = MakeAbiCompatibilityVersion(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR);
    ui16 Reserved1_ = 0;
    ui32 Reserved2_ = 0;

    void UnusedPrivates() {
        Y_UNUSED(Reserved1_);
        Y_UNUSED(Reserved2_);
    }
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
class ITypeVisitor2: public ITypeVisitor1 {
public:
    virtual void OnDecimal(ui8 precision, ui8 scale) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
class ITypeVisitor3: public ITypeVisitor2 {
public:
    virtual void OnResource(TStringRef tag) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
class ITypeVisitor4: public ITypeVisitor3 {
public:
    virtual void OnTagged(const TType* baseType, TStringRef tag) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class ITypeVisitor5: public ITypeVisitor4 {
public:
    virtual void OnPg(ui32 typeId) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class ITypeVisitor6: public ITypeVisitor5 {
public:
    virtual void OnBlock(const TType* itemType, bool isScalar) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class ITypeVisitor : public ITypeVisitor6 {
protected:
    ITypeVisitor();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class ITypeVisitor : public ITypeVisitor5 {
protected:
    ITypeVisitor();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
class ITypeVisitor : public ITypeVisitor4 {
protected:
    ITypeVisitor();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
class ITypeVisitor : public ITypeVisitor3 {
protected:
    ITypeVisitor();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
class ITypeVisitor : public ITypeVisitor2 {
protected:
    ITypeVisitor();
};
#else
class ITypeVisitor : public ITypeVisitor1 {
protected:
    ITypeVisitor();
};
#endif

UDF_ASSERT_TYPE_SIZE(ITypeVisitor, 16);

//////////////////////////////////////////////////////////////////////////////
// ITypeInfoHelper
//////////////////////////////////////////////////////////////////////////////
class ITypeInfoHelper1
{
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper1>;

public:
    virtual ~ITypeInfoHelper1() = default;

    virtual ETypeKind GetTypeKind(const TType* type) const = 0;
    virtual void VisitType(const TType* type, ITypeVisitor* visitor) const = 0;
    virtual bool IsSameType(const TType* type1, const TType* type2) const = 0;

    // reference counting
    inline void Ref() noexcept {
        Refs_++;
    }

    inline void UnRef() noexcept {
        Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
        if (--Refs_ == 0) {
            delete this;
        }
    }

    inline ui32 RefCount() const noexcept {
        return Refs_;
    }

private:
    ui32 Refs_ = 0;
    ui32 Reserved_ = 0;

    void UnusedPrivates() {
        Y_UNUSED(Reserved_);
    }
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class ITypeInfoHelper2 : public ITypeInfoHelper1 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper2>;

public:
    virtual const TPgTypeDescription* FindPgTypeDescription(ui32 typeId) const = 0;
};
#endif

//////////////////////////////////////////////////////////////////////////////
// IArrowType
//////////////////////////////////////////////////////////////////////////////
class IArrowType
{
public:
    using TPtr = TUniquePtr<IArrowType>;

    virtual ~IArrowType() = default;

    virtual void Export(ArrowSchema* out) const = 0;
};

UDF_ASSERT_TYPE_SIZE(IArrowType, 8);

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class ITypeInfoHelper3 : public ITypeInfoHelper2 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper3>;

public:
    // returns nullptr if type isn't supported
    virtual IArrowType::TPtr MakeArrowType(const TType* type) const = 0;
    // The given ArrowSchema struct is released, even if this function fails. 
    virtual IArrowType::TPtr ImportArrowType(ArrowSchema* schema) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 29)
class ITypeInfoHelper4 : public ITypeInfoHelper3 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper4>;

public:
    virtual ui64 GetMaxBlockLength(const TType* type) const = 0;
    virtual ui64 GetMaxBlockBytes() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 29)
class ITypeInfoHelper : public ITypeInfoHelper4 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper>;
    ITypeInfoHelper();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class ITypeInfoHelper : public ITypeInfoHelper3 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper>;
    ITypeInfoHelper();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class ITypeInfoHelper : public ITypeInfoHelper2 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper>;
    ITypeInfoHelper();
};
#else
class ITypeInfoHelper : public ITypeInfoHelper1 {
public:
    using TPtr = TRefCountedPtr<ITypeInfoHelper>;
    ITypeInfoHelper();
};
#endif

UDF_ASSERT_TYPE_SIZE(ITypeInfoHelper, 16);
UDF_ASSERT_TYPE_SIZE(ITypeInfoHelper::TPtr, 8);

} // namspace NUdf
} // namspace NYql
