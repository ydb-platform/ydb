#pragma once 
 
#include "udf_string_ref.h" 
#include "udf_types.h" 
 
namespace NYql { 
namespace NUdf { 
 
class TTypePrinter1 : private ITypeVisitor
{ 
public: 
    TTypePrinter1(const ITypeInfoHelper& typeHelper, const TType* type);
 
    void Out(IOutputStream &o) const; 
 
protected:
    void OnDataType(TDataTypeId typeId) final; 
    void OnStruct(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes) final; 
    void OnList(const TType* itemType) final; 
    void OnOptional(const TType* itemType) final; 
    void OnTuple(ui32 elementsCount, const TType** elementsTypes) final; 
    void OnDict(const TType* keyType, const TType* valueType) final; 
    void OnCallable(const TType* returnType, ui32 argsCount, const TType** argsTypes, ui32 optionalArgsCount, const ICallablePayload* payload) final; 
    void OnVariant(const TType* underlyingType) final; 
    void OnStream(const TType* itemType) final; 
    void OutImpl(const TType* type) const;
    void OnDecimalImpl(ui8 precision, ui8 scale);
    void OnResourceImpl(TStringRef tag);
    void OnTaggedImpl(const TType* baseType, TStringRef tag);

    const ITypeInfoHelper& TypeHelper_;
    const TType* Type_;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13) 
class TTypePrinter2 : public TTypePrinter1 {
public:
    using TTypePrinter1::TTypePrinter1;

protected:
    void OnDecimal(ui8 precision, ui8 scale) final {
        OnDecimalImpl(precision, scale);
    }
};
#endif 

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15) 
class TTypePrinter3 : public TTypePrinter2 {
public:
    using TTypePrinter2::TTypePrinter2;

protected:
    void OnResource(TStringRef tag) final {
        OnResourceImpl(tag);
    }
};
#endif 

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21) 
class TTypePrinter4 : public TTypePrinter3 {
public:
    using TTypePrinter3::TTypePrinter3;
 
protected:
    void OnTagged(const TType* baseType, TStringRef tag) final {
        OnTaggedImpl(baseType, tag);
    }
}; 
#endif
 
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
using TTypePrinter = TTypePrinter4;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
using TTypePrinter = TTypePrinter3;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
using TTypePrinter = TTypePrinter2;
#else
using TTypePrinter = TTypePrinter1;
#endif


} 
} 
