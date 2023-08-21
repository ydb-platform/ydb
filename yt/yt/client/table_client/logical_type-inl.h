#ifndef LOGICAL_TYPE_INL_H_
#error "Direct inclusion of this file is not allowed, include logical_type.h"
// For the sake of sane code completion.
#include "logical_type.h"
#endif
#undef LOGICAL_TYPE_INL_H_

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ELogicalMetatype TLogicalType::GetMetatype() const
{
    return Metatype_;
}

const TSimpleLogicalType& TLogicalType::UncheckedAsSimpleTypeRef() const
{
    return static_cast<const TSimpleLogicalType&>(*this);
}

const TDecimalLogicalType& TLogicalType::UncheckedAsDecimalTypeRef() const
{
    return static_cast<const TDecimalLogicalType&>(*this);
}

const TOptionalLogicalType& TLogicalType::UncheckedAsOptionalTypeRef() const
{
    return static_cast<const TOptionalLogicalType&>(*this);
}

const TListLogicalType& TLogicalType::UncheckedAsListTypeRef() const
{
    return static_cast<const TListLogicalType&>(*this);
}

const TStructLogicalType& TLogicalType::UncheckedAsStructTypeRef() const
{
    return static_cast<const TStructLogicalType&>(*this);
}

const TTupleLogicalType& TLogicalType::UncheckedAsTupleTypeRef() const
{
    return static_cast<const TTupleLogicalType&>(*this);
}

const TVariantTupleLogicalType& TLogicalType::UncheckedAsVariantTupleTypeRef() const
{
    return static_cast<const TVariantTupleLogicalType&>(*this);
}

const TVariantStructLogicalType& TLogicalType::UncheckedAsVariantStructTypeRef() const
{
    return static_cast<const TVariantStructLogicalType&>(*this);
}

const TDictLogicalType& TLogicalType::UncheckedAsDictTypeRef() const
{
    return static_cast<const TDictLogicalType&>(*this);
}

const TTaggedLogicalType& TLogicalType::UncheckedAsTaggedTypeRef() const
{
    return static_cast<const TTaggedLogicalType&>(*this);
}

////////////////////////////////////////////////////////////////////////////////

int TDecimalLogicalType::GetPrecision() const
{
    return Precision_;
}

int TDecimalLogicalType::GetScale() const
{
    return Scale_;
}

////////////////////////////////////////////////////////////////////////////////

const TLogicalTypePtr& TOptionalLogicalType::GetElement() const
{
    return Element_;
}

bool TOptionalLogicalType::IsElementNullable() const
{
    return ElementIsNullable_;
}

////////////////////////////////////////////////////////////////////////////////

ESimpleLogicalValueType TSimpleLogicalType::GetElement() const
{
    return Element_;
}

////////////////////////////////////////////////////////////////////////////////

const TLogicalTypePtr& TListLogicalType::GetElement() const
{
    return Element_;
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TStructField>& TStructLogicalTypeBase::GetFields() const
{
    return Fields_;
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TLogicalTypePtr>& TTupleLogicalTypeBase::GetElements() const
{
    return Elements_;
}

////////////////////////////////////////////////////////////////////////////////

const TLogicalTypePtr& TDictLogicalType::GetKey() const
{
    return Key_;
}

const TLogicalTypePtr& TDictLogicalType::GetValue() const
{
    return Value_;
}

////////////////////////////////////////////////////////////////////////////////

const TString& TTaggedLogicalType::GetTag() const
{
    return Tag_;
}

const TLogicalTypePtr& TTaggedLogicalType::GetElement() const
{
    return Element_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient