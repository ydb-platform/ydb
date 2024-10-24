#include "merge_complex_types.h"

#include "check_type_compatibility.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NComplexTypes {

using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TLogicalTypePtr> MergeTupleTypes(
    const TComplexTypeFieldDescriptor& firstDescriptor,
    const TComplexTypeFieldDescriptor& secondDescriptor)
{
    YT_VERIFY(firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Tuple
        || firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantTuple);

    YT_VERIFY(firstDescriptor.GetType()->GetMetatype() == secondDescriptor.GetType()->GetMetatype());

    auto allowNewElements = firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantTuple;

    auto firstSize = std::ssize(firstDescriptor.GetType()->GetElements());
    auto secondSize = std::ssize(secondDescriptor.GetType()->GetElements());

    if (firstSize > secondSize) {
        return MergeTupleTypes(
            secondDescriptor,
            firstDescriptor);
    }

    if (!allowNewElements && firstSize != secondSize) {
        THROW_ERROR_EXCEPTION(
            "Tuple type of fields %Qv and %Qv of different size cannot be merged",
            firstDescriptor.GetDescription(),
            secondDescriptor.GetDescription());
    }

    std::vector<TLogicalTypePtr> resultElements;
    resultElements.reserve(secondSize);

    int elementIndex = 0;
    for (; elementIndex < firstSize; ++elementIndex) {
        auto mergedType = MergeTypes(
            firstDescriptor.Element(elementIndex).GetType(),
            secondDescriptor.Element(elementIndex).GetType());

        resultElements.push_back(std::move(mergedType));
    }
    for (; elementIndex < secondSize; ++elementIndex) {
        resultElements.push_back(secondDescriptor.Element(elementIndex).GetType());
    }
    return resultElements;
}

std::vector<TStructField> MergeStructTypes(
    const TComplexTypeFieldDescriptor& firstDescriptor,
    const TComplexTypeFieldDescriptor& secondDescriptor)
{
    YT_VERIFY(firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Struct
        || firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantStruct);

    YT_VERIFY(firstDescriptor.GetType()->GetMetatype() == secondDescriptor.GetType()->GetMetatype());

    auto firstFields = firstDescriptor.GetType()->GetFields();
    auto secondFields = secondDescriptor.GetType()->GetFields();

    auto firstSize = std::ssize(firstFields);
    auto secondSize = std::ssize(secondFields);

    if (firstSize > secondSize) {
        return MergeStructTypes(secondDescriptor, firstDescriptor);
    }

    auto makeNullability = firstDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Struct;

    std::vector<TStructField> resultFields;
    resultFields.reserve(secondSize);

    ssize_t fieldIndex = 0;
    for (; fieldIndex < firstSize; ++fieldIndex) {
        const auto& firstName = firstFields[fieldIndex].Name;
        const auto& secondName = secondFields[fieldIndex].Name;
        if (firstName != secondName) {
            THROW_ERROR_EXCEPTION(
                "Struct member name mismatch in %Qv",
                firstDescriptor.GetDescription())
                << TErrorAttribute("first_name", firstName)
                << TErrorAttribute("second_name", secondName);
        }
        const auto& firstFieldDescriptor = firstDescriptor.Field(fieldIndex);
        const auto& secondFieldDescriptor = secondDescriptor.Field(fieldIndex);
        try {
            auto mergedField = MergeTypes(
                firstFieldDescriptor.GetType(),
                secondFieldDescriptor.GetType());
            resultFields.push_back(TStructField{
                .Name = firstName,
                .Type = std::move(mergedField),
            });
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Struct member type mismatch in %Qv",
                firstDescriptor.GetDescription())
                << ex;
        }
    }

    for (; fieldIndex < secondSize; ++fieldIndex) {
        const auto& secondFieldDescriptor = secondDescriptor.Field(fieldIndex);
        if (!secondFieldDescriptor.GetType()->IsNullable() && makeNullability) {
            resultFields.push_back(TStructField{
                .Name = secondFields[fieldIndex].Name,
                .Type = New<TOptionalLogicalType>(secondFieldDescriptor.GetType()),
            });
        } else {
            resultFields.push_back(secondFields[fieldIndex]);
        }
    }

    return resultFields;
}

TLogicalTypePtr MergeDictTypes(
    const TComplexTypeFieldDescriptor& firstDescriptor,
    const TComplexTypeFieldDescriptor& secondDescriptor)
{
    auto mergedKey = MergeTypes(
        firstDescriptor.DictKey().GetType(),
        secondDescriptor.DictKey().GetType());

    auto mergedValue = MergeTypes(
        firstDescriptor.DictValue().GetType(),
        secondDescriptor.DictValue().GetType());

    return New<TDictLogicalType>(std::move(mergedKey), std::move(mergedValue));
}

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr UnwrapOptionalType(const TLogicalTypePtr& type)
{
    if (type->GetMetatype() == ELogicalMetatype::Optional) {
        auto descriptor = TComplexTypeFieldDescriptor(type);
        return descriptor.OptionalElement().GetType();
    }
    return type;
}

TLogicalTypePtr UnwrapTaggedType(const TLogicalTypePtr& type)
{
    if (type->GetMetatype() == ELogicalMetatype::Tagged) {
        auto descriptor = TComplexTypeFieldDescriptor(type);
        return descriptor.TaggedElement().GetType();
    }
    return type;
}

TString GetTag(const TLogicalTypePtr& type)
{
    return type->AsTaggedTypeRef().GetTag();
}

TString ExtractTagFromOneOfTypes(
    const TLogicalTypePtr& firstType,
    const TLogicalTypePtr& secondType)
{
    if (firstType->GetMetatype() == ELogicalMetatype::Tagged) {
        return GetTag(firstType);
    } else if (secondType->GetMetatype() == ELogicalMetatype::Tagged) {
        return GetTag(secondType);
    }
    YT_ABORT();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr MergeTypes(
    const TLogicalTypePtr& firstType,
    const TLogicalTypePtr& secondType)
{
    auto firstDescriptor = TComplexTypeFieldDescriptor(firstType);
    auto secondDescriptor = TComplexTypeFieldDescriptor(secondType);

    const auto firstMetatype = firstDescriptor.GetType()->GetMetatype();
    const auto secondMetatype = secondDescriptor.GetType()->GetMetatype();

    // It needs to handle tag before optional.
    if (firstMetatype == ELogicalMetatype::Tagged
        || secondMetatype == ELogicalMetatype::Tagged)
    {
        if (firstMetatype == ELogicalMetatype::Tagged
            && secondMetatype == ELogicalMetatype::Tagged
            && GetTag(firstType) != GetTag(secondType))
        {
                THROW_ERROR_EXCEPTION(
                    "The type tags do not match: first tag %Qv and second tag %Qv in %Qv",
                    GetTag(firstType),
                    GetTag(secondType),
                    firstDescriptor.GetDescription());
        }
        auto mergedType = MergeTypes(
            UnwrapTaggedType(firstType),
            UnwrapTaggedType(secondType));

        return New<TTaggedLogicalType>(
            ExtractTagFromOneOfTypes(firstType, secondType),
            std::move(mergedType));
    }

    if (firstMetatype == ELogicalMetatype::Optional
        || secondMetatype == ELogicalMetatype::Optional)
    {
        int firstLayerCount = UnwrapOptionalAndTagged(firstDescriptor).second;
        int secondLayerCount = UnwrapOptionalAndTagged(secondDescriptor).second;

        if (firstLayerCount != secondLayerCount && (firstLayerCount > 1 || secondLayerCount > 1)) {
            THROW_ERROR_EXCEPTION(
                "Type of fields %Qv and %Qv cannot be merged",
                firstDescriptor.GetDescription(),
                secondDescriptor.GetDescription())
                << TErrorAttribute("first_type", ToString(*firstDescriptor.GetType()))
                << TErrorAttribute("second_type", ToString(*secondDescriptor.GetType()));
        }

        auto mergedType = MergeTypes(
            UnwrapOptionalType(firstType),
            UnwrapOptionalType(secondType));

        return New<TOptionalLogicalType>(std::move(mergedType));
    }

    if (firstMetatype != secondMetatype) {
        THROW_ERROR_EXCEPTION(
            "Type of %Qv field cannot be merged: metatypes are incompatible",
            firstDescriptor.GetDescription())
            << TErrorAttribute("first_type", ToString(*firstDescriptor.GetType()))
            << TErrorAttribute("second_type", ToString(*secondDescriptor.GetType()));
    }

    switch (firstMetatype) {
        case ELogicalMetatype::Simple:
        {
            if (CheckTypeCompatibility(firstType, secondType).first == ESchemaCompatibility::FullyCompatible) {
                return secondType;
            }
            if (CheckTypeCompatibility(secondType, firstType).first == ESchemaCompatibility::FullyCompatible) {
                return firstType;
            }
            THROW_ERROR_EXCEPTION(
                "Type of fields %Qv and %Qv cannot be merged",
                firstDescriptor.GetDescription(),
                secondDescriptor.GetDescription())
                << TErrorAttribute("first_type", ToString(*firstDescriptor.GetType()))
                << TErrorAttribute("second_type", ToString(*secondDescriptor.GetType()));

        }
        case ELogicalMetatype::List:
        {
            auto mergedType = MergeTypes(
                firstType->AsListTypeRef().GetElement(),
                secondType->AsListTypeRef().GetElement());

            return New<TListLogicalType>(mergedType);
        }
        case ELogicalMetatype::VariantStruct:
        {
            auto mergedFields = MergeStructTypes(
                firstDescriptor,
                secondDescriptor);

            return New<TVariantStructLogicalType>(mergedFields);
        }

        case ELogicalMetatype::Struct:
        {
            auto mergedFields = MergeStructTypes(
                firstDescriptor,
                secondDescriptor);

            return New<TStructLogicalType>(mergedFields);
        }

        case ELogicalMetatype::Tuple:
        {
            auto mergedElements = MergeTupleTypes(
                firstDescriptor,
                secondDescriptor);

            return New<TTupleLogicalType>(mergedElements);
        }

        case ELogicalMetatype::VariantTuple:
        {
            auto mergedElements = MergeTupleTypes(
                firstDescriptor,
                secondDescriptor);

            return New<TVariantTupleLogicalType>(mergedElements);
        }

        case ELogicalMetatype::Dict:
            return MergeDictTypes(firstDescriptor, secondDescriptor);

        case ELogicalMetatype::Decimal:
        {
            if (*firstDescriptor.GetType() == *secondDescriptor.GetType()) {
                return firstType;
            } else {
                THROW_ERROR_EXCEPTION(
                    "Type of fields %Qv and %Qv cannot be merged",
                    firstDescriptor.GetDescription(),
                    secondDescriptor.GetDescription())
                    << TErrorAttribute("first_type", ToString(*firstDescriptor.GetType()))
                    << TErrorAttribute("second_type", ToString(*secondDescriptor.GetType()));
            }
        }

        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tagged:
            // Optional and Tagged cases were checked earlier in this function.
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
