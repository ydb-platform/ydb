#include "check_type_compatibility.h"

#include <yt/yt/client/table_client/logical_type.h>

using namespace NYT::NTableClient;

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

using TCompatibilityPair = std::pair<ESchemaCompatibility, TError>;

////////////////////////////////////////////////////////////////////////////////

static TCompatibilityPair CheckTypeCompatibilityImpl(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor);

static const TCompatibilityPair& MinCompatibility(const TCompatibilityPair& lhs, const TCompatibilityPair& rhs)
{
    if (lhs.first <= rhs.first) {
        return lhs;
    } else {
        return rhs;
    }
}

static TCompatibilityPair CreateResultPair(
    ESchemaCompatibility compatibility,
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor)
{
    if (compatibility == ESchemaCompatibility::FullyCompatible) {
        return {compatibility, TError()};
    }
    return {
        compatibility,
        TError(
            "Type of %Qv field is modified in non backward compatible manner",
            oldDescriptor.GetDescription())
            << TErrorAttribute("old_type", ToString(*oldDescriptor.GetType()))
            << TErrorAttribute("new_type", ToString(*newDescriptor.GetType()))
    };
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESimpleTypeClass,
    (Int)
    (Uint)
    (Floating)
    (String)
);

template <ESimpleTypeClass typeClass>
static int GetSimpleTypeRank(ESimpleLogicalValueType type)
{
    if constexpr (typeClass == ESimpleTypeClass::Int) {
        switch (type) {
            case ESimpleLogicalValueType::Int8:
                return 8;
            case ESimpleLogicalValueType::Int16:
                return 16;
            case ESimpleLogicalValueType::Int32:
                return 32;
            case ESimpleLogicalValueType::Int64:
                return 64;
            default:
                return -1;
        }
    } else if constexpr (typeClass == ESimpleTypeClass::Uint) {
        switch (type) {
            case ESimpleLogicalValueType::Uint8:
                return 8;
            case ESimpleLogicalValueType::Uint16:
                return 16;
            case ESimpleLogicalValueType::Uint32:
                return 32;
            case ESimpleLogicalValueType::Uint64:
                return 64;
            default:
                return -1;
        }
    } else if constexpr (typeClass == ESimpleTypeClass::Floating) {
        switch (type) {
            case ESimpleLogicalValueType::Float:
                return 32;
            case ESimpleLogicalValueType::Double:
                return 64;
            default:
                return -1;
        }
    } else if constexpr (typeClass == ESimpleTypeClass::String) {
        switch (type) {
            case ESimpleLogicalValueType::Utf8:
                return 1;
            case ESimpleLogicalValueType::String:
                return 2;
            default:
                return -1;
        }
    } else {
        // Poor man static_assert(false).
        static_assert(typeClass == ESimpleTypeClass::Int);
    }
}

template <ESimpleTypeClass typeClass>
static constexpr ESchemaCompatibility CheckCompatibilityUsingClass(ESimpleLogicalValueType oldType, ESimpleLogicalValueType newType)
{
    int oldRank = GetSimpleTypeRank<typeClass>(oldType);
    if (oldRank <= 0) {
        THROW_ERROR_EXCEPTION("Internal error; unexpected rank %v of type %Qlv",
            oldRank,
            oldType);
    }
    int newRank = GetSimpleTypeRank<typeClass>(newType);
    if (newRank < 0) {
        return ESchemaCompatibility::Incompatible;
    }
    if (newRank >= oldRank) {
        return ESchemaCompatibility::FullyCompatible;
    } else {
        return ESchemaCompatibility::RequireValidation;
    }
}

////////////////////////////////////////////////////////////////////////////////

static TCompatibilityPair CheckTypeCompatibilitySimple(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor)
{
    auto oldElement = oldDescriptor.GetType()->AsSimpleTypeRef().GetElement();
    auto newElement = newDescriptor.GetType()->AsSimpleTypeRef().GetElement();

    if (newElement == ESimpleLogicalValueType::Any) {
        // N.B. for historical reasons a bunch of types could be cast to Any type.
        ESchemaCompatibility result;
        switch (oldElement) {
            case ESimpleLogicalValueType::Int8:
            case ESimpleLogicalValueType::Int16:
            case ESimpleLogicalValueType::Int32:
            case ESimpleLogicalValueType::Int64:

            case ESimpleLogicalValueType::Uint8:
            case ESimpleLogicalValueType::Uint16:
            case ESimpleLogicalValueType::Uint32:
            case ESimpleLogicalValueType::Uint64:

            case ESimpleLogicalValueType::Float:
            case ESimpleLogicalValueType::Double:

            case ESimpleLogicalValueType::String:
            case ESimpleLogicalValueType::Utf8:

            case ESimpleLogicalValueType::Boolean:

            case ESimpleLogicalValueType::Date:
            case ESimpleLogicalValueType::Datetime:
            case ESimpleLogicalValueType::Timestamp:
            case ESimpleLogicalValueType::Interval:

            case ESimpleLogicalValueType::Null:
            case ESimpleLogicalValueType::Void:

            case ESimpleLogicalValueType::Date32:
            case ESimpleLogicalValueType::Datetime64:
            case ESimpleLogicalValueType::Timestamp64:
            case ESimpleLogicalValueType::Interval64:

            case ESimpleLogicalValueType::Any:
                result = ESchemaCompatibility::FullyCompatible;
                break;

            default:
                result = ESchemaCompatibility::Incompatible;
                break;
        }
        return CreateResultPair(
            result,
            oldDescriptor,
            newDescriptor);
    }

    switch (oldElement) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Int64:
            return CreateResultPair(
                CheckCompatibilityUsingClass<ESimpleTypeClass::Int>(oldElement, newElement),
                oldDescriptor,
                newDescriptor);

        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
        case ESimpleLogicalValueType::Uint64:
            return CreateResultPair(
                CheckCompatibilityUsingClass<ESimpleTypeClass::Uint>(oldElement, newElement),
                oldDescriptor,
                newDescriptor);

        case ESimpleLogicalValueType::Float:
        case ESimpleLogicalValueType::Double:
            return CreateResultPair(
                CheckCompatibilityUsingClass<ESimpleTypeClass::Floating>(oldElement, newElement),
                oldDescriptor,
                newDescriptor);

        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::String:
            return CreateResultPair(
                CheckCompatibilityUsingClass<ESimpleTypeClass::String>(oldElement, newElement),
                oldDescriptor,
                newDescriptor);

        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
        case ESimpleLogicalValueType::Boolean:
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
        case ESimpleLogicalValueType::Interval:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Uuid:
        case ESimpleLogicalValueType::Date32:
        case ESimpleLogicalValueType::Datetime64:
        case ESimpleLogicalValueType::Timestamp64:
        case ESimpleLogicalValueType::Interval64:
        case ESimpleLogicalValueType::Any: {
            const auto compatibility =
                oldElement == newElement ? ESchemaCompatibility::FullyCompatible : ESchemaCompatibility::Incompatible;
            return CreateResultPair(compatibility, oldDescriptor, newDescriptor);
        }
    }
    THROW_ERROR_EXCEPTION("Internal error: unknown logical type: %Qlv",
        oldElement);
}

TCompatibilityPair CheckElementsCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    bool allowNewElements)
{
    const auto oldSize = std::ssize(oldDescriptor.GetType()->GetElements());
    const auto newSize = std::ssize(newDescriptor.GetType()->GetElements());
    if (oldSize > newSize) {
        return {
            ESchemaCompatibility::Incompatible,
            TError("Some elements of %Qv are removed",
                oldDescriptor.GetDescription()),
        };
    }
    if (!allowNewElements && oldSize != newSize) {
        return {
            ESchemaCompatibility::Incompatible,
            TError("Added new elements to tuple %Qv",
                oldDescriptor.GetDescription()),
        };
    }
    auto result = std::pair(ESchemaCompatibility::FullyCompatible, TError());
    for (int i = 0; result.first != ESchemaCompatibility::Incompatible && i < oldSize; ++i) {
        result = MinCompatibility(
            result,
            CheckTypeCompatibilityImpl(oldDescriptor.Element(i), newDescriptor.Element(i)));
    }
    return result;
}

TCompatibilityPair CheckFieldsCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    bool checkNewFieldsNullability)
{
    const auto& oldFields = oldDescriptor.GetType()->GetFields();
    const auto& newFields = newDescriptor.GetType()->GetFields();
    if (oldFields.size() > newFields.size()) {
        return {
            ESchemaCompatibility::Incompatible,
            TError("Some members of %Qv are removed",
                oldDescriptor.GetDescription()),
        };
    }

    int fieldIndex = 0;
    //
    auto result = std::pair(ESchemaCompatibility::FullyCompatible, TError());
    for (; fieldIndex < std::ssize(oldFields) && result.first != ESchemaCompatibility::Incompatible; ++fieldIndex) {
        const auto& oldName = oldFields[fieldIndex].Name;
        const auto& newName = newFields[fieldIndex].Name;
        if (oldName != newName) {
            result = {
                ESchemaCompatibility::Incompatible,
                TError(
                    "Member name mismatch in %Qv: old name %Qv, new name %Qv",
                    oldDescriptor.GetDescription(),
                    oldName,
                    newName)
            };
        } else {
            const auto& oldFieldDescriptor = oldDescriptor.Field(fieldIndex);
            const auto& newFieldDescriptor = newDescriptor.Field(fieldIndex);
            auto currentCompatibility = CheckTypeCompatibilityImpl(oldFieldDescriptor, newFieldDescriptor);
            result = MinCompatibility(result, currentCompatibility);
        }
    }

    // All added fields must be nullable
    if (checkNewFieldsNullability) {
        for (; fieldIndex < std::ssize(newFields) && result.first != ESchemaCompatibility::Incompatible; ++fieldIndex) {
            const auto& newFieldDescriptor = newDescriptor.Field(fieldIndex);
            if (!newFieldDescriptor.GetType()->IsNullable()) {
                result = {
                    ESchemaCompatibility::Incompatible,
                    TError(
                        "Newly added member %Qv is not optional",
                        newFieldDescriptor.GetDescription())
                        << TErrorAttribute("new_type", ToString(*newFieldDescriptor.GetType()))
                };
            }
        }
    }
    return result;
}

TCompatibilityPair CheckDictTypeCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor)
{
    auto keyCompatibility = CheckTypeCompatibilityImpl(
        oldDescriptor.DictKey(),
        newDescriptor.DictKey());
    if (keyCompatibility.first == ESchemaCompatibility::Incompatible) {
        return keyCompatibility;
    }
    const auto valueCompatibility = CheckTypeCompatibilityImpl(
        oldDescriptor.DictValue(),
        newDescriptor.DictValue());
    return MinCompatibility(keyCompatibility, valueCompatibility);
}

TCompatibilityPair CheckDecimalTypeCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor)
{
    if (*oldDescriptor.GetType() == *newDescriptor.GetType()) {
        return {ESchemaCompatibility::FullyCompatible, {}};
    } else {
        return CreateResultPair(ESchemaCompatibility::Incompatible, oldDescriptor, newDescriptor);
    }
}

std::pair<TComplexTypeFieldDescriptor, int> UnwrapOptionalAndTagged(const TComplexTypeFieldDescriptor& descriptor)
{
    int nesting = 0;
    auto current = descriptor;
    while (true) {
        const auto metatype = current.GetType()->GetMetatype();
        if (metatype == ELogicalMetatype::Optional) {
            ++nesting;
            current = current.OptionalElement();
        } else if (metatype == ELogicalMetatype::Tagged) {
            current = current.TaggedElement();
        } else {
            break;
        }
    }
    return {current, nesting};
}

static TCompatibilityPair CheckTypeCompatibilityImpl(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor)
{
    const auto oldMetatype = oldDescriptor.GetType()->GetMetatype();
    const auto newMetatype = newDescriptor.GetType()->GetMetatype();

    if (oldMetatype == ELogicalMetatype::Optional ||
        oldMetatype == ELogicalMetatype::Tagged ||
        newMetatype == ELogicalMetatype::Optional ||
        newMetatype == ELogicalMetatype::Tagged)
    {
        const auto [oldElement, oldNesting] = UnwrapOptionalAndTagged(oldDescriptor);
        const auto [newElement, newNesting] = UnwrapOptionalAndTagged(newDescriptor);

        if (oldNesting == newNesting || oldNesting == 0 && newNesting == 1) {
            return CheckTypeCompatibilityImpl(oldElement, newElement);
        } else if (oldNesting == 1 && newNesting == 0) {
            auto elementCompatibility = CheckTypeCompatibilityImpl(oldElement, newElement);
            return MinCompatibility(
                elementCompatibility,
                CreateResultPair(ESchemaCompatibility::RequireValidation, oldElement, newElement));
        } else {
            return CreateResultPair(ESchemaCompatibility::Incompatible, oldElement, newElement);
        }
    }

    if (oldMetatype != newMetatype) {
        return CreateResultPair(ESchemaCompatibility::Incompatible, oldDescriptor, newDescriptor);
    }

    switch (oldMetatype) {
        case ELogicalMetatype::Simple:
            return CheckTypeCompatibilitySimple(oldDescriptor, newDescriptor);
        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tagged:
            // Optional and Tagged cases were checked earlier in this function.
            THROW_ERROR_EXCEPTION("Internal error; unexpected optional or tagged");
        case ELogicalMetatype::List:
            return CheckTypeCompatibilityImpl(oldDescriptor.ListElement(), newDescriptor.ListElement());
        case ELogicalMetatype::VariantStruct:
            return CheckFieldsCompatibility(
                oldDescriptor,
                newDescriptor,
                /*checkNewFieldsNullability*/ false);
        case ELogicalMetatype::Struct:
            return CheckFieldsCompatibility(
                oldDescriptor,
                newDescriptor,
                /*checkNewFieldsNullability*/ true);
        case ELogicalMetatype::Tuple:
            return CheckElementsCompatibility(
                oldDescriptor,
                newDescriptor,
                /*allowNewElements*/ false);
        case ELogicalMetatype::VariantTuple:
            return CheckElementsCompatibility(
                oldDescriptor,
                newDescriptor,
                /*allowNewElement*/ true);
        case ELogicalMetatype::Dict:
            return CheckDictTypeCompatibility(oldDescriptor, newDescriptor);
        case ELogicalMetatype::Decimal:
            return CheckDecimalTypeCompatibility(oldDescriptor, newDescriptor);
    }
    THROW_ERROR_EXCEPTION("Internal error; unexpected metatype: %Qlv", oldMetatype);
}

TCompatibilityPair CheckTypeCompatibility(
    const NYT::NTableClient::TLogicalTypePtr& oldType,
    const NYT::NTableClient::TLogicalTypePtr& newType)
{
    return CheckTypeCompatibilityImpl(TComplexTypeFieldDescriptor(oldType), TComplexTypeFieldDescriptor(newType));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
