#include "check_type_compatibility.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

using TCompatibilityPair = std::pair<ESchemaCompatibility, TError>;

////////////////////////////////////////////////////////////////////////////////

TCompatibilityPair CheckTypeCompatibilityImpl(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    const TTypeCompatibilityOptions& options);

const TCompatibilityPair& MinCompatibility(const TCompatibilityPair& lhs, const TCompatibilityPair& rhs)
{
    if (lhs.first <= rhs.first) {
        return lhs;
    } else {
        return rhs;
    }
}

TCompatibilityPair CreateResultPair(
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
int GetSimpleTypeRank(ESimpleLogicalValueType type)
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
constexpr ESchemaCompatibility CheckCompatibilityUsingClass(ESimpleLogicalValueType oldType, ESimpleLogicalValueType newType)
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

TCompatibilityPair CheckTypeCompatibilitySimple(
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
        case ESimpleLogicalValueType::TzDate:
        case ESimpleLogicalValueType::TzDatetime:
        case ESimpleLogicalValueType::TzTimestamp:
        case ESimpleLogicalValueType::TzDate32:
        case ESimpleLogicalValueType::TzDatetime64:
        case ESimpleLogicalValueType::TzTimestamp64:
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
    const TTypeCompatibilityOptions& options,
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
            CheckTypeCompatibilityImpl(
                oldDescriptor.Element(i),
                newDescriptor.Element(i),
                options));
    }
    return result;
}

TCompatibilityPair CheckFieldsCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    const TTypeCompatibilityOptions& options,
    ELogicalMetatype metatype)
{
    // Existing fields are mapped to their indices, removed fields are
    // mapped to null (in case of struct).
    auto buildStableNameToIndexMapping = [&] (const TComplexTypeFieldDescriptor& descriptor) {
        THashMap<std::string, std::optional<int>> result;

        const auto& type = descriptor.GetType();
        const auto& fields = type->GetFields();
        for (int fieldIndex = 0; fieldIndex < std::ssize(fields); ++fieldIndex) {
            EmplaceOrCrash(result, fields[fieldIndex].StableName, fieldIndex);
        }

        if (metatype == ELogicalMetatype::Struct) {
            for (const auto& stableName : type->GetRemovedFieldStableNames()) {
                EmplaceOrCrash(result, stableName, std::nullopt);
            }
        }
        return result;
    };
    auto stableNameToOldIndex = buildStableNameToIndexMapping(oldDescriptor);
    auto stableNameToNewIndex = buildStableNameToIndexMapping(newDescriptor);

    const auto& oldFields = oldDescriptor.GetType()->GetFields();
    const auto& newFields = newDescriptor.GetType()->GetFields();

    auto result = std::pair(ESchemaCompatibility::FullyCompatible, TError());

    auto onUnknownRemovedField = [&] (const auto& stableName) {
        if (options.IgnoreUnknownRemovedFieldNames) {
            return;
        }
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Newly added name at \"removed_fields\" of %Qv does not refer to "
                "any previously existing field",
                oldDescriptor.GetDescription())
                << TErrorAttribute("added_name", stableName)
        };
    };

    auto onNewField = [&] (int newIndex) {
        const auto& newFieldDescriptor = newDescriptor.Field(newIndex);
        if (metatype == ELogicalMetatype::VariantStruct) {
            return;
        }
        if (newFieldDescriptor.GetType()->IsNullable()) {
            return;
        }
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Newly added member %Qv is not optional",
                newFieldDescriptor.GetDescription())
                << TErrorAttribute("new_type", ToString(*newFieldDescriptor.GetType()))
        };
    };

    auto onRemovedField = [&] (auto oldIndex) {
        if (!options.AllowStructFieldRemoval) {
            result = {
                ESchemaCompatibility::Incompatible,
                TError(
                    "Field of %Qv cannot be removed since field removal is disabled",
                    oldDescriptor.GetDescription())
                    << TErrorAttribute("field_name", oldFields[oldIndex].Name),
            };
            return;
        }
        if (metatype == ELogicalMetatype::Struct) {
            return;
        }
        result = MinCompatibility(
            result,
            {
                ESchemaCompatibility::RequireValidation,
                TError(
                    "Field of variant struct %Qv was removed",
                    oldDescriptor.GetDescription())
                    << TErrorAttribute("field_name", oldFields[oldIndex].Name),
            });
    };

    auto onDroppedField = [&] (auto oldIndex, const auto& stableName) {
        if (metatype == ELogicalMetatype::VariantStruct) {
            return onRemovedField(oldIndex);
        }
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Field of %Qv cannot be simply dropped; instead, its stable name "
                "must be added to \"removed_fields\" list",
                oldDescriptor.GetDescription())
                << TErrorAttribute("field_name", oldFields[oldIndex].Name)
                << TErrorAttribute("field_stable_name", stableName),
        };
    };

    auto onDroppedRemovedField = [&] (const auto& stableName) {
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Removing items from \"removed_fields\" of %Qv is not allowed",
                oldDescriptor.GetDescription())
                << TErrorAttribute("removed_name", stableName)
        };
    };

    auto onUndeadField = [&] (const auto& stableName) {
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Removed field name of %Qv cannot be reused as a stable name of an "
                "existing field",
                oldDescriptor.GetDescription())
                << TErrorAttribute("removed_field_name", stableName),
        };
    };

    auto onRenamedField = [&] (auto oldIndex, auto newIndex, const auto& stableName) {
        if (options.AllowStructFieldRenaming) {
            return;
        }
        result = {
            ESchemaCompatibility::Incompatible,
            TError(
                "Field of %Qv cannot be renamed since field renaming is disabled",
                oldDescriptor.GetDescription())
                << TErrorAttribute("field_old_name", oldFields[oldIndex].Name)
                << TErrorAttribute("field_new_name", newFields[newIndex].Name)
                << TErrorAttribute("field_stable_name", stableName),
        };
    };

    bool areSomeFieldsRetained = false;
    auto onRetainedField = [&] (auto oldIndex, auto newIndex) {
        areSomeFieldsRetained = true;
        result = MinCompatibility(
            result,
            CheckTypeCompatibilityImpl(
                oldDescriptor.Field(oldIndex),
                newDescriptor.Field(newIndex),
                options));
    };

    // Each struct field can be in either of three states:
    //   a. Unknown (aka absent from schema entirely).
    //   b. Removed (present in removed field names, only for structs).
    //   c. Existing (present as a regular field).
    // Therefore, there is a total of 3 * 3 = 9 possible transitions.
    // Unknown -> unknown as well as removed -> removed transitions are not
    // particularly interesting, whereas the other 7 are handled below.

    // Unknown -> * transitions.
    for (const auto& [stableName, newIndex] : stableNameToNewIndex) {
        if (stableNameToOldIndex.contains(stableName)) {
            continue;
        }

        if (newIndex.has_value()) {
            // Unknown -> existing.
            onNewField(*newIndex);
        } else {
            // Unknown -> removed.
            onUnknownRemovedField(stableName);
        }

        if (result.first == ESchemaCompatibility::Incompatible) {
            return result;
        }
    }

    // Removed|existing -> * transitions.
    for (const auto& [stableName, oldIndex] : stableNameToOldIndex) {
        if (auto it = stableNameToNewIndex.find(stableName); it != stableNameToNewIndex.end()) {
            auto newIndex = it->second;

            if (oldIndex.has_value() && !newIndex.has_value()) {
                // Existing -> removed.
                onRemovedField(*oldIndex);
            } else if (!oldIndex.has_value() && newIndex.has_value()) {
                // Removed -> existing.
                onUndeadField(stableName);
            } else if (oldIndex.has_value() && newIndex.has_value()) {
                // Existing -> existing.
                if (oldFields[*oldIndex].Name != newFields[*newIndex].Name) {
                    onRenamedField(*oldIndex, *newIndex, stableName);
                }
                onRetainedField(*oldIndex, *newIndex);
            }
        } else if (oldIndex.has_value()) {
            // Existing -> unknown.
            onDroppedField(*oldIndex, stableName);
        } else {
            // Removed -> unknown.
            onDroppedRemovedField(stableName);
        }

        if (result.first == ESchemaCompatibility::Incompatible) {
            return result;
        }
    }

    if (metatype == ELogicalMetatype::VariantStruct && !areSomeFieldsRetained) {
        return {
            ESchemaCompatibility::Incompatible,
            TError(
                "None of variant struct %Qv fields are retained",
                oldDescriptor.GetDescription()),
        };
    }

    if (options.AllowStructFieldRenaming && options.AllowStructFieldRemoval) {
        return result;
    }

    // Unless both renaming and removal are allowed, the following restrictions apply:
    //   a. Existing fields must retain the original order.
    //   b. All newly added fields must be at the end.
    std::optional<int> lastOldIndex;
    const TStructField* lastNewField = nullptr;
    for (const auto& field : newFields) {
        auto oldIndexIt = stableNameToOldIndex.find(field.StableName);

        // Field is newly created.
        if (oldIndexIt == stableNameToOldIndex.end()) {
            lastNewField = &field;
            continue;
        }

        // Field already existed. Old index cannot be null, since reusing removed
        // field names is forbidden (see onUndeadField above).
        auto oldIndex = oldIndexIt->second;
        YT_VERIFY(oldIndex.has_value());

        if (lastNewField) {
            return {
                ESchemaCompatibility::Incompatible,
                TError(
                    "Newly added field of %Qv cannot preceed already existing field "
                    "unless both field renaming and removal are enabled",
                    oldDescriptor.GetDescription())
                    << TErrorAttribute("new_field_name", lastNewField->Name)
                    << TErrorAttribute("existing_field_name", field.Name),
            };
        }

        if (lastOldIndex.has_value() && *lastOldIndex > *oldIndex) {
            return {
                ESchemaCompatibility::Incompatible,
                TError(
                    "Fields of %Qv cannot be reordered unless both "
                    "field renaming and removal are enabled",
                    oldDescriptor.GetDescription())
                    << TErrorAttribute("first_field_name", field.Name)
                    << TErrorAttribute("second_field_name", oldFields[*lastOldIndex].Name),
            };
        }
        lastOldIndex = oldIndex;
    }

    return result;
}

TCompatibilityPair CheckDictTypeCompatibility(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    const TTypeCompatibilityOptions& options)
{
    auto keyCompatibility = CheckTypeCompatibilityImpl(
        oldDescriptor.DictKey(),
        newDescriptor.DictKey(),
        options);
    if (keyCompatibility.first == ESchemaCompatibility::Incompatible) {
        return keyCompatibility;
    }
    const auto valueCompatibility = CheckTypeCompatibilityImpl(
        oldDescriptor.DictValue(),
        newDescriptor.DictValue(),
        options);
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

TCompatibilityPair CheckTypeCompatibilityImpl(
    const TComplexTypeFieldDescriptor& oldDescriptor,
    const TComplexTypeFieldDescriptor& newDescriptor,
    const TTypeCompatibilityOptions& options)
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
            return CheckTypeCompatibilityImpl(oldElement, newElement, options);
        } else if (oldNesting == 1 && newNesting == 0) {
            return MinCompatibility(
                CheckTypeCompatibilityImpl(oldElement, newElement, options),
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
            return CheckTypeCompatibilityImpl(
                oldDescriptor.ListElement(),
                newDescriptor.ListElement(),
                options);
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::Struct:
            return CheckFieldsCompatibility(
                oldDescriptor,
                newDescriptor,
                options,
                oldMetatype);
        case ELogicalMetatype::Tuple:
            return CheckElementsCompatibility(
                oldDescriptor,
                newDescriptor,
                options,
                /*allowNewElements*/ false);
        case ELogicalMetatype::VariantTuple:
            return CheckElementsCompatibility(
                oldDescriptor,
                newDescriptor,
                options,
                /*allowNewElement*/ true);
        case ELogicalMetatype::Dict:
            return CheckDictTypeCompatibility(oldDescriptor, newDescriptor, options);
        case ELogicalMetatype::Decimal:
            return CheckDecimalTypeCompatibility(oldDescriptor, newDescriptor);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::pair<TComplexTypeFieldDescriptor, int> UnwrapOptionalAndTagged(
    const TComplexTypeFieldDescriptor& descriptor)
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

TCompatibilityPair CheckTypeCompatibility(
    const NYT::NTableClient::TLogicalTypePtr& oldType,
    const NYT::NTableClient::TLogicalTypePtr& newType,
    const TTypeCompatibilityOptions& options)
{
    return CheckTypeCompatibilityImpl(
        TComplexTypeFieldDescriptor(oldType),
        TComplexTypeFieldDescriptor(newType),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
