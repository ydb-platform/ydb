#pragma once

#include "check_yson_token.h"
#include "infinite_entity.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

using TComplexTypeYsonScanner = std::function<
    void(NYson::TYsonPullParserCursor*, NYson::IYsonConsumer*)>;

////////////////////////////////////////////////////////////////////////////////

template <typename TApplier, typename TElement>
TComplexTypeYsonScanner CreateOptionalScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    TElement element)
{
    using namespace NYson;
    bool isElementNullable = descriptor.GetType()->AsOptionalTypeRef().IsElementNullable();

    if (isElementNullable) {
        return [
            element=std::move(element),
            descriptor=std::move(descriptor),
            applier=std::move(applier)
        ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
            const auto ysonType = (*cursor)->GetType();
            if (ysonType == EYsonItemType::EntityValue) {
                applier.OnEmptyOptional(consumer);
                cursor->Next();
            } else if (ysonType != EYsonItemType::BeginList) {
                ThrowUnexpectedYsonTokenException(descriptor, *cursor, {EYsonItemType::EntityValue, EYsonItemType::BeginList});
            } else {
                cursor->Next();
                applier.OnFilledOptional(element, cursor, consumer);

                EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
                cursor->Next();
            }
        };
    } else {
        return [
            element=std::move(element),
            descriptor=std::move(descriptor),
            applier=std::move(applier)
        ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
            auto ysonType = (*cursor)->GetType();
            if (ysonType == EYsonItemType::EntityValue) {
                applier.OnEmptyOptional(consumer);
                cursor->Next();
            } else {
                applier.OnFilledOptional(element, cursor, consumer);
            }
        };
    }
}

template <typename TApplier, typename TElement>
TComplexTypeYsonScanner CreateListScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    TElement element)
{
    using namespace NYson;
    return [
        element=std::move(element),
        descriptor=std::move(descriptor),
        applier=std::move(applier)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        applier.OnListBegin(consumer);
        while ((*cursor)->GetType() != EYsonItemType::EndList) {
            applier.OnListItem(element, cursor, consumer);
        }
        applier.OnListEnd(consumer);

        // We are on list end token since we exited while loop.
        cursor->Next();
    };
}

template <typename TApplier, typename TElement>
TComplexTypeYsonScanner CreateTupleScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    std::vector<TElement> elements)
{
    using namespace NYson;
    return [
        elements=std::move(elements),
        descriptor=std::move(descriptor),
        applier=std::move(applier)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        applier.OnTupleBegin(consumer);
        for (const auto& scanner : elements) {
            applier.OnTupleItem(scanner, cursor, consumer);
        }
        applier.OnTupleEnd(consumer);

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

template <typename TApplier, typename TField>
TComplexTypeYsonScanner CreateStructScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    std::vector<TField> fields)
{
    using namespace NYson;
    return [
        fields=std::move(fields),
        descriptor=std::move(descriptor),
        applier=std::move(applier)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        applier.OnStructBegin(consumer);
        for (auto it = fields.begin(), end = fields.end(); it != end; ++it) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::EndList) {
                TInfiniteEntity infiniteEntity;
                auto entityCursor = infiniteEntity.GetCursor();
                do {
                    applier.OnStructField(*it, entityCursor, consumer);
                    ++it;
                } while (it != end);
                break;
            }
            applier.OnStructField(*it, cursor, consumer);
        }
        applier.OnStructEnd(consumer);

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

template <typename TApplier, typename TAlternatives>
TComplexTypeYsonScanner CreateVariantScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    std::vector<TAlternatives> alternatives)
{
    using namespace NYson;
    return [
        descriptor=std::move(descriptor),
        applier=std::move(applier),
        alternatives=std::move(alternatives)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::Int64Value);
        auto tag = cursor->GetCurrent().UncheckedAsInt64();
        if (tag < 0) {
            THROW_ERROR_EXCEPTION(
                "Error while parsing %Qv: variant tag (%v) is negative",
                descriptor.GetDescription(),
                tag);
        }
        if (tag >= std::ssize(alternatives)) {
            THROW_ERROR_EXCEPTION(
                "Error while parsing %Qv: variant tag (%v) exceeds variant alternative count (%v)",
                descriptor.GetDescription(),
                tag,
                alternatives.size());
        }
        cursor->Next();

        applier.OnVariantAlternative(alternatives[tag], cursor, consumer);

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

template <typename TApplier, typename TElement>
TComplexTypeYsonScanner CreateDictScanner(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TApplier applier,
    TElement key,
    TElement value)
{
    using namespace NYson;
    return [
        descriptor=std::move(descriptor),
        applier=std::move(applier),
        key=std::move(key),
        value=std::move(value)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* consumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        applier.OnDictBegin(consumer);
        while ((*cursor)->GetType() != EYsonItemType::EndList) {
            EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            applier.OnKey(key, cursor, consumer);

            applier.OnValue(value, cursor, consumer);

            EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
            cursor->Next();
        }
        applier.OnDictEnd(consumer);

        // Skip end list token.
        cursor->Next();
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NComplexTypes
