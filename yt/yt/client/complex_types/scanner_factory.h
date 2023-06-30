#pragma once

#include "check_yson_token.h"
#include "infinite_entity.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

// Helper class to generate efficient scanners of complex values.
template <typename... TArgs>
class TScannerFactory
{
public:
    using TScanner = TComplexTypeYsonScanner<TArgs...>;

    template <typename TApplier, typename TElement>
    static TScanner CreateOptionalScanner(
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
            ] (TYsonPullParserCursor* cursor, TArgs... args) {
                const auto ysonType = (*cursor)->GetType();
                if (ysonType == EYsonItemType::EntityValue) {
                    applier.OnEmptyOptional(args...);
                    cursor->Next();
                } else if (ysonType != EYsonItemType::BeginList) {
                    ThrowUnexpectedYsonTokenException(descriptor, *cursor, {EYsonItemType::EntityValue, EYsonItemType::BeginList});
                } else {
                    cursor->Next();
                    applier.OnFilledOptional(element, cursor, args...);

                    EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
                    cursor->Next();
                }
            };
        } else {
            return [
                element=std::move(element),
                descriptor=std::move(descriptor),
                applier=std::move(applier)
            ] (TYsonPullParserCursor* cursor, TArgs... args) {
                auto ysonType = (*cursor)->GetType();
                if (ysonType == EYsonItemType::EntityValue) {
                    applier.OnEmptyOptional(args...);
                    cursor->Next();
                } else {
                    applier.OnFilledOptional(element, cursor, args...);
                }
            };
        }
    }

    template <typename TApplier, typename TElement>
    static TScanner CreateListScanner(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TApplier applier,
        TElement element)
    {
        using namespace NYson;
        return [
            element=std::move(element),
            descriptor=std::move(descriptor),
            applier=std::move(applier)
        ] (TYsonPullParserCursor* cursor, TArgs... args) {
            EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            applier.OnListBegin(args...);
            while ((*cursor)->GetType() != EYsonItemType::EndList) {
                applier.OnListItem(element, cursor, args...);
            }
            applier.OnListEnd(args...);

            // We are on list end token since we exited while loop
            cursor->Next();
        };
    }

    template <typename TApplier, typename TElement>
    static TScanner CreateTupleScanner(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TApplier applier,
        std::vector<TElement> elements)
    {
        using namespace NYson;
        return [
            elements=std::move(elements),
            descriptor=std::move(descriptor),
            applier=std::move(applier)
        ] (TYsonPullParserCursor* cursor, TArgs... args) {
            EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            applier.OnTupleBegin(args...);
            for (const auto& scanner : elements) {
                applier.OnTupleItem(scanner, cursor, args...);
            }
            applier.OnTupleEnd(args...);

            EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
            cursor->Next();
        };
    }

    template <typename TApplier, typename TField>
    static TScanner CreateStructScanner(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TApplier applier,
        std::vector<TField> fields)
    {
        using namespace NYson;
        return [
            fields=std::move(fields),
            descriptor=std::move(descriptor),
            applier=std::move(applier)
        ] (TYsonPullParserCursor* cursor, TArgs... args) {
            EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            applier.OnStructBegin(args...);
            for (auto it = fields.begin(), end = fields.end(); it != end; ++it) {
                if (cursor->GetCurrent().GetType() == EYsonItemType::EndList) {
                    TInfiniteEntity infiniteEntity;
                    auto entityCursor = infiniteEntity.GetCursor();
                    do {
                        applier.OnStructField(*it, entityCursor, args...);
                        ++it;
                    } while (it != end);
                    break;
                }
                applier.OnStructField(*it, cursor, args...);
            }
            applier.OnStructEnd(args...);

            EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
            cursor->Next();
        };
    }

    template <typename TApplier, typename TAlternatives>
    static TScanner CreateVariantScanner(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TApplier applier,
        std::vector<TAlternatives> alternatives)
    {
        using namespace NYson;
        return [
            descriptor=std::move(descriptor),
            applier=std::move(applier),
            alternatives=std::move(alternatives)
        ] (TYsonPullParserCursor* cursor, TArgs... args) {
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

            applier.OnVariantAlternative(alternatives[tag], cursor, args...);

            EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
            cursor->Next();
        };
    }

    template <typename TApplier, typename TElement>
    static TScanner CreateDictScanner(
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
        ] (TYsonPullParserCursor* cursor, TArgs... args) {
            EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            applier.OnDictBegin(args...);
            while ((*cursor)->GetType() != EYsonItemType::EndList) {
                EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
                cursor->Next();

                applier.OnKey(key, cursor, args...);

                applier.OnValue(value, cursor, args...);

                EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
                cursor->Next();
            }
            applier.OnDictEnd(args...);

            // Skip end list token
            cursor->Next();
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NComplexTypes
