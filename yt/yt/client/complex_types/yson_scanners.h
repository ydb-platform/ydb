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
