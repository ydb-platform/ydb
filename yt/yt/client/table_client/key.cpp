#include "key.h"

#include "serialize.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_FATAL below.
YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TableClientKey");

////////////////////////////////////////////////////////////////////////////////

TKey::TKey(TUnversionedValueRange range)
    : Elements_(range)
{ }

TKey TKey::FromRow(TUnversionedRow row, std::optional<int> length)
{
    if (!row) {
        return TKey();
    }

    int keyLength = length.value_or(row.GetCount());
    YT_VERIFY(keyLength <= static_cast<int>(row.GetCount()));

    ValidateValueTypes(row.FirstNElements(keyLength));

    return TKey(row.FirstNElements(keyLength));
}

TKey TKey::FromRowUnchecked(TUnversionedRow row, std::optional<int> length)
{
    if (!row) {
        return TKey();
    }

    int keyLength = length.value_or(row.GetCount());
    YT_VERIFY(keyLength <= static_cast<int>(row.GetCount()));

#ifndef NDEBUG
    try {
        ValidateValueTypes(row.FirstNElements(keyLength));
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key from row");
    }
#endif

    return TKey(row.FirstNElements(keyLength));
}

TKey::operator bool() const
{
    return static_cast<bool>(Elements_);
}

TUnversionedOwningRow TKey::AsOwningRow() const
{
    return *this ? TUnversionedOwningRow(Elements()) : TUnversionedOwningRow();
}

const TUnversionedValue& TKey::operator[](int index) const
{
    return Elements_[index];
}

int TKey::GetLength() const
{
    return std::ssize(Elements_);
}

const TUnversionedValue* TKey::Begin() const
{
    return Elements_.begin();
}

const TUnversionedValue* TKey::End() const
{
    return Elements_.end();
}

TUnversionedValueRange TKey::Elements() const
{
    return Elements_;
}

void TKey::ValidateValueTypes(TUnversionedValueRange range)
{
    for (const auto& value : range) {
        ValidateDataValueType(value.Type);
    }
}

void TKey::Persist(const TPersistenceContext& context)
{
    if (context.IsSave()) {
        auto representation = *this
            ? SerializeToString(Elements())
            : SerializedNullRow;
        NYT::Save(context.SaveContext(), representation);
    } else {
        TUnversionedRow row;
        Load(context.LoadContext(), row);
        if (row) {
            // Row lifetime is ensured by row buffer in load context.
            Elements_ = row.Elements();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TKey& lhs, const TKey& rhs)
{
    if (!lhs || !rhs) {
        return static_cast<bool>(lhs) == static_cast<bool>(rhs);
    }

    return CompareValueRanges(lhs.Elements(), rhs.Elements()) == 0;
}

void FormatValue(TStringBuilderBase* builder, const TKey& key, TStringBuf /*spec*/)
{
    if (key) {
        builder->AppendFormat("[%v]", JoinToString(key.Begin(), key.End()));
    } else {
        builder->AppendString("#");
    }
}

void Serialize(const TKey& key, IYsonConsumer* consumer)
{
    if (key) {
        BuildYsonFluently(consumer)
            .DoListFor(MakeRange(key.Begin(), key.End()), [&] (TFluentList fluent, const TUnversionedValue& value) {
                fluent
                    .Item()
                    .Value(value);
            });
    } else {
        BuildYsonFluently(consumer)
            .Entity();
    }
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow LegacyKeyToKeyFriendlyOwningRow(TUnversionedRow row, int keyLength)
{
    if (!row) {
        return TUnversionedOwningRow();
    }

    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < keyLength; ++index) {
        TUnversionedValue value;
        if (index < static_cast<int>(row.GetCount())) {
            value = row[index];
            if (value.Type == EValueType::Min || value.Type == EValueType::Max) {
                value.Type = EValueType::Null;
            }
        } else {
            value = MakeUnversionedNullValue();
        }
        builder.AddValue(value);
    }
    auto result = builder.FinishRow();
    YT_VERIFY(result.GetCount() == keyLength);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
