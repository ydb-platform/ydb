#include "table_consumer.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/string/cast.h>

#include <cmath>
#include <variant>

namespace NYT::NTableClient {

using namespace NFormats;
using namespace NYson;
using namespace NConcurrency;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

TYsonToUnversionedValueConverter::TYsonToUnversionedValueConverter(
    const TYsonConverterConfig& config,
    IValueConsumer* valueConsumer)
    : TYsonToUnversionedValueConverter(config, std::vector<IValueConsumer*>{valueConsumer})
{ }

TYsonToUnversionedValueConverter::TYsonToUnversionedValueConverter(
    const TYsonConverterConfig& config,
    std::vector<IValueConsumer*> valueConsumers,
    int tableIndex)
    : ValueConsumers_(std::move(valueConsumers))
    , ValueWriter_(&ValueBuffer_)
    , ConvertedWriter_(&ConvertedBuffer_)
{
    SwitchToTable(tableIndex);

    for (size_t tableIndex = 0; tableIndex < ValueConsumers_.size(); ++tableIndex) {
        const auto& valueConsumer = ValueConsumers_[tableIndex];
        const auto& nameTable = valueConsumer->GetNameTable();

        for (const auto& column : valueConsumer->GetSchema()->Columns()) {
            const auto id = nameTable->GetIdOrRegisterName(column.Name());
            const auto key = std::pair(tableIndex, id);
            auto converter = CreateYsonClientToServerConverter(TComplexTypeFieldDescriptor(column), config);
            if (IsV3Composite(column.LogicalType())) {
                ComplexTypeConverters_.emplace(key, std::move(converter));
            } else if (converter) {
                SimpleValueConverters_.emplace(key, std::move(converter));
            }
        }
    }
}

IValueConsumer* TYsonToUnversionedValueConverter::SwitchToTable(int tableIndex)
{
    TableIndex_ = tableIndex;
    YT_VERIFY(0 <= tableIndex && tableIndex < std::ssize(ValueConsumers_));
    CurrentValueConsumer_ = ValueConsumers_[tableIndex];
    YT_VERIFY(CurrentValueConsumer_ != nullptr);
    return CurrentValueConsumer_;
}

void TYsonToUnversionedValueConverter::SetColumnIndex(int columnIndex)
{
    ColumnIndex_ = columnIndex;
}

void TYsonToUnversionedValueConverter::OnStringScalar(TStringBuf value)
{
    if (Depth_ == 0) {
        auto unversionedValue = MakeUnversionedStringValue(value, ColumnIndex_);
        if (!TryConvertAndFeedValueConsumer(unversionedValue)) {
            CurrentValueConsumer_->OnValue(unversionedValue);
        }
    } else {
        ValueWriter_.OnStringScalar(value);
    }
}

void TYsonToUnversionedValueConverter::OnInt64Scalar(i64 value)
{
    if (Depth_ == 0) {
        auto unversionedValue = MakeUnversionedInt64Value(value, ColumnIndex_);
        if (!TryConvertAndFeedValueConsumer(unversionedValue)) {
            CurrentValueConsumer_->OnValue(unversionedValue);
        }
    } else {
        ValueWriter_.OnInt64Scalar(value);
    }
}

void TYsonToUnversionedValueConverter::OnUint64Scalar(ui64 value)
{
    if (Depth_ == 0) {
        auto unversionedValue = MakeUnversionedUint64Value(value, ColumnIndex_);
        if (!TryConvertAndFeedValueConsumer(unversionedValue)) {
            CurrentValueConsumer_->OnValue(unversionedValue);
        }
    } else {
        ValueWriter_.OnUint64Scalar(value);
    }
}

void TYsonToUnversionedValueConverter::OnDoubleScalar(double value)
{
    if (Depth_ == 0) {
        CurrentValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnDoubleScalar(value);
    }
}

void TYsonToUnversionedValueConverter::OnBooleanScalar(bool value)
{
    if (Depth_ == 0) {
        CurrentValueConsumer_->OnValue(MakeUnversionedBooleanValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnBooleanScalar(value);
    }
}

void TYsonToUnversionedValueConverter::OnEntity()
{
    if (Depth_ == 0) {
        CurrentValueConsumer_->OnValue(MakeUnversionedSentinelValue(EValueType::Null, ColumnIndex_));
    } else {
        ValueWriter_.OnEntity();
    }
}

void TYsonToUnversionedValueConverter::OnBeginList()
{
    ValueWriter_.OnBeginList();
    ++Depth_;
}

void TYsonToUnversionedValueConverter::OnBeginAttributes()
{
    if (Depth_ == 0) {
        THROW_ERROR_EXCEPTION("Table values cannot have top-level attributes");
    }

    ValueWriter_.OnBeginAttributes();
    ++Depth_;
}

void TYsonToUnversionedValueConverter::OnListItem()
{
    if (Depth_ > 0) {
        ValueWriter_.OnListItem();
    }
}

void TYsonToUnversionedValueConverter::OnBeginMap()
{
    ValueWriter_.OnBeginMap();
    ++Depth_;
}

void TYsonToUnversionedValueConverter::OnKeyedItem(TStringBuf name)
{
    ValueWriter_.OnKeyedItem(name);
}

void TYsonToUnversionedValueConverter::OnEndMap()
{
    YT_VERIFY(Depth_ > 0);

    --Depth_;
    ValueWriter_.OnEndMap();
    FlushCurrentValueIfCompleted();
}

void TYsonToUnversionedValueConverter::OnEndList()
{
    YT_VERIFY(Depth_ > 0);

    --Depth_;
    ValueWriter_.OnEndList();
    FlushCurrentValueIfCompleted();
}

void TYsonToUnversionedValueConverter::OnEndAttributes()
{
    --Depth_;

    YT_VERIFY(Depth_ > 0);
    ValueWriter_.OnEndAttributes();
}

void TYsonToUnversionedValueConverter::FlushCurrentValueIfCompleted()
{
    if (Depth_ == 0) {
        ValueWriter_.Flush();
        auto accumulatedYson = TStringBuf(ValueBuffer_.Begin(), ValueBuffer_.Begin() + ValueBuffer_.Size());
        auto it = ComplexTypeConverters_.find(std::pair(TableIndex_, ColumnIndex_));
        TUnversionedValue value;
        if (it == ComplexTypeConverters_.end()) {
            value = MakeUnversionedAnyValue(accumulatedYson, ColumnIndex_);
        } else {
            const auto& converter = it->second;
            if (converter) {
                value = converter(MakeUnversionedStringValue(ValueBuffer_.Blob().ToStringBuf()));
                value.Id = ColumnIndex_;
            } else {
                value = MakeUnversionedCompositeValue(accumulatedYson, ColumnIndex_);
            }
        }
        CurrentValueConsumer_->OnValue(value);
        ValueBuffer_.Clear();
    }
}

bool TYsonToUnversionedValueConverter::TryConvertAndFeedValueConsumer(TUnversionedValue value)
{
    auto it = SimpleValueConverters_.find(std::pair(TableIndex_, ColumnIndex_));
    if (it != SimpleValueConverters_.end()) {
        auto converted = it->second(value);
        converted.Id = ColumnIndex_;
        CurrentValueConsumer_->OnValue(converted);
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(
    const TYsonConverterConfig& config,
    std::vector<IValueConsumer*> valueConsumers,
    int tableIndex)
    : YsonToUnversionedValueConverter_(config, std::move(valueConsumers))
{
    for (auto* consumer : YsonToUnversionedValueConverter_.ValueConsumers()) {
        NameTableWriters_.emplace_back(std::make_unique<TNameTableWriter>(consumer->GetNameTable()));
    }
    SwitchToTable(tableIndex);
}

TTableConsumer::TTableConsumer(const TYsonConverterConfig& config, IValueConsumer* valueConsumer)
    : TTableConsumer(config, std::vector<IValueConsumer*>(1, valueConsumer))
{ }

TError TTableConsumer::AttachLocationAttributes(TError error) const
{
    return error << TErrorAttribute("row_index", RowIndex_);
}

void TTableConsumer::OnControlInt64Scalar(i64 value)
{
    switch (ControlAttribute_) {
        case EControlAttribute::TableIndex:
            if (value < 0 || value >= GetTableCount()) {
                THROW_ERROR AttachLocationAttributes(TError(
                    "Invalid table index %v: expected integer in range [0,%v]",
                    value,
                    GetTableCount() - 1));
            }
            SwitchToTable(value);
            break;

        default:
            ThrowControlAttributesNotSupported();
    }
}

void TTableConsumer::OnControlStringScalar(TStringBuf /*value*/)
{
    ThrowControlAttributesNotSupported();
}

void TTableConsumer::OnStringScalar(TStringBuf value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        OnControlStringScalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnStringScalar(value);
    }
}

void TTableConsumer::OnInt64Scalar(i64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        OnControlInt64Scalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnInt64Scalar(value);
    }
}

void TTableConsumer::OnUint64Scalar(ui64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        ThrowInvalidControlAttribute("be an unsigned integer");
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnUint64Scalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a double value");
        return;
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnDoubleScalar(value);
    }
}

void TTableConsumer::OnBooleanScalar(bool value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a boolean value");
        return;
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnBooleanScalar(value);
    }
}

void TTableConsumer::OnEntity()
{
    switch (ControlState_) {
        case EControlState::None:
            break;

        case EControlState::ExpectEntity:
            YT_ASSERT(Depth_ == 0);
            // Successfully processed control statement.
            ControlState_ = EControlState::None;
            return;

        case EControlState::ExpectValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            YT_ABORT();
    }

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnEntity();
    }
}

void TTableConsumer::OnBeginList()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a list");
        return;
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        YsonToUnversionedValueConverter_.OnBeginList();
    }
    ++Depth_;
}

void TTableConsumer::OnBeginAttributes()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ControlState_ = EControlState::ExpectName;
    } else {
        YsonToUnversionedValueConverter_.OnBeginAttributes();
    }

    ++Depth_;
}

void TTableConsumer::ThrowControlAttributesNotSupported() const
{
    THROW_ERROR AttachLocationAttributes(TError("Control attributes are not supported"));
}

void TTableConsumer::ThrowMapExpected() const
{
    THROW_ERROR AttachLocationAttributes(TError("Invalid row format, map expected"));
}

void TTableConsumer::ThrowEntityExpected() const
{
    THROW_ERROR AttachLocationAttributes(TError("Invalid control attributes syntax, entity expected"));
}

void TTableConsumer::ThrowInvalidControlAttribute(const TString& whatsWrong) const
{
    THROW_ERROR AttachLocationAttributes(TError("Control attribute %Qlv cannot %v",
        ControlAttribute_,
        whatsWrong));
}

void TTableConsumer::OnListItem()
{
    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        // Row separator, do nothing.
    } else {
        YsonToUnversionedValueConverter_.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YT_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a map");
    } else if (ControlState_ == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YT_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        CurrentValueConsumer_->OnBeginRow();
    } else {
        YsonToUnversionedValueConverter_.OnBeginMap();
    }
    ++Depth_;
}

void TTableConsumer::OnKeyedItem(TStringBuf name)
{
    switch (ControlState_) {
        case EControlState::None:
            break;

        case EControlState::ExpectName:
            YT_ASSERT(Depth_ == 1);
            try {
                ControlAttribute_ = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR AttachLocationAttributes(TError("Failed to parse control attribute name %Qv", name));
            }
            ControlState_ = EControlState::ExpectValue;
            return;

        case EControlState::ExpectEndAttributes:
            YT_ASSERT(Depth_ == 1);
            THROW_ERROR AttachLocationAttributes(TError("Too many control attributes per record: at most one attribute is allowed"));

        default:
            YT_ABORT();
    }

    YT_ASSERT(Depth_ > 0);
    if (Depth_ == 1) {
        int columnIndex = -1;
        if (CurrentValueConsumer_->GetAllowUnknownColumns()) {
            try {
                columnIndex = CurrentNameTableWriter_->GetIdOrRegisterName(name);
            } catch (const std::exception& ex) {
                THROW_ERROR AttachLocationAttributes(TError("Failed to add column to name table for table writer")
                    << ex);
            }
        } else {
            auto id = CurrentNameTableWriter_->FindId(name);
            if (!id) {
                THROW_ERROR AttachLocationAttributes(
                    TError(NTableClient::EErrorCode::SchemaViolation, "No column %Qv in table schema",
                        name));
            }
            columnIndex = *id;
        }
        YT_VERIFY(columnIndex != -1);
        YsonToUnversionedValueConverter_.SetColumnIndex(columnIndex);
    } else {
        YsonToUnversionedValueConverter_.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YT_ASSERT(Depth_ > 0);
    // No control attribute allows map or composite values.
    YT_ASSERT(ControlState_ == EControlState::None);

    --Depth_;
    if (Depth_ == 0) {
        CurrentValueConsumer_->OnEndRow();
        ++RowIndex_;
    } else {
        YsonToUnversionedValueConverter_.OnEndMap();
    }
}

void TTableConsumer::OnEndList()
{
    // No control attribute allow list or composite values.
    YT_ASSERT(ControlState_ == EControlState::None);

    --Depth_;
    YT_ASSERT(Depth_ > 0);

    YsonToUnversionedValueConverter_.OnEndList();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth_;

    switch (ControlState_) {
        case EControlState::ExpectName:
            THROW_ERROR AttachLocationAttributes(TError("Too few control attributes per record: at least one attribute is required"));
            break;

        case EControlState::ExpectEndAttributes:
            YT_ASSERT(Depth_ == 0);
            ControlState_ = EControlState::ExpectEntity;
            break;

        case EControlState::None:
            YT_ASSERT(Depth_ > 0);
            YsonToUnversionedValueConverter_.OnEndAttributes();
            break;

        default:
            YT_ABORT();
    }
}

void TTableConsumer::SwitchToTable(int tableIndex)
{
    YT_VERIFY(tableIndex >= 0 && tableIndex < GetTableCount());
    CurrentValueConsumer_ = YsonToUnversionedValueConverter_.SwitchToTable(tableIndex);
    CurrentNameTableWriter_ = NameTableWriters_[tableIndex].get();
}

int TTableConsumer::GetTableCount() const
{
    return NameTableWriters_.size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
