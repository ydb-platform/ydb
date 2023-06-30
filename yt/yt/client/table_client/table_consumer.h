#pragma once

#include "public.h"

#include <yt/yt/client/complex_types/yson_format_conversion.h>
#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TYsonToUnversionedValueConverter
    : public NYson::TYsonConsumerBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<IValueConsumer*>, ValueConsumers);

public:
    TYsonToUnversionedValueConverter(
        const NComplexTypes::TYsonConverterConfig& config,
        IValueConsumer* valueConsumers);

    TYsonToUnversionedValueConverter(
        const NComplexTypes::TYsonConverterConfig& config,
        std::vector<IValueConsumer*> valueConsumers,
        int tableIndex = 0);

    IValueConsumer* SwitchToTable(int tableIndex);

    // Set column index of next emitted value.
    void SetColumnIndex(int columnIndex);

    int GetDepth() const;

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf name) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndList() override;
    void OnEndAttributes() override;

private:
    bool TryConvertAndFeedValueConsumer(TUnversionedValue value);

    TBlobOutput ValueBuffer_;
    NYson::TBufferedBinaryYsonWriter ValueWriter_;

    // Key of ComplexTypeConverters_ map is <TableIndex;ColumnId>.
    // All complex columns are present in this map.
    //
    // If no conversion is needed then values are empty functions.
    // Otherwise values are converter functions.
    THashMap<std::pair<int,int>, NComplexTypes::TYsonClientToServerConverter> ComplexTypeConverters_;
    THashMap<std::pair<int,int>, NComplexTypes::TYsonClientToServerConverter> SimpleValueConverters_;

    TBlobOutput ConvertedBuffer_;
    NYson::TBufferedBinaryYsonWriter ConvertedWriter_;

    IValueConsumer* CurrentValueConsumer_;
    int Depth_ = 0;
    int ColumnIndex_ = 0;
    int TableIndex_ = 0;

private:
    void FlushCurrentValueIfCompleted();
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETableConsumerControlState,
    (None)
    (ExpectName)
    (ExpectValue)
    (ExpectEndAttributes)
    (ExpectEntity)
);

class TTableConsumer
    : public NYson::TYsonConsumerBase
{
public:
    TTableConsumer(
        const NComplexTypes::TYsonConverterConfig& config,
        IValueConsumer* consumer);
    TTableConsumer(
        const NComplexTypes::TYsonConverterConfig& config,
        std::vector<IValueConsumer*> consumers,
        int tableIndex = 0);

protected:
    using EControlState = ETableConsumerControlState;

    TError AttachLocationAttributes(TError error) const;

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf name) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;

    void ThrowMapExpected() const;
    void ThrowEntityExpected() const;
    void ThrowControlAttributesNotSupported() const;
    void ThrowInvalidControlAttribute(const TString& whatsWrong) const;

    void OnEndList() override;
    void OnEndAttributes() override;

    void OnControlInt64Scalar(i64 value);
    void OnControlStringScalar(TStringBuf value);

    void SwitchToTable(int tableIndex);

private:
    int GetTableCount() const;

protected:
    std::vector<std::unique_ptr<TNameTableWriter>> NameTableWriters_;

    IValueConsumer* CurrentValueConsumer_ = nullptr;
    TNameTableWriter* CurrentNameTableWriter_ = nullptr;

    EControlState ControlState_ = EControlState::None;
    EControlAttribute ControlAttribute_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;

    int Depth_ = 0;

    i64 RowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
