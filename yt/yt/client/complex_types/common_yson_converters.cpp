#include "common_yson_converters.h"

#include "check_yson_token.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <bool Unwrap>
class TOptionalCursorConverter
{
public:
    TOptionalCursorConverter(
        TComplexTypeFieldDescriptor descriptor,
        TYsonCursorConverter elementConverter)
        : Descriptor_(std::move(descriptor))
        , ElementConverter_(std::move(elementConverter))
        , IsElementNullable_(Descriptor_.GetType()->AsOptionalTypeRef().IsElementNullable())
    { }

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        auto ysonType = (*cursor)->GetType();
        if (ysonType == EYsonItemType::EntityValue) {
            return OnEmptyOptional(cursor, consumer);
        }
        if (!IsElementNullable_) {
            return ElementConverter_(cursor, consumer);
        }

        if (ysonType != EYsonItemType::BeginList) {
            ThrowUnexpectedYsonTokenException(
                Descriptor_,
                *cursor,
                {EYsonItemType::EntityValue, EYsonItemType::BeginList});
        }
        cursor->Next();

        OnNullableFilledOptional(cursor, consumer);

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::EndList);
        cursor->Next();
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;
    const TYsonCursorConverter ElementConverter_;
    const bool IsElementNullable_;

    void OnEmptyOptional(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (Unwrap) {
            THROW_ERROR_EXCEPTION(
                "Cannot unwrap empty optional at %Qv",
                Descriptor_.GetDescription());
        } else {
            consumer->OnEntity();
            cursor->Next();
        }
    }

    void OnNullableFilledOptional(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (Unwrap) {
            ElementConverter_(cursor, consumer);
        } else {
            consumer->OnBeginList();
            consumer->OnListItem();
            ElementConverter_(cursor, consumer);
            consumer->OnEndList();
        }
    }
};

class TListCursorConverter
{
public:
    TListCursorConverter(
        TComplexTypeFieldDescriptor descriptor,
        TYsonCursorConverter elementConverter)
        : Descriptor_(std::move(descriptor))
        , ElementConverter_(std::move(elementConverter))
    { }

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        consumer->OnBeginList();
        while ((*cursor)->GetType() != EYsonItemType::EndList) {
            consumer->OnListItem();
            ElementConverter_(cursor, consumer);
        }
        consumer->OnEndList();

        // Skip final list token.
        cursor->Next();
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;
    const TYsonCursorConverter ElementConverter_;
};

class TTupleCursorConverterBase
{
public:
    TTupleCursorConverterBase(
        TComplexTypeFieldDescriptor descriptor,
        std::vector<TYsonCursorConverter> elementConverters)
        : Descriptor_(std::move(descriptor))
        , ElementConverters_(std::move(elementConverters))
    { }

protected:
    const TComplexTypeFieldDescriptor Descriptor_;
    const std::vector<TYsonCursorConverter> ElementConverters_;
};

struct TTupleCursorConverter
    : public TTupleCursorConverterBase
{
    using TTupleCursorConverterBase::TTupleCursorConverterBase;

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        consumer->OnBeginList();
        for (const auto& converter : ElementConverters_) {
            consumer->OnListItem();
            converter(cursor, consumer);
        }
        consumer->OnEndList();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::EndList);
        cursor->Next();
    }
};

struct TVariantTupleCursorConverter
    : public TTupleCursorConverterBase
{
    using TTupleCursorConverterBase::TTupleCursorConverterBase;

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::Int64Value);
        auto tag = cursor->GetCurrent().UncheckedAsInt64();

        THROW_ERROR_EXCEPTION_IF(
            tag < 0,
            "Error while parsing %Qv: variant tag (%v) is negative",
            Descriptor_.GetDescription(),
            tag);
        THROW_ERROR_EXCEPTION_IF(
            tag >= std::ssize(ElementConverters_),
            "Error while parsing %Qv: variant tag (%v) exceeds variant alternative count (%v)",
            Descriptor_.GetDescription(),
            tag,
            ElementConverters_.size());

        cursor->Next();

        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnInt64Scalar(tag);

        consumer->OnListItem();
        ElementConverters_[tag](cursor, consumer);

        consumer->OnEndList();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::EndList);
        cursor->Next();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedValueAdapter
{
public:
    explicit TUnversionedValueAdapter(TYsonCursorConverter converter)
        : CursorConverter_(std::move(converter))
        , Writer_(&Buffer_)
    { }

    TUnversionedValueAdapter(const TUnversionedValueAdapter& other)
        : CursorConverter_(other.CursorConverter_)
        , Writer_(&Buffer_)
    { }

    // This operator should be called only after previous result is consumed.
    // So use-after-free won't occur.
    TUnversionedValue operator()(TUnversionedValue sourceValue)
    {
        TMemoryInput input(sourceValue.Data.String, sourceValue.Length);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        Buffer_.Clear();
        CursorConverter_(&cursor, &Writer_);
        Writer_.Flush();
        return MakeUnversionedCompositeValue(Buffer_.Blob().ToStringBuf());
    }

private:
    const TYsonCursorConverter CursorConverter_;

    TBlobOutput Buffer_;
    TBufferedBinaryYsonWriter Writer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

void IdentityYsonCursorConverter(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
{
    cursor->TransferComplexValue(consumer);
}

TYsonCursorConverter CreateOptionalYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter)
{
    return TOptionalCursorConverter</*Unwrap*/ false>(
        std::move(descriptor),
        std::move(elementConverter));
}

TYsonCursorConverter CreateUnwrappingOptionalYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter)
{
    return TOptionalCursorConverter</*Unwrap*/ true>(
        std::move(descriptor),
        std::move(elementConverter));
}

TYsonCursorConverter CreateWrappingOptionalYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter)
{
    if (descriptor.GetType()->IsNullable()) {
        return [elementConverter = std::move(elementConverter)] (
            TYsonPullParserCursor* cursor,
            IYsonConsumer* consumer)
        {
            consumer->OnBeginList();
            consumer->OnListItem();
            elementConverter(cursor, consumer);
            consumer->OnEndList();
        };
    } else {
        return elementConverter;
    }
}

TYsonCursorConverter CreateListYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter)
{
    return TListCursorConverter(std::move(descriptor), std::move(elementConverter));
}

TYsonCursorConverter CreateTupleYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<TYsonCursorConverter> elementConverters)
{
    return TTupleCursorConverter(std::move(descriptor), std::move(elementConverters));
}

TYsonCursorConverter CreateVariantTupleYsonCursorConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<TYsonCursorConverter> elementConverters)
{
    return TVariantTupleCursorConverter(std::move(descriptor), std::move(elementConverters));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueConverter CreateUnversionedValueConverter(
    TYsonCursorConverter ysonCursorConverter)
{
    return TUnversionedValueAdapter(std::move(ysonCursorConverter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
