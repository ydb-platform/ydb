#pragma once

#include "position_independent_value.h"

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TMutablePIValueRange AllocatePIValueRange(TRowBuffer* buffer, int valueCount);

void CapturePIValue(TRowBuffer* buffer, TPIValue* value);

TMutablePIValueRange CapturePIValueRange(
    TRowBuffer* buffer,
    TPIValueRange values,
    bool captureValues = true);
TMutablePIValueRange CapturePIValueRange(
    TRowBuffer* buffer,
    TUnversionedValueRange Values,
    bool captureValues = true);

TSharedRange<TRange<TPIValue>> CopyAndConvertToPI(
    const TSharedRange<TUnversionedRow>& rows,
    bool captureValues = true);
TSharedRange<TPIRowRange> CopyAndConvertToPI(
    const TSharedRange<TRowRange>& range,
    bool captureValues = true);

TMutableUnversionedRow CopyAndConvertFromPI(
    TRowBuffer* buffer,
    TPIValueRange values,
    bool captureValues = true);
std::vector<TUnversionedRow> CopyAndConvertFromPI(
    TRowBuffer* buffer,
    const std::vector<TPIValueRange>& rows,
    bool captureValues = true);

TMutablePIValueRange InplaceConvertToPI(TMutableUnversionedValueRange range);
TMutablePIValueRange InplaceConvertToPI(const TUnversionedRow& row);

TMutableUnversionedValueRange InplaceConvertFromPI(TMutablePIValueRange range);

////////////////////////////////////////////////////////////////////////////////

template <class TNonPI>
class TBorrowingPIValueGuard;

template <class TNonPI>
TBorrowingPIValueGuard<TNonPI> BorrowFromNonPI(TNonPI value);

template <class TNonPI>
class TBorrowingNonPIValueGuard;

template <class TPI>
TBorrowingNonPIValueGuard<TPI> BorrowFromPI(TPI value);

////////////////////////////////////////////////////////////////////////////////

template <>
class TBorrowingPIValueGuard<TUnversionedValue*>
    : public TNonCopyable
{
public:
    explicit TBorrowingPIValueGuard(TUnversionedValue* value);
    ~TBorrowingPIValueGuard();

    TPIValue* GetPIValue();

private:
    TUnversionedValue* Value_ = nullptr;
    TPIValue* PIValue_ = nullptr;
};

template <>
class TBorrowingPIValueGuard<TUnversionedValueRange>
    : public TNonCopyable
{
public:
    explicit TBorrowingPIValueGuard(TUnversionedValueRange valueRange);
    ~TBorrowingPIValueGuard();

    TPIValue* Begin();
    const TPIValue& operator[](int index) const;
    size_t Size();

private:
    TMutableUnversionedValueRange ValueRange_{};
    TMutablePIValueRange PIValueRange_{};
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TBorrowingNonPIValueGuard<TPIValue*>
    : public TNonCopyable
{
public:
    explicit TBorrowingNonPIValueGuard(TPIValue* piValue);
    ~TBorrowingNonPIValueGuard();

    TUnversionedValue* GetValue();

private:
    TUnversionedValue* Value_ = nullptr;
    TPIValue* PIValue_ = nullptr;
};

template <>
class TBorrowingNonPIValueGuard<TPIValueRange>
    : public TNonCopyable
{
public:
    explicit TBorrowingNonPIValueGuard(TPIValueRange valueRange);
    ~TBorrowingNonPIValueGuard();

    TUnversionedValue* Begin();
    size_t Size();

private:
    TMutableUnversionedValueRange ValueRange_{};
    TMutablePIValueRange PIValueRange_{};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H
#include "position_independent_value_transfer-inl.h"
#undef POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H
