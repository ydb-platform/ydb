#pragma once

#include "row_base.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/string/format_analyser.h>

#include <util/system/defaults.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Wire protocol readers/writer rely on this fixed layout.
union TUnversionedValueData
{
    //! |Int64| value.
    i64 Int64;
    //! |Uint64| value.
    ui64 Uint64;
    //! |Double| value.
    double Double;
    //! |Boolean| value.
    bool Boolean;
    //! String value for |String| type or YSON-encoded value for |Any| type.
    //! NB: string is not zero-terminated, so never use it as a TString.
    //! Use #TUnversionedValue::AsStringBuf() or #TUnversionedValue::AsString() instead.
    const char* String;
};

static_assert(
    sizeof(TUnversionedValueData) == 8,
    "TUnversionedValueData has to be exactly 8 bytes.");

// NB: Wire protocol readers/writer rely on this fixed layout.
struct TUnversionedValue
{
    //! Column id w.r.t. the name table.
    ui16 Id;

    //! Column type.
    EValueType Type;

    //! Various bit-packed flags.
    EValueFlags Flags;

    //! Length of a variable-sized value (only meaningful for string-like types).
    ui32 Length;

    //! Payload.
    TUnversionedValueData Data;

    //! Assuming #IsStringLikeType(Type), return string data as a TStringBuf.
    TStringBuf AsStringBuf() const;
    //! Assuming #IsStringLikeType(Type), return string data as a TString.
    TString AsString() const;
};

static_assert(
    sizeof(TUnversionedValue) == 16,
    "TUnversionedValue has to be exactly 16 bytes.");
static_assert(
    (std::is_standard_layout_v<TUnversionedValue> && std::is_trivial_v<TUnversionedValue>),
    "TUnversionedValue must be a POD type.");

////////////////////////////////////////////////////////////////////////////////

//! Computes FarmHash forever-fixed fingerprint for a given TUnversionedValue.
TFingerprint GetFarmFingerprint(const TUnversionedValue& value);

////////////////////////////////////////////////////////////////////////////////

//! Debug printer for Gtest unittests.
void PrintTo(const TUnversionedValue& value, ::std::ostream* os);

void FormatValue(TStringBuilderBase* builder, const TUnversionedValue& value, TStringBuf format);
TString ToString(const TUnversionedValue& value, bool valueOnly = false);

////////////////////////////////////////////////////////////////////////////////

// NB: Hash function may change in future. Use fingerprints for stability.
struct TDefaultUnversionedValueHash
{
    size_t operator()(const TUnversionedValue& value) const;
};

struct TDefaultUnversionedValueEqual
{
    bool operator()(const TUnversionedValue& lhs, const TUnversionedValue& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TBitwiseUnversionedValueHash
{
    size_t operator()(const TUnversionedValue& value) const;
};

struct TBitwiseUnversionedValueEqual
{
    bool operator()(const TUnversionedValue& lhs, const TUnversionedValue& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

Y_DECLARE_PODTYPE(NYT::NTableClient::TUnversionedValue);

//! A hasher for TUnversionedValue.
template <>
struct THash<NYT::NTableClient::TUnversionedValue>
{
    inline size_t operator()(const NYT::NTableClient::TUnversionedValue& value) const
    {
        return NYT::NTableClient::TDefaultUnversionedValueHash()(value);
    }
};

template <class T>
    requires std::derived_from<std::remove_cvref_t<T>, NYT::NTableClient::TUnversionedValue>
struct NYT::TFormatArg<T>
    : public NYT::TFormatArgBase
{
    static constexpr auto FlagSpecifiers
        = TFormatArgBase::ExtendFlags</*Hot*/ true, 1, std::array{'k'}>();
};
