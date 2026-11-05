#pragma once

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <library/cpp/skiff/skiff.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

using TYsonToSkiffConverter = std::function<void(NYson::TYsonPullParserCursor*, NSkiff::TCheckedInDebugSkiffWriter*)>;
using TSkiffToYsonConverter = std::function<void(NSkiff::TCheckedInDebugSkiffParser*, NYson::TCheckedInDebugYsonTokenWriter*)>;

struct TYsonToSkiffConverterConfig
{
    // Usually skiffSchema MUST match descriptor.LogicalType.
    // But when AllowOmitTopLevelOptional is set to true and descriptor.LogicalType is Optional<SomeInnerType>
    // skiffSchema CAN match SomeInnerType. In that case returned converter will throw error when it gets empty value.
    //
    // Useful for sparse fields.
    bool AllowOmitTopLevelOptional = false;
};

TYsonToSkiffConverter CreateYsonToSkiffConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const std::shared_ptr<NSkiff::TSkiffSchema>& skiffSchema,
    const TYsonToSkiffConverterConfig& config = {});

struct TSkiffToYsonConverterConfig
{
    // Similar to TYsonToSkiffConverterConfig::AllowOmitTopLevelOptional.
    bool AllowOmitTopLevelOptional = false;
};

TSkiffToYsonConverter CreateSkiffToYsonConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const std::shared_ptr<NSkiff::TSkiffSchema>& skiffSchema,
    const TSkiffToYsonConverterConfig& config = {});

////////////////////////////////////////////////////////////////////////////////

// A direct wire type is a wire type that maps to a physical type without the logical type or other schema data.
template <NSkiff::EWireType WireType>
struct TDirectSkiffTypeTraits;

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType WireType>
struct TDirectParseFunction
{
    using TTraits = TDirectSkiffTypeTraits<WireType>;

    Y_FORCE_INLINE auto operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const -> typename TTraits::TPhysical;
};

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType WireType>
struct TDirectWriteFunction
{
    using TTraits = TDirectSkiffTypeTraits<WireType>;

    Y_FORCE_INLINE void operator()(typename TTraits::TPhysical value, NSkiff::TCheckedInDebugSkiffWriter* writer) const;
};

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
class TTzTupleParseFunction
{
public:
    TTzTupleParseFunction();
    Y_FORCE_INLINE TStringBuf operator()(NSkiff::TCheckedInDebugSkiffParser* parser);

private:
    std::string Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
struct TTzTupleWriteFunction
{
    Y_FORCE_INLINE void operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const;
};

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
class TDecimalParseFunction
{
public:
    explicit TDecimalParseFunction(int precision);
    Y_FORCE_INLINE TStringBuf operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const;

private:
    const int Precision_;
    mutable char Buffer_[NDecimal::TDecimal::MaxBinarySize];
};

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
class TDecimalWriteFunction
{
public:
    explicit TDecimalWriteFunction(int precision);

    void operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const;

private:
    const int Precision_;
};

////////////////////////////////////////////////////////////////////////////////

class TUuidParseFunction
{
public:
    Y_FORCE_INLINE TStringBuf operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const;

private:
    mutable ui64 Buffer_[2];
};

////////////////////////////////////////////////////////////////////////////////

class TUuidWriteFunction
{
public:
    Y_FORCE_INLINE void operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TParseFunctions
{
    using TUuidFunction = TUuidParseFunction;

    template <NSkiff::EWireType TimeValueWireType>
    using TTzTupleFunction = TTzTupleParseFunction<TimeValueWireType>;

    template <NSkiff::EWireType WireType>
    using TDirectFunction = TDirectParseFunction<WireType>;

    template <NSkiff::EWireType WireType>
    using TDecimalFunction = TDecimalParseFunction<WireType>;
};

struct TWriteFunctions
{
    using TUuidFunction = TUuidWriteFunction;

    template <NSkiff::EWireType TimeValueWireType>
    using TTzTupleFunction = TTzTupleWriteFunction<TimeValueWireType>;

    template <NSkiff::EWireType WireType>
    using TDirectFunction = TDirectWriteFunction<WireType>;

    template <NSkiff::EWireType WireType>
    using TDecimalFunction = TDecimalWriteFunction<WireType>;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TCppType>
struct TCppTypeToValueType;

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
auto DispatchNonComplexWireType(NSkiff::EWireType wireType, TFunction&& function);

template <typename TFunction>
auto DispatchOptionalKind(NSkiffExt::EOptionalKind optionalKind, TFunction&& function);

template <
    typename TConverter,
    typename TConverters,
    typename TFunctions,
    typename... TArgs
>
TConverter CreateSimpleConverter(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    NTableClient::ESimpleLogicalValueType simpleType,
    TArgs&&... args);

template <
    typename TConverter,
    typename TConverters,
    typename TFunctions,
    typename... TArgs
>
TConverter CreateDecimalConverter(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    int precision,
    TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

void ValidateMatchesSimpleType(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    NTableClient::ESimpleLogicalValueType simpleType);
void ValidateSkiffWireTypeForDecimal(int precision, NSkiff::EWireType wireType);
void ValidateWireTypeIsOneOf(NSkiff::EWireType wireType, const std::initializer_list<NSkiff::EWireType>& expected);

template <typename TWire, typename TValue>
Y_FORCE_INLINE bool IsInRange(TValue value);

template <typename TContainer>
std::string FormatExpectedTypes(const TContainer& expected);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

#define SKIFF_YSON_CONVERTER_INL_H_
#include "skiff_yson_converter-inl.h"
#undef SKIFF_YSON_CONVERTER_INL_H_
