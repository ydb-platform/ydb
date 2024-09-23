#include "type_codecs.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <util/generic/typetraits.h>

namespace NKikimr {
namespace NScheme {

void InitDefaults(TTypeCodecs* codecs, TCodecType type) {
    auto nullable = TCodecSig(type, true);
    auto nonNullable = TCodecSig(type, false);

    codecs->AddAlias(TCodecSig(TCodecType::Default, true), nullable);
    codecs->AddAlias(TCodecSig(TCodecType::Default, false), nonNullable);

    codecs->AddAlias(TCodecSig(TCodecType::ZeroCopy, true), nullable);
    codecs->AddAlias(TCodecSig(TCodecType::ZeroCopy, false), nonNullable);
    codecs->AddAlias(TCodecSig(TCodecType::Compact, true), nullable);
    codecs->AddAlias(TCodecSig(TCodecType::Compact, false), nonNullable);
    codecs->AddAlias(TCodecSig(TCodecType::RandomAccess, true), nullable);
    codecs->AddAlias(TCodecSig(TCodecType::RandomAccess, false), nonNullable);

    // codecs->AddAlias(TCodecSig(TCodecType::Adaptive, true), nullable);
    // codecs->AddAlias(TCodecSig(TCodecType::Adaptive, false), nonNullable);
}

template <size_t Size>
inline void AddFixedLen(TTypeCodecs* codecs, bool initDefaults = true) {
    codecs->AddCodec<TFixedLenCodec<Size, true>>();
    codecs->AddCodec<TFixedLenCodec<Size, false>>();
    if (initDefaults)
        InitDefaults(codecs, TCodecType::FixedLen);
};

template <typename TType>
inline void AddFixedLen(TTypeCodecs* codecs, bool initDefaults = true) {
    AddFixedLen<sizeof(typename TType::TValueType)>(codecs, initDefaults);
}

template <typename TType>
void AddIntCodecs(TTypeCodecs* codecs) {
    static_assert(std::is_integral<typename TType::TValueType>::value, "Not an integral type.");
    using TSigned = std::make_signed_t<typename TType::TValueType>;
    using TUnsigned = std::make_unsigned_t<typename TType::TValueType>;

    codecs->AddCodec<TVarIntCodec<TUnsigned, true>>();
    codecs->AddCodec<TVarIntCodec<TUnsigned, false>>();
    codecs->AddCodec<TZigZagCodec<TSigned, true>>();
    codecs->AddCodec<TZigZagCodec<TSigned, false>>();
    codecs->AddCodec<TDeltaVarIntCodec<TUnsigned, true>>();
    codecs->AddCodec<TDeltaVarIntCodec<TUnsigned, false>>();
    codecs->AddCodec<TDeltaRevVarIntCodec<TUnsigned, true>>();
    codecs->AddCodec<TDeltaRevVarIntCodec<TUnsigned, false>>();
    codecs->AddCodec<TDeltaZigZagCodec<TSigned, true>>();
    codecs->AddCodec<TDeltaZigZagCodec<TSigned, false>>();

    if (std::is_signed<typename TType::TValueType>::value) {
        codecs->AddAlias(TCodecSig(TCodecType::Compact, true), TCodecSig(TCodecType::ZigZag, true), true);
        codecs->AddAlias(TCodecSig(TCodecType::Compact, false), TCodecSig(TCodecType::ZigZag, false), true);
    } else  { // std::is_unsigned<typename TType::TValueType>::value)
        codecs->AddAlias(TCodecSig(TCodecType::Compact, true), TCodecSig(TCodecType::VarInt, true), true);
        codecs->AddAlias(TCodecSig(TCodecType::Compact, false), TCodecSig(TCodecType::VarInt, false), true);
    }
}


TTypeCodecs::TTypeCodecs(TTypeId typeId) {
    using namespace NScheme;
    AddCodec<TVarLenCodec<true>>();
    AddCodec<TVarLenCodec<false>>();

    switch (typeId) {
    case NTypeIds::Date32:
    case NTypeIds::Int32:
        AddFixedLen<TInt32>(this);
        AddIntCodecs<TInt32>(this);
        break;
    case NTypeIds::Uint32:
        AddFixedLen<TUint32>(this);
        AddIntCodecs<TUint32>(this);
        break;
    case NTypeIds::Int64:
    case NTypeIds::Datetime64:
    case NTypeIds::Timestamp64:
    case NTypeIds::Interval64:
        AddFixedLen<TInt64>(this);
        AddIntCodecs<TInt64>(this);
        break;
    case NTypeIds::Uint64:
        AddFixedLen<TUint64>(this);
        AddIntCodecs<TUint64>(this);
        break;
    case NTypeIds::Byte:
        AddFixedLen<TUint8>(this);
        break;
    case NTypeIds::Bool:
        AddFixedLen<TBool>(this, false);
        AddCodec<TBoolCodec<true>>();
        AddCodec<TBoolCodec<false>>();
        InitDefaults(this, TCodecType::Bool);
        break;

    case NTypeIds::Double:
        AddFixedLen<TDouble>(this);
        break;
    case NTypeIds::Float:
        AddFixedLen<TFloat>(this);
        break;

    case NTypeIds::PairUi64Ui64:
        AddFixedLen<TPairUi64Ui64>(this);
        break;

    case NTypeIds::String:
    case NTypeIds::String4k:
    case NTypeIds::String2m:
    case NTypeIds::Utf8:
    case NTypeIds::Json:
    case NTypeIds::JsonDocument:
    case NTypeIds::DyNumber:
        InitDefaults(this, TCodecType::VarLen);
        break;

    case NTypeIds::ActorId:
        AddFixedLen<TActorId>(this);
        break;
    case NTypeIds::StepOrderId:
        AddFixedLen<TStepOrderId>(this);
        break;
    case NTypeIds::Decimal:
        AddFixedLen<TDecimal>(this);
        break;

    case NTypeIds::Date:
        AddFixedLen<TDate>(this);
        break;
    case NTypeIds::Datetime:
        AddFixedLen<TDatetime>(this);
        break;
    case NTypeIds::Timestamp:
        AddFixedLen<TTimestamp>(this);
        break;
    case NTypeIds::Interval:
        AddFixedLen<TInterval>(this);
        break;
    }
}

} // namespace NScheme
} // namespace NKikimr
