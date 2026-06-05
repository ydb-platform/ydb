#pragma once

#include "bench_common.h"

struct TEvBenchPbMessage
    : TEventPB<TEvBenchPbMessage, NActorsBench::TFiveU64, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbMessage, NActorsBench::TFiveU64, EvBenchPbMessage>;

    TEvBenchPbMessage() = default;

    explicit TEvBenchPbMessage(ui64 base) {
        Record.SetValue1(base + 1);
        Record.SetValue2(base + 2);
        Record.SetValue3(base + 3);
        Record.SetValue4(base + 4);
        Record.SetValue5(base + 5);
    }
};

struct TEvBenchPbBytesMessage
    : TEventPB<TEvBenchPbBytesMessage, NActorsBench::TPayloadBytes, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbBytesMessage, NActorsBench::TPayloadBytes, EvBenchPbMessage>;

    TEvBenchPbBytesMessage() = default;
};

struct TEvBenchPbThirtyMessage
    : TEventPB<TEvBenchPbThirtyMessage, NActorsBench::TThirtyU64, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbThirtyMessage, NActorsBench::TThirtyU64, EvBenchPbMessage>;

    TEvBenchPbThirtyMessage() = default;
};

struct TEvBenchPbArrayMessage
    : TEventPB<TEvBenchPbArrayMessage, NActorsBench::TFixed64Array30, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbArrayMessage, NActorsBench::TFixed64Array30, EvBenchPbMessage>;

    TEvBenchPbArrayMessage() = default;
};

struct TEvBenchPbStructArrayMessage
    : TEventPB<TEvBenchPbStructArrayMessage, NActorsBench::TTripleArray10, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbStructArrayMessage, NActorsBench::TTripleArray10, EvBenchPbMessage>;

    TEvBenchPbStructArrayMessage() = default;
};

struct TEvBenchPbVPutLikeMessage
    : TEventPB<TEvBenchPbVPutLikeMessage, NActorsBench::TVPutLike, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbVPutLikeMessage, NActorsBench::TVPutLike, EvBenchPbMessage>;

    TEvBenchPbVPutLikeMessage() = default;
};

struct TTripleInts {
    ui32 A;
    ui32 B;
    ui32 C;
};

struct TBlobIdRaw {
    ui64 RawX1;
    ui64 RawX2;
    ui64 RawX3;
};

struct Y_PACKED TVDiskIdRaw {
    ui32 GroupID;
    ui32 GroupGeneration;
    ui8 Ring;
    ui8 Domain;
    ui8 VDisk;
};

struct Y_PACKED TMsgQoSRaw {
    ui64 Cost;
    ui32 DeadlineSeconds;
    ui8 ExtQueueId;
};

struct TTimestampsRaw {
    ui64 SentByDSProxyUs;
    ui64 ReceivedByVDiskUs;
    ui64 SentByVDiskUs;
    ui64 ReceivedByDSProxyUs;
};

struct TVPutFlagsRaw {
    ui8 Bits = 0;

    static constexpr ui8 IssueKeepFlagMask = 1u << 0;
    static constexpr ui8 IsZeroEntryMask = 1u << 1;
    static constexpr ui8 IgnoreBlockMask = 1u << 2;
    static constexpr ui8 NotifyIfNotReadyMask = 1u << 3;

    ui8 GetIssueKeepFlag() const {
        return (Bits & IssueKeepFlagMask) != 0;
    }

    ui8 GetIsZeroEntry() const {
        return (Bits & IsZeroEntryMask) != 0;
    }

    ui8 GetIgnoreBlock() const {
        return (Bits & IgnoreBlockMask) != 0;
    }

    ui8 GetNotifyIfNotReady() const {
        return (Bits & NotifyIfNotReadyMask) != 0;
    }
};

struct Y_PACKED TExtraBlockCheckRaw {
    ui64 TabletId;
    ui32 Generation;
};

static_assert(sizeof(TVDiskIdRaw) == 11);
static_assert(sizeof(TMsgQoSRaw) == 13);
static_assert(sizeof(TVPutFlagsRaw) == 1);
static_assert(sizeof(TExtraBlockCheckRaw) == 12);

static_assert(std::is_trivially_copyable_v<TTripleInts>);
static_assert(std::is_trivially_copyable_v<TBlobIdRaw>);
static_assert(std::is_trivially_copyable_v<TVDiskIdRaw>);
static_assert(std::is_trivially_copyable_v<TMsgQoSRaw>);
static_assert(std::is_trivially_copyable_v<TTimestampsRaw>);
static_assert(std::is_trivially_copyable_v<TVPutFlagsRaw>);
static_assert(std::is_trivially_copyable_v<TExtraBlockCheckRaw>);

using TFlatEventDefs = TEventFlatLayout;

struct TEvBenchFlatMessage;
struct TEvBenchFlatMessageScheme {
    using TValue1Tag = TFlatEventDefs::FixedField<ui64, 0>;
    using TValue2Tag = TFlatEventDefs::FixedField<ui64, 1>;
    using TValue3Tag = TFlatEventDefs::FixedField<ui64, 2>;
    using TValue4Tag = TFlatEventDefs::FixedField<ui64, 3>;
    using TValue5Tag = TFlatEventDefs::FixedField<ui64, 4>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TValue1Tag, TValue2Tag, TValue3Tag, TValue4Tag, TValue5Tag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatMessage : TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatMessageScheme;
    using TValue1Tag = TScheme::TValue1Tag;
    using TValue2Tag = TScheme::TValue2Tag;
    using TValue3Tag = TScheme::TValue3Tag;
    using TValue4Tag = TScheme::TValue4Tag;
    using TValue5Tag = TScheme::TValue5Tag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageScheme>;

    auto Value1() { return this->template Field<TValue1Tag>(); }
    auto Value1() const { return this->template Field<TValue1Tag>(); }
    auto Value2() { return this->template Field<TValue2Tag>(); }
    auto Value2() const { return this->template Field<TValue2Tag>(); }
    auto Value3() { return this->template Field<TValue3Tag>(); }
    auto Value3() const { return this->template Field<TValue3Tag>(); }
    auto Value4() { return this->template Field<TValue4Tag>(); }
    auto Value4() const { return this->template Field<TValue4Tag>(); }
    auto Value5() { return this->template Field<TValue5Tag>(); }
    auto Value5() const { return this->template Field<TValue5Tag>(); }

    static TEvBenchFlatMessage* Make(ui64 base) {
        THolder<TEvBenchFlatMessage> holder(TBase::MakeEvent());
        auto frontend = holder->GetFrontend<TSchemeV1>();
        frontend.template Field<TValue1Tag>() = base + 1;
        frontend.template Field<TValue2Tag>() = base + 2;
        frontend.template Field<TValue3Tag>() = base + 3;
        frontend.template Field<TValue4Tag>() = base + 4;
        frontend.template Field<TValue5Tag>() = base + 5;
        return holder.Release();
    }
};

struct TEvBenchFlatBytesMessage;
struct TEvBenchFlatBytesMessageScheme {
    using TBlobTag = TFlatEventDefs::BytesField<0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>, TBlobTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatBytesMessage : TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatBytesMessageScheme;
    using TBlobTag = TScheme::TBlobTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme>;
};

struct TEvBenchFlatVPutLikeMessage;
struct TEvBenchFlatVPutLikeMessageScheme {
    using TBlobIdTag = TFlatEventDefs::FixedField<TBlobIdRaw, 0>;
    using TChecksumTag = TFlatEventDefs::FixedField<ui64, 1>;
    using TFlagsTag = TFlatEventDefs::FixedField<TVPutFlagsRaw, 2>;
    using TVDiskIdTag = TFlatEventDefs::FixedField<TVDiskIdRaw, 3>;
    using TFullDataSizeTag = TFlatEventDefs::FixedField<ui64, 4>;
    using TCookieTag = TFlatEventDefs::FixedField<ui64, 5>;
    using THandleClassTag = TFlatEventDefs::FixedField<ui32, 6>;
    using TMsgQoSTag = TFlatEventDefs::FixedField<TMsgQoSRaw, 7>;
    using TTimestampsTag = TFlatEventDefs::FixedField<TTimestampsRaw, 8>;
    using TExtraChecksTag = TFlatEventDefs::InlineArrayField<TExtraBlockCheckRaw, VPutLikeExtraChecks, 9>;
    using TPayloadTag = TFlatEventDefs::BytesField<10>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
        TBlobIdTag, TChecksumTag, TFlagsTag, TVDiskIdTag, TFullDataSizeTag, TCookieTag, THandleClassTag,
        TMsgQoSTag, TTimestampsTag, TExtraChecksTag, TPayloadTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatVPutLikeMessage : TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatVPutLikeMessageScheme;
    using TBlobIdTag = TScheme::TBlobIdTag;
    using TChecksumTag = TScheme::TChecksumTag;
    using TFlagsTag = TScheme::TFlagsTag;
    using TVDiskIdTag = TScheme::TVDiskIdTag;
    using TFullDataSizeTag = TScheme::TFullDataSizeTag;
    using TCookieTag = TScheme::TCookieTag;
    using THandleClassTag = TScheme::THandleClassTag;
    using TMsgQoSTag = TScheme::TMsgQoSTag;
    using TTimestampsTag = TScheme::TTimestampsTag;
    using TExtraChecksTag = TScheme::TExtraChecksTag;
    using TPayloadTag = TScheme::TPayloadTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme>;
};

#define BENCH_30_U64_FIELDS(X) \
    X(1) X(2) X(3) X(4) X(5) X(6) X(7) X(8) X(9) X(10) \
    X(11) X(12) X(13) X(14) X(15) X(16) X(17) X(18) X(19) X(20) \
    X(21) X(22) X(23) X(24) X(25) X(26) X(27) X(28) X(29) X(30)

struct TEvBenchFlatThirtyMessage;
struct TEvBenchFlatThirtyMessageScheme {
#define DECLARE_BENCH_30_U64_TAG(N) using TValue##N##Tag = TFlatEventDefs::FixedField<ui64, N - 1>;
    BENCH_30_U64_FIELDS(DECLARE_BENCH_30_U64_TAG)
#undef DECLARE_BENCH_30_U64_TAG

    using TSchemeV1 = TFlatEventDefs::Scheme<
        TValue1Tag, TValue2Tag, TValue3Tag, TValue4Tag, TValue5Tag, TValue6Tag, TValue7Tag, TValue8Tag, TValue9Tag,
        TValue10Tag, TValue11Tag, TValue12Tag, TValue13Tag, TValue14Tag, TValue15Tag, TValue16Tag, TValue17Tag,
        TValue18Tag, TValue19Tag, TValue20Tag, TValue21Tag, TValue22Tag, TValue23Tag, TValue24Tag, TValue25Tag,
        TValue26Tag, TValue27Tag, TValue28Tag, TValue29Tag, TValue30Tag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatThirtyMessage : TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatThirtyMessageScheme;
    using TValue1Tag = TScheme::TValue1Tag;
    using TValue2Tag = TScheme::TValue2Tag;
    using TValue3Tag = TScheme::TValue3Tag;
    using TValue4Tag = TScheme::TValue4Tag;
    using TValue5Tag = TScheme::TValue5Tag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme>;

    static TEvBenchFlatThirtyMessage* Make(ui64 base) {
        THolder<TEvBenchFlatThirtyMessage> holder(TBase::MakeEvent());
        auto frontend = holder->GetFrontend<TSchemeV1>();
#define SET_VALUE_TAG(N) frontend.template Field<TScheme::TValue##N##Tag>() = base + N;
        BENCH_30_U64_FIELDS(SET_VALUE_TAG)
#undef SET_VALUE_TAG
        return holder.Release();
    }
};

struct TEvBenchFlatArrayMessage;
struct TEvBenchFlatArrayMessageScheme {
    using TValuesTag = TFlatEventDefs::ArrayField<ui64, 0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>, TValuesTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatArrayMessage : TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;
    using TScheme = TEvBenchFlatArrayMessageScheme;
    using TValuesTag = TScheme::TValuesTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme>;
};

struct TEvBenchFlatInlineArray30Message;
struct TEvBenchFlatInlineArray30MessageScheme {
    using TValuesTag = TFlatEventDefs::InlineArrayField<ui64, 30, 0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TValuesTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatInlineArray30Message : TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatInlineArray30MessageScheme;
    using TValuesTag = TScheme::TValuesTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme>;
};

struct TEvBenchFlatStructArrayMessage;
struct TEvBenchFlatStructArrayMessageScheme {
    using TValuesTag = TFlatEventDefs::InlineArrayField<TTripleInts, 10, 0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TValuesTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlatStructArrayMessage : TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TScheme = TEvBenchFlatStructArrayMessageScheme;
    using TValuesTag = TScheme::TValuesTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme>;
};

#undef BENCH_30_U64_FIELDS

struct TEvBenchAck;
struct TEvBenchAckScheme {
    using TCountTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TChecksumTag = TFlatEventDefs::FixedField<ui64, 1>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TCountTag, TChecksumTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchAck : TEventFlat<TEvBenchAck, TEvBenchAckScheme> {
    using TBase = TEventFlat<TEvBenchAck, TEvBenchAckScheme>;

    static constexpr ui32 EventType = EvBenchAck;

    using TScheme = TEvBenchAckScheme;
    using TCountTag = TScheme::TCountTag;
    using TChecksumTag = TScheme::TChecksumTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchAck, TEvBenchAckScheme>;

    auto Count() { return this->template Field<TCountTag>(); }
    auto Count() const { return this->template Field<TCountTag>(); }
    auto Checksum() { return this->template Field<TChecksumTag>(); }
    auto Checksum() const { return this->template Field<TChecksumTag>(); }

    static TEvBenchAck* Make(ui32 count, ui64 checksum) {
        THolder<TEvBenchAck> holder(TBase::MakeEvent());
        holder->Count() = count;
        holder->Checksum() = checksum;
        return holder.Release();
    }
};

struct TEvBenchFlush;
struct TEvBenchFlushScheme {
    using TTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvBenchFlush : TEventFlat<TEvBenchFlush, TEvBenchFlushScheme> {
    using TBase = TEventFlat<TEvBenchFlush, TEvBenchFlushScheme>;

    static constexpr ui32 EventType = EvBenchFlush;

    using TScheme = TEvBenchFlushScheme;
    using TTag = TScheme::TTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvBenchFlush, TEvBenchFlushScheme>;

    auto Value() { return this->template Field<TTag>(); }
    auto Value() const { return this->template Field<TTag>(); }

    static TEvBenchFlush* Make() {
        THolder<TEvBenchFlush> holder(TBase::MakeEvent());
        holder->Value() = 0;
        return holder.Release();
    }
};

template <class TEvent>
