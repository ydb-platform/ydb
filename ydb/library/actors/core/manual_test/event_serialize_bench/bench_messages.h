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
using TEvBenchFlatMessageTValue1Tag = TFlatEventDefs::FixedField<ui64, 0>;
using TEvBenchFlatMessageTValue2Tag = TFlatEventDefs::FixedField<ui64, 1>;
using TEvBenchFlatMessageTValue3Tag = TFlatEventDefs::FixedField<ui64, 2>;
using TEvBenchFlatMessageTValue4Tag = TFlatEventDefs::FixedField<ui64, 3>;
using TEvBenchFlatMessageTValue5Tag = TFlatEventDefs::FixedField<ui64, 4>;
using TEvBenchFlatMessageTSchemeV1 = TFlatEventDefs::Scheme<
    TEvBenchFlatMessageTValue1Tag, TEvBenchFlatMessageTValue2Tag, TEvBenchFlatMessageTValue3Tag,
    TEvBenchFlatMessageTValue4Tag, TEvBenchFlatMessageTValue5Tag>;
using TEvBenchFlatMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatMessageTSchemeV1>;
struct TEvBenchFlatMessageScheme {
    using TVersions = TEvBenchFlatMessageTVersions;
};

struct TEvBenchFlatMessage : TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValue1Tag = TEvBenchFlatMessageTValue1Tag;
    using TValue2Tag = TEvBenchFlatMessageTValue2Tag;
    using TValue3Tag = TEvBenchFlatMessageTValue3Tag;
    using TValue4Tag = TEvBenchFlatMessageTValue4Tag;
    using TValue5Tag = TEvBenchFlatMessageTValue5Tag;
    using TSchemeV1 = TEvBenchFlatMessageTSchemeV1;
    using TScheme = TEvBenchFlatMessageScheme;

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
using TEvBenchFlatBytesMessageTBlobTag = TFlatEventDefs::BytesField<0>;
using TEvBenchFlatBytesMessageTSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>, TEvBenchFlatBytesMessageTBlobTag>;
using TEvBenchFlatBytesMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatBytesMessageTSchemeV1>;
struct TEvBenchFlatBytesMessageScheme {
    using TVersions = TEvBenchFlatBytesMessageTVersions;
};

struct TEvBenchFlatBytesMessage : TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TBlobTag = TEvBenchFlatBytesMessageTBlobTag;
    using TSchemeV1 = TEvBenchFlatBytesMessageTSchemeV1;
    using TScheme = TEvBenchFlatBytesMessageScheme;

    friend class TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageScheme>;
};

struct TEvBenchFlatVPutLikeMessage;
using TEvBenchFlatVPutLikeMessageTBlobIdTag = TFlatEventDefs::FixedField<TBlobIdRaw, 0>;
using TEvBenchFlatVPutLikeMessageTChecksumTag = TFlatEventDefs::FixedField<ui64, 1>;
using TEvBenchFlatVPutLikeMessageTFlagsTag = TFlatEventDefs::FixedField<TVPutFlagsRaw, 2>;
using TEvBenchFlatVPutLikeMessageTVDiskIdTag = TFlatEventDefs::FixedField<TVDiskIdRaw, 3>;
using TEvBenchFlatVPutLikeMessageTFullDataSizeTag = TFlatEventDefs::FixedField<ui64, 4>;
using TEvBenchFlatVPutLikeMessageTCookieTag = TFlatEventDefs::FixedField<ui64, 5>;
using TEvBenchFlatVPutLikeMessageTHandleClassTag = TFlatEventDefs::FixedField<ui32, 6>;
using TEvBenchFlatVPutLikeMessageTMsgQoSTag = TFlatEventDefs::FixedField<TMsgQoSRaw, 7>;
using TEvBenchFlatVPutLikeMessageTTimestampsTag = TFlatEventDefs::FixedField<TTimestampsRaw, 8>;
using TEvBenchFlatVPutLikeMessageTExtraChecksTag = TFlatEventDefs::InlineArrayField<TExtraBlockCheckRaw, VPutLikeExtraChecks, 9>;
using TEvBenchFlatVPutLikeMessageTPayloadTag = TFlatEventDefs::BytesField<10>;
using TEvBenchFlatVPutLikeMessageTSchemeV1 = TFlatEventDefs::Scheme<
    TFlatEventDefs::WithPayloadType<ui8>,
    TEvBenchFlatVPutLikeMessageTBlobIdTag,
    TEvBenchFlatVPutLikeMessageTChecksumTag,
    TEvBenchFlatVPutLikeMessageTFlagsTag,
    TEvBenchFlatVPutLikeMessageTVDiskIdTag,
    TEvBenchFlatVPutLikeMessageTFullDataSizeTag,
    TEvBenchFlatVPutLikeMessageTCookieTag,
    TEvBenchFlatVPutLikeMessageTHandleClassTag,
    TEvBenchFlatVPutLikeMessageTMsgQoSTag,
    TEvBenchFlatVPutLikeMessageTTimestampsTag,
    TEvBenchFlatVPutLikeMessageTExtraChecksTag,
    TEvBenchFlatVPutLikeMessageTPayloadTag>;
using TEvBenchFlatVPutLikeMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatVPutLikeMessageTSchemeV1>;
struct TEvBenchFlatVPutLikeMessageScheme {
    using TVersions = TEvBenchFlatVPutLikeMessageTVersions;
};

struct TEvBenchFlatVPutLikeMessage : TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TBlobIdTag = TEvBenchFlatVPutLikeMessageTBlobIdTag;
    using TChecksumTag = TEvBenchFlatVPutLikeMessageTChecksumTag;
    using TFlagsTag = TEvBenchFlatVPutLikeMessageTFlagsTag;
    using TVDiskIdTag = TEvBenchFlatVPutLikeMessageTVDiskIdTag;
    using TFullDataSizeTag = TEvBenchFlatVPutLikeMessageTFullDataSizeTag;
    using TCookieTag = TEvBenchFlatVPutLikeMessageTCookieTag;
    using THandleClassTag = TEvBenchFlatVPutLikeMessageTHandleClassTag;
    using TMsgQoSTag = TEvBenchFlatVPutLikeMessageTMsgQoSTag;
    using TTimestampsTag = TEvBenchFlatVPutLikeMessageTTimestampsTag;
    using TExtraChecksTag = TEvBenchFlatVPutLikeMessageTExtraChecksTag;
    using TPayloadTag = TEvBenchFlatVPutLikeMessageTPayloadTag;
    using TSchemeV1 = TEvBenchFlatVPutLikeMessageTSchemeV1;
    using TScheme = TEvBenchFlatVPutLikeMessageScheme;

    friend class TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageScheme>;
};

#define BENCH_30_U64_FIELDS(X) \
    X(1) X(2) X(3) X(4) X(5) X(6) X(7) X(8) X(9) X(10) \
    X(11) X(12) X(13) X(14) X(15) X(16) X(17) X(18) X(19) X(20) \
    X(21) X(22) X(23) X(24) X(25) X(26) X(27) X(28) X(29) X(30)

struct TEvBenchFlatThirtyMessage;
#define DECLARE_BENCH_30_U64_TAG(N) using TEvBenchFlatThirtyMessageTValue##N##Tag = TFlatEventDefs::FixedField<ui64, N - 1>;
BENCH_30_U64_FIELDS(DECLARE_BENCH_30_U64_TAG)
#undef DECLARE_BENCH_30_U64_TAG

using TEvBenchFlatThirtyMessageTSchemeV1 = TFlatEventDefs::Scheme<
    TEvBenchFlatThirtyMessageTValue1Tag, TEvBenchFlatThirtyMessageTValue2Tag, TEvBenchFlatThirtyMessageTValue3Tag,
    TEvBenchFlatThirtyMessageTValue4Tag, TEvBenchFlatThirtyMessageTValue5Tag, TEvBenchFlatThirtyMessageTValue6Tag,
    TEvBenchFlatThirtyMessageTValue7Tag, TEvBenchFlatThirtyMessageTValue8Tag, TEvBenchFlatThirtyMessageTValue9Tag,
    TEvBenchFlatThirtyMessageTValue10Tag, TEvBenchFlatThirtyMessageTValue11Tag, TEvBenchFlatThirtyMessageTValue12Tag,
    TEvBenchFlatThirtyMessageTValue13Tag, TEvBenchFlatThirtyMessageTValue14Tag, TEvBenchFlatThirtyMessageTValue15Tag,
    TEvBenchFlatThirtyMessageTValue16Tag, TEvBenchFlatThirtyMessageTValue17Tag, TEvBenchFlatThirtyMessageTValue18Tag,
    TEvBenchFlatThirtyMessageTValue19Tag, TEvBenchFlatThirtyMessageTValue20Tag, TEvBenchFlatThirtyMessageTValue21Tag,
    TEvBenchFlatThirtyMessageTValue22Tag, TEvBenchFlatThirtyMessageTValue23Tag, TEvBenchFlatThirtyMessageTValue24Tag,
    TEvBenchFlatThirtyMessageTValue25Tag, TEvBenchFlatThirtyMessageTValue26Tag, TEvBenchFlatThirtyMessageTValue27Tag,
    TEvBenchFlatThirtyMessageTValue28Tag, TEvBenchFlatThirtyMessageTValue29Tag, TEvBenchFlatThirtyMessageTValue30Tag>;
using TEvBenchFlatThirtyMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatThirtyMessageTSchemeV1>;
struct TEvBenchFlatThirtyMessageScheme {
    using TVersions = TEvBenchFlatThirtyMessageTVersions;
};

struct TEvBenchFlatThirtyMessage : TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValue1Tag = TEvBenchFlatThirtyMessageTValue1Tag;
    using TValue2Tag = TEvBenchFlatThirtyMessageTValue2Tag;
    using TValue3Tag = TEvBenchFlatThirtyMessageTValue3Tag;
    using TValue4Tag = TEvBenchFlatThirtyMessageTValue4Tag;
    using TValue5Tag = TEvBenchFlatThirtyMessageTValue5Tag;
    using TSchemeV1 = TEvBenchFlatThirtyMessageTSchemeV1;
    using TScheme = TEvBenchFlatThirtyMessageScheme;

    friend class TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageScheme>;

    static TEvBenchFlatThirtyMessage* Make(ui64 base) {
        THolder<TEvBenchFlatThirtyMessage> holder(TBase::MakeEvent());
        auto frontend = holder->GetFrontend<TSchemeV1>();
#define SET_VALUE_TAG(N) frontend.template Field<TEvBenchFlatThirtyMessageTValue##N##Tag>() = base + N;
        BENCH_30_U64_FIELDS(SET_VALUE_TAG)
#undef SET_VALUE_TAG
        return holder.Release();
    }
};

struct TEvBenchFlatArrayMessage;
using TEvBenchFlatArrayMessageTValuesTag = TFlatEventDefs::ArrayField<ui64, 0>;
using TEvBenchFlatArrayMessageTSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>, TEvBenchFlatArrayMessageTValuesTag>;
using TEvBenchFlatArrayMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatArrayMessageTSchemeV1>;
struct TEvBenchFlatArrayMessageScheme {
    using TVersions = TEvBenchFlatArrayMessageTVersions;
};

struct TEvBenchFlatArrayMessage : TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;
    using TValuesTag = TEvBenchFlatArrayMessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatArrayMessageTSchemeV1;
    using TScheme = TEvBenchFlatArrayMessageScheme;

    friend class TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageScheme>;
};

struct TEvBenchFlatInlineArray30Message;
using TEvBenchFlatInlineArray30MessageTValuesTag = TFlatEventDefs::InlineArrayField<ui64, 30, 0>;
using TEvBenchFlatInlineArray30MessageTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchFlatInlineArray30MessageTValuesTag>;
using TEvBenchFlatInlineArray30MessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatInlineArray30MessageTSchemeV1>;
struct TEvBenchFlatInlineArray30MessageScheme {
    using TVersions = TEvBenchFlatInlineArray30MessageTVersions;
};

struct TEvBenchFlatInlineArray30Message : TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValuesTag = TEvBenchFlatInlineArray30MessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatInlineArray30MessageTSchemeV1;
    using TScheme = TEvBenchFlatInlineArray30MessageScheme;

    friend class TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageScheme>;
};

struct TEvBenchFlatStructArrayMessage;
using TEvBenchFlatStructArrayMessageTValuesTag = TFlatEventDefs::InlineArrayField<TTripleInts, 10, 0>;
using TEvBenchFlatStructArrayMessageTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchFlatStructArrayMessageTValuesTag>;
using TEvBenchFlatStructArrayMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatStructArrayMessageTSchemeV1>;
struct TEvBenchFlatStructArrayMessageScheme {
    using TVersions = TEvBenchFlatStructArrayMessageTVersions;
};

struct TEvBenchFlatStructArrayMessage : TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme> {
    using TBase = TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValuesTag = TEvBenchFlatStructArrayMessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatStructArrayMessageTSchemeV1;
    using TScheme = TEvBenchFlatStructArrayMessageScheme;

    friend class TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageScheme>;
};

#undef BENCH_30_U64_FIELDS

struct TEvBenchAck;
using TEvBenchAckTCountTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvBenchAckTChecksumTag = TFlatEventDefs::FixedField<ui64, 1>;
using TEvBenchAckTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchAckTCountTag, TEvBenchAckTChecksumTag>;
using TEvBenchAckTVersions = TFlatEventDefs::Versions<TEvBenchAckTSchemeV1>;
struct TEvBenchAckScheme {
    using TVersions = TEvBenchAckTVersions;
};

struct TEvBenchAck : TEventFlat<TEvBenchAck, TEvBenchAckScheme> {
    using TBase = TEventFlat<TEvBenchAck, TEvBenchAckScheme>;

    static constexpr ui32 EventType = EvBenchAck;

    using TCountTag = TEvBenchAckTCountTag;
    using TChecksumTag = TEvBenchAckTChecksumTag;
    using TSchemeV1 = TEvBenchAckTSchemeV1;
    using TScheme = TEvBenchAckScheme;

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
using TEvBenchFlushTTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvBenchFlushTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchFlushTTag>;
using TEvBenchFlushTVersions = TFlatEventDefs::Versions<TEvBenchFlushTSchemeV1>;
struct TEvBenchFlushScheme {
    using TVersions = TEvBenchFlushTVersions;
};

struct TEvBenchFlush : TEventFlat<TEvBenchFlush, TEvBenchFlushScheme> {
    using TBase = TEventFlat<TEvBenchFlush, TEvBenchFlushScheme>;

    static constexpr ui32 EventType = EvBenchFlush;

    using TTag = TEvBenchFlushTTag;
    using TSchemeV1 = TEvBenchFlushTSchemeV1;
    using TScheme = TEvBenchFlushScheme;

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
