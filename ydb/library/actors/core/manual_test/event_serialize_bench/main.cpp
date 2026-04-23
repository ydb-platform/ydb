#include <ydb/library/actors/core/manual_test/event_serialize_bench/bench.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_flat.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/subsystems/stats.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/sigset.h>

#include <array>
#include <chrono>
#include <future>

using namespace NActors;
using namespace NActors::NDnsResolver;

namespace {

enum EBenchEvents {
    EvBenchPbMessage = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvBenchFlatMessage,
    EvBenchAck,
    EvBenchFlush,
};

constexpr ui32 Node1 = 1;
constexpr ui32 Node2 = 2;
constexpr size_t Payload4KB = 4 * 1024;
constexpr size_t VPutLikeExtraChecks = 4;

TActorId MakeBenchReceiverServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("bnchrecv"));
}

struct TBenchConfig {
    TString Scenario = "all";
    TString Format = "all";
    TDuration Duration = TDuration::Seconds(5);
    ui64 Window = 10000;
    ui32 AckBatch = 256;
    ui32 Threads = 2;
    ui16 PortBase = 19000;
};

struct TBenchResult {
    TString Scenario;
    TString Format;
    TString Test;
    ui64 EffectiveWindow = 0;
    ui64 SentMessages = 0;
    ui64 AckedMessages = 0;
    ui64 Checksum = 0;
    ui32 SerializedSizeStart = 0;
    ui32 SerializedSizeEnd = 0;
    double Seconds = 0;
    bool Connected = true;
    ui64 ActorCpuUs = 0;
    double ActorCpuUtilPct = 0.0;
};

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

struct TVDiskIdRaw {
    ui32 GroupID;
    ui32 GroupGeneration;
    ui8 Ring;
    ui8 Domain;
    ui8 VDisk;
};

struct TMsgQoSRaw {
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
    ui8 IssueKeepFlag;
    ui8 IsZeroEntry;
    ui8 IgnoreBlock;
    ui8 NotifyIfNotReady;
};

struct TExtraBlockCheckRaw {
    ui64 TabletId;
    ui32 Generation;
};

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

struct TEvBenchFlatMessage : TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValue1Tag = TEvBenchFlatMessageTValue1Tag;
    using TValue2Tag = TEvBenchFlatMessageTValue2Tag;
    using TValue3Tag = TEvBenchFlatMessageTValue3Tag;
    using TValue4Tag = TEvBenchFlatMessageTValue4Tag;
    using TValue5Tag = TEvBenchFlatMessageTValue5Tag;
    using TSchemeV1 = TEvBenchFlatMessageTSchemeV1;
    using TScheme = TEvBenchFlatMessageTVersions;

    friend class TEventFlat<TEvBenchFlatMessage, TEvBenchFlatMessageTVersions>;

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

struct TEvBenchFlatBytesMessage : TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TBlobTag = TEvBenchFlatBytesMessageTBlobTag;
    using TSchemeV1 = TEvBenchFlatBytesMessageTSchemeV1;
    using TScheme = TEvBenchFlatBytesMessageTVersions;

    friend class TEventFlat<TEvBenchFlatBytesMessage, TEvBenchFlatBytesMessageTVersions>;
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

struct TEvBenchFlatVPutLikeMessage : TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageTVersions>;

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
    using TScheme = TEvBenchFlatVPutLikeMessageTVersions;

    friend class TEventFlat<TEvBenchFlatVPutLikeMessage, TEvBenchFlatVPutLikeMessageTVersions>;
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

struct TEvBenchFlatThirtyMessage : TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValue1Tag = TEvBenchFlatThirtyMessageTValue1Tag;
    using TValue2Tag = TEvBenchFlatThirtyMessageTValue2Tag;
    using TValue3Tag = TEvBenchFlatThirtyMessageTValue3Tag;
    using TValue4Tag = TEvBenchFlatThirtyMessageTValue4Tag;
    using TValue5Tag = TEvBenchFlatThirtyMessageTValue5Tag;
    using TSchemeV1 = TEvBenchFlatThirtyMessageTSchemeV1;
    using TScheme = TEvBenchFlatThirtyMessageTVersions;

    friend class TEventFlat<TEvBenchFlatThirtyMessage, TEvBenchFlatThirtyMessageTVersions>;

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

struct TEvBenchFlatArrayMessage : TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;
    using TValuesTag = TEvBenchFlatArrayMessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatArrayMessageTSchemeV1;
    using TScheme = TEvBenchFlatArrayMessageTVersions;

    friend class TEventFlat<TEvBenchFlatArrayMessage, TEvBenchFlatArrayMessageTVersions>;
};

struct TEvBenchFlatInlineArray30Message;
using TEvBenchFlatInlineArray30MessageTValuesTag = TFlatEventDefs::InlineArrayField<ui64, 30, 0>;
using TEvBenchFlatInlineArray30MessageTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchFlatInlineArray30MessageTValuesTag>;
using TEvBenchFlatInlineArray30MessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatInlineArray30MessageTSchemeV1>;

struct TEvBenchFlatInlineArray30Message : TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValuesTag = TEvBenchFlatInlineArray30MessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatInlineArray30MessageTSchemeV1;
    using TScheme = TEvBenchFlatInlineArray30MessageTVersions;

    friend class TEventFlat<TEvBenchFlatInlineArray30Message, TEvBenchFlatInlineArray30MessageTVersions>;
};

struct TEvBenchFlatStructArrayMessage;
using TEvBenchFlatStructArrayMessageTValuesTag = TFlatEventDefs::InlineArrayField<TTripleInts, 10, 0>;
using TEvBenchFlatStructArrayMessageTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchFlatStructArrayMessageTValuesTag>;
using TEvBenchFlatStructArrayMessageTVersions = TFlatEventDefs::Versions<TEvBenchFlatStructArrayMessageTSchemeV1>;

struct TEvBenchFlatStructArrayMessage : TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageTVersions> {
    using TBase = TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageTVersions>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValuesTag = TEvBenchFlatStructArrayMessageTValuesTag;
    using TSchemeV1 = TEvBenchFlatStructArrayMessageTSchemeV1;
    using TScheme = TEvBenchFlatStructArrayMessageTVersions;

    friend class TEventFlat<TEvBenchFlatStructArrayMessage, TEvBenchFlatStructArrayMessageTVersions>;
};

#undef BENCH_30_U64_FIELDS

struct TEvBenchAck;
using TEvBenchAckTCountTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvBenchAckTChecksumTag = TFlatEventDefs::FixedField<ui64, 1>;
using TEvBenchAckTSchemeV1 = TFlatEventDefs::Scheme<TEvBenchAckTCountTag, TEvBenchAckTChecksumTag>;
using TEvBenchAckTVersions = TFlatEventDefs::Versions<TEvBenchAckTSchemeV1>;

struct TEvBenchAck : TEventFlat<TEvBenchAck, TEvBenchAckTVersions> {
    using TBase = TEventFlat<TEvBenchAck, TEvBenchAckTVersions>;

    static constexpr ui32 EventType = EvBenchAck;

    using TCountTag = TEvBenchAckTCountTag;
    using TChecksumTag = TEvBenchAckTChecksumTag;
    using TSchemeV1 = TEvBenchAckTSchemeV1;
    using TScheme = TEvBenchAckTVersions;

    friend class TEventFlat<TEvBenchAck, TEvBenchAckTVersions>;

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

struct TEvBenchFlush : TEventFlat<TEvBenchFlush, TEvBenchFlushTVersions> {
    using TBase = TEventFlat<TEvBenchFlush, TEvBenchFlushTVersions>;

    static constexpr ui32 EventType = EvBenchFlush;

    using TTag = TEvBenchFlushTTag;
    using TSchemeV1 = TEvBenchFlushTSchemeV1;
    using TScheme = TEvBenchFlushTVersions;

    friend class TEventFlat<TEvBenchFlush, TEvBenchFlushTVersions>;

    auto Value() { return this->template Field<TTag>(); }
    auto Value() const { return this->template Field<TTag>(); }

    static TEvBenchFlush* Make() {
        THolder<TEvBenchFlush> holder(TBase::MakeEvent());
        holder->Value() = 0;
        return holder.Release();
    }
};

template <class TEvent>
struct TMessageTraits;

ui64 SamplePayloadChecksum(const TRope& payload) {
    const size_t size = payload.GetSize();
    if (!size) {
        return 0;
    }

    auto first = payload.Begin();
    auto last = payload.Position(size - 1);

    return static_cast<ui64>(*first.ContiguousData())
        + (static_cast<ui64>(*last.ContiguousData()) << 8)
        + (static_cast<ui64>(size) << 16);
}

TBlobIdRaw MakeBlobIdRaw(ui64 base) {
    return {
        .RawX1 = base * 17 + 1,
        .RawX2 = base * 17 + 2,
        .RawX3 = base * 17 + 3,
    };
}

TVDiskIdRaw MakeVDiskIdRaw(ui64 base) {
    return {
        .GroupID = static_cast<ui32>(base % 17 + 1),
        .GroupGeneration = static_cast<ui32>(base % 7 + 2),
        .Ring = static_cast<ui8>(base % 3),
        .Domain = static_cast<ui8>(base % 5),
        .VDisk = static_cast<ui8>(base % 8),
    };
}

TMsgQoSRaw MakeMsgQoSRaw(ui64 base) {
    return {
        .Cost = base * 29 + 7,
        .DeadlineSeconds = static_cast<ui32>(base % 61 + 10),
        .ExtQueueId = static_cast<ui8>(base % 3 + 1),
    };
}

TTimestampsRaw MakeTimestampsRaw(ui64 base) {
    return {
        .SentByDSProxyUs = base * 101 + 11,
        .ReceivedByVDiskUs = base * 101 + 22,
        .SentByVDiskUs = base * 101 + 33,
        .ReceivedByDSProxyUs = base * 101 + 44,
    };
}

TVPutFlagsRaw MakeVPutFlagsRaw(ui64 base) {
    return {
        .IssueKeepFlag = static_cast<ui8>((base >> 0) & 1),
        .IsZeroEntry = static_cast<ui8>((base >> 1) & 1),
        .IgnoreBlock = static_cast<ui8>((base >> 2) & 1),
        .NotifyIfNotReady = static_cast<ui8>((base >> 3) & 1),
    };
}

std::array<TExtraBlockCheckRaw, VPutLikeExtraChecks> MakeExtraChecks(ui64 base) {
    std::array<TExtraBlockCheckRaw, VPutLikeExtraChecks> checks;
    for (size_t i = 0; i < checks.size(); ++i) {
        checks[i] = {
            .TabletId = base * 37 + i + 1,
            .Generation = static_cast<ui32>(base % 11 + i + 1),
        };
    }
    return checks;
}

void FillVPutLikeRecord(NActorsBench::TVPutLike& record, ui64 base, ui32 payloadId) {
    const TBlobIdRaw blobId = MakeBlobIdRaw(base);
    const TVDiskIdRaw vdiskId = MakeVDiskIdRaw(base);
    const TMsgQoSRaw msgQoS = MakeMsgQoSRaw(base);
    const TTimestampsRaw timestamps = MakeTimestampsRaw(base);
    const TVPutFlagsRaw flags = MakeVPutFlagsRaw(base);
    const auto checks = MakeExtraChecks(base);

    auto* blob = record.MutableBlobID();
    blob->SetRawX1(blobId.RawX1);
    blob->SetRawX2(blobId.RawX2);
    blob->SetRawX3(blobId.RawX3);
    record.SetChecksum(base * 19 + 5);
    record.SetIssueKeepFlag(flags.IssueKeepFlag);
    record.SetIsZeroEntry(flags.IsZeroEntry);

    auto* vdisk = record.MutableVDiskID();
    vdisk->SetGroupID(vdiskId.GroupID);
    vdisk->SetGroupGeneration(vdiskId.GroupGeneration);
    vdisk->SetRing(vdiskId.Ring);
    vdisk->SetDomain(vdiskId.Domain);
    vdisk->SetVDisk(vdiskId.VDisk);

    record.SetFullDataSize(Payload4KB);
    record.SetIgnoreBlock(flags.IgnoreBlock);
    record.SetNotifyIfNotReady(flags.NotifyIfNotReady);
    record.SetCookie(base * 23 + 9);
    record.SetHandleClass(static_cast<ui32>(base % 3 + 1));

    auto* qos = record.MutableMsgQoS();
    qos->SetDeadlineSeconds(msgQoS.DeadlineSeconds);
    qos->SetCost(msgQoS.Cost);
    qos->SetExtQueueId(msgQoS.ExtQueueId);

    auto* ts = record.MutableTimestamps();
    ts->SetSentByDSProxyUs(timestamps.SentByDSProxyUs);
    ts->SetReceivedByVDiskUs(timestamps.ReceivedByVDiskUs);
    ts->SetSentByVDiskUs(timestamps.SentByVDiskUs);
    ts->SetReceivedByDSProxyUs(timestamps.ReceivedByDSProxyUs);

    for (const TExtraBlockCheckRaw& check : checks) {
        auto* item = record.AddExtraBlockChecks();
        item->SetTabletId(check.TabletId);
        item->SetGeneration(check.Generation);
    }

    record.SetPayloadId(payloadId);
}

ui64 ReadVPutLikeRecord(const NActorsBench::TVPutLike& record, const TRope& payload) {
    ui64 sum = SamplePayloadChecksum(payload);
    const auto& blob = record.GetBlobID();
    const auto& vdisk = record.GetVDiskID();
    const auto& qos = record.GetMsgQoS();
    const auto& ts = record.GetTimestamps();

    sum += blob.GetRawX1() + blob.GetRawX2() + blob.GetRawX3();
    sum += record.GetChecksum();
    sum += static_cast<ui64>(record.GetIssueKeepFlag());
    sum += static_cast<ui64>(record.GetIsZeroEntry());
    sum += vdisk.GetGroupID() + vdisk.GetGroupGeneration() + vdisk.GetRing() + vdisk.GetDomain() + vdisk.GetVDisk();
    sum += record.GetFullDataSize();
    sum += static_cast<ui64>(record.GetIgnoreBlock());
    sum += static_cast<ui64>(record.GetNotifyIfNotReady());
    sum += record.GetCookie();
    sum += record.GetHandleClass();
    sum += qos.GetDeadlineSeconds() + qos.GetCost() + qos.GetExtQueueId();
    sum += ts.GetSentByDSProxyUs() + ts.GetReceivedByVDiskUs() + ts.GetSentByVDiskUs() + ts.GetReceivedByDSProxyUs();

    for (const auto& check : record.GetExtraBlockChecks()) {
        sum += check.GetTabletId() + check.GetGeneration();
    }

    return sum;
}

const TRope& GetSharedPayload(size_t size) {
    static THashMap<size_t, TRope> payloads;

    auto [it, inserted] = payloads.try_emplace(size);
    if (inserted) {
        TString data = TString::Uninitialized(size);
        char* ptr = data.Detach();
        for (size_t i = 0; i < size; ++i) {
            ptr[i] = static_cast<char>((i * 131u + size) & 0xFF);
        }
        it->second = TRope(std::move(data));
    }

    return it->second;
}

void FillThirtyU64Record(NActorsBench::TThirtyU64& record, ui64 base) {
    record.SetValue1(base + 1);
    record.SetValue2(base + 2);
    record.SetValue3(base + 3);
    record.SetValue4(base + 4);
    record.SetValue5(base + 5);
    record.SetValue6(base + 6);
    record.SetValue7(base + 7);
    record.SetValue8(base + 8);
    record.SetValue9(base + 9);
    record.SetValue10(base + 10);
    record.SetValue11(base + 11);
    record.SetValue12(base + 12);
    record.SetValue13(base + 13);
    record.SetValue14(base + 14);
    record.SetValue15(base + 15);
    record.SetValue16(base + 16);
    record.SetValue17(base + 17);
    record.SetValue18(base + 18);
    record.SetValue19(base + 19);
    record.SetValue20(base + 20);
    record.SetValue21(base + 21);
    record.SetValue22(base + 22);
    record.SetValue23(base + 23);
    record.SetValue24(base + 24);
    record.SetValue25(base + 25);
    record.SetValue26(base + 26);
    record.SetValue27(base + 27);
    record.SetValue28(base + 28);
    record.SetValue29(base + 29);
    record.SetValue30(base + 30);
}

ui64 ReadFirstFiveU64Record(const NActorsBench::TThirtyU64& record) {
    return record.GetValue1()
        + record.GetValue2()
        + record.GetValue3()
        + record.GetValue4()
        + record.GetValue5();
}

template <>
struct TMessageTraits<TEvBenchPbMessage> {
    using TEvent = TEvBenchPbMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        return "fixed-u64";
    }

    static TEvent* Make(ui64 base) {
        return new TEvent(base);
    }

    static ui64 Read(const TEvent& ev) {
        const auto& r = ev.Record;
        return r.GetValue1() + r.GetValue2() + r.GetValue3() + r.GetValue4() + r.GetValue5();
    }

    static ui32 SerializedSize(ui64 base) {
        TEvent ev(base);
        return ev.CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchFlatMessage> {
    using TEvent = TEvBenchFlatMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        return "fixed-u64";
    }

    static TEvent* Make(ui64 base) {
        return TEvent::Make(base);
    }

    static ui64 Read(const TEvent& ev) {
        auto frontend = ev.template GetFrontend<typename TEvent::TSchemeV1>();
        return static_cast<ui64>(frontend.template Field<typename TEvent::TValue1Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue2Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue3Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue4Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue5Tag>());
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(TEvent::Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <size_t PayloadBytes>
struct TPayloadPbTraits {
    using TEvent = TEvBenchPbBytesMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (PayloadBytes == Payload4KB) {
            return "payload-bytes-4k";
        } else {
            return "payload-bytes";
        }
    }

    static TEvent* Make(ui64) {
        THolder<TEvent> holder(new TEvent());
        const ui64 payloadId = holder->AddPayload(TRope(GetSharedPayload(PayloadBytes)));
        holder->Record.SetPayloadId(payloadId);
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        const ui64 payloadId = ev.Record.GetPayloadId();
        Y_ABORT_UNLESS(payloadId < ev.GetPayloadCount());
        return SamplePayloadChecksum(ev.GetPayload(payloadId));
    }

    static ui32 SerializedSize(ui64) {
        THolder<TEvent> ev(Make(0));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <size_t PayloadBytes>
struct TPayloadFlatTraits {
    using TEvent = TEvBenchFlatBytesMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (PayloadBytes == Payload4KB) {
            return "payload-bytes-4k";
        } else {
            return "payload-bytes";
        }
    }

    static TEvent* Make(ui64) {
        THolder<TEvent> holder(TEvent::MakeEvent());
        holder->template Bytes<typename TEvent::TBlobTag>().Set(TRope(GetSharedPayload(PayloadBytes)));
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        return SamplePayloadChecksum(ev.template Bytes<typename TEvent::TBlobTag>().Rope());
    }

    static ui32 SerializedSize(ui64) {
        THolder<TEvent> ev(Make(0));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchPbThirtyMessage> {
    using TEvent = TEvBenchPbThirtyMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        return "fixed30-read5";
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(new TEvent());
        FillThirtyU64Record(holder->Record, base);
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        return ReadFirstFiveU64Record(ev.Record);
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchFlatThirtyMessage> {
    using TEvent = TEvBenchFlatThirtyMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        return "fixed30-read5";
    }

    static TEvent* Make(ui64 base) {
        return TEvent::Make(base);
    }

    static ui64 Read(const TEvent& ev) {
        auto frontend = ev.template GetFrontend<typename TEvent::TSchemeV1>();
        return static_cast<ui64>(frontend.template Field<typename TEvent::TValue1Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue2Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue3Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue4Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue5Tag>());
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <size_t ItemCount, size_t ReadCount>
struct TArrayPbTraits {
    static_assert(ReadCount <= ItemCount);

    using TEvent = TEvBenchPbArrayMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (ItemCount == 30 && ReadCount == 5) {
            return "array30-read5";
        } else if constexpr (ItemCount == 1000 && ReadCount == 1000) {
            return "array1000-readall";
        } else {
            return "array";
        }
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(new TEvent());
        auto* values = holder->Record.MutableValues();
        values->Reserve(ItemCount);
        for (ui64 i = 0; i < ItemCount; ++i) {
            values->Add(base + i + 1);
        }
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        const auto& values = ev.Record.GetValues();
        ui64 sum = 0;
        for (size_t i = 0; i < ReadCount; ++i) {
            sum += values.Get(i);
        }
        return sum;
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <size_t ItemCount, size_t ReadCount>
struct TArrayFlatTraits {
    static_assert(ReadCount <= ItemCount);

    using TEvent = std::conditional_t<ItemCount == 30, TEvBenchFlatInlineArray30Message, TEvBenchFlatArrayMessage>;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (ItemCount == 30 && ReadCount == 5) {
            return "array30-read5";
        } else if constexpr (ItemCount == 1000 && ReadCount == 1000) {
            return "array1000-readall";
        } else {
            return "array";
        }
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(TEvent::MakeEvent());
        auto array = holder->template Array<typename TEvent::TValuesTag>();
        if constexpr (ItemCount == 30) {
            std::array<ui64, ItemCount> values;
            for (size_t i = 0; i < ItemCount; ++i) {
                values[i] = base + i + 1;
            }
            array.CopyFrom(values.data(), values.size());
        } else {
            ui64* values = array.Init(ItemCount);
            for (size_t i = 0; i < ItemCount; ++i) {
                values[i] = base + i + 1;
            }
        }
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        ui64 sum = 0;
        if constexpr (ItemCount == 30) {
            auto values = ev.template Array<typename TEvent::TValuesTag>();
            for (size_t i = 0; i < ReadCount; ++i) {
                sum += static_cast<ui64>(values[i]);
            }
        } else {
            const ui64* values = ev.template ArrayData<typename TEvent::TValuesTag>();
            for (size_t i = 0; i < ReadCount; ++i) {
                sum += values[i];
            }
        }
        return sum;
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchPbStructArrayMessage> {
    using TEvent = TEvBenchPbStructArrayMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        return "struct-array10-read5";
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(new TEvent());
        for (ui64 i = 0; i < 10; ++i) {
            auto* item = holder->Record.AddValues();
            item->SetA(static_cast<ui32>(base + i * 3 + 1));
            item->SetB(static_cast<ui32>(base + i * 3 + 2));
            item->SetC(static_cast<ui32>(base + i * 3 + 3));
        }
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        ui64 sum = 0;
        const auto& values = ev.Record.GetValues();
        for (int i = 0; i < 5; ++i) {
            sum += values.Get(i).GetA() + values.Get(i).GetB() + values.Get(i).GetC();
        }
        return sum;
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchFlatStructArrayMessage> {
    using TEvent = TEvBenchFlatStructArrayMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        return "struct-array10-read5";
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(TEvent::MakeEvent());
        std::array<TTripleInts, 10> values;
        for (size_t i = 0; i < values.size(); ++i) {
            values[i] = TTripleInts{
                .A = static_cast<ui32>(base + i * 3 + 1),
                .B = static_cast<ui32>(base + i * 3 + 2),
                .C = static_cast<ui32>(base + i * 3 + 3),
            };
        }
        auto array = holder->template Array<typename TEvent::TValuesTag>();
        array.CopyFrom(values.data(), values.size());
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        auto values = ev.template Array<typename TEvent::TValuesTag>();
        ui64 sum = 0;
        for (size_t i = 0; i < 5; ++i) {
            const TTripleInts value = values[i];
            sum += value.A + value.B + value.C;
        }
        return sum;
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchPbVPutLikeMessage> {
    using TEvent = TEvBenchPbVPutLikeMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        return "vput-like-4k";
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(new TEvent());
        const ui32 payloadId = holder->AddPayload(TRope(GetSharedPayload(Payload4KB)));
        FillVPutLikeRecord(holder->Record, base, payloadId);
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        const ui32 payloadId = ev.Record.GetPayloadId();
        Y_ABORT_UNLESS(payloadId < ev.GetPayloadCount());
        return ReadVPutLikeRecord(ev.Record, ev.GetPayload(payloadId));
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <>
struct TMessageTraits<TEvBenchFlatVPutLikeMessage> {
    using TEvent = TEvBenchFlatVPutLikeMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        return "vput-like-4k";
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(TEvent::MakeEvent());
        auto frontend = holder->template GetFrontend<typename TEvent::TSchemeV1>();
        frontend.template Field<typename TEvent::TBlobIdTag>() = MakeBlobIdRaw(base);
        frontend.template Field<typename TEvent::TChecksumTag>() = base * 19 + 5;
        frontend.template Field<typename TEvent::TFlagsTag>() = MakeVPutFlagsRaw(base);
        frontend.template Field<typename TEvent::TVDiskIdTag>() = MakeVDiskIdRaw(base);
        frontend.template Field<typename TEvent::TFullDataSizeTag>() = Payload4KB;
        frontend.template Field<typename TEvent::TCookieTag>() = base * 23 + 9;
        frontend.template Field<typename TEvent::THandleClassTag>() = static_cast<ui32>(base % 3 + 1);
        frontend.template Field<typename TEvent::TMsgQoSTag>() = MakeMsgQoSRaw(base);
        frontend.template Field<typename TEvent::TTimestampsTag>() = MakeTimestampsRaw(base);

        const auto checks = MakeExtraChecks(base);
        auto extraChecks = holder->template Array<typename TEvent::TExtraChecksTag>();
        extraChecks.CopyFrom(checks.data(), checks.size());

        holder->template Bytes<typename TEvent::TPayloadTag>().Set(TRope(GetSharedPayload(Payload4KB)));
        return holder.Release();
    }

    static ui64 Read(const TEvent& ev) {
        auto frontend = ev.template GetFrontend<typename TEvent::TSchemeV1>();
        const TBlobIdRaw blob = frontend.template Field<typename TEvent::TBlobIdTag>();
        const TVPutFlagsRaw flags = frontend.template Field<typename TEvent::TFlagsTag>();
        const TVDiskIdRaw vdisk = frontend.template Field<typename TEvent::TVDiskIdTag>();
        const TMsgQoSRaw qos = frontend.template Field<typename TEvent::TMsgQoSTag>();
        const TTimestampsRaw ts = frontend.template Field<typename TEvent::TTimestampsTag>();
        auto extraChecks = ev.template Array<typename TEvent::TExtraChecksTag>();

        ui64 sum = SamplePayloadChecksum(ev.template Bytes<typename TEvent::TPayloadTag>().Rope());
        sum += blob.RawX1 + blob.RawX2 + blob.RawX3;
        sum += static_cast<ui64>(frontend.template Field<typename TEvent::TChecksumTag>());
        sum += flags.IssueKeepFlag + flags.IsZeroEntry + flags.IgnoreBlock + flags.NotifyIfNotReady;
        sum += vdisk.GroupID + vdisk.GroupGeneration + vdisk.Ring + vdisk.Domain + vdisk.VDisk;
        sum += static_cast<ui64>(frontend.template Field<typename TEvent::TFullDataSizeTag>());
        sum += static_cast<ui64>(frontend.template Field<typename TEvent::TCookieTag>());
        sum += static_cast<ui64>(frontend.template Field<typename TEvent::THandleClassTag>());
        sum += qos.DeadlineSeconds + qos.Cost + qos.ExtQueueId;
        sum += ts.SentByDSProxyUs + ts.ReceivedByVDiskUs + ts.SentByVDiskUs + ts.ReceivedByDSProxyUs;

        for (size_t i = 0; i < extraChecks.size(); ++i) {
            const TExtraBlockCheckRaw item = extraChecks[i];
            sum += item.TabletId + item.Generation;
        }

        return sum;
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(Make(base));
        return ev->CalculateSerializedSize();
    }

    static ui64 EffectiveWindow(ui64 window) {
        return window;
    }
};

template <class TTraits>
class TReceiverActor : public TActorBootstrapped<TReceiverActor<TTraits>> {
    using TMessage = typename TTraits::TEvent;

public:
    explicit TReceiverActor(ui32 ackBatch)
        : AckBatch(ackBatch)
    {}

    void Bootstrap() {
        this->Become(&TReceiverActor::StateWork);
    }

private:
    void Handle(typename TMessage::TPtr& ev) {
        PendingCount += 1;
        PendingChecksum += TTraits::Read(*ev->Get());

        if (PendingCount >= AckBatch) {
            SendAck(ev->Sender);
        }
    }

    void Handle(TEvBenchFlush::TPtr& ev) {
        if (PendingCount) {
            SendAck(ev->Sender);
        }
    }

    void SendAck(const TActorId& recipient) {
        this->Send(recipient, TEvBenchAck::Make(PendingCount, PendingChecksum));
        PendingCount = 0;
        PendingChecksum = 0;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TMessage, Handle);
            hFunc(TEvBenchFlush, Handle);
        }
    }

private:
    const ui32 AckBatch;
    ui32 PendingCount = 0;
    ui64 PendingChecksum = 0;
};

template <class TTraits>
class TSenderActor : public TActorBootstrapped<TSenderActor<TTraits>> {
    using TMessage = typename TTraits::TEvent;

public:
    TSenderActor(
            TActorId target,
            TDuration duration,
            ui64 window,
            TString scenarioName,
            std::promise<TBenchResult> promise)
        : Target(target)
        , Duration(duration)
        , Window(window)
        , ScenarioName(std::move(scenarioName))
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        this->Become(&TSenderActor::StateWork);
        if (Target.NodeId() != this->SelfId().NodeId()) {
            this->Send(TActivationContext::InterconnectProxy(Target.NodeId()), new TEvInterconnect::TEvConnectNode, IEventHandle::FlagTrackDelivery);
        } else {
            StartBenchmark();
        }
    }

private:
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        if (ev->Get()->NodeId == Target.NodeId() && !Started) {
            StartBenchmark();
        }
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        if (ev->Get()->NodeId == Target.NodeId()) {
            Finish(false);
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        Finish(false);
    }

    void HandleAck(TEvBenchAck::TPtr& ev) {
        const ui32 count = static_cast<ui32>(ev->Get()->Count());
        const ui64 checksum = static_cast<ui64>(ev->Get()->Checksum());

        Y_ABORT_UNLESS(InFlight >= count);
        InFlight -= count;
        AckedMessages += count;
        Checksum += checksum;

        if (!Stopping) {
            SendBurst(count);
        } else if (InFlight == 0) {
            Finish(true);
        }
    }

    void HandleWakeup() {
        Stopping = true;
        this->Send(Target, TEvBenchFlush::Make());
        if (InFlight == 0) {
            Finish(true);
        }
    }

    void StartBenchmark() {
        Started = true;
        Start = TInstant::Now();
        this->Schedule(Duration, new TEvents::TEvWakeup());
        SendBurst(Window);
    }

    void SendBurst(ui64 count) {
        for (ui64 i = 0; i < count; ++i) {
            this->Send(Target, TTraits::Make(NextBaseValue++));
            ++InFlight;
            ++SentMessages;
        }
    }

    void Finish(bool connected) {
        if (Finished) {
            return;
        }
        Finished = true;

        TBenchResult result;
        result.Scenario = ScenarioName;
        result.Format = TString(TTraits::Name());
        result.Test = TString(TTraits::TestName());
        result.EffectiveWindow = Window;
        result.SentMessages = SentMessages;
        result.AckedMessages = AckedMessages;
        result.Checksum = Checksum;
        result.SerializedSizeStart = TTraits::SerializedSize(1);
        result.SerializedSizeEnd = TTraits::SerializedSize(NextBaseValue > 1 ? NextBaseValue - 1 : 1);
        result.Connected = connected;
        result.Seconds = Started ? (TInstant::Now() - Start).SecondsFloat() : 0.0;

        Promise.set_value(std::move(result));
        this->PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBenchAck, HandleAck);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        }
    }

private:
    const TActorId Target;
    const TDuration Duration;
    const ui64 Window;
    const TString ScenarioName;
    std::promise<TBenchResult> Promise;

    bool Started = false;
    bool Stopping = false;
    bool Finished = false;
    TInstant Start;
    ui64 NextBaseValue = 1;
    ui64 InFlight = 0;
    ui64 SentMessages = 0;
    ui64 AckedMessages = 0;
    ui64 Checksum = 0;
};

THolder<TActorSystemSetup> BuildLocalActorSystemSetup(ui32 nodeId, ui32 threads) {
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

THolder<TActorSystemSetup> BuildInterconnectActorSystemSetup(
        ui32 nodeId,
        ui32 threads,
        ui16 portBase,
        NMonitoring::TDynamicCounters& counters)
{
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));
    setup->LocalServices.emplace_back(MakeDnsResolverActorId(), TActorSetupCmd(CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
    nameserverTable->StaticNodeTable[Node1] = std::make_pair("127.0.0.1", portBase + Node1);
    nameserverTable->StaticNodeTable[Node2] = std::make_pair("127.0.0.1", portBase + Node2);
    setup->LocalServices.emplace_back(GetNameserviceActorId(), TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
    icCommon->NameserviceId = GetNameserviceActorId();
    icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
    icCommon->TechnicalSelfHostName = "127.0.0.1";

    setup->Interconnect.ProxyActors.resize(Node2 + 1);
    for (ui32 peerNodeId : {Node1, Node2}) {
        if (peerNodeId == nodeId) {
            IActor* listener = new TInterconnectListenerTCP("127.0.0.1", portBase + nodeId, icCommon);
            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0));
        } else {
            setup->Interconnect.ProxyActors[peerNodeId] = TActorSetupCmd(
                new TInterconnectProxyTCP(peerNodeId, icCommon),
                TMailboxType::ReadAsFilled,
                0);
        }
    }

    return setup;
}

ui64 CollectActorSystemCpuUs(const TActorSystem& actorSystem) {
    ui64 totalCpuUs = 0;
    const auto& statsSubSystem = GetActorSystemStats(actorSystem);
    const ui32 poolCount = actorSystem.GetBasicExecutorPools().size();
    for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
        TExecutorPoolStats poolStats;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        statsSubSystem.GetPoolStats(poolId, poolStats, stats, sharedStats);
        for (const auto& item : stats) {
            totalCpuUs += item.CpuUs;
        }
        for (const auto& item : sharedStats) {
            totalCpuUs += item.CpuUs;
        }
    }
    return totalCpuUs;
}

double CalcActorCpuUtilPct(ui64 actorCpuUs, double seconds, ui32 totalThreads) {
    if (seconds <= 0.0 || totalThreads == 0) {
        return 0.0;
    }
    const double capacityUs = seconds * 1000000.0 * totalThreads;
    return capacityUs > 0.0 ? actorCpuUs * 100.0 / capacityUs : 0.0;
}

template <class TTraits>
TBenchResult RunLocalBenchmark(const TBenchConfig& config) {
    const ui64 window = TTraits::EffectiveWindow(config.Window);
    THolder<TActorSystemSetup> setup = BuildLocalActorSystemSetup(Node1, config.Threads);
    TActorSystem actorSystem(setup);
    actorSystem.Start();

    const TActorId receiverId = actorSystem.Register(new TReceiverActor<TTraits>(config.AckBatch));
    actorSystem.RegisterLocalService(MakeBenchReceiverServiceId(Node1), receiverId);

    std::promise<TBenchResult> promise;
    std::future<TBenchResult> future = promise.get_future();
    actorSystem.Register(new TSenderActor<TTraits>(
        MakeBenchReceiverServiceId(Node1),
        config.Duration,
        window,
        "local",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();
    result.ActorCpuUs = CollectActorSystemCpuUs(actorSystem);
    result.ActorCpuUtilPct = CalcActorCpuUtilPct(result.ActorCpuUs, result.Seconds, config.Threads);

    actorSystem.Stop();
    actorSystem.Cleanup();
    return result;
}

template <class TTraits>
TBenchResult RunInterconnectBenchmark(const TBenchConfig& config) {
    const ui64 window = TTraits::EffectiveWindow(config.Window);
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters1 = new NMonitoring::TDynamicCounters();
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters2 = new NMonitoring::TDynamicCounters();

    THolder<TActorSystemSetup> setup2 = BuildInterconnectActorSystemSetup(Node2, config.Threads, config.PortBase, *counters2);
    THolder<TActorSystemSetup> setup1 = BuildInterconnectActorSystemSetup(Node1, config.Threads, config.PortBase, *counters1);
    TActorSystem node2(setup2);
    TActorSystem node1(setup1);

    node2.Start();
    node1.Start();

    const TActorId receiverId = node2.Register(new TReceiverActor<TTraits>(config.AckBatch));
    node2.RegisterLocalService(MakeBenchReceiverServiceId(Node2), receiverId);

    std::promise<TBenchResult> promise;
    std::future<TBenchResult> future = promise.get_future();
    node1.Register(new TSenderActor<TTraits>(
        MakeBenchReceiverServiceId(Node2),
        config.Duration,
        window,
        "interconnect",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();
    result.ActorCpuUs = CollectActorSystemCpuUs(node1) + CollectActorSystemCpuUs(node2);
    result.ActorCpuUtilPct = CalcActorCpuUtilPct(result.ActorCpuUs, result.Seconds, config.Threads * 2);

    node1.Stop();
    node2.Stop();
    node1.Cleanup();
    node2.Cleanup();
    return result;
}

void PrintUsage(const char* argv0) {
    Cout
        << "Usage: " << argv0 << " [--scenario local|interconnect|all] [--format protobuf|flat|all]" << Endl
        << "       [--duration-sec N] [--window N] [--ack-batch N] [--threads N] [--port-base N]" << Endl;
}

bool ParseArgs(int argc, char** argv, TBenchConfig& config) {
    for (int i = 1; i < argc; ++i) {
        TStringBuf arg(argv[i]);
        const size_t pos = arg.find('=');
        const TStringBuf key = pos == TStringBuf::npos ? arg : arg.Head(pos);
        const TStringBuf value = pos == TStringBuf::npos ? TStringBuf() : arg.Skip(pos + 1);

        if (key == "--help") {
            return false;
        } else if (key == "--scenario" && value) {
            config.Scenario = TString(value);
        } else if (key == "--format" && value) {
            config.Format = TString(value);
        } else if (key == "--duration-sec" && value) {
            config.Duration = TDuration::Seconds(FromString<ui32>(value));
        } else if (key == "--window" && value) {
            config.Window = FromString<ui64>(value);
        } else if (key == "--ack-batch" && value) {
            config.AckBatch = FromString<ui32>(value);
        } else if (key == "--threads" && value) {
            config.Threads = FromString<ui32>(value);
        } else if (key == "--port-base" && value) {
            config.PortBase = FromString<ui16>(value);
        } else {
            Cerr << "Unknown argument: " << arg << Endl;
            return false;
        }
    }

    Y_ABORT_UNLESS(config.Window > 0);
    Y_ABORT_UNLESS(config.AckBatch > 0);
    Y_ABORT_UNLESS(config.Threads > 0);
    Y_ABORT_UNLESS(config.Scenario == "all" || config.Scenario == "local" || config.Scenario == "interconnect");
    Y_ABORT_UNLESS(config.Format == "all" || config.Format == "protobuf" || config.Format == "flat");
    return true;
}

template <class TTraits>
void MaybeRunScenario(const TBenchConfig& config, TStringBuf scenarioName) {
    if (config.Format != "all" && config.Format != TTraits::Name()) {
        return;
    }

    TBenchResult result;
    if (scenarioName == "local") {
        result = RunLocalBenchmark<TTraits>(config);
    } else if (scenarioName == "interconnect") {
        result = RunInterconnectBenchmark<TTraits>(config);
    } else {
        Y_ABORT("Unexpected scenario");
    }

    const double messageRate = result.Seconds > 0 ? result.AckedMessages / result.Seconds : 0.0;
    const double mibRate = result.Seconds > 0
        ? (result.AckedMessages * result.SerializedSizeEnd) / result.Seconds / (1024.0 * 1024.0)
        : 0.0;

    Cout
        << "scenario=" << result.Scenario
        << " format=" << result.Format
        << " test=" << result.Test
        << " effective_window=" << result.EffectiveWindow
        << " connected=" << (result.Connected ? "true" : "false")
        << " sample_size_start=" << result.SerializedSizeStart
        << " sample_size_end=" << result.SerializedSizeEnd
        << " sent=" << result.SentMessages
        << " acked=" << result.AckedMessages
        << " seconds=" << Sprintf("%.3f", result.Seconds)
        << " msg_per_sec=" << Sprintf("%.2f", messageRate)
        << " mib_per_sec=" << Sprintf("%.2f", mibRate)
        << " actor_cpu_us=" << result.ActorCpuUs
        << " actor_cpu_util_pct=" << Sprintf("%.2f", result.ActorCpuUtilPct)
        << " checksum=" << result.Checksum
        << Endl;
}

void RunBenchmarks(const TBenchConfig& config) {
    Cout
        << "duration=" << config.Duration
        << " window=" << config.Window
        << " ack_batch=" << config.AckBatch
        << " threads=" << config.Threads
        << " port_base=" << config.PortBase
        << Endl;

    const bool runLocal = config.Scenario == "all" || config.Scenario == "local";
    const bool runInterconnect = config.Scenario == "all" || config.Scenario == "interconnect";

    if (runLocal) {
        MaybeRunScenario<TMessageTraits<TEvBenchPbMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchPbThirtyMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatThirtyMessage>>(config, "local");
        MaybeRunScenario<TArrayPbTraits<30, 5>>(config, "local");
        MaybeRunScenario<TArrayFlatTraits<30, 5>>(config, "local");
        MaybeRunScenario<TArrayPbTraits<1000, 1000>>(config, "local");
        MaybeRunScenario<TArrayFlatTraits<1000, 1000>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchPbStructArrayMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatStructArrayMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchPbVPutLikeMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatVPutLikeMessage>>(config, "local");
        MaybeRunScenario<TPayloadPbTraits<Payload4KB>>(config, "local");
        MaybeRunScenario<TPayloadFlatTraits<Payload4KB>>(config, "local");
    }

    if (runInterconnect) {
        MaybeRunScenario<TMessageTraits<TEvBenchPbMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbThirtyMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatThirtyMessage>>(config, "interconnect");
        MaybeRunScenario<TArrayPbTraits<30, 5>>(config, "interconnect");
        MaybeRunScenario<TArrayFlatTraits<30, 5>>(config, "interconnect");
        MaybeRunScenario<TArrayPbTraits<1000, 1000>>(config, "interconnect");
        MaybeRunScenario<TArrayFlatTraits<1000, 1000>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbStructArrayMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatStructArrayMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbVPutLikeMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatVPutLikeMessage>>(config, "interconnect");
        MaybeRunScenario<TPayloadPbTraits<Payload4KB>>(config, "interconnect");
        MaybeRunScenario<TPayloadFlatTraits<Payload4KB>>(config, "interconnect");
    }
}

} // namespace

int main(int argc, char** argv) {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    TBenchConfig config;
    if (!ParseArgs(argc, argv, config)) {
        PrintUsage(argv[0]);
        return 1;
    }

    RunBenchmarks(config);
    return 0;
}
