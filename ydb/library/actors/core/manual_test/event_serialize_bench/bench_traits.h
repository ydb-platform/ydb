#pragma once

#include "bench_messages.h"
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
        .Bits = static_cast<ui8>((((base >> 0) & 1) ? TVPutFlagsRaw::IssueKeepFlagMask : 0)
            | (((base >> 1) & 1) ? TVPutFlagsRaw::IsZeroEntryMask : 0)
            | (((base >> 2) & 1) ? TVPutFlagsRaw::IgnoreBlockMask : 0)
            | (((base >> 3) & 1) ? TVPutFlagsRaw::NotifyIfNotReadyMask : 0)),
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
    record.SetIssueKeepFlag(flags.GetIssueKeepFlag());
    record.SetIsZeroEntry(flags.GetIsZeroEntry());

    auto* vdisk = record.MutableVDiskID();
    vdisk->SetGroupID(vdiskId.GroupID);
    vdisk->SetGroupGeneration(vdiskId.GroupGeneration);
    vdisk->SetRing(vdiskId.Ring);
    vdisk->SetDomain(vdiskId.Domain);
    vdisk->SetVDisk(vdiskId.VDisk);

    record.SetFullDataSize(Payload4KB);
    record.SetIgnoreBlock(flags.GetIgnoreBlock());
    record.SetNotifyIfNotReady(flags.GetNotifyIfNotReady());
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

template <size_t PayloadBytes>
struct TPbVPutLikeTraits {
    using TEvent = TEvBenchPbVPutLikeMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (PayloadBytes == Payload256) {
            return "vput-like-256";
        } else if constexpr (PayloadBytes == Payload4KB) {
            return "vput-like-4k";
        } else {
            return "vput-like";
        }
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(new TEvent());
        const ui32 payloadId = holder->AddPayload(TRope(GetSharedPayload(PayloadBytes)));
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

template <size_t PayloadBytes>
struct TFlatVPutLikeTraits {
    using TEvent = TEvBenchFlatVPutLikeMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static constexpr TStringBuf TestName() {
        if constexpr (PayloadBytes == Payload256) {
            return "vput-like-256";
        } else if constexpr (PayloadBytes == Payload4KB) {
            return "vput-like-4k";
        } else {
            return "vput-like";
        }
    }

    static TEvent* Make(ui64 base) {
        THolder<TEvent> holder(TEvent::MakeEvent());
        auto frontend = holder->template GetFrontend<typename TEvent::TSchemeV1>();
        frontend.template Field<typename TEvent::TBlobIdTag>() = MakeBlobIdRaw(base);
        frontend.template Field<typename TEvent::TChecksumTag>() = base * 19 + 5;
        frontend.template Field<typename TEvent::TFlagsTag>() = MakeVPutFlagsRaw(base);
        frontend.template Field<typename TEvent::TVDiskIdTag>() = MakeVDiskIdRaw(base);
        frontend.template Field<typename TEvent::TFullDataSizeTag>() = PayloadBytes;
        frontend.template Field<typename TEvent::TCookieTag>() = base * 23 + 9;
        frontend.template Field<typename TEvent::THandleClassTag>() = static_cast<ui32>(base % 3 + 1);
        frontend.template Field<typename TEvent::TMsgQoSTag>() = MakeMsgQoSRaw(base);
        frontend.template Field<typename TEvent::TTimestampsTag>() = MakeTimestampsRaw(base);

        const auto checks = MakeExtraChecks(base);
        auto extraChecks = holder->template Array<typename TEvent::TExtraChecksTag>();
        extraChecks.CopyFrom(checks.data(), checks.size());

        holder->template Bytes<typename TEvent::TPayloadTag>().Set(TRope(GetSharedPayload(PayloadBytes)));
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
        sum += flags.GetIssueKeepFlag() + flags.GetIsZeroEntry()
            + flags.GetIgnoreBlock() + flags.GetNotifyIfNotReady();
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

template <>
struct TMessageTraits<TEvBenchPbVPutLikeMessage> : TPbVPutLikeTraits<Payload4KB> {
};

template <>
struct TMessageTraits<TEvBenchFlatVPutLikeMessage> : TFlatVPutLikeTraits<Payload4KB> {
};
