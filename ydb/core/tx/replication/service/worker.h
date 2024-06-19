#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <functional>

namespace NKikimr::NReplication::NService {

struct TEvWorker {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_WORKER),

        EvHandshake,
        EvPoll,
        EvData,
        EvGone,
        EvStatus,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_WORKER));

    struct TEvHandshake: public TEventLocal<TEvHandshake, EvHandshake> {};
    struct TEvPoll: public TEventLocal<TEvPoll, EvPoll> {};

    struct TEvData: public TEventLocal<TEvData, EvData> {
        struct TRecord {
            ui64 Offset;
            TString Data;
            TInstant CreateTime;

            explicit TRecord(ui64 offset, const TString& data, TInstant createTime = TInstant::Zero());
            explicit TRecord(ui64 offset, TString&& data, TInstant createTime = TInstant::Zero());
            void Out(IOutputStream& out) const;
        };

        TString Source;
        TVector<TRecord> Records;

        explicit TEvData(const TString& source, const TVector<TRecord>& records);
        explicit TEvData(const TString& source, TVector<TRecord>&& records);
        TString ToString() const override;
    };

    struct TEvGone: public TEventLocal<TEvGone, EvGone> {
        enum EStatus {
            DONE,
            S3_ERROR,
            SCHEME_ERROR,
            UNAVAILABLE,
        };

        EStatus Status;
        TString ErrorDescription;

        explicit TEvGone(EStatus status, const TString& errorDescription = {});
        TString ToString() const override;
    };

    struct TEvStatus: public TEventLocal<TEvStatus, EvStatus> {
        TDuration Lag;

        explicit TEvStatus(TDuration lag);
        TString ToString() const override;
    };
};

IActor* CreateWorker(
    const TActorId& parent,
    std::function<IActor*(void)>&& createReaderFn,
    std::function<IActor*(void)>&& createWriterFn);

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NReplication::NService::TEvWorker::TEvData::TRecord, o, x) {
    return x.Out(o);
}
