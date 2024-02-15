#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>

#include <util/generic/vector.h>

namespace NKikimr::NReplication::NService {

struct TEvWorker {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_SERVICE),

        EvHandshake,
        EvPoll,
        EvData,
        EvGone,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_SERVICE));

    struct TEvHandshake: public TEventLocal<TEvHandshake, EvHandshake> {};
    struct TEvPoll: public TEventLocal<TEvPoll, EvPoll> {};

    struct TEvData: public TEventLocal<TEvData, EvData> {
        struct TRecord {
            ui64 Offset;
            TString Data;

            explicit TRecord(ui64 offset, const TString& data);
            explicit TRecord(ui64 offset, TString&& data);
            void Out(IOutputStream& out) const;
        };

        TVector<TRecord> Records;

        explicit TEvData(TVector<TRecord>&& records);
        TString ToString() const override;
    };

    struct TEvGone: public TEventLocal<TEvGone, EvGone> {
        enum EStatus {
            SCHEME_ERROR,
            UNAVAILABLE,
        };

        EStatus Status;

        explicit TEvGone(EStatus status);
        TString ToString() const override;
    };
};

IActor* CreateWorker(THolder<IActor>&& reader, THolder<IActor>&& writer);

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NReplication::NService::TEvWorker::TEvData::TRecord, o, x) {
    return x.Out(o);
}
