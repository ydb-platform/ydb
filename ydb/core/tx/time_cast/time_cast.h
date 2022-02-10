#pragma once

#include "defs.h"

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/tx_mediator_timecast.pb.h>

#include <util/stream/str.h>
#include <util/string/builder.h> 

namespace NKikimr {

class TMediatorTimecastEntry : public TThrRefBase {
    TAtomic Step;
public:
    TMediatorTimecastEntry()
        : Step(0)
    {}

    ui64 Get(ui64 tabletId) const;
    void Update(ui64 step, ui64 *exemption, ui64 exsz);
};

struct TEvMediatorTimecast {
    enum EEv {
        // local part
        EvRegisterTablet = EventSpaceBegin(TKikimrEvents::ES_TX_MEDIATORTIMECAST),
        EvUnregisterTablet,
        EvWaitPlanStep, 

        EvRegisterTabletResult = EvRegisterTablet + 1 * 512,
        EvNotifyPlanStep, 

        // mediator part
        EvWatch = EvRegisterTablet + 2 * 512,

        EvUpdate = EvRegisterTablet + 3 * 512,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_MEDIATORTIMECAST), "expected EvEnd < EventSpaceEnd()");

    struct TEvRegisterTablet : public TEventLocal<TEvRegisterTablet, EvRegisterTablet> {
        const ui64 TabletId;
        NKikimrSubDomains::TProcessingParams ProcessingParams;


        TEvRegisterTablet(ui64 tabletId, const NKikimrSubDomains::TProcessingParams &processing)
            : TabletId(tabletId)
            , ProcessingParams(processing)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvRegisterTablet";
            str << " TabletId# " << TabletId;
            if (ProcessingParams.HasVersion()) {
                str << " ProcessingParams { " <<  ProcessingParams.ShortDebugString() << " }";
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvRegisterTabletResult : public TEventLocal<TEvRegisterTabletResult, EvRegisterTabletResult> {
        const ui64 TabletId;
        const TIntrusivePtr<TMediatorTimecastEntry> Entry;

        TEvRegisterTabletResult(ui64 tabletId, TIntrusivePtr<TMediatorTimecastEntry> &entry)
            : TabletId(tabletId)
            , Entry(entry)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvRegisterTabletResult";
            str << " TabletId# " << TabletId;
            if (Entry) {
                str << " Entry# " << Entry->Get(TabletId);
            } else {
                str << " Entry# nullptr";
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvUnregisterTablet : public TEventLocal<TEvUnregisterTablet, EvUnregisterTablet> {
        const ui64 TabletId;

        TEvUnregisterTablet(ui64 tabletId)
            : TabletId(tabletId)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvUnregisterTablet";
            str << " TabletId# " << TabletId;
            str << "}";
            return str.Str();
        }
    };

    struct TEvWaitPlanStep : public TEventLocal<TEvWaitPlanStep, EvWaitPlanStep> { 
        const ui64 TabletId; 
        const ui64 PlanStep; 
 
        TEvWaitPlanStep(ui64 tabletId, ui64 planStep) 
            : TabletId(tabletId) 
            , PlanStep(planStep) 
        { } 
 
        TString ToString() const { 
            return TStringBuilder() 
                << "{TEvWaitPlanStep" 
                << " TabletId# " << TabletId 
                << " PlanStep# " << PlanStep 
                << "}"; 
        } 
    }; 
 
    struct TEvNotifyPlanStep : public TEventLocal<TEvNotifyPlanStep, EvNotifyPlanStep> { 
        const ui64 TabletId; 
        const ui64 PlanStep; 
 
        TEvNotifyPlanStep(ui64 tabletId, ui64 planStep) 
            : TabletId(tabletId) 
            , PlanStep(planStep) 
        { } 
 
        TString ToString() const { 
            return TStringBuilder() 
                << "{TEvNotifyPlanStep" 
                << " TabletId# " << TabletId 
                << " PlanStep# " << PlanStep 
                << "}"; 
        } 
    }; 
 
    struct TEvWatch : public TEventPB<TEvWatch, NKikimrTxMediatorTimecast::TEvWatch, EvWatch> {
        TEvWatch()
        {}

        TEvWatch(ui32 bucket)
        {
            Record.AddBucket(bucket);
        }

        TString ToString() const {
            TStringStream str;
            str << "{TEvWatch";
            for (size_t i = 0; i < Record.BucketSize(); ++i) {
                str << " Bucket# " << Record.GetBucket(i);
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvUpdate : public TEventPB<TEvUpdate, NKikimrTxMediatorTimecast::TEvUpdate, EvUpdate> {
        TString ToString() const {
            TStringStream str;
            str << "{TEvUpdate ";
            if (Record.HasMediator()) {
                str << " Mediator# " << Record.GetMediator();
            }
            if (Record.HasBucket()) {
                str << " Bucket# " << Record.GetBucket();
            }
            if (Record.HasTimeBarrier()) {
                str << " TimeBarrier# " << Record.GetTimeBarrier();
            }
            for (size_t i = 0; i < Record.ExemptionSize(); ++i) {
                str << " Exemption# " << Record.GetExemption(i);
            }
            str << "}";
            return str.Str();
        }
    };
};

IActor* CreateMediatorTimecastProxy();

inline TActorId MakeMediatorTimecastProxyID() {
    return TActorId(0, TStringBuf("txmdtimecast"));
}

}
