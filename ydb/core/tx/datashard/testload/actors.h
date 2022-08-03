#pragma once

#include "defs.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NDataShard {

NActors::IActor *CreateBulkUpsertActor(const NKikimrTxDataShard::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

NActors::IActor *CreateUpsertActor(const NKikimrTxDataShard::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

class TLoadActorException : public yexception {
};

struct TLoadReport {
    TDuration Duration;
    ui64 OperationsOK = 0;
    ui64 OperationsError = 0;

    TString ToString() const {
        TStringStream ss;
        ss << "Load duration: " << Duration << ", OK=" << OperationsOK << ", Error=" << OperationsError;
        if (OperationsOK && Duration.Seconds()) {
            ui64 throughput = OperationsOK / Duration.Seconds();
            ss << ", throughput=" << throughput << " OK_ops/s";
        }
        return ss.Str();
    }
};

struct TEvTestLoadFinished : public TEventLocal<TEvTestLoadFinished, TEvDataShard::EvTestLoadFinished> {
    ui64 Tag;
    std::optional<TLoadReport> Report;
    TString ErrorReason;

    TEvTestLoadFinished(ui64 tag, const TString& error = {})
        : Tag(tag)
        , ErrorReason(error)
    {}
};

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadActorException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

#define VERIFY_PARAM(NAME) VERIFY_PARAM2(cmd, NAME)

} // NKikimr::NDataShard
