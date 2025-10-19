#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <deque>

namespace NKikimr::NColumnShard::NDiagnostics  {

class TScanDiagnosticsActor: public NActors::TActor<TScanDiagnosticsActor> {
    struct TScanDiagnosticsInfo {
        TString RequestMessage;
        TString DotGraph;
        TString SSAProgram;
        TString PKRangesFilter;
    };
    std::deque<TScanDiagnosticsInfo> LastPublicScans;
    std::deque<TScanDiagnosticsInfo> LastInternalScans;
    static constexpr uint64_t MaxScans = 10;

    STRICT_STFUNC(
        StateMain,
        hFunc(NMon::TEvRemoteHttpInfo, Handle)
        hFunc(NColumnShard::TEvPrivate::TEvReportScanDiagnostics, Handle)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
    )
    
    void Handle(const NMon::TEvRemoteHttpInfo::TPtr& ev);
    void Handle(const NColumnShard::TEvPrivate::TEvReportScanDiagnostics::TPtr& ev);
    
    TString RenderScanDiagnosticsInfo(const TScanDiagnosticsInfo& info, int id, const TString& tag);
    TString RenderScanDiagnostics(const std::deque<TScanDiagnosticsInfo>& lastScans, const TString& tag);
    void AddScanDiagnostics(TScanDiagnosticsInfo&& info, std::deque<TScanDiagnosticsInfo>& lastScans);

public:
    TScanDiagnosticsActor();
};

}