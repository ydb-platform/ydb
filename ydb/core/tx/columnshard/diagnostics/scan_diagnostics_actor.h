#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <deque>

namespace NKikimr::NColumnShard::NDiagnostics  {

class TScanDiagnosticsActor: public NActors::TActorBootstrapped<TScanDiagnosticsActor> {
    struct TScanDiagnosticsInfo {
        ui64 RequestId;
        TString RequestMessage;
        TString DotGraph;
        TString SSAProgram;
        TString PKRangesFilter;
        TString ScanIterator;
    };
    std::deque<std::shared_ptr<TScanDiagnosticsInfo>> LastPublicScans;
    std::deque<std::shared_ptr<TScanDiagnosticsInfo>> LastInternalScans;
    std::unordered_map<ui64, std::shared_ptr<TScanDiagnosticsInfo>> RequestToInfo;
    static constexpr uint64_t MaxScans = 10;

    STRICT_STFUNC(
        StateMain,
        hFunc(NMon::TEvRemoteHttpInfo, Handle)
        hFunc(NColumnShard::TEvPrivate::TEvReportScanDiagnostics, Handle)
        hFunc(NColumnShard::TEvPrivate::TEvReportScanIteratorDiagnostics, Handle)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
    )
    
    void Handle(const NMon::TEvRemoteHttpInfo::TPtr& ev);
    void Handle(const NColumnShard::TEvPrivate::TEvReportScanDiagnostics::TPtr& ev);
    void Handle(const NColumnShard::TEvPrivate::TEvReportScanIteratorDiagnostics::TPtr& ev);
    
    TString RenderScanDiagnosticsInfo(const TScanDiagnosticsInfo& info, int id, const TString& tag);
    TString RenderScanDiagnostics(const std::deque<std::shared_ptr<TScanDiagnosticsInfo>>& lastScans, const TString& tag);
    void AddScanDiagnostics(const std::shared_ptr<TScanDiagnosticsInfo>& info, std::deque<std::shared_ptr<TScanDiagnosticsInfo>>& lastScans);

public:
    void Bootstrap();
};

}