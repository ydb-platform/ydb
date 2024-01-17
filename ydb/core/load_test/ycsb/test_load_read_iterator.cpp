#include "actors.h"
#include "common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>
#include <util/system/hp_timer.h>

#include <google/protobuf/text_format.h>

#include <random>

// * Scheme is hardcoded and it is like default YCSB setup:
// 1 utf8 "key" column, 10 utf8 "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

namespace {

const ui64 RECONNECT_LIMIT = 10;

// TReadIteratorPoints

class TReadIteratorPoints : public TActorBootstrapped<TReadIteratorPoints> {
    const std::unique_ptr<const TEvDataShard::TEvRead> BaseRequest;
    const NKikimrDataEvents::EDataFormat Format;
    const ui64 TabletId;
    const TActorId Parent;
    const TSubLoadId Id;

    const TVector<TOwnedCellVec>& Points;
    const ui64 ReadCount = 0;
    const bool Infinite;

    ui64 PointsRead = 0;

    std::default_random_engine Rng;

    TActorId Pipe;
    bool WasConnected = false;
    ui64 ReconnectLimit = RECONNECT_LIMIT;

    TInstant StartTs; // actor started to send requests

    THPTimer RequestTimer;

    TVector<TDuration> RequestTimes;

public:
    TReadIteratorPoints(TEvDataShard::TEvRead* request,
                        ui64 tablet,
                        const TActorId& parent,
                        const TSubLoadId& id,
                        const TVector<TOwnedCellVec>& points,
                        ui64 readCount,
                        bool infinite)
        : BaseRequest(request)
        , Format(BaseRequest->Record.GetResultFormat())
        , TabletId(tablet)
        , Parent(parent)
        , Id(id)
        , Points(points)
        , ReadCount(readCount)
        , Infinite(infinite)
    {
        RequestTimes.reserve(Points.size());
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " Bootstrap called, will read keys# " << Points.size());

        Become(&TReadIteratorPoints::StateFunc);

        Rng.seed(SelfId().Hash());

        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        if (ReconnectLimit != RECONNECT_LIMIT) {
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
                << " will reconnect to tablet# " << TabletId
                << " retries left# " << (ReconnectLimit - 1));
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
                << " will connect to tablet# " << TabletId);
        }

        --ReconnectLimit;
        if (ReconnectLimit == 0) {
            TStringStream ss;
            ss << "Failed to set pipe to " << TabletId;
            return StopWithError(ctx, ss.Str());
        }
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            Pipe = {};
            return Connect(ctx);
        }

        StartTs = TInstant::Now();
        WasConnected = true;
        SendRead(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " Handle TEvClientDestroyed called");

        // sanity check
        if (!WasConnected) {
            return Connect(ctx);
        }

        return StopWithError(ctx, "broken pipe");
    }

    void Handle(TEvents::TEvUndelivered::TPtr, const TActorContext& ctx) {
        return StopWithError(ctx, "delivery failed");
    }

    void SendRead(const TActorContext &ctx) {
        auto index = Rng() % Points.size();

        const auto& currentKeyCells = Points[index];

        if (currentKeyCells.size() != 1) {
            TStringStream ss;
            ss << "Wrong keyNum: " << PointsRead << " with cells count: " << currentKeyCells.size();
            return StopWithError(ctx, ss.Str());
        }

        auto request = std::make_unique<TEvDataShard::TEvRead>();
        request->Record = BaseRequest->Record;
        AddKeyQuery(*request, currentKeyCells);

        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " sends request# " << PointsRead << ": " << request->ToString());

        RequestTimer.Reset();
        NTabletPipe::SendData(ctx, Pipe, request.release());

        ++PointsRead;
    }

    void Handle(const TEvDataShard::TEvReadResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " received from " << ev->Sender << ": " << msg->ToString());

        if (record.HasStatus() && record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TStringStream ss;
            ss << "Failed to read from ds# " << TabletId << ", code# " << record.GetStatus().GetCode();
            if (record.GetStatus().IssuesSize()) {
                for (const auto& issue: record.GetStatus().GetIssues()) {
                    ss << ", issue: " << issue;
                }
            }

            return StopWithError(ctx, ss.Str());
        }

        if (Format != NKikimrDataEvents::FORMAT_CELLVEC) {
            return StopWithError(ctx, "Unsupported format");
        }

        if (msg->GetRowsCount() != 1) {
            TStringStream ss;
            ss << "Wrong reply with data, rows: " << msg->GetRowsCount();

            return StopWithError(ctx, ss.Str());
        }

        RequestTimes.push_back(TDuration::Seconds(RequestTimer.Passed()));

        if (Infinite && PointsRead >= ReadCount) {
            PointsRead = 0;
        }

        if (PointsRead < ReadCount) {
            SendRead(ctx);
            return;
        }

        // finish
        ctx.Send(Parent, new TEvPrivate::TEvPointTimes(std::move(RequestTimes)));
        Die(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << ", stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(0, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        return Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << Id
            << " tablet recieved PoisonPill, going to die");

        // TODO: cancel iterator
        return Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(TEvents::TEvUndelivered, Handle)
        HFunc(TEvTabletPipe::TEvClientConnected, Handle)
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
        HFunc(TEvDataShard::TEvReadResult, Handle)
    )
};

// TReadIteratorLoadScenario

enum class EState {
    DescribePath,
    FullScan,
    FullScanGetKeys,
    ReadHeadPoints,
};

class TReadIteratorLoadScenario : public TActorBootstrapped<TReadIteratorLoadScenario> {
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart Config;
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard Target;
    const TActorId Parent;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const TSubLoadId Id;

    // used to measure full run of this actor
    TInstant StartTs;

    TString ConfingString;

    ui64 TabletId = 0;
    ui64 TableId = 0;
    ui64 OwnerId = 0;

    TVector<ui32> KeyColumnIds;
    TVector<ui32> AllColumnIds;

    TVector<TActorId> StartedActors;
    ui64 LastReadId = 0;

    TVector<TOwnedCellVec> Keys;

    size_t Oks = 0;
    size_t Errors = 0;
    TVector<NKikimrDataShardLoad::TLoadReport> Results;

    // accumulates results from read actors: between different inflights/chunks must be reset
    NHdr::THistogram HeadReadsHist;
    TInstant StartTsSubTest;

    EState State = EState::DescribePath;
    ui64 Inflight = 0;

    ui64 LastSubTag = 0;

    // setup for fullscan
    TVector<ui64> ChunkSizes = {0, 0, 1, 1, 10, 10, 100, 100, 1000, 1000}; // each twice intentionally
    size_t ChunkIndex = 0;

    // note that might be overwritten by test incoming test config
    TVector<ui64> Inflights = {1, 2, 10, 50, 100, 200, 400, 1000, 2000, 5000};
    size_t InflightIndex = 0;

    ui64 ReadCount = 0;

public:
    TReadIteratorLoadScenario(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
                              const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
                              const TActorId& parent,
                              TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                              const TSubLoadId& id)
        : Config(cmd)
        , Target(target)
        , Parent(parent)
        , Counters(std::move(counters))
        , Id(id)
        , HeadReadsHist(1000, 4)
    {
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);

        if (Config.InflightsSize()) {
            Inflights.clear();
            for (auto inflight: Config.GetInflights()) {
                Inflights.push_back(inflight);
            }
        }

        if (Config.ChunksSize()) {
            ChunkSizes.clear();
            for (auto chunk: Config.GetChunks()) {
                ChunkSizes.push_back(chunk);
            }
        }

        if (Config.GetNoFullScan() || Config.GetInfinite()) {
            ChunkSizes.clear();
        }

        if (Config.HasReadCount()) {
            ReadCount = Config.GetReadCount();
        } else {
            ReadCount = Config.GetRowCount();
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << SelfId()
            << " with id# " << Id << " Bootstrap called: " << ConfingString);

        Become(&TReadIteratorLoadScenario::StateFunc);
        StartTs = TInstant::Now();
        Run(ctx);
    }

private:
    void Run(const TActorContext& ctx) {
        switch (State) {
        case EState::DescribePath:
            DescribePath(ctx);
            return;
        case EState::FullScan:
            RunFullScan(ctx, 0);
            break;
        case EState::FullScanGetKeys:
            RunFullScan(ctx, Config.GetRowCount());
            break;
        case EState::ReadHeadPoints:
            RunHeadReads(ctx);
            break;
        }
    }

    void DescribePath(const TActorContext& ctx) {
        TString path = Target.GetWorkingDir() + "/" + Target.GetTableName();
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(path);
        ctx.Send(MakeTxProxyID(), request.release());
    }

    void Handle(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        OwnerId = record.GetPathOwnerId();
        TableId = record.GetPathId();

        const auto& description = record.GetPathDescription();
        if (description.TablePartitionsSize() != 1) {
            return StopWithError(
                ctx,
                TStringBuilder() << "Path must have exactly 1 part, has: " << description.TablePartitionsSize());
        }
        const auto& partition = description.GetTablePartitions(0);
        TabletId = partition.GetDatashardId();

        const auto& table = description.GetTable();

        KeyColumnIds.reserve(table.KeyColumnIdsSize());
        for (const auto& id: table.GetKeyColumnIds()) {
            KeyColumnIds.push_back(id);
        }

        AllColumnIds.reserve(table.ColumnsSize());
        for (const auto& column: table.GetColumns()) {
            AllColumnIds.push_back(column.GetId());
        }

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " will work with tablet# " << TabletId << " with ownerId# " << OwnerId
            << " with tableId# " << TableId << " resolved for path# "
            << Target.GetWorkingDir() << "/" << Target.GetTableName()
            << " with columnsCount# " << AllColumnIds.size() << ", keyColumnCount# " << KeyColumnIds.size());

        if (!ChunkSizes.empty()) {
            State = EState::FullScan;
        } else {
            State = EState::FullScanGetKeys;
        }
        Run(ctx);
    }

    void RunFullScan(const TActorContext& ctx, ui64 sampleKeys) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(++LastReadId);
        record.MutableTableId()->SetOwnerId(OwnerId);
        record.MutableTableId()->SetTableId(TableId);

        if (sampleKeys) {
            for (const auto& id: KeyColumnIds) {
                record.AddColumns(id);
            }
        } else {
            for (const auto& id: AllColumnIds) {
                record.AddColumns(id);
            }
        }

        if (!sampleKeys && ChunkSizes[ChunkIndex])
            record.SetMaxRowsInResult(ChunkSizes[ChunkIndex]);

        TVector<TString> from = {TString("user")};
        TVector<TString> to = {TString("zzz")};
        AddRangeQuery(*request, from, true, to, true);

        record.SetResultFormat(::NKikimrDataEvents::FORMAT_CELLVEC);

        TSubLoadId subId(Id.Tag, SelfId(), ++LastSubTag);
        auto* actor = CreateReadIteratorScan(request.release(), TabletId, SelfId(), subId, sampleKeys);
        StartedActors.emplace_back(ctx.Register(actor));

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "started fullscan actor# " << StartedActors.back());
    }

    void Handle(const TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.HasErrorReason() || !record.HasReport()) {
            TStringStream ss;
            ss << "read iterator actor# " << record.GetTag() << " finished with error: " << record.GetErrorReason()
               << " in State# " << (int)State;
            if (record.HasReport())
                ss << ", report: " << ev->Get()->ToString();

            return StopWithError(ctx, ss.Str());
        }

        switch (State) {
        case EState::FullScan: {
            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "fullscan actor# " << ev->Sender
                << " with chunkSize# " << ChunkSizes[ChunkIndex]
                << " finished: " << ev->Get()->ToString());
            Errors += record.GetReport().GetOperationsError();
            Oks += record.GetReport().GetOperationsOK();
            Results.emplace_back(record.GetReport());

            auto& lastResult = Results.back();
            TStringStream ss;
            ss << "Test run# " << Results.size() << ", type# FullScan with chunk# ";
            if (ChunkSizes[ChunkIndex]) {
                ss << ChunkSizes[ChunkIndex];
            } else {
                ss << "inf";
            }
            lastResult.SetPrefixInfo(ss.Str());

            ++ChunkIndex;
            if (ChunkIndex == ChunkSizes.size())
                State = EState::FullScanGetKeys;
            return Run(ctx);
        }
        case EState::DescribePath:
        case EState::FullScanGetKeys:
            return StopWithError(ctx, TStringBuilder() << "TEvTestLoadFinished while in " << State);
        case EState::ReadHeadPoints: {
            Y_ABORT_UNLESS(Inflight == 0);
            LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "headread with inflight# " << Inflights[InflightIndex]
                << " finished: " << ev->Get()->ToString());
            Errors += record.GetReport().GetOperationsError();
            Oks += record.GetReport().GetOperationsOK();
            Results.emplace_back(record.GetReport());

            auto& lastResult = Results.back();
            TStringStream ss;
            ss << "Test run# " << Results.size() << ", type# ReadHeadPoints with inflight# "
               << Inflights[InflightIndex];
            lastResult.SetPrefixInfo(ss.Str());

            ++InflightIndex;
            if (InflightIndex == Inflights.size())
                return Finish(ctx);

            return Run(ctx);
        }
        }
    }

    void Handle(TEvPrivate::TEvKeys::TPtr& ev, const TActorContext& ctx) {
        Keys = std::move(ev->Get()->Keys);

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " received keyCount# " << Keys.size());

        State = EState::ReadHeadPoints;
        Run(ctx);
    }

    void RunHeadReads(const TActorContext& ctx) {
        Y_ABORT_UNLESS(Inflight == 0);
        Y_ABORT_UNLESS(InflightIndex < Inflights.size());

        HeadReadsHist.Reset();
        StartTsSubTest = TInstant::Now();

        Inflight = Inflights[InflightIndex];
        for (size_t i = 0; i < Inflight; ++i)
            RunSingleHeadRead(ctx);
    }

    void RunSingleHeadRead(const TActorContext& ctx) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(++LastReadId);
        record.MutableTableId()->SetOwnerId(OwnerId);
        record.MutableTableId()->SetTableId(TableId);

        for (const auto& id: AllColumnIds) {
            record.AddColumns(id);
        }

        record.SetResultFormat(::NKikimrDataEvents::FORMAT_CELLVEC);

        TSubLoadId subId(Id.Tag, SelfId(), ++LastSubTag);
        auto* readActor = new TReadIteratorPoints(
            request.release(),
            TabletId,
            SelfId(),
            subId,
            Keys,
            ReadCount,
            Config.GetInfinite());

        StartedActors.emplace_back(ctx.Register(readActor));

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " started read actor with id# " << StartedActors.back());
    }

    void Handle(TEvPrivate::TEvPointTimes::TPtr& ev, const TActorContext& ctx) {
        --Inflight;

        const auto& requestTimes = ev->Get()->RequestTimes;
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " received point times# " << requestTimes.size() << ", Inflight left# " << Inflight);

        for (auto t: requestTimes) {
            auto ms = t.MilliSeconds();
            if (ms == 0)
                ms = 1; // round up
            HeadReadsHist.RecordValue(ms);
        }

        if (Inflight == 0) {
            auto ts = TInstant::Now();
            auto delta = ts - StartTsSubTest;

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(0);
            auto& report = *response->Record.MutableReport();
            report.SetDurationMs(delta.MilliSeconds());
            report.SetOperationsOK(Inflights[InflightIndex] * ReadCount);
            report.SetOperationsError(0);

            TStringStream ss;
            i64 v50 = HeadReadsHist.GetValueAtPercentile(50.0);
            i64 v95 = HeadReadsHist.GetValueAtPercentile(95.00);
            i64 v99 = HeadReadsHist.GetValueAtPercentile(99.00);
            i64 v999 = HeadReadsHist.GetValueAtPercentile(99.9);

            ss << "single row head read hist (ms):"
               << "\n50%: " << v50
               << "\n95%: " << v95
               << "\n99%: " << v99
               << "\n99.9%: " << v999
               << Endl;

            report.SetInfo(ss.Str());
            ctx.Send(SelfId(), response.release());
        }
    }

    void Finish(const TActorContext& ctx) {
        auto ts = TInstant::Now();
        auto delta = ts - StartTs;

        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Id.SubTag);
        auto& report = *response->Record.MutableReport();
        report.SetTag(Id.SubTag);
        report.SetDurationMs(delta.MilliSeconds());
        report.SetOperationsOK(Oks);
        report.SetOperationsError(0);

        TStringStream ss;
        for (const auto& report: Results) {
            ss << report << Endl;
        }

        report.SetInfo(ss.Str());
        report.SetSubtestCount(Results.size());

        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " finished in " << delta << " with report:\n" << report.GetInfo());

        ctx.Send(Parent, response.release());

        return Die(ctx);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoRequest::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "ReadIteratorLoadScenario# " << Id << " started on " << StartTs;
        }
        ctx.Send(ev->Sender, new TEvDataShardLoad::TEvTestLoadInfoResponse(Id.SubTag, str.Str()));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " tablet recieved PoisonPill, going to die");
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Id
            << " stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Id.SubTag, reason));
        Stop(ctx);
    }

    void Stop(const TActorContext& ctx) {
        for (const auto& actorId: StartedActors) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
        return Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoRequest, Handle)
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvPrivate::TEvKeys, Handle)
        HFunc(TEvPrivate::TEvPointTimes, Handle)
    )
};

} // anonymous

IActor *CreateReadIteratorActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id)
{
    return new TReadIteratorLoadScenario(cmd, target, parent, std::move(counters), id);
}

} // NKikimr::NDataShardLoad

template <>
inline void Out<NKikimr::NDataShardLoad::EState>(IOutputStream& o, NKikimr::NDataShardLoad::EState state) {
    switch (state) {
    case NKikimr::NDataShardLoad::EState::DescribePath:
        o << "describepath";
        break;
    case NKikimr::NDataShardLoad::EState::FullScan:
        o << "fullscan";
        break;
    case NKikimr::NDataShardLoad::EState::FullScanGetKeys:
        o << "fullscangetkeys";
        break;
    case NKikimr::NDataShardLoad::EState::ReadHeadPoints:
        o << "readheadpoints";
        break;
    default:
        o << (int)state;
        break;
    }
}

#ifndef _win_
template <>
inline void Out<NKikimrDataShardLoad::TLoadReport>(IOutputStream& o, const NKikimrDataShardLoad::TLoadReport& report) {
    if (report.HasPrefixInfo())
        o << report.GetPrefixInfo() << ". ";

    auto duration = TDuration::MilliSeconds(report.GetDurationMs());
    o << "Load duration: " << duration
      << ", OK=" << report.GetOperationsOK()
      << ", Error=" << report.GetOperationsError();

    // note that we check Seconds() instead of Milliseconds() to ensure
    // that there was enough load to make calculations
    if (report.GetOperationsOK() && duration.Seconds()) {
        ui64 throughput = report.GetOperationsOK() * 1000 / duration.MilliSeconds();
        o << ", throughput=" << throughput << " OK_ops/s";
    }
    if (report.HasSubtestCount()) {
        o << ", subtests: " << report.GetSubtestCount();
    }
    if (report.HasInfo()) {
        o << ", Info: " << report.GetInfo();
    }
}
#endif
