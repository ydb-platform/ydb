#include "actors.h"

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

#include <algorithm>
#include <random>

// * Scheme is hardcoded and it is like default YCSB setup:
// table name is "usertable", 1 utf8 "key" column, 10 utf8 "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

namespace {

struct TEvPrivate {
    enum EEv {
        EvKeys = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvPointTimes,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE));

    struct TEvKeys : public TEventLocal<TEvKeys, EvKeys> {
        TVector<TOwnedCellVec> Keys;

        TEvKeys(TVector<TOwnedCellVec>&& keys)
            : Keys(std::move(keys))
        {
        }
    };

    struct TEvPointTimes : public TEventLocal<TEvPointTimes, EvPointTimes> {
        TVector<TDuration> RequestTimes;

        TEvPointTimes(TVector<TDuration>&& requestTime)
            : RequestTimes(std::move(requestTime))
        {
        }
    };
};

TVector<TCell> ToCells(const std::vector<TString>& keys) {
    TVector<TCell> cells;
    for (auto& key: keys) {
        cells.emplace_back(TCell(key.data(), key.size()));
    }
    return cells;
}

void AddRangeQuery(
    TEvDataShard::TEvRead& request,
    const std::vector<TString>& from,
    bool fromInclusive,
    const std::vector<TString>& to,
    bool toInclusive)
{
    auto fromCells = ToCells(from);
    auto toCells = ToCells(to);

    // convertion is ugly, but for tests is OK
    auto fromBuf = TSerializedCellVec::Serialize(fromCells);
    auto toBuf = TSerializedCellVec::Serialize(toCells);

    request.Ranges.emplace_back(fromBuf, toBuf, fromInclusive, toInclusive);
}

void AddKeyQuery(
    TEvDataShard::TEvRead& request,
    const TOwnedCellVec& key)
{
    auto buf = TSerializedCellVec::Serialize(key);
    request.Keys.emplace_back(buf);
}

// TReadIteratorPoints

class TReadIteratorPoints : public TActorBootstrapped<TReadIteratorPoints> {
    const std::unique_ptr<const TEvDataShard::TEvRead> BaseRequest;
    const NKikimrTxDataShard::EScanDataFormat Format;
    const ui64 TabletId;
    const TActorId Parent;

    TActorId Pipe;

    TInstant StartTs; // actor started to send requests
    size_t Oks = 0;

    TVector<TOwnedCellVec> Points;
    size_t CurrentPoint = 0;
    THPTimer RequestTimer;

    TVector<TDuration> RequestTimes;

public:
    TReadIteratorPoints(TEvDataShard::TEvRead* request,
                        ui64 tablet,
                        const TActorId& parent,
                        const TVector<TOwnedCellVec>& points)
        : BaseRequest(request)
        , Format(BaseRequest->Record.GetResultFormat())
        , TabletId(tablet)
        , Parent(parent)
        , Points(points)
    {
        RequestTimes.reserve(Points.size());
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << " Bootstrap called, will read keys# " << Points.size());

        Become(&TReadIteratorPoints::StateFunc);

        auto rng = std::default_random_engine {};
        std::shuffle(Points.begin(), Points.end(), rng);

        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << " Connect to# " << TabletId << " called");
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << " Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            TStringStream ss;
            ss << "Failed to connect to " << TabletId << ", status: " << msg->Status;
            return StopWithError(ctx, ss.Str());
        }

        StartTs = TInstant::Now();
        SendRead(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << " Handle TEvClientDestroyed called");
        return StopWithError(ctx, "broken pipe");
    }

    void Handle(TEvents::TEvUndelivered::TPtr, const TActorContext& ctx) {
        return StopWithError(ctx, "delivery failed");
    }

    void SendRead(const TActorContext &ctx) {
        Y_VERIFY(CurrentPoint < Points.size());

        auto request = std::make_unique<TEvDataShard::TEvRead>();
        request->Record = BaseRequest->Record;
        AddKeyQuery(*request, Points[CurrentPoint++]);

        RequestTimer.Reset();
        NTabletPipe::SendData(ctx, Pipe, request.release());
    }

    void Handle(const TEvDataShard::TEvReadResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        if (record.HasStatus() && record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TStringStream ss;
            ss << "Failed to read from ds# " << TabletId << ", code# " << record.GetStatus().GetCode();
            if (record.GetStatus().IssuesSize()) {
                for (const auto& issue: record.GetStatus().GetIssues()) {
                    ss << ", issue: " << issue;
                }
            }

            return StopWithError(ctx, ss.Str());
            return;
        }

        if (Format != NKikimrTxDataShard::CELLVEC) {
            return StopWithError(ctx, "Unsupported format");
        }

        Oks += msg->GetRowsCount();
        RequestTimes.push_back(TDuration::Seconds(RequestTimer.Passed()));

        if (CurrentPoint < Points.size()) {
            SendRead(ctx);
            return;
        }

        // finish
        ctx.Send(Parent, new TEvPrivate::TEvPointTimes(std::move(RequestTimes)));
        Die(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << ", stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(0, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        return Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TReadIteratorPoints# " << SelfId()
            << " with parent# " << Parent << " tablet recieved PoisonPill, going to die");

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

// TReadIteratorScan

class TReadIteratorScan : public TActorBootstrapped<TReadIteratorScan> {
    std::unique_ptr<TEvDataShard::TEvRead> Request;
    const NKikimrTxDataShard::EScanDataFormat Format;
    const ui64 TabletId;
    const TActorId Parent;
    const ui64 SampleKeyCount;

    TActorId Pipe;

    TInstant StartTs;
    size_t Oks = 0;

    TVector<TOwnedCellVec> SampledKeys;

public:
    TReadIteratorScan(TEvDataShard::TEvRead* request, ui64 tablet, const TActorId& parent, ui64 sample)
        : Request(request)
        , Format(Request->Record.GetResultFormat())
        , TabletId(tablet)
        , Parent(parent)
        , SampleKeyCount(sample)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << " Bootstrap called, sample# " << SampleKeyCount);

        Become(&TReadIteratorScan::StateFunc);
        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << " Connect to# " << TabletId << " called");
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << " Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            TStringStream ss;
            ss << "Failed to connect to " << TabletId << ", status: " << msg->Status;
            return StopWithError(ctx, ss.Str());
        }

        StartTs = TInstant::Now();
        NTabletPipe::SendData(ctx, Pipe, Request.release());
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << " Handle TEvClientDestroyed called");
        return StopWithError(ctx, "broken pipe");
    }

    void Handle(TEvents::TEvUndelivered::TPtr, const TActorContext& ctx) {
        return StopWithError(ctx, "delivery failed");
    }

    void Handle(const TEvDataShard::TEvReadResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

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

        if (Format != NKikimrTxDataShard::CELLVEC) {
            return StopWithError(ctx, "Unsupported format");
        }

        Oks += msg->GetRowsCount();

        auto ts = TInstant::Now();
        auto delta = ts - StartTs;

        if (SampleKeyCount) {
            for (size_t i = 0; i < msg->GetRowsCount() && SampledKeys.size() < SampleKeyCount; ++i) {
                SampledKeys.emplace_back(msg->GetCells(i));
            }

            if (record.GetFinished() || SampledKeys.size() >= SampleKeyCount) {
                LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
                    << " with parent# " << Parent << " finished in " << delta
                    << ", sampled# " << SampledKeys.size()
                    << ", iter finished# " << record.GetFinished()
                    << ", oks# " << Oks);

                ctx.Send(Parent, new TEvPrivate::TEvKeys(std::move(SampledKeys)));
                return Die(ctx);
            }

            return;
        } else if (record.GetFinished()) {
            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
                << " with parent# " << Parent << " finished in " << delta
                << ", read# " << Oks);

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(0);
            response->Report = TEvDataShardLoad::TLoadReport();
            response->Report->Duration = delta;
            response->Report->OperationsOK = Oks;
            response->Report->OperationsError = 0;
            ctx.Send(Parent, response.release());

            return Die(ctx);
        }
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << ", stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(0, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        return Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << SelfId()
            << " with parent# " << Parent << " tablet recieved PoisonPill, going to die");

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
    Upsert,
    FullScan,
    FullScanGetKeys,
    ReadHeadPoints,
};

class TReadIteratorLoadScenario : public TActorBootstrapped<TReadIteratorLoadScenario> {
    const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart Config;
    const TActorId Parent;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const ui64 Tag;

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
    TVector<TEvDataShardLoad::TLoadReport> Results;

    // accumulates results from read actors: between different inflights/chunks must be reset
    NHdr::THistogram HeadReadsHist;
    TInstant StartTsSubTest;

    EState State = EState::DescribePath;
    ui64 Inflight = 0;

    // setup for fullscan
    TVector<ui64> ChunkSizes = {0, 0, 1, 1, 10, 10, 100, 100, 1000, 1000}; // each twice intentionally
    size_t ChunkIndex = 0;

    // note that might be overwritten by test incoming test config
    TVector<ui64> Inflights = {1, 2, 10, 50, 100, 200, 400};
    size_t InflightIndex = 0;

public:
    TReadIteratorLoadScenario(const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
        : Config(cmd)
        , Parent(parent)
        , Counters(std::move(counters))
        , Tag(tag)
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
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << SelfId()
            << " with tag# " << Tag << " Bootstrap called: " << ConfingString);

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
        case EState::Upsert:
            UpsertData(ctx);
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
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(Config.GetPath());
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

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " will work with tablet# " << TabletId << " with ownerId# " << OwnerId
            << " with tableId# " << TableId << " resolved for path# " << Config.GetPath()
            << " with columnsCount# " << AllColumnIds.size() << ", keyColumnCount# " << KeyColumnIds.size());

        State = EState::Upsert;
        Run(ctx);
    }

    void UpsertData(const TActorContext& ctx) {
        NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart upsertConfig;
        upsertConfig.SetRowCount(Config.GetRowCount());
        upsertConfig.SetTabletId(TabletId);
        upsertConfig.SetTableId(TableId);
        upsertConfig.SetInflight(100); // some good value to upsert fast

        auto* upsertActor = CreateUpsertBulkActor(upsertConfig, SelfId(), Counters, /* meaningless tag */ 1000000);
        StartedActors.emplace_back(ctx.Register(upsertActor));

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " started upsert actor with id# " << StartedActors.back());
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

        record.SetResultFormat(::NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        auto* actor = new TReadIteratorScan(request.release(), TabletId, SelfId(), sampleKeys);
        StartedActors.emplace_back(ctx.Register(actor));

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "started fullscan actor# " << StartedActors.back());
    }

    void Handle(const TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        if (msg->ErrorReason || !msg->Report) {
            TStringStream ss;
            ss << "read iterator actor# " << msg->Tag << " finished with error: " << msg->ErrorReason
               << " in State# " << (int)State;
            if (msg->Report)
                ss << ", report: " << msg->Report->ToString();

            return StopWithError(ctx, ss.Str());
        }

        switch (State) {
        case EState::Upsert: {
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "upsert actor# " << ev->Sender
                << " finished: " << msg->Report->ToString());
            State = EState::FullScan;
            return Run(ctx);
        }
        case EState::FullScan: {
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "fullscan actor# " << ev->Sender
                << " with chunkSize# " << ChunkSizes[ChunkIndex]
                << " finished: " << msg->Report->ToString());
            Errors += msg->Report->OperationsError;
            Oks += msg->Report->OperationsOK;
            Results.emplace_back(*msg->Report);

            auto& lastResult = Results.back();
            TStringStream ss;
            ss << "Test run# " << Results.size() << ", type# FullScan with chunk# ";
            if (ChunkSizes[ChunkIndex]) {
                ss << ChunkSizes[ChunkIndex];
            } else {
                ss << "inf";
            }
            lastResult.PrefixInfo = ss.Str();

            ++ChunkIndex;
            if (ChunkIndex == ChunkSizes.size())
                State = EState::FullScanGetKeys;
            return Run(ctx);
        }
        case EState::DescribePath:
        case EState::FullScanGetKeys:
            return StopWithError(ctx, TStringBuilder() << "TEvTestLoadFinished while in " << State);
        case EState::ReadHeadPoints: {
            Y_VERIFY(Inflight == 0);
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "headread with inflight# " << Inflights[InflightIndex]
                << " finished: " << msg->Report->ToString());
            Errors += msg->Report->OperationsError;
            Oks += msg->Report->OperationsOK;
            Results.emplace_back(*msg->Report);

            auto& lastResult = Results.back();
            TStringStream ss;
            ss << "Test run# " << Results.size() << ", type# ReadHeadPoints with inflight# "
               << Inflights[InflightIndex];
            lastResult.PrefixInfo = ss.Str();

            ++InflightIndex;
            if (InflightIndex == Inflights.size())
                return Finish(ctx);

            return Run(ctx);
        }
        }
    }

    void Handle(TEvPrivate::TEvKeys::TPtr& ev, const TActorContext& ctx) {
        Keys = std::move(ev->Get()->Keys);

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " received keyCount# " << Keys.size());

        State = EState::ReadHeadPoints;
        Run(ctx);
    }

    void RunHeadReads(const TActorContext& ctx) {
        Y_VERIFY(Inflight == 0);
        Y_VERIFY(InflightIndex < Inflights.size());

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

        record.SetResultFormat(::NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        auto* readActor = new TReadIteratorPoints(request.release(), TabletId, SelfId(), Keys);
        StartedActors.emplace_back(ctx.Register(readActor));

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " started read actor with id# " << StartedActors.back());
    }

    void Handle(TEvPrivate::TEvPointTimes::TPtr& ev, const TActorContext& ctx) {
        --Inflight;

        const auto& requestTimes = ev->Get()->RequestTimes;
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
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
            response->Report = TEvDataShardLoad::TLoadReport();
            response->Report->Duration = delta;
            response->Report->OperationsOK = Inflights[InflightIndex] * Config.GetRowCount();
            response->Report->OperationsError = 0;

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

            response->Report->Info = ss.Str();
            ctx.Send(SelfId(), response.release());
        }
    }

    void Finish(const TActorContext& ctx) {
        auto ts = TInstant::Now();
        auto delta = ts - StartTs;

        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Tag);
        response->Report = TEvDataShardLoad::TLoadReport();
        response->Report->Duration = delta;
        response->Report->OperationsOK = Oks;
        response->Report->OperationsError = 0;

        TStringStream ss;
        for (const auto& report: Results) {
            ss << report.ToString() << Endl;
        }

        response->Report->Info = ss.Str();
        response->Report->SubtestCount = Results.size();

        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " finished in " << delta << " with report:\n" << response->Report->Info);

        ctx.Send(Parent, response.release());

        return Die(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "ReadIteratorLoadScenario# " << Tag << " started on " << StartTs;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " tablet recieved PoisonPill, going to die");
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
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
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvPrivate::TEvKeys, Handle)
        HFunc(TEvPrivate::TEvPointTimes, Handle)
    )
};

} // anonymous

NActors::IActor *CreateReadIteratorActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TReadIteratorLoadScenario(cmd, parent, std::move(counters), tag);
}

} // NKikimr::NDataShardLoad

template <>
inline void Out<NKikimr::NDataShardLoad::EState>(IOutputStream& o, NKikimr::NDataShardLoad::EState state) {
    switch (state) {
    case NKikimr::NDataShardLoad::EState::DescribePath:
        o << "describepath";
        break;
    case NKikimr::NDataShardLoad::EState::Upsert:
        o << "upsert";
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
