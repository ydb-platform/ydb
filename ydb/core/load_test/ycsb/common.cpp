#include "actors.h"
#include "common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

namespace NKikimr::NDataShardLoad {

namespace {

// TReadIteratorScan

class TReadIteratorScan : public TActorBootstrapped<TReadIteratorScan> {
    std::unique_ptr<TEvDataShard::TEvRead> Request;
    const NKikimrDataEvents::EDataFormat Format;
    const ui64 TabletId;
    const TActorId Parent;
    const TSubLoadId Id;
    const ui64 SampleKeyCount;

    TActorId Pipe;
    bool WasConnected = false;
    ui64 ReconnectLimit = 10;

    TInstant StartTs;
    size_t Oks = 0;

    TVector<TOwnedCellVec> SampledKeys;

public:
    TReadIteratorScan(TEvDataShard::TEvRead* request,
                      ui64 tablet,
                      const TActorId& parent,
                      const TSubLoadId& id,
                      ui64 sample)
        : Request(request)
        , Format(Request->Record.GetResultFormat())
        , TabletId(tablet)
        , Parent(parent)
        , Id(id)
        , SampleKeyCount(sample)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
            << " Bootstrap called, sample# " << SampleKeyCount);

        Become(&TReadIteratorScan::StateFunc);
        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
            << " Connect to# " << TabletId << " called");
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

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
            << " Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            return Connect(ctx);
        }

        StartTs = TInstant::Now();
        WasConnected = true;
        NTabletPipe::SendData(ctx, Pipe, Request.release());
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
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

        if (Format != NKikimrDataEvents::FORMAT_CELLVEC) {
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
                LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
                    << " finished in " << delta
                    << ", sampled# " << SampledKeys.size()
                    << ", iter finished# " << record.GetFinished()
                    << ", oks# " << Oks);

                ctx.Send(Parent, new TEvPrivate::TEvKeys(std::move(SampledKeys)));
                return Die(ctx);
            }

            return;
        } else if (record.GetFinished()) {
            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
                << " finished in " << delta
                << ", read# " << Oks);

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(0);
            auto& report = *response->Record.MutableReport();
            report.SetDurationMs(delta.MilliSeconds());
            report.SetOperationsOK(Oks);
            report.SetOperationsError(0);
            ctx.Send(Parent, response.release());

            return Die(ctx);
        }
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
            << ", stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(0, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        return Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorScan# " << Id
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

} // anonymous

TString GetKey(size_t n) {
    // user1000385178204227360
    return Sprintf("user%.19lu", n);
}

TVector<TCell> ToCells(const std::vector<TString> &keys) {
    TVector<TCell> cells;
    for (auto &key : keys) {
      cells.emplace_back(TCell(key.data(), key.size()));
    }
    return cells;
}

void AddRangeQuery(
    TEvDataShard::TEvRead &request,
    const std::vector<TString> &from,
    bool fromInclusive,
    const std::vector<TString> &to,
    bool toInclusive)
{
    auto fromCells = ToCells(from);
    auto toCells = ToCells(to);

    // convertion is ugly, but for tests is OK
    auto fromBuf = TSerializedCellVec::Serialize(fromCells);
    auto toBuf = TSerializedCellVec::Serialize(toCells);

    request.Ranges.emplace_back(std::move(fromBuf), std::move(toBuf), fromInclusive, toInclusive);
}

void AddKeyQuery(
    TEvDataShard::TEvRead &request,
    const TOwnedCellVec &key)
{
    request.Keys.emplace_back(key);
}

IActor *CreateReadIteratorScan(
    TEvDataShard::TEvRead* request,
    ui64 tablet,
    const TActorId& parent,
    const TSubLoadId& id,
    ui64 sample)
{
    return new TReadIteratorScan(request, tablet, parent, id, sample);
}

} // NKikimr::NDataShardLoad
