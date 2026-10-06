#pragma once
#include <ydb/library/yql/providers/common/message_stream/async_io/read_actor.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <util/string/cast.h>

namespace NYql::NDq::NMessageStreamTest {
using namespace NFq;

class TControl final : public IMessageStreamPartitionControl {
public:
    TMessageStreamPartitionId GetPartitionId() const override { return {0}; }
    void ConfirmStart(std::optional<ui64> offset, std::optional<ui64> bound) override { Start = offset; Bound = bound; }
    void ConfirmStop() override { ++Stopped; }
    void ConfirmExhausted() override { ++Exhausted; }
    void RequestStatus() override { ++StatusRequests; }
    bool AcknowledgeRange(ui64 begin, ui64 end) override { Ranges.emplace_back(begin,end); return AcceptCommits; }
    std::optional<ui64> Start, Bound;
    ui32 Stopped = 0, Exhausted = 0, StatusRequests = 0;
    bool AcceptCommits = true;
    TVector<std::pair<ui64,ui64>> Ranges;
};
class TSession final : public IMessageStreamReadSession {
public:
    NThreading::TFuture<void> WaitEvent() override { return Ready.GetFuture(); }
    std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamGetEventsSettings&) override {
        ++Polls;
        return std::exchange(Events, {});
    }
    NThreading::TFuture<void> Close() override { Closed = true; return NThreading::MakeFuture(); }
    TString GetSessionId() const override { return "mock"; }
    std::vector<TMessageStreamReadEvent> Events;
    NThreading::TPromise<void> Ready = NThreading::NewPromise();
    bool Closed = false;
    ui32 Polls = 0;
};
class TClient final : public IMessageStreamClient {
public:
    explicit TClient(std::shared_ptr<TSession> session) : Session(std::move(session)) {}
    const TString& GetStream() const override { static const TString path = "stream"; return path; }
    std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSessionSettings& settings) override {
        Settings = settings;
        return Session;
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(TMessageStreamPartitionId, const TString&, ui64) override {
        ythrow yexception() << "Unexpected out-of-session commit";
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream() override { ythrow yexception() << "Unexpected describe"; }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(const TString&, const TMessageStreamDescribeConsumerSettings&) override { ythrow yexception() << "Unexpected describe"; }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(TMessageStreamPartitionId) override { ythrow yexception() << "Unexpected describe"; }
    std::shared_ptr<TSession> Session;
    TMessageStreamReadSessionSettings Settings;
};
class TState final : public IMessageStreamReadActorState {
public:
    TMessageStreamReadState& GetReadState() override { return State; }
    void SaveState(const NDqProto::TCheckpoint&, TSourceState& state) override {
        for (const auto& [key, progress] : State.Partitions) {
            if (progress.Offset) { state.Data.emplace_back(ToString(*progress.Offset), 1); }
        }
    }
    void LoadState(const TSourceState& state) override {
        for (const auto& data : state.Data) { State.Partitions[TPartitionKey{TString(), 0}].Offset = FromString<ui64>(data.Blob); }
    }
private:
    TMessageStreamReadState State;
};

struct TFixture {
    std::shared_ptr<TSession> Session = std::make_shared<TSession>();
    std::shared_ptr<TControl> Control = std::make_shared<TControl>();
    std::shared_ptr<TClient> Client = std::make_shared<TClient>(Session);
    TFakeCASetup Setup;
    bool Finished = false;
    bool WatermarksEnabled = false;
    TMaybe<TInstant> LastWatermark;
    bool EnableStreamingAutopartitioning = false;
    NThreading::TFuture<std::shared_ptr<IMessageStreamReadSession>> PendingSession;

    void Init(bool streaming = false, bool requireTime = false,
        std::unique_ptr<IMessageStreamReadActorState> state = std::make_unique<TState>()) {
        Setup.Execute([&](TFakeActor& actor) {
            TMessageStreamReadActorSettings settings;
            settings.InputIndex = 7;
            settings.ComputeActorId = actor.SelfId();
            settings.Stream = "stream";
            settings.Consumer = "consumer";
            settings.StopAtCurrentEndOffsets = !streaming;
            settings.RequireWriteTime = requireTime;
            settings.WatermarksEnabled = WatermarksEnabled;
            settings.WatermarkGranularity = TDuration::Seconds(1);
            settings.EnableStreamingAutopartitioning = EnableStreamingAutopartitioning;
            settings.MetricsSource = "test";
            settings.HolderFactory = &actor.GetHolderFactory();
            TMessageStreamReadCluster cluster;
            cluster.PartitionsCount = 1;
            cluster.Partitions = {0};
            cluster.CreateClient = [client = Client](const auto&) { return client; };
            cluster.CreateSession = [pending = PendingSession](const auto&, IMessageStreamClient& client, const auto& read) {
                if (pending.Initialized()) { return pending; }
                return NThreading::MakeFuture(client.CreateReadSession(read));
            };
            settings.Clusters.push_back(std::move(cluster));
            auto [input, inputActor] = CreateMessageStreamReadActor(std::move(settings), std::move(state));
            actor.InitAsyncInput(input, inputActor);
        });
    }
    void Start(std::optional<ui64> end, ui64 committed = 0) {
        Session->Events.emplace_back(TMessageStreamPartitionStartRequestedEvent{Control, committed, end});
    }
    void Data(ui64 offset, TString payload = "payload", bool writeTime = false) {
        TMessageStreamRecord record;
        record.Id.PartitionId = {0};
        record.Id.Offset = offset;
        record.Data = std::move(payload);
        if (writeTime) { record.WriteTime = TInstant::Seconds(1); }
        Session->Events.emplace_back(TMessageStreamDataEvent{Control, {std::move(record)}});
    }
    TVector<TString> Read(i64 space = 1024) {
        TVector<TString> rows;
        Setup.Execute([&](TFakeActor& actor) {
            NKikimr::NMiniKQL::TUnboxedValueBatch batch;
            TMaybe<TInstant> watermark;
            actor.DqAsyncInput->GetAsyncInputData(batch, watermark, Finished, space);
            LastWatermark = watermark;
            batch.ForEachRow([&](const NUdf::TUnboxedValue& value) { rows.emplace_back(value.AsStringRef()); });
        });
        return rows;
    }
    void Commit(ui64 id) {
        Setup.Execute([&](TFakeActor& actor) { actor.DqAsyncInput->CommitState(CreateCheckpoint(id)); });
    }
};
} // namespace NYql::NDq::NMessageStreamTest
