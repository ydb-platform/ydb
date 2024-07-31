#include "local_partition_reader.h"
#include "logging.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/replication/service/worker.h>

using namespace NActors;
using namespace NKikimr::NReplication::NService;

namespace {

constexpr static char OFFLOAD_ACTOR_CLIENT_ID[] = "__OFFLOAD_ACTOR__";
constexpr static ui64 READ_TIMEOUT_MS = 1000;
constexpr static ui64 READ_LIMIT_BYTES = 1_MB;

} // anonymous namespace

namespace NKikimr::NBackup::NImpl {

class TLocalPartitionReader
    : public TActor<TLocalPartitionReader>
{
private:
    const TActorId PQTablet;
    const ui32 Partition;
    mutable TMaybe<TString> LogPrefix;

    TActorId Worker;
    ui64 Offset = 0;
    ui64 SentOffset = 0;

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[LocalPartitionReader]"
                << "[" << PQTablet << "]"
                << "[" << Partition << "]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    THolder<TEvPersQueue::TEvRequest> CreateGetOffsetRequest() const {
        THolder<TEvPersQueue::TEvRequest> request(new TEvPersQueue::TEvRequest);

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(Partition);
        auto& offset = *req.MutableCmdGetClientOffset();
        offset.SetClientId(OFFLOAD_ACTOR_CLIENT_ID);

        return request;
    }

    void HandleInit() {
        Send(PQTablet, CreateGetOffsetRequest().Release());
    }

    void HandleInit(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        LOG_D("Handshake"
            << ": worker# " << Worker);

        Send(PQTablet, CreateGetOffsetRequest().Release());
    }

    void HandleInit(TEvPersQueue::TEvResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        auto& record = ev->Get()->Record;
        if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup);
            return;
        }
        Y_VERIFY_S(record.GetErrorCode() == NPersQueue::NErrorCode::OK, "Unimplemented!");
        Y_VERIFY_S(record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdGetClientOffsetResult(), "Unimplemented!");
        auto resp = record.GetPartitionResponse().GetCmdGetClientOffsetResult();
        Offset = resp.GetOffset();
        SentOffset = Offset;

        Y_ABORT_UNLESS(Worker);
        Send(Worker, new TEvWorker::TEvHandshake());

        Become(&TLocalPartitionReader::StateWork);
    }

    THolder<TEvPersQueue::TEvRequest> CreateReadRequest() const {
        THolder<TEvPersQueue::TEvRequest> request(new TEvPersQueue::TEvRequest);

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(Partition);
        auto& read = *req.MutableCmdRead();
        read.SetOffset(Offset);
        read.SetClientId(OFFLOAD_ACTOR_CLIENT_ID);
        read.SetTimeoutMs(READ_TIMEOUT_MS);
        read.SetBytes(READ_LIMIT_BYTES);

        return request;
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Offset = SentOffset;
        // TODO: commit offset to PQ

        Y_ABORT_UNLESS(PQTablet);
        Send(PQTablet, CreateReadRequest().Release());
    }

    static NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string) {
        NKikimrPQClient::TDataChunk proto;
        bool res = proto.ParseFromString(string);
        Y_ABORT_UNLESS(res, "Got invalid data from PQTablet");
        return proto;
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();

        if (!readResult.ResultSize()) {
            Y_ABORT_UNLESS(PQTablet);
            Send(PQTablet, CreateReadRequest().Release());
            return;
        }

        auto gotOffset = Offset;
        TVector<TEvWorker::TEvData::TRecord> records(::Reserve(readResult.ResultSize()));

        for (auto& result : readResult.GetResult()) {
            gotOffset = std::max(gotOffset, result.GetOffset());
            records.emplace_back(result.GetOffset(), GetDeserializedData(result.GetData()).GetData());
        }
        SentOffset = gotOffset + 1;

        Send(Worker, new TEvWorker::TEvData(ToString(Partition), std::move(records)));
    }

    void Leave(TEvWorker::TEvGone::EStatus status) {
        LOG_I("Leave");
        Send(Worker, new TEvWorker::TEvGone(status));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BACKUP_LOCAL_PARTITION_READER;
    }

    explicit TLocalPartitionReader(const TActorId& PQTabletMbox, ui32 partition)
        : TActor(&TThis::StateInit)
        , PQTablet(PQTabletMbox)
        , Partition(partition)
    {}

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, HandleInit);
            hFunc(TEvPersQueue::TEvResponse, HandleInit);
            sFunc(TEvents::TEvWakeup, HandleInit);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            Y_VERIFY_S(false, "Unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            Y_VERIFY_S(false, "Unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
        }
    }
}; // TLocalPartitionReader

IActor* CreateLocalPartitionReader(const TActorId& PQTabletMbox, ui32 partition) {
    return new TLocalPartitionReader(PQTabletMbox, partition);
}

} // namespace NKikimr::NBackup::NImpl
