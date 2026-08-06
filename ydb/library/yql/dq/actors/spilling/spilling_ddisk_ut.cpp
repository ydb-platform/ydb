#include "spilling_ddisk.h"
#include "spilling.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>

#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr;

namespace {

class TTestActorRuntime: public TTestActorRuntimeBase {
public:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }

    void Initialize() override {
        TTestActorRuntimeBase::Initialize();
        SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
    }
};

NYql::TChunkedBuffer CreateBlob(size_t size, char fill) {
    TString data(size, fill);
    return NYql::TChunkedBuffer(TString(data));
}

// Minimal PersistentBuffer stand-in that speaks the subset of the DDisk protocol
// used by TDqDDiskSpillingActor.
class TMockPersistentBufferActor : public TActorBootstrapped<TMockPersistentBufferActor> {
public:
    void Bootstrap() {
        Become(&TMockPersistentBufferActor::StateFunc);
    }

    static constexpr char ActorName[] = "MOCK_PB";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NDDisk::TEvConnect, Handle)
        hFunc(NDDisk::TEvDisconnect, Handle)
        hFunc(NDDisk::TEvWritePersistentBuffer, Handle)
        hFunc(NDDisk::TEvReadPersistentBuffer, Handle)
        hFunc(NDDisk::TEvErasePersistentBuffer, Handle)
        hFunc(NDDisk::TEvBatchErasePersistentBuffer, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    void Handle(NDDisk::TEvConnect::TPtr& ev) {
        const NDDisk::TQueryCredentials creds(ev->Get()->Record.GetCredentials());
        ConnectedTablets_.insert(creds.TabletId);
        Send(ev->Sender, new NDDisk::TEvConnectResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, /*guid=*/42), 0, ev->Cookie);
    }

    void Handle(NDDisk::TEvDisconnect::TPtr& ev) {
        const NDDisk::TQueryCredentials creds(ev->Get()->Record.GetCredentials());
        ConnectedTablets_.erase(creds.TabletId);
        // Drop all records for this tablet
        for (auto it = Records_.begin(); it != Records_.end(); ) {
            if (it->first.TabletId == creds.TabletId) {
                it = Records_.erase(it);
            } else {
                ++it;
            }
        }
        Send(ev->Sender, new NDDisk::TEvDisconnectResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK), 0, ev->Cookie);
    }

    void Handle(NDDisk::TEvWritePersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const NDDisk::TQueryCredentials creds(record.GetCredentials());
        if (!ConnectedTablets_.contains(creds.TabletId)) {
            Send(ev->Sender, new NDDisk::TEvWritePersistentBufferResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH, "not connected"), 0, ev->Cookie);
            return;
        }

        const NDDisk::TBlockSelector selector(record.GetSelector());
        const NDDisk::TWriteInstruction instr(record.GetInstruction());
        Y_ABORT_UNLESS(instr.PayloadId);
        TRope payload = ev->Get()->GetPayload(*instr.PayloadId);
        Y_ABORT_UNLESS(payload.size() == selector.Size);

        TKey key{creds.TabletId, creds.Generation, record.GetLsn()};
        Records_[key] = std::move(payload);

        Send(ev->Sender, new NDDisk::TEvWritePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK), 0, ev->Cookie);
    }

    void Handle(NDDisk::TEvReadPersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const NDDisk::TQueryCredentials creds(record.GetCredentials());
        TKey key{creds.TabletId, record.GetGeneration(), record.GetLsn()};
        auto it = Records_.find(key);
        if (it == Records_.end()) {
            Send(ev->Sender, new NDDisk::TEvReadPersistentBufferResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD), 0, ev->Cookie);
            return;
        }

        TRope data = it->second;
        Send(ev->Sender, new NDDisk::TEvReadPersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            /*vChunkIndex=*/0,
            /*offsetInBytes=*/0,
            static_cast<ui32>(data.size()),
            std::move(data)), 0, ev->Cookie);
    }

    void Handle(NDDisk::TEvErasePersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const NDDisk::TQueryCredentials creds(record.GetCredentials());
        const ui64 lsn = record.GetLsn();
        for (auto it = Records_.begin(); it != Records_.end(); ) {
            if (it->first.TabletId == creds.TabletId && it->first.Lsn <= lsn) {
                it = Records_.erase(it);
            } else {
                ++it;
            }
        }
        Send(ev->Sender, new NDDisk::TEvErasePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK), 0, ev->Cookie);
    }

    void Handle(NDDisk::TEvBatchErasePersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const NDDisk::TQueryCredentials creds(record.GetCredentials());
        for (const auto& erase : record.GetErases()) {
            TKey key{creds.TabletId, erase.GetGeneration(), erase.GetLsn()};
            Records_.erase(key);
        }
        Send(ev->Sender, new NDDisk::TEvErasePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK), 0, ev->Cookie);
    }

private:
    struct TKey {
        ui64 TabletId = 0;
        ui32 Generation = 0;
        ui64 Lsn = 0;

        bool operator==(const TKey& o) const {
            return TabletId == o.TabletId && Generation == o.Generation && Lsn == o.Lsn;
        }
    };

    struct TKeyHash {
        size_t operator()(const TKey& k) const {
            return MultiHash(k.TabletId, k.Generation, k.Lsn);
        }
    };

    THashSet<ui64> ConnectedTablets_;
    THashMap<TKey, TRope, TKeyHash> Records_;
};

class TMockNodeWarden : public TActorBootstrapped<TMockNodeWarden> {
public:
    explicit TMockNodeWarden(TActorId pbId)
        : PBId_(pbId)
    {}

    void Bootstrap() {
        Become(&TMockNodeWarden::StateFunc);
    }

    static constexpr char ActorName[] = "MOCK_NW";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvNodeWardenListLocalDDisks, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    void Handle(TEvNodeWardenListLocalDDisks::TPtr& ev) {
        auto res = std::make_unique<TEvNodeWardenListLocalDDisksResult>();
        if (PBId_) {
            res->Infos.push_back({TActorId{}, PBId_});
        }
        Send(ev->Sender, res.release(), 0, ev->Cookie);
    }

    TActorId PBId_;
};

struct TFixture : public NUnitTest::TBaseFixture {
    void SetUp(NUnitTest::TTestContext&) override {
        Runtime = MakeHolder<TTestActorRuntime>();
        Runtime->Initialize();
        Runtime->SetScheduledEventFilter([](auto&&...) { return false; });

        Edge = Runtime->AllocateEdgeActor();
        MockPB = Runtime->Register(new TMockPersistentBufferActor());
        auto nw = Runtime->Register(new TMockNodeWarden(MockPB));
        Runtime->RegisterService(MakeBlobStorageNodeWardenID(Runtime->GetNodeId(0)), nw);

        TDDiskSpillingConfig cfg;
        cfg.Enable = true;
        ConfigureDqSpillingBackend(EDqSpillingBackend::DDisk, cfg);
    }

    void TearDown(NUnitTest::TTestContext&) override {
        ConfigureDqSpillingBackend(EDqSpillingBackend::LocalFile);
        Runtime.Reset();
    }

    TActorId StartSpillingActor(bool removeBlobsAfterRead = false) {
        auto* actor = CreateDqDDiskSpillingActor(
            /*txId=*/ui64(42),
            "test",
            Edge,
            removeBlobsAfterRead,
            ESpillingType::Compute,
            GetDqDDiskSpillingConfig(),
            MockPB);
        return Runtime->Register(actor);
    }

    THolder<TTestActorRuntime> Runtime;
    TActorId Edge;
    TActorId MockPB;
};

} // namespace

Y_UNIT_TEST_SUITE(DqSpillingDDiskTests) {

Y_UNIT_TEST_F(WriteReadRoundtrip, TFixture) {
    auto spilling = StartSpillingActor();

    const TString expected(100, 'x');
    Runtime->Send(new IEventHandle(spilling, Edge,
        new TEvDqSpilling::TEvWrite(1, CreateBlob(expected.size(), 'x'))));

    {
        auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 1u);
    }

    Runtime->Send(new IEventHandle(spilling, Edge, new TEvDqSpilling::TEvRead(1)));

    {
        auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(TString(resp->Get()->Blob.Data(), resp->Get()->Blob.Size()), expected);
    }

    Runtime->Send(new IEventHandle(spilling, Edge, new TEvents::TEvPoison()));
}

Y_UNIT_TEST_F(WriteReadRemove, TFixture) {
    auto spilling = StartSpillingActor(/*removeBlobsAfterRead=*/false);

    Runtime->Send(new IEventHandle(spilling, Edge,
        new TEvDqSpilling::TEvWrite(7, CreateBlob(50, 'a'))));
    UNIT_ASSERT(Runtime->GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(Edge, TDuration::Seconds(5)));

    Runtime->Send(new IEventHandle(spilling, Edge, new TEvDqSpilling::TEvRead(7, /*removeBlob=*/true)));
    {
        auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Blob.size(), 50u);
    }

    // Second read should fail — blob was erased.
    Runtime->Send(new IEventHandle(spilling, Edge, new TEvDqSpilling::TEvRead(7)));
    auto err = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvError>(Edge, TDuration::Seconds(5));
    UNIT_ASSERT(err);
    UNIT_ASSERT(err->Get()->Message.find("not found") != TString::npos ||
                err->Get()->Message.find("Blob not found") != TString::npos);
}

Y_UNIT_TEST_F(MultiPartBlob, TFixture) {
    auto spilling = StartSpillingActor();

    // Force multi-part: payload larger than one PB record (512 KiB).
    const size_t size = 600_KB;
    Runtime->Send(new IEventHandle(spilling, Edge,
        new TEvDqSpilling::TEvWrite(3, CreateBlob(size, 'm'))));

    {
        auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(Edge, TDuration::Seconds(10));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 3u);
    }

    Runtime->Send(new IEventHandle(spilling, Edge, new TEvDqSpilling::TEvRead(3)));
    {
        auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(Edge, TDuration::Seconds(10));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Blob.size(), size);
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Blob.Data()[0], 'm');
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Blob.Data()[size - 1], 'm');
    }

    Runtime->Send(new IEventHandle(spilling, Edge, new TEvents::TEvPoison()));
}

Y_UNIT_TEST_F(CreateDqSpillingActorPicksDDisk, TFixture) {
    UNIT_ASSERT_VALUES_EQUAL(GetDqSpillingBackend(), EDqSpillingBackend::DDisk);
    auto* actor = CreateDqSpillingActor(ui64(1), "pick", Edge, false, ESpillingType::Channel);
    auto id = Runtime->Register(actor);

    Runtime->Send(new IEventHandle(id, Edge, new TEvDqSpilling::TEvWrite(0, CreateBlob(16, 'z'))));
    auto resp = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(Edge, TDuration::Seconds(5));
    UNIT_ASSERT(resp);
    UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 0u);

    Runtime->Send(new IEventHandle(id, Edge, new TEvents::TEvPoison()));
}

Y_UNIT_TEST_F(ConnectFailureWhenNoLocalDDisk, TFixture) {
    // NodeWarden returns an empty list.
    auto emptyNw = Runtime->Register(new TMockNodeWarden(TActorId{}));
    Runtime->RegisterService(MakeBlobStorageNodeWardenID(Runtime->GetNodeId(0)), emptyNw);

    TDDiskSpillingConfig cfg;
    cfg.Enable = true;
    auto* actor = CreateDqDDiskSpillingActor(ui64(1), "noservice", Edge, false, ESpillingType::Compute, cfg);
    Runtime->Register(actor);

    auto err = Runtime->GrabEdgeEvent<TEvDqSpilling::TEvError>(Edge, TDuration::Seconds(5));
    UNIT_ASSERT(err);
    UNIT_ASSERT(err->Get()->Message.find("No local DDisk") != TString::npos);
}

} // Y_UNIT_TEST_SUITE(DqSpillingDDiskTests)

} // namespace NYql::NDq
