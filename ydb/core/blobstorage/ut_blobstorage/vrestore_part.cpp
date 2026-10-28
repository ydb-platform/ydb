#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

#include <array>
#include <optional>

namespace {

    struct TFixture {
        struct TVDiskResponse {
            NKikimrProto::EReplyStatus Status = NKikimrProto::NODATA;
            ui32 PartId = 0;
            TString Data;
            bool Keep = false;
            bool DoNotKeep = false;
            TDuration ReplyDelay = TDuration::Zero();
        };

        struct TStored {
            TString Data;
            TIngress Ingress;
        };

        TEnvironmentSetup Env{{
            .NodeCount = 9,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        }};
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TString Data = TString(1024, 'x');
        const TLogoBlobID FullId{1, 1, 1, 0, ui32(Data.size()), 0};
        std::array<TVDiskResponse, 9> VDiskResponses;
        TDuration LocalReadDelay = TDuration::Zero();

        TFixture() {
            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvProxyQueueState) {
                    const auto* recipient = Env.Runtime->GetActor(ev->GetRecipientRewrite());
                    if (recipient && recipient->GetActivityType() == NKikimrServices::TActivity::BS_BLOB_RECOVERY_ACTOR) {
                        // RestorePart must issue TEvVGet even without connection quorum.
                        ev->Get<TEvProxyQueueState>()->IsConnected = false;
                    }
                    return true;
                }
                if (!Info || ev->GetTypeRewrite() != TEvBlobStorage::EvVGet) {
                    return true;
                }
                const auto* get = ev->Get<TEvBlobStorage::TEvVGet>();
                if (LocalReadDelay != TDuration::Zero() && get->SuppressCorruptedBlobRestoration
                        && get->Record.GetHandleClass() == NKikimrBlobStorage::AsyncRead) {
                    Env.Runtime->Schedule(std::exchange(LocalReadDelay, TDuration::Zero()), ev.release(),
                        nullptr, nodeId);
                    return false;
                }
                const auto* sender = Env.Runtime->GetActor(ev->Sender);
                if (!sender || sender->GetActivityType() != NKikimrServices::TActivity::BS_BLOB_RECOVERY_ACTOR) {
                    return true;
                }
                const auto& request = ev->Get<TEvBlobStorage::TEvVGet>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(request.ExtremeQueriesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(request.GetExtremeQueries(0).GetId()), FullId);
                const auto vdiskId = VDiskIDFromVDiskID(request.GetVDiskID());
                const auto& response = VDiskResponses.at(Info->GetIdxInSubgroup(vdiskId, FullId.Hash()));
                if (response.Status == NKikimrProto::UNKNOWN) {
                    return false;
                }
                auto reply = std::make_unique<TEvBlobStorage::TEvVGetResult>();
                reply->Record.SetStatus(NKikimrProto::OK);
                reply->Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
                if (response.Status == NKikimrProto::OK) {
                    reply->AddResult(response.Status, TLogoBlobID(FullId, response.PartId), 0,
                        TRope(response.Data), nullptr, nullptr, response.Keep, response.DoNotKeep);
                } else {
                    reply->AddResult(response.Status, FullId);
                }
                Env.Runtime->Schedule(response.ReplyDelay, new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    reply.release(), 0, ev->Cookie), nullptr, ev->Sender.NodeId());
                return false;
            };
            Env.CreateBoxAndPool(1, 1);
            Env.Sim(TDuration::Minutes(1));
            const auto groups = Env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            Info = Env.GetGroupInfo(groups.front());
        }

        ~TFixture() {
            Env.Runtime->FilterFunction = nullptr;
        }

        TVDiskID VDisk(ui32 subgroupIdx) const {
            return Info->GetVDiskInSubgroup(subgroupIdx, FullId.Hash());
        }

        void PutLocalPart(ui32 subgroupIdx, ui32 partId) {
            Env.PutBlob(VDisk(subgroupIdx), TLogoBlobID(FullId, partId), Data);
        }

        NKikimrBlobStorage::TEvVRestorePartResult RestorePart(ui32 subgroupIdx, ui32 partId,
                TDuration timeout = TDuration::Seconds(30)) {
            const auto vdiskId = VDisk(subgroupIdx);
            const auto target = Info->GetActorId(TVDiskIdShort(vdiskId));
            const auto edge = Env.Runtime->AllocateEdgeActor(target.NodeId(), __FILE__, __LINE__);
            const auto deadline = Env.Runtime->GetClock() + timeout;
            auto request = std::make_unique<TEvVRestorePart>();
            VDiskIDFromVDiskID(vdiskId, request->Record.MutableVDiskID());
            LogoBlobIDFromLogoBlobID(TLogoBlobID(FullId, partId), request->Record.MutableBlobID());
            request->Record.SetDeadline(deadline.GetValue());
            Env.Runtime->Send(new IEventHandle(target, edge, request.release()), target.NodeId());
            const auto result = Env.WaitForEdgeActorEvent<TEvVRestorePartResult>(edge, true,
                deadline + TDuration::Seconds(1));
            UNIT_ASSERT(result);
            return result->Get()->Record;
        }

        std::optional<TStored> Read(ui32 subgroupIdx, ui32 partId) {
            const auto vdiskId = VDisk(subgroupIdx);
            const auto target = Info->GetActorId(TVDiskIdShort(vdiskId));
            const auto queue = Env.CreateQueueActor(vdiskId, NKikimrBlobStorage::GetFastRead, 123, target.NodeId());
            const auto edge = Env.Runtime->AllocateEdgeActor(target.NodeId(), __FILE__, __LINE__);
            auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {},
                {TLogoBlobID(FullId, partId)});
            query->SuppressCorruptedBlobRestoration = true;
            Env.Runtime->Send(new IEventHandle(queue, edge, query.release()), edge.NodeId());
            const auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
            Env.Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, queue, {}, nullptr, 0), queue.NodeId());
            const auto& record = result->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
            const auto& item = record.GetResult(0);
            if (item.GetStatus() == NKikimrProto::NODATA) {
                return std::nullopt;
            }
            UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT(item.HasIngress());
            return TStored{result->Get()->GetBlobData(item).ConvertToString(), TIngress(item.GetIngress())};
        }

        void CheckWritten(ui32 subgroupIdx, ui32 partId, bool keep = false) {
            const auto stored = Read(subgroupIdx, partId);
            UNIT_ASSERT(stored);
            UNIT_ASSERT_VALUES_EQUAL(stored->Data, Data);
            UNIT_ASSERT_VALUES_EQUAL(stored->Ingress.LocalParts(Info->Type).Raw(),
                NMatrix::TVectorType::MakeOneHot(partId - 1, Info->Type.TotalPartCount()).Raw());
            UNIT_ASSERT_VALUES_EQUAL(stored->Ingress.IsKeep(Info->Type), keep);
            UNIT_ASSERT(!stored->Ingress.IsDoNotKeep(Info->Type));
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(VRestorePart) {
    Y_UNIT_TEST(MissingPart) {
        TFixture f;
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::OK);
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(HandoffPart) {
        TFixture f;
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(3, 1).GetStatus(), NKikimrProto::OK);
        f.CheckWritten(3, 1);
    }

    Y_UNIT_TEST(Already) {
        TFixture f;
        f.PutLocalPart(0, 1);
        f.VDiskResponses[1].Status = NKikimrProto::UNKNOWN;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::ALREADY);
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(AlreadyAfterRecovery) {
        TFixture f;
        f.PutLocalPart(0, 1);
        f.LocalReadDelay = TDuration::Seconds(1);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::ALREADY);
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(AlreadyAfterSuccessfulRecovery) {
        TFixture f;
        f.PutLocalPart(0, 1);
        f.LocalReadDelay = TDuration::Seconds(1);
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data, .Keep = true};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::ALREADY);
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(Corrupted) {
        TFixture f;
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data};
        f.VDiskResponses[2] = {.Status = NKikimrProto::OK, .PartId = 3, .Data = TString(f.Data.size(), 'y'),
            .ReplyDelay = TDuration::Seconds(1)};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::OK);
        f.Env.Sim(TDuration::Seconds(2));
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(NoData) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::NODATA);
        UNIT_ASSERT(!f.Read(0, 1));
    }

    Y_UNIT_TEST(Keep) {
        TFixture f;
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data, .Keep = true};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::OK);
        f.CheckWritten(0, 1, /*keep=*/true);
    }

    Y_UNIT_TEST(DoNotKeep) {
        TFixture f;
        f.VDiskResponses[1] = {.Status = NKikimrProto::OK, .PartId = 2, .Data = f.Data, .DoNotKeep = true};
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1).GetStatus(), NKikimrProto::OK);
        f.CheckWritten(0, 1);
    }

    Y_UNIT_TEST(RecoveryTimeout) {
        TFixture f;
        f.VDiskResponses[1].Status = NKikimrProto::UNKNOWN;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, 1, TDuration::Seconds(1)).GetStatus(), NKikimrProto::DEADLINE);
        UNIT_ASSERT(!f.Read(0, 1));
    }
}
