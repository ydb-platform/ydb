#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

#include <functional>
#include <memory>
#include <set>
#include <tuple>
#include <vector>

namespace NKikimr {
namespace {

using TLoadConfig = TEvLoadTestRequest::TDDiskLoad;
using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

struct TControlledLoad {
    TTestActorSystem Runtime{1};
    TActorId Parent;
    TActorId Fake;
    TActorId Barrier;
    TActorId Load;
    TIntrusivePtr<ITimeProvider> OldTimeProvider;
    std::vector<std::unique_ptr<IEventHandle>> Writes;
    std::vector<std::unique_ptr<IEventHandle>> Reads;
    std::vector<std::unique_ptr<IEventHandle>> Disconnects;
    std::vector<std::unique_ptr<IEventHandle>> Finished;
    std::vector<TActorId> PoisonedRecipients;
    ui32 DisconnectSubmissions = 0;
    ui32 WriteSubmissions = 0;
    ui32 ReadSubmissions = 0;

    explicit TControlledLoad(bool readLoad = false, ui32 areaSize = 8192,
            ui32 inFlight = 3, bool initialize = false, float backgroundRatio = 0,
            std::function<void(TLoadConfig&)> configure = {})
    {
        OldTimeProvider = TAppData::TimeProvider;
        TAppData::TimeProvider = TTestActorSystem::CreateTimeProvider();
        Runtime.Start();
        Parent = Runtime.AllocateEdgeActor(1, __FILE__, __LINE__);
        Fake = Runtime.AllocateEdgeActor(1, __FILE__, __LINE__);
        Barrier = Runtime.AllocateEdgeActor(1, __FILE__, __LINE__);
        Runtime.RegisterService(MakeBlobStorageDDiskId(1, 1, 1), Fake);
        Runtime.FilterFunction = [this](ui32, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (type == NDDisk::TEvWrite::EventType) {
                ++WriteSubmissions;
                Writes.emplace_back(std::move(ev));
                return false;
            }
            if (type == NDDisk::TEvRead::EventType) {
                ++ReadSubmissions;
                Reads.emplace_back(std::move(ev));
                return false;
            }
            if (type == NDDisk::TEvDisconnect::EventType) {
                ++DisconnectSubmissions;
                Disconnects.emplace_back(std::move(ev));
                return false;
            }
            if (type == TEvLoad::TEvLoadTestFinished::EventType) {
                Finished.emplace_back(std::move(ev));
                return false;
            }
            if (type == TEvents::TEvPoisonPill::EventType && ev->Sender == Load) {
                PoisonedRecipients.push_back(ev->Recipient);
            }
            return true;
        };

        TEvLoadTestRequest::TDDiskLoad cmd;
        cmd.SetDurationSeconds(3600);
        cmd.SetDelayBeforeMeasurementsSeconds(0);
        cmd.SetInFlight(inFlight);
        cmd.SetInitInFlight(inFlight);
        cmd.SetExpectedChunkSize(16384);
        cmd.SetIoSizeBytes(4096);
        cmd.SetIsReadLoad(readLoad);
        cmd.SetBackgroundWriteRatio(backgroundRatio);
        cmd.SetBackgroundWriteSizeKiB(8);
        auto* id = cmd.MutableDDiskId();
        id->SetNodeId(1);
        id->SetPDiskId(1);
        id->SetDDiskSlotId(1);
        auto* area = cmd.AddAreas();
        area->SetAreaSize(areaSize);
        area->SetSequential(true);
        area->SetInitType(initialize
            ? TEvLoadTestRequest::TDDiskLoad::TArea::INIT_ZEROES_FULL
            : TEvLoadTestRequest::TDDiskLoad::TArea::INIT_NONE);

        if (configure) {
            configure(cmd);
        }

        Load = Runtime.Register(CreateDDiskLoadTest(cmd, Parent,
            MakeIntrusive<::NMonitoring::TDynamicCounters>(), 0, 77, false), 1);
        if (!cmd.GetSimulate()) {
            const auto connect = Runtime.WaitForEdgeActorEvent<NDDisk::TEvConnect>(Fake, false);
            UNIT_ASSERT(connect);
            Runtime.Send(new IEventHandle(Load, Fake,
                new NDDisk::TEvConnectResult(TStatus::OK, std::nullopt, 1,
                    NDDisk::TConnectionToken(1, 1)), 0, connect->Cookie), 1);
        }
        Pump();
    }

    ~TControlledLoad() {
        Runtime.Stop();
        TAppData::TimeProvider = std::move(OldTimeProvider);
    }

    void Pump() {
        Runtime.Schedule(TDuration::MicroSeconds(1),
            new IEventHandle(Barrier, Fake, new TEvents::TEvWakeup()), nullptr, 1);
        Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(Barrier, false);
    }

    static ui32 Offset(IEventHandle& ev) {
        if (ev.GetTypeRewrite() == NDDisk::TEvWrite::EventType) {
            return ev.Get<NDDisk::TEvWrite>()->Record.GetSelector().GetOffsetInBytes();
        }
        return ev.Get<NDDisk::TEvRead>()->Record.GetSelector().GetOffsetInBytes();
    }

    static ui32 Size(IEventHandle& ev) {
        if (ev.GetTypeRewrite() == NDDisk::TEvWrite::EventType) {
            return ev.Get<NDDisk::TEvWrite>()->Record.GetSelector().GetSize();
        }
        return ev.Get<NDDisk::TEvRead>()->Record.GetSelector().GetSize();
    }

    std::unique_ptr<IEventHandle> TakeWrite(ui32 offset, ui32 size = 4096) {
        for (auto it = Writes.begin(); it != Writes.end(); ++it) {
            if (Offset(**it) == offset && Size(**it) == size) {
                auto ev = std::move(*it);
                Writes.erase(it);
                return ev;
            }
        }
        return {};
    }

    std::unique_ptr<IEventHandle> TakeRead(ui32 offset) {
        for (auto it = Reads.begin(); it != Reads.end(); ++it) {
            if (Offset(**it) == offset) {
                auto ev = std::move(*it);
                Reads.erase(it);
                return ev;
            }
        }
        return {};
    }

    void Reply(std::unique_ptr<IEventHandle> ev, TStatus::E status = TStatus::OK,
            const TString& reason = {})
    {
        UNIT_ASSERT(ev);
        IEventBase* result = ev->GetTypeRewrite() == NDDisk::TEvWrite::EventType
            ? static_cast<IEventBase*>(new NDDisk::TEvWriteResult(status, reason))
            : static_cast<IEventBase*>(new NDDisk::TEvReadResult(status, reason));
        Runtime.Send(new IEventHandle(ev->Sender, Fake, result, 0, ev->Cookie), 1);
        Pump();
    }

    void Wakeup() {
        Runtime.Send(new IEventHandle(Load, Parent, new TEvents::TEvWakeup()), 1);
        Pump();
    }

    void Poison() {
        Runtime.Send(new IEventHandle(Load, Parent, new TEvents::TEvPoisonPill()), 1);
        Pump();
    }

    void CompleteDisconnect() {
        UNIT_ASSERT_VALUES_EQUAL(Disconnects.size(), 1);
        auto ev = std::move(Disconnects.back());
        Disconnects.clear();
        Runtime.Send(new IEventHandle(ev->Sender, Fake,
            new NDDisk::TEvDisconnectResult(TStatus::OK), 0, ev->Cookie), 1);
        Pump();
        UNIT_ASSERT_VALUES_EQUAL(Finished.size(), 1);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DDiskLoadRangeAdmission) {
    Y_UNIT_TEST(WrappedAreaWaitsForConflictAndWakesOnRetirement) {
        TControlledLoad f;
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Writes.size(), 2);
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        UNIT_ASSERT(first);
        UNIT_ASSERT(second);
        // Keep the second accepted write outstanding while the first retires.
        f.Reply(std::move(first));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 3);
        auto replacement = f.TakeWrite(0);
        UNIT_ASSERT(replacement);
        f.Poison();
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 3);
        f.Reply(std::move(replacement));
        f.CompleteDisconnect();
    }

    Y_UNIT_TEST(InitializationPoisonDrainsAcceptedWritesBeforeDisconnect) {
        TControlledLoad f(false, 12288, 2, true);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        UNIT_ASSERT(first && second);
        f.Poison();
        UNIT_ASSERT(f.Disconnects.empty());
        UNIT_ASSERT(f.Finished.empty());
        f.Reply(std::move(first));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        UNIT_ASSERT(f.Disconnects.empty());
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.Disconnects.size(), 1);
        UNIT_ASSERT(f.Finished.empty());
        f.CompleteDisconnect();
        UNIT_ASSERT_VALUES_EQUAL(f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason, "OK");
    }

    Y_UNIT_TEST(InitializationErrorStopsUnsentWorkAndDrainsAcceptedWrites) {
        TControlledLoad f(false, 12288, 2, true);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        UNIT_ASSERT(first && second);
        f.Reply(std::move(first), TStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        UNIT_ASSERT(f.Disconnects.empty());
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Disconnects.size(), 1);
        f.CompleteDisconnect();
        UNIT_ASSERT_STRING_CONTAINS(
            f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason,
            "TEvWriteResult error");
    }

    Y_UNIT_TEST(MeasuredErrorDrainsOtherAcceptedWriteBeforeDisconnect) {
        TControlledLoad f;
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        UNIT_ASSERT(first && second);
        f.Reply(std::move(first), TStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        UNIT_ASSERT(f.Disconnects.empty());
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.Disconnects.size(), 1);
        f.CompleteDisconnect();
        UNIT_ASSERT_STRING_CONTAINS(
            f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason,
            "TEvWriteResult error");
    }

    Y_UNIT_TEST(StaleErrorRepliesCannotStopHealthyWritesOrReads) {
        {
            TControlledLoad f;
            auto first = f.TakeWrite(0);
            auto second = f.TakeWrite(4096);
            UNIT_ASSERT(first && second);
            const ui64 retiredCookie = first->Cookie;
            f.Reply(std::move(first));
            auto replacement0 = f.TakeWrite(0);
            UNIT_ASSERT(replacement0);
            f.Runtime.Send(new IEventHandle(f.Load, f.Fake,
                new NDDisk::TEvWriteResult(TStatus::ERROR), 0, retiredCookie), 1);
            f.Pump();
            UNIT_ASSERT(f.Disconnects.empty());
            f.Reply(std::move(second));
            UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 4);
            auto replacement4 = f.TakeWrite(4096);
            UNIT_ASSERT(replacement4);
            f.Poison();
            f.Reply(std::move(replacement0));
            f.Reply(std::move(replacement4));
            f.CompleteDisconnect();
            UNIT_ASSERT_VALUES_EQUAL(
                f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason, "OK");
        }
        {
            TControlledLoad f(true, 4096, 3, true);
            f.Reply(f.TakeWrite(0)); // initialization barrier
            UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
            auto first = f.TakeRead(0);
            auto second = f.TakeRead(0);
            auto third = f.TakeRead(0);
            UNIT_ASSERT(first && second && third);
            const ui64 retiredCookie = first->Cookie;
            f.Reply(std::move(first));
            auto replacement = f.TakeRead(0);
            UNIT_ASSERT(replacement);
            f.Runtime.Send(new IEventHandle(f.Load, f.Fake,
                new NDDisk::TEvReadResult(TStatus::ERROR), 0, retiredCookie), 1);
            f.Pump();
            UNIT_ASSERT(f.Disconnects.empty());
            f.Reply(std::move(second));
            UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 5);
            auto next = f.TakeRead(0);
            UNIT_ASSERT(next);
            f.Poison();
            f.Reply(std::move(third));
            f.Reply(std::move(replacement));
            f.Reply(std::move(next));
            f.CompleteDisconnect();
            UNIT_ASSERT_VALUES_EQUAL(
                f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason, "OK");
        }
    }

    Y_UNIT_TEST(InitBarrierAndDifferentBackgroundRangeSizes) {
        TControlledLoad f(true, 16384, 3, true, 1.0f);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 3);
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        auto third = f.TakeWrite(8192);
        UNIT_ASSERT(first && second && third);
        f.Reply(std::move(second));
        f.Reply(std::move(third));
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 0);
        auto fourth = f.TakeWrite(12288);
        UNIT_ASSERT(fourth);
        f.Reply(std::move(fourth));
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 0);
        f.Reply(std::move(first));
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 5);
        auto read0 = f.TakeRead(0);
        auto read4 = f.TakeRead(4096);
        auto background8 = f.TakeWrite(8192, 8192);
        UNIT_ASSERT(read0 && read4 && background8);
        f.Reply(std::move(read0));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 5); // both 8 KiB slots conflict
        f.Reply(std::move(read4));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 6);
        auto background0 = f.TakeWrite(0, 8192);
        UNIT_ASSERT(background0);
        f.Poison();
        f.Reply(std::move(background8));
        f.Reply(std::move(background0));
        f.CompleteDisconnect();
    }

    Y_UNIT_TEST(OverlappingReadsShareOneSlot) {
        TControlledLoad f(true, 4096, 3, true);
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 1);
        f.Reply(f.TakeWrite(0));
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
        std::vector<std::unique_ptr<IEventHandle>> reads;
        for (ui32 i = 0; i < 3; ++i) {
            auto read = f.TakeRead(0);
            UNIT_ASSERT(read);
            reads.push_back(std::move(read));
        }
        f.Poison();
        for (auto& read : reads) {
            f.Reply(std::move(read));
        }
        f.CompleteDisconnect();
    }

    Y_UNIT_TEST(WrongKindRepliesCannotRetireRequestsOrStopAdmission) {
        {
            TControlledLoad f;
            auto first = f.TakeWrite(0);
            auto second = f.TakeWrite(4096);
            UNIT_ASSERT(first && second);
            for (auto status : {TStatus::OK, TStatus::ERROR}) {
                f.Runtime.Send(new IEventHandle(f.Load, f.Fake,
                    new NDDisk::TEvReadResult(status), 0, first->Cookie), 1);
                f.Pump();
                f.Wakeup();
                UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
                UNIT_ASSERT(f.Disconnects.empty());
                UNIT_ASSERT(f.Finished.empty());
            }
            f.Reply(std::move(second));
            UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 3);
            auto replacement = f.TakeWrite(4096);
            UNIT_ASSERT(replacement);
            f.Poison();
            f.Reply(std::move(replacement));
            UNIT_ASSERT(f.Disconnects.empty());
            f.Reply(std::move(first));
            f.CompleteDisconnect();
            UNIT_ASSERT_VALUES_EQUAL(
                f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason, "OK");
        }
        {
            TControlledLoad f(true, 4096, 3, true);
            f.Reply(f.TakeWrite(0));
            auto first = f.TakeRead(0);
            auto second = f.TakeRead(0);
            auto third = f.TakeRead(0);
            UNIT_ASSERT(first && second && third);
            for (auto status : {TStatus::OK, TStatus::ERROR}) {
                f.Runtime.Send(new IEventHandle(f.Load, f.Fake,
                    new NDDisk::TEvWriteResult(status), 0, first->Cookie), 1);
                f.Pump();
                UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
                UNIT_ASSERT(f.Disconnects.empty());
                UNIT_ASSERT(f.Finished.empty());
            }
            f.Reply(std::move(second));
            UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 4);
            auto replacement = f.TakeRead(0);
            UNIT_ASSERT(replacement);
            f.Poison();
            f.Reply(std::move(third));
            f.Reply(std::move(replacement));
            UNIT_ASSERT(f.Disconnects.empty());
            f.Reply(std::move(first));
            f.CompleteDisconnect();
            const auto* finished = f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>();
            UNIT_ASSERT_VALUES_EQUAL(finished->ErrorReason, "OK");
            UNIT_ASSERT_VALUES_EQUAL(finished->Report->MeasuredReadsSent, 4);
        }
    }

    Y_UNIT_TEST(IndependentChunksAreasAndAdjacentRangesFillWindow) {
        TControlledLoad f(false, 16384, 7, false, 0, [](TLoadConfig& cmd) {
            cmd.SetExpectedChunkSize(8192);
            auto* area = cmd.AddAreas();
            area->SetAreaSize(8192);
            area->SetSequential(true);
            area->SetInitType(TLoadConfig::TArea::INIT_NONE);
        });
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 6);
        std::set<std::tuple<ui64, ui32>> locations;
        for (auto& write : f.Writes) {
            const auto& selector = write->Get<NDDisk::TEvWrite>()->Record.GetSelector();
            UNIT_ASSERT_VALUES_EQUAL(selector.GetSize(), 4096);
            UNIT_ASSERT(locations.emplace(selector.GetVChunkIndex(), selector.GetOffsetInBytes()).second);
        }
        for (ui64 chunk = 0; chunk < 3; ++chunk) {
            UNIT_ASSERT(locations.contains({chunk, 0}));
            UNIT_ASSERT(locations.contains({chunk, 4096}));
        }
        f.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 6);
        f.Poison();
        for (auto& write : f.Writes) {
            f.Reply(std::move(write));
        }
        f.CompleteDisconnect();
    }

    Y_UNIT_TEST(SimulatedDestinationsRotateOnlyOnAdmissionAndDrainOnPoison) {
        TControlledLoad f(false, 4096, 3, false, 0, [](TLoadConfig& cmd) {
            cmd.SetSimulate(true);
            cmd.SetSimulateActorsCount(2);
        });
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(0);
        UNIT_ASSERT(first && second);
        const TActorId firstDestination = first->Recipient;
        const TActorId secondDestination = second->Recipient;
        UNIT_ASSERT(firstDestination != secondDestination);
        // The next destination is still the first, even after blocked attempts.
        f.Wakeup();
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        f.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
        f.Reply(std::move(first));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 4);
        auto replacementFirst = f.TakeWrite(0);
        auto replacementSecond = f.TakeWrite(0);
        UNIT_ASSERT(replacementFirst && replacementSecond);
        UNIT_ASSERT_VALUES_EQUAL(replacementFirst->Recipient, firstDestination);
        UNIT_ASSERT_VALUES_EQUAL(replacementSecond->Recipient, secondDestination);
        f.Poison();
        UNIT_ASSERT(f.PoisonedRecipients.empty());
        UNIT_ASSERT(f.Finished.empty());
        f.Reply(std::move(replacementFirst));
        f.Poison();
        f.Wakeup();
        UNIT_ASSERT(f.PoisonedRecipients.empty());
        UNIT_ASSERT(f.Finished.empty());
        f.Reply(std::move(replacementSecond));
        UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 4);
        UNIT_ASSERT(f.Disconnects.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.PoisonedRecipients.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.PoisonedRecipients[0], firstDestination);
        UNIT_ASSERT_VALUES_EQUAL(f.PoisonedRecipients[1], secondDestination);
        UNIT_ASSERT_VALUES_EQUAL(f.Finished.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason, "OK");
    }

    Y_UNIT_TEST(DisconnectAcknowledgmentRequiredDespiteRepeatedWakeupsAndPoison) {
        TControlledLoad f;
        auto first = f.TakeWrite(0);
        auto second = f.TakeWrite(4096);
        UNIT_ASSERT(first && second);
        f.Poison();
        f.Reply(std::move(first));
        UNIT_ASSERT(f.Disconnects.empty());
        f.Reply(std::move(second));
        UNIT_ASSERT_VALUES_EQUAL(f.DisconnectSubmissions, 1);
        UNIT_ASSERT(f.Finished.empty());
        for (ui32 i = 0; i < 3; ++i) {
            f.Wakeup();
            f.Poison();
            UNIT_ASSERT_VALUES_EQUAL(f.WriteSubmissions, 2);
            UNIT_ASSERT_VALUES_EQUAL(f.DisconnectSubmissions, 1);
            UNIT_ASSERT(f.Finished.empty());
        }
        f.CompleteDisconnect();
        UNIT_ASSERT_VALUES_EQUAL(f.DisconnectSubmissions, 1);
    }

    Y_UNIT_TEST(ReadFailuresDrainAcceptedReadsAndRetainFirstError) {
        TControlledLoad f(true, 4096, 3, true);
        f.Reply(f.TakeWrite(0));
        auto first = f.TakeRead(0);
        auto second = f.TakeRead(0);
        auto third = f.TakeRead(0);
        UNIT_ASSERT(first && second && third);
        f.Reply(std::move(first), TStatus::ERROR, "first read failure");
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
        UNIT_ASSERT(f.Disconnects.empty());
        UNIT_ASSERT(f.Finished.empty());
        f.Reply(std::move(second), TStatus::ERROR, "second read failure");
        f.Wakeup();
        f.Poison();
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
        UNIT_ASSERT(f.Disconnects.empty());
        UNIT_ASSERT(f.Finished.empty());
        f.Reply(std::move(third));
        UNIT_ASSERT_VALUES_EQUAL(f.ReadSubmissions, 3);
        UNIT_ASSERT(f.Finished.empty());
        f.CompleteDisconnect();
        const auto* finished = f.Finished.front()->Get<TEvLoad::TEvLoadTestFinished>();
        UNIT_ASSERT_VALUES_EQUAL(finished->ErrorReason,
            "TEvReadResult error, Status# ERROR ErrorReason# first read failure");
        UNIT_ASSERT_VALUES_EQUAL(finished->Report->MeasuredReadsSent, 3);
    }
}

} // namespace NKikimr
