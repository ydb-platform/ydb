#include "env.h"

TEnvironmentSetup *TEnvironmentSetup::Env = nullptr;

const ui64 NKikimr::NPDisk::YdbDefaultPDiskSequence = 0x7e5700007e570000;

Y_UNIT_TEST_SUITE(BlobDepotWithTestShard) {

    Y_UNIT_TEST(PlainGroup) {
        return; // Fix group overseer, KIKIMR-20069

        THPTimer timer;

        TEnvironmentSetup env{{}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Seconds(1));

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const ui64 hiveId = MakeDefaultHiveID();
        const TActorId clientId = env.Runtime->Register(NTabletPipe::CreateClient(edge, hiveId,
            NTabletPipe::TClientRetryPolicy::WithRetries()), edge.NodeId());

        {
            auto response = env.Runtime->WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        }

        std::vector<ui64> tabletIds;

        for (ui32 i = 0; i < 2; ++i) {
            env.Runtime->WrapInActorContext(edge, [&] {
                auto ev = std::make_unique<TEvHive::TEvCreateTablet>();
                auto& record = ev->Record;
                record.SetOwner(1);
                record.SetOwnerIdx(1 + i);
                record.SetTabletType(TTabletTypes::TestShard);
                for (ui32 j = 0; j < 3; ++j) {
                    auto *ch = record.AddBindedChannels();
                    ch->SetStoragePoolName("virtual");
                    ch->SetPhysicalGroupsOnly(false);
                }
                NTabletPipe::SendData(edge, clientId, ev.release());
            });

            ui64 tabletId;
            {
                auto res = env.Runtime->WaitForEdgeActorEvent<TEvHive::TEvCreateTabletReply>(edge, false);
                auto& record = res->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                tabletId = record.GetTabletID();
            }

            {
                auto res = env.Runtime->WaitForEdgeActorEvent<TEvHive::TEvTabletCreationResult>(edge, false);
                auto& record = res->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            }

            tabletIds.push_back(tabletId);
        }

        env.Runtime->WrapInActorContext(edge, [&] { NTabletPipe::CloseClient(TActivationContext::AsActorContext(), clientId); });
        env.Runtime->DestroyActor(edge);

        for (ui64 tabletId : tabletIds) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

            const TActorId pipeId = env.Runtime->Register(NTabletPipe::CreateClient(edge, tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()), edge.NodeId());

            {
                auto response = env.Runtime->WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
            }

            env.Runtime->WrapInActorContext(edge, [&] {
                auto ev = std::make_unique<NKikimr::NTestShard::TEvControlRequest>();
                auto& record = ev->Record;
                record.SetTabletId(tabletId);
                auto *cmd = record.MutableInitialize();
                cmd->SetStorageServerHost("");
                cmd->SetMaxDataBytes(1'000'000'000);
                cmd->SetMinDataBytes(100'000'000);
                cmd->SetMaxInFlight(2);
                cmd->SetValidateAfterBytes(1'000'000'000);
                auto *s = cmd->AddSizes();
                s->SetWeight(1);
                s->SetMin(1'000);
                s->SetMax(8'000'000);
                auto *w = cmd->AddWritePeriods();
                w->SetWeight(1);
                w->SetFrequency(1);
                w->SetMaxIntervalMs(5'000);
                auto *r = cmd->AddRestartPeriods();
                r->SetWeight(1);
                r->SetFrequency(0.05);
                r->SetMaxIntervalMs(20'000);
                NTabletPipe::SendData(edge, pipeId, ev.release());
            });

            env.Runtime->WaitForEdgeActorEvent<NKikimr::NTestShard::TEvControlResponse>(edge, false);

            env.Runtime->WrapInActorContext(edge, [&] { NTabletPipe::CloseClient(TActivationContext::AsActorContext(), pipeId); });
            env.Runtime->DestroyActor(edge);

            Cerr << "TabletId# " << tabletId << " configured" << Endl;
        }

        std::vector<IActor*> blobDepots;
        env.Runtime->EnumActors([&](IActor *actor) {
            if (NBlobDepot::IsBlobDepotActor(actor)) {
                blobDepots.push_back(actor);
            }
        });

        TDuration cycleTime;

        while (cycleTime + TDuration::Seconds(timer.Passed()) <= TDuration::Seconds(300)) {
            const TDuration begin = TDuration::Seconds(timer.Passed());

            for (IActor *actor : blobDepots) {
                NBlobDepot::ValidateBlobDepot(actor, env.GroupOverseer);
            }
            env.Sim(TDuration::MilliSeconds(5));

            cycleTime = TDuration::Seconds(timer.Passed()) - begin;
        }
    }

}
