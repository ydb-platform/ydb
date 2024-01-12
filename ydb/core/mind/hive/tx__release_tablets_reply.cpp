#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TReleaseTabletsWaitActor : public TActorBootstrapped<TReleaseTabletsWaitActor>, public ISubActor {
public:
    ui32 TabletsTotal = 0;
    ui32 TabletsDone = 0;
    THive* Hive;

    TReleaseTabletsWaitActor(THive* hive)
        : Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr&) {
        ++TabletsDone;
        if (TabletsDone >= TabletsTotal) {
            BLOG_D("THive::TTxReleaseTabletsReply::Complete - continue migration");
            Hive->SendToRootHivePipe(new TEvHive::TEvSeizeTablets(Hive->MigrationFilter));
            PassAway();
        }
    }

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateWork);
        if (TabletsTotal == 0) {
            BLOG_D("THive::TTxReleaseTabletsReply::Complete - continue migration");
            Hive->SendToRootHivePipe(new TEvHive::TEvSeizeTablets(Hive->MigrationFilter));
            PassAway();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
        }
    }
};


class TTxReleaseTabletsReply : public TTransactionBase<THive> {
    THolder<TEvHive::TEvReleaseTabletsReply::THandle> Request;

public:
    TTxReleaseTabletsReply(THolder<TEvHive::TEvReleaseTabletsReply::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RELEASE_TABLETS_REPLY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvReleaseTabletsReply& request(Request->Get()->Record);
        BLOG_D("THive::TTxReleaseTabletsReply::Execute " << request);
        NIceDb::TNiceDb db(txc.DB);
        for (const TTabletId tabletId : request.GetTabletIDs()) {
            db.Table<Schema::Tablet>().Key(tabletId).Update<Schema::Tablet::NeedToReleaseFromParent>(false);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const NKikimrHive::TEvReleaseTabletsReply& request(Request->Get()->Record);
        BLOG_D("THive::TTxReleaseTabletsReply::Complete");

        TActorId waitActorId;
        TReleaseTabletsWaitActor* waitActor = nullptr;
        if (Self->MigrationFilter.GetWaitForTabletsToRise()) {
            waitActor = new TReleaseTabletsWaitActor(Self);
            waitActorId = ctx.RegisterWithSameMailbox(waitActor);
            Self->SubActors.emplace_back(waitActor);
        }

        for (TTabletId tabletId : request.GetTabletIDs()) {
            TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
            if (tablet != nullptr) {
                tablet->NeedToReleaseFromParent = false;
                if (tablet->IsReadyToAssignGroups()) {
                    tablet->InitiateAssignTabletGroups();
                } else if (tablet->IsBootingSuppressed()) {
                    // Tablet will never boot, so notify about creation right now
//                    for (const TActorId& actor : tablet->ActorsToNotify) {
//                        ctx.Send(actor, new TEvHive::TEvTabletCreationResult(NKikimrProto::OK, TabletId));
//                    }
                    tablet->ActorsToNotify.clear();
                } else {
                    tablet->TryToBoot();
                    if (waitActor) {
                        waitActor->TabletsTotal++;
                        tablet->ActorsToNotifyOnRestart.emplace_back(waitActorId);
                    }
                }
            }
        }
        Self->MigrationProgress += request.TabletIDsSize();
        // continue migration
        if (waitActor) {
            BLOG_D("THive::TTxReleaseTabletsReply::Complete - waiting for tablets to rise");
            return; // waiting for tablets
        } else {
            Self->SendToRootHivePipe(new TEvHive::TEvSeizeTablets(Self->MigrationFilter));
        }
    }
};

ITransaction* THive::CreateReleaseTabletsReply(TEvHive::TEvReleaseTabletsReply::TPtr event) {
    return new TTxReleaseTabletsReply(THolder(event.Release()), this);
}

} // NHive
} // NKikimr
