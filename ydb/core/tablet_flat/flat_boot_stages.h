#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_snap.h"
#include "flat_boot_turns.h"
#include "flat_boot_gclog.h"
#include "flat_boot_loans.h"
#include "flat_boot_alter.h"
#include "flat_boot_bundle.h"
#include "flat_boot_redo.h"
#include "flat_boot_warm.h"
#include "flat_boot_txstatus.h"
#include "flat_dbase_naked.h"
#include "logic_redo_queue.h"
#include "flat_database.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TStages final: public NBoot::IStep {

        enum class EStage : unsigned {
            Snap           = 1, /* Read snapshot and deps graph log     */
            Meta           = 2, /* Executor system metalog queues       */
            DatabaseImpl   = 3, /* Roll up TDatabaseImpl to actual state*/
            Blobs          = 4, /* Load TMemTable blobs to its cache    */
            Result         = 5, /* Finalize NBoot::TResult object       */
            Ready          = 6,
        };

    public:
        TStages() = delete;

        TStages(IStep *owner, TIntrusivePtr<TDependency> deps, TAutoPtr<TBody> snap)
            : IStep(owner, NBoot::EStep::Stages)
            , Deps(std::move(deps))
            , Snap(std::move(snap))
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() noexcept override
        {
            Execute();
        }

        void HandleStep(TIntrusivePtr<IStep>) noexcept override
        {
            Pending -=1, Execute();
        }

    private:
        void Execute() noexcept
        {
            while (!Pending && Next < EStage::Ready) {
                if (EStage::Snap == Next) {
                    StartStageSnap();
                } else if (EStage::Meta == Next) {
                    StartStageMeta();
                } else if (EStage::DatabaseImpl == Next) {
                    StartStageDatabaseImpl();
                } else if (EStage::Blobs == Next) {
                    Pending += Spawn<TMemTable>();
                } else if (EStage::Result == Next) {
                    StartStageResult(Logic->Result());
                }

                Next = EStage(unsigned(Next) + 1);

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back)
                        << " fired stage " << (unsigned(Next) - 1) << ", has "
                        << Pending.Get() << " jobs, " << NFmt::Do(*Env);
                }
            }
        }

        void StartStageSnap() noexcept
        {
            if (auto logl = Env->Logger()->Log(ELnLev::Info)) {
                logl
                    << NFmt::Do(*Back) << " booting"
                    << " " << NFmt::If(Deps.Get())
                    << " " << NFmt::If(Snap.Get());
            }

            Pending += Spawn<TSnap>(std::move(Deps), Snap);
        }

        void StartStageMeta() noexcept
        {
            if (auto logl = Env->Logger()->Log(ELnLev::Info)) {
                logl
                    << NFmt::Do(*Back) << " loading {"
                    << " Alter " << Back->AlterLog.size()
                    << ", Turns " << Back->Switches.size()
                    << ", Loans " << Back->LoansLog.size()
                    << ", GCExt " << Back->GCELog.size() << " }";
            }

            Pending += Spawn<TGCLog>(std::move(Back->GCELog));
            Pending += Spawn<TLoans>(std::move(Back->LoansLog));
            Pending += Spawn<TAlter>(std::move(Back->AlterLog));
            Pending += Spawn<TTurns>();
        }

        void StartStageDatabaseImpl() noexcept
        {
            auto weak = Back->RedoLog ? Back->RedoLog.back().Stamp + 1 : 0;

            Back->DatabaseImpl = new NTable::TDatabaseImpl(weak, Back->Scheme, &Back->Edges);

            if (!Back->Follower) {
                Back->Redo = new NRedo::TQueue(std::move(Back->Edges));
            }

            for (auto &se: std::exchange(Back->Switches, { })) {
                auto &wrap = Back->DatabaseImpl->Get(se.Table, false);

                if (!wrap) {
                    continue; // ignore dropped tables
                }

                auto &schema = Back->DatabaseImpl->Scheme->Tables.at(se.Table);

                auto processBundles = [&](TVector<TSwitch::TBundle> &bundles) {
                    for (auto &bundle: bundles) {
                        if (bundle.Load) {
                            bundle.AddGroups([this](const TLogoBlobID &logo)
                                { return Logic->GetBSGroupFor(logo); });

                            // FIXME: currently it's only safe to use cold mode
                            // for borrowed parts, otherwise we cannot perform
                            // many important gc tasks without knowing the full
                            // list of blobs.
                            if (schema.ColdBorrow && !bundle.Deltas && bundle.LargeGlobIds[0].Lead.TabletID() != Back->Tablet) {
                                Back->DatabaseImpl->Merge(
                                    se.Table,
                                    new NTable::TColdPartStore(
                                        std::move(bundle.LargeGlobIds),
                                        std::move(bundle.Legacy),
                                        std::move(bundle.Opaque),
                                        bundle.Epoch));
                                continue;
                            }

                            Pending += Spawn<TBundleLoadStep>(se.Table, bundle);
                        }
                    }
                };

                processBundles(se.Bundles);
                processBundles(se.MovedBundles);

                for (auto &txStatus : se.TxStatus) {
                    if (txStatus.Load) {
                        Pending += Spawn<TBootTxStatus>(se.Table, txStatus);
                    }
                }

                for (auto &range : std::exchange(se.RemovedRowVersions, { })) {
                    wrap->RemoveRowVersions(range.Lower, range.Upper);
                }
            }

            Pending += Spawn<TRedo>(std::move(Back->RedoLog));
        }

        void StartStageResult(TResult &result) noexcept
        {
            /* Tail of redo log before snapshot may have holes in space of
                db change serial numbers. Thus embedded serials into log may
                leave serial less than the real last change. This last hole
                is healed by db serial embedded to snapshot.


                        snapshot >--.        head   --.
                                     `.                `.
                  .....   ......      :.................:
                  | 1 |   | 7  |      | 13 |  14  |  15 |    < redo log
                  '''''   ''''''      :''''''''''''''''':
                                      :                 :
                      sparse area     :   dense area    :

                                         >----------------> time

             */

            const auto was = Back->DatabaseImpl->Rewind(Back->Serial);

            result.Database = new NTable::TDatabase(Back->DatabaseImpl.Release());

            if (auto logl = Env->Logger()->Log(ELnLev::Info)) {
                auto serial = result.Database->Head(Max<ui32>()).Serial;

                logl
                    << NFmt::Do(*Back) << " result: db change {" << was
                    << " -> " << serial << "} snap on " << Back->Serial;
            }

            if (!Back->Follower) {
                FinalizeLeaderLogics(result, *Back->SteppedCookieAllocatorFactory);
            }
        }

        void FinalizeLeaderLogics(TResult&, TSteppedCookieAllocatorFactory&) noexcept;

    private:
        EStage Next = EStage::Snap;   /* Next stage to execute */
        TLeft Pending;
        TIntrusivePtr<TDependency> Deps;
        TAutoPtr<TBody> Snap;
    };
}
}
}
