#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_bio_events.h"
#include "flat_dbase_naked.h"
#include "flat_executor_txloglogic.h"
#include "util_fmt_flat.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TRedo final: public NBoot::IStep {
    public:
        using TQueue = TDeque<TLogEntry>;

        TRedo() = delete;

        TRedo(IStep *owner, TQueue queue)
            : IStep(owner, NBoot::EStep::Redo)
            , Codec(NBlockCodecs::Codec("lz4fast"))
            , Queue(std::move(queue))
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() noexcept override
        {
            if (auto logl = Env->Logger()->Log(ELnLev::Info)) {
                const auto last = Queue ? Queue.back().Stamp + 1 : 0;

                logl
                    << NFmt::Do(*Back) << " redo log has " << Queue.size()
                    << " records, last before " << NFmt::TStamp(last);
            }

            for (auto slot: xrange(Queue.size()))
                if (const auto &largeGlobId = Queue.at(slot).LargeGlobId)
                    Pending += Spawn<TLoadBlobs>(largeGlobId, slot);

            Flush();
        }

        void HandleStep(TIntrusivePtr<IStep> step) noexcept override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(Pending);

            if (load->Cookie < Skip || load->Cookie - Skip >= Queue.size())
                Y_ABORT("Got TLoadBlobs result cookie out of queue range");

            Queue.at(load->Cookie - Skip).Body = load->Plain();

            if (auto logl = Env->Logger()->Log(ELnLev::Crit)) {
                logl
                    << NFmt::Do(*Back) << "redo log applying " << load->Blobs();
            }
            Flush();
        }

    private:
        void Flush() noexcept
        {
            for (TLogEntry *head = nullptr; Queue && *(head = &Queue[0]); ) {
                auto index = TCookie(head->LargeGlobId.Lead.Cookie()).Index();

                if (head->LargeGlobId && index != TCookie::EIdx::RedoLz4) {
                    Apply(head->Stamp, *head, head->Body);
                } else {
                    Apply(head->Stamp, *head, Codec->Decode(head->Body));
                }

                ++Skip, Queue.pop_front();
            }

            Y_ABORT_UNLESS(Queue || !Pending, "TRedo boot actor has lost entries");

            if (!Queue) {
                Env->Finish(this);
            }
        }

        void Apply(ui64 stamp, const TLogEntry &entry, TString redo) noexcept
        {
            const auto begin_ = Back->DatabaseImpl->Serial();

            Back->DatabaseImpl->Switch(stamp).ApplyRedo(redo).GrabAnnex();

            const auto affects = Back->DatabaseImpl->GrabAffects();

            if (auto logl = Env->Logger()->Log(ELnLev::Crit)) {
                logl
                    << NFmt::Do(*Back) << " redo log " << NFmt::TStamp(stamp)
                    << " serial " << begin_ << " -> [" << Back->DatabaseImpl->First_
                    << ", " << Back->DatabaseImpl->Serial() << "] applied"
                    << ", affects " << NFmt::Arr(affects);
            }

            if (affects) {
                if (auto *logic = Back->Redo.Get()) {
                    if (entry.LargeGlobId) {
                        logic->Push(stamp, affects, entry.LargeGlobId);
                    } else { /* has no TLargeGlobId, thus was embedded to entry */
                        logic->Push(stamp, affects, std::move(entry.Body));
                    }
                }

                if (auto *compaction = Logic->Result().Comp.Get()) {
                    for (ui32 table: affects) {
                        compaction->Snapshots[table].InMemSteps++;
                    }
                }
            }
        }

    private:
        const NBlockCodecs::ICodec *Codec = nullptr;
        TLeft Pending;
        ui64 Skip = 0;
        TQueue Queue;
    };
}
}
}
