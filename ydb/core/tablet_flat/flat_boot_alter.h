#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_dbase_apply.h"
#include "logic_alter_main.h"

#include <ydb/core/util/pb.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TAlter final: public NBoot::IStep {
    public:
        using TQueue = TDeque<TBody>;

        TAlter(IStep *owner, TQueue queue)
            : IStep(owner, NBoot::EStep::Alter)
            , Queue(std::move(queue))
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() noexcept override
        {
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

            Flush();
        }

    private:
        void Flush() noexcept
        {
            for (TBody *head = nullptr; Queue && *(head = &Queue[0]); ) {
                Apply(head->LargeGlobId, head->Body);

                ++Skip, Queue.pop_front();
            }

            Y_ABORT_UNLESS(Queue || !Pending, "TAlter boot actor has lost entries");

            if (!Queue) {
                Env->Finish(this);
            }
        }

        void Apply(const NPageCollection::TLargeGlobId &largeGlobId, TArrayRef<const char> body) noexcept
        {
            bool rewrite = false;
            if (body) {
                TProtoBox<NTable::TSchemeChanges> alter(body);

                NTable::TSchemeModifier apply(*Back->Scheme);

                auto changed = apply.Apply(alter);
                rewrite = alter.GetRewrite();

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " alter log "
                        << NFmt::TStamp(NTable::TTxStamp(largeGlobId.Lead).Raw)
                        << ", " << (changed ? "update" : "noop")
                        << " affects " << NFmt::Arr(apply.Affects)
                        << ", is " << (rewrite ? "" : "not a ") << "rewrite";
                }
            }

            if (auto *logic = Logic->Result().Alter.Get()) {
                if (rewrite) {
                    logic->Clear();
                }
                logic->RestoreLog(largeGlobId);
            }
        }

    private:
        TLeft Pending;
        ui64 Skip = 0;
        TQueue Queue;
    };
}
}
}
