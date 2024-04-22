#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_executor_borrowlogic.h"

#include <ydb/core/util/pb.h>
#include <library/cpp/blockcodecs/codecs.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TLoans final: public NBoot::IStep {
    public:
        using TQueue = TDeque<TBody>;

        TLoans(IStep *owner, TQueue queue)
            : IStep(owner, NBoot::EStep::Loans)
            , Codec(NBlockCodecs::Codec("lz4fast"))
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
                Apply(head->LargeGlobId.Lead, Codec->Decode(head->Body));

                ++Skip, Queue.pop_front();
            }

            Y_ABORT_UNLESS(Queue || !Pending, "TLoans boot actor has lost entries");

            if (!Queue) {
                Env->Finish(this);
            }
        }

        void Apply(const TLogoBlobID &label, TArrayRef<const char> body) noexcept
        {
            TProtoBox<NKikimrExecutorFlat::TBorrowedPart> proto(body);

            Logic->Result().Loans->RestoreBorrowedInfo(label, proto);

            auto& historyCutter = Logic->Result().GcLogic->HistoryCutter;
            historyCutter.SeenBlob(LogoBlobIDFromLogoBlobID(proto.GetMetaId()));
            for (const auto& x : proto.GetBorrowKeepList()) {
                historyCutter.SeenBlob(LogoBlobIDFromLogoBlobID(x));
            }
            for (const auto& x : proto.GetLoanKeepList()) {
                historyCutter.SeenBlob(LogoBlobIDFromLogoBlobID(x));
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
