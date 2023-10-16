#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_bio_events.h"

#include <ydb/core/util/pb.h>
#include <library/cpp/blockcodecs/codecs.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TGCLog final: public NBoot::IStep {
    public:
        using TQueue = TDeque<TBody>;

        TGCLog(IStep *owner, TQueue queue)
            : IStep(owner, NBoot::EStep::GCELog)
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
                auto gen = head->LargeGlobId.Lead.Generation();
                auto step = head->LargeGlobId.Lead.Step();

                Apply(gen, step, Codec->Decode(head->Body));

                ++Skip, Queue.pop_front();
            }

            Y_ABORT_UNLESS(Queue || !Pending, "TGCLog boot actor has lost entries");

            if (!Queue) {
                Env->Finish(this);
            }
        }

        void Apply(ui32 gen, ui32 step, TArrayRef<const char> body) noexcept
        {
            TProtoBox<NKikimrExecutorFlat::TExternalGcEntry> proto(body);

            TGCLogEntry gcx(TGCTime(gen, step));
            LogoBlobIDVectorFromLogoBlobIDRepeated(&gcx.Delta.Created, proto.GetGcDiscovered());
            LogoBlobIDVectorFromLogoBlobIDRepeated(&gcx.Delta.Deleted, proto.GetGcLeft());
            Back->Waste->Account(gcx.Delta);
            Logic->Result().GcLogic->ApplyLogEntry(gcx);
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
