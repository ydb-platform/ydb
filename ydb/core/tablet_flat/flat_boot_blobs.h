#pragma once
#include "defs.h"

#include "flat_boot_iface.h"
#include "flat_sausage_solid.h"
#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TExecutorBootLogic;

namespace NBoot {

    class TLoadBlobs: public NBoot::IStep {
    public:
        using TBlobIds = TVector<TLogoBlobID>;

        static constexpr NBoot::EStep StepKind = NBoot::EStep::Blobs;

        TLoadBlobs() = default;
        TLoadBlobs(IStep *owner, NPageCollection::TLargeGlobId largeGlobId, ui64 cookie);

        ~TLoadBlobs()
        {
            Y_ABORT_UNLESS(!RefCount(), "TLoadBlobs is still referenced somewhere");
        }

        void Start() noexcept override { }

        void Feed(TLogoBlobID blobId, TString body)
        {
            if (State.Apply(blobId, std::move(body)) && Env)
                Env->Finish(this);
        }

        const TVector<TLogoBlobID>& Blobs() const
        {
            return State.GetBlobs();
        }

        TString Plain()
        {
            return State.ExtractString();
        }

        TSharedData PlainData()
        {
            return State.ExtractSharedData();
        }

    public:
        const ui64 Cookie = Max<ui64>();
        NPageCollection::TLargeGlobId LargeGlobId; /* new method of holding blobs range */

    private:
        NPageCollection::TLargeGlobIdRestoreState State;
    };
}
}
}
