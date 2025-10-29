#pragma once

#include "poller_tcp_unit.h"
#include "poller.h"

#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NInterconnect {
    class TPollerThreads: public NActors::IPoller {
    public:
        TPollerThreads(size_t units = 1U, bool useSelect = false);
        ~TPollerThreads();

        void Start();
        void Stop();

        void StartRead(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) override;
        void StartWrite(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) override;

    private:
        TVector<TPollerUnit::TPtr> Units;
    };

}
