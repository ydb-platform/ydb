#pragma once

#include "poller_tcp_unit.h"

namespace NInterconnect {
    class TPollerUnitEpoll: public TPollerUnit {
    public:
        TPollerUnitEpoll();
        virtual ~TPollerUnitEpoll();

    private:
        virtual void StartReadOperation(
            const TIntrusivePtr<TSharedDescriptor>& s,
            TFDDelegate&& operation) override;

        virtual void StartWriteOperation(
            const TIntrusivePtr<TSharedDescriptor>& s,
            TFDDelegate&& operation) override;

        virtual void ProcessRead() override;
        virtual void ProcessWrite() override;

        template <bool Write>
        void Process();

        template <bool Write>
        int GetDescriptor() const;

        const int ReadDescriptor, WriteDescriptor;
        ::sigset_t sigmask;
    };

}
