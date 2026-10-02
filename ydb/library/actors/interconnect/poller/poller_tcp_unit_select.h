#pragma once

#include "poller_tcp_unit.h"

namespace NInterconnect {
    class TPollerUnitSelect: public TPollerUnit {
    public:
        TPollerUnitSelect();
        virtual ~TPollerUnitSelect();

    private:
        virtual void ProcessRead() override;
        virtual void ProcessWrite() override;

        template <bool IsWrite>
        void Process();
    };

}
