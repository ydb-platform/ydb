#include "bind_in_range.h"

#include <ydb/library/yql/utils/log/log.h>
#include <util/datetime/base.h>

namespace NYql {

TVector<NBus::TBindResult> BindInRange(TRangeWalker<int>& portWalker) {
    const int cyclesLimit = 3;
    const int rangeSize = portWalker.GetRangeSize();
    const auto cycleDelay = TDuration::Seconds(3);

    for (int cycle = 0; cycle < cyclesLimit; ++cycle) {
        for (int i = 0; i < rangeSize; ++i) {
            try {
                return NBus::BindOnPort(portWalker.MoveToNext(), false).second;
            } catch (const TSystemError&) {
                YQL_LOG(DEBUG) << CurrentExceptionMessage();
            }
        }

        Sleep(cycleDelay);
    }

    ythrow yexception() << "Unable to bind within port range [" << portWalker.GetStart() << ", " << portWalker.GetFinish() << "]";
}
}
