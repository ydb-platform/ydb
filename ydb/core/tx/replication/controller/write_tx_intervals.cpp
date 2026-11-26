#include "write_tx_intervals.h"

namespace NKikimr::NReplication::NController {

void TWriteTxIntervals::Start(ui64 writeTxId, const TRowVersion& intervalEnd) {
    FirstIntervalEnds[writeTxId] = intervalEnd;
}

void TWriteTxIntervals::AddInterval(ui64 writeTxId, const TRowVersion& intervalEnd) {
    const auto it = FirstIntervalEnds.find(writeTxId);
    if (it != FirstIntervalEnds.end() && intervalEnd < it->second) {
        it->second = intervalEnd;
    }
}

bool TWriteTxIntervals::CanReuse(ui64 writeTxId, const TRowVersion& intervalEnd) const {
    const auto it = FirstIntervalEnds.find(writeTxId);
    return it == FirstIntervalEnds.end() || it->second <= intervalEnd;
}

void TWriteTxIntervals::Complete(ui64 writeTxId) {
    FirstIntervalEnds.erase(writeTxId);
}

void TWriteTxIntervals::Clear() {
    FirstIntervalEnds.clear();
}

}
