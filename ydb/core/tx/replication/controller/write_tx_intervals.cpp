#include "write_tx_intervals.h"

namespace NKikimr::NReplication::NController {

void TWriteTxIntervals::Start(ui64 writeTxId, const TRowVersion& intervalEnd) {
    FirstIntervalEnds[writeTxId] = intervalEnd;
}

bool TWriteTxIntervals::AddInterval(ui64 writeTxId, const TRowVersion& intervalEnd) {
    const auto it = FirstIntervalEnds.find(writeTxId);
    if (it == FirstIntervalEnds.end() || it->second <= intervalEnd) {
        return false;
    }

    it->second = intervalEnd;
    return true;
}

bool TWriteTxIntervals::CanReuse(ui64 writeTxId, const TRowVersion& intervalEnd) const {
    const auto first = FindFirstIntervalEnd(writeTxId);
    return first && *first <= intervalEnd;
}

std::optional<TRowVersion> TWriteTxIntervals::FindFirstIntervalEnd(ui64 writeTxId) const {
    const auto it = FirstIntervalEnds.find(writeTxId);
    if (it == FirstIntervalEnds.end()) {
        return std::nullopt;
    }

    return it->second;
}

void TWriteTxIntervals::Complete(ui64 writeTxId) {
    FirstIntervalEnds.erase(writeTxId);
}

void TWriteTxIntervals::Clear() {
    FirstIntervalEnds.clear();
}

}
