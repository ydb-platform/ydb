#include "test_events_writer.h"

namespace NKikimr::NSQS {

void TTestEventsWriter::Write(const TString& data) {
    with_lock(Lock) {
        Messages.emplace_back(data);
    }
}

TVector<TString> TTestEventsWriter::GetMessages() {
    TVector<TString> result;
    with_lock(Lock) {
        result = std::move(Messages);
        Messages.clear();
    }
    return result;
}

} //namespace NKikimr::NSQS
