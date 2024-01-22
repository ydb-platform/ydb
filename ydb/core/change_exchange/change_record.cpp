#include "change_record.h"

#include <util/stream/str.h>

namespace NKikimr::NChangeExchange {

TString TChangeRecordBase::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TChangeRecordBase::Out(IOutputStream& out) const {
    out << "{"
        << " Order: " << GetOrder()
        << " Group: " << GetGroup()
        << " Step: " << GetStep()
        << " TxId: " << GetTxId()
        << " Kind: " << GetKind()
        << " Source: " << GetSource()
        << " Body: " << GetBody().size() << "b"
    << " }";
}

}
