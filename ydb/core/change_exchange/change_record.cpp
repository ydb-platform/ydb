#include "change_record.h"

#include <ydb/library/aclib/user_context.h>

#include <util/stream/str.h>

namespace NKikimr::NChangeExchange {

TChangeRecordBase::~TChangeRecordBase() {}

TIntrusivePtr<NACLib::TUserContext> TChangeRecordBase::GetUserCtx() const {
    return UserCtx;
}

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
