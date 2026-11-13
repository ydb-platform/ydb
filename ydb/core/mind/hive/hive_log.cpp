#include "hive_impl.h"
#include "hive_log.h"

#include <library/cpp/json/json_writer.h>

namespace NKikimr {
namespace NHive {

TLoggedTransaction::TLoggedTransaction(TString user, THive* self)
    : Index(++self->OperationsLogIndex)
    , User(std::move(user))
    , LogPrefix(self->GetLogPrefix())
{}

void TLoggedTransaction::WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op, TStringBuf description) {
    TStringStream str;
    NJson::WriteJson(&str, &op);
    TInstant timestamp = TActivationContext::Now();
    db.Table<Schema::OperationsLog>().Key(Index).Update<Schema::OperationsLog::User, Schema::OperationsLog::OperationTimestamp, Schema::OperationsLog::Operation>(User, timestamp.MilliSeconds(), str.Str());
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::HIVE, LogPrefix << description
        << " user=" << GetUserForLog()
        << " operation=" << str.Str());
}

TString GetLogPrefix() {
    return TString();
}

}
}

