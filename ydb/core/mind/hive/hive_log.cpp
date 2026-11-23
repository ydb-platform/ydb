#include "hive_impl.h"
#include "hive_log.h"

#include <library/cpp/json/json_writer.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

TLoggedTransaction::TLoggedTransaction(TString user, THive* self)
    : Index(++self->OperationsLogIndex)
    , User(std::move(user))
    , LogPrefix(self->GetLogPrefix())
{}

// JSON object -> structured log fields: a nested object becomes "key.subkey" fields, an array is written as JSON text
static NActors::NStructuredLog::TStructuredMessage JsonToStructuredMessage(const NJson::TJsonValue& json) {
    using namespace NActors::NStructuredLog;
    TStructuredMessage message;
    for (const auto& [key, value] : json.GetMap()) {
        switch (value.GetType()) {
            case NJson::JSON_UNDEFINED:
            case NJson::JSON_NULL:
                break;
            case NJson::JSON_MAP:
                message.AppendSubMessage(TKeyName(key), JsonToStructuredMessage(value));
                break;
            case NJson::JSON_STRING:
                message.AppendValue({TKeyName(key)}, value.GetString());
                break;
            default:
                message.AppendValue({TKeyName(key)}, NJson::WriteJson(value, false));
                break;
        }
    }
    return message;
}

void TLoggedTransaction::WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op, TStringBuf description) {
    TStringStream str;
    NJson::WriteJson(&str, &op);
    TInstant timestamp = TActivationContext::Now();
    db.Table<Schema::OperationsLog>().Key(Index).Update<Schema::OperationsLog::User, Schema::OperationsLog::OperationTimestamp, Schema::OperationsLog::Operation>(User, timestamp.MilliSeconds(), str.Str());
    YDB_LOG_INFO(description,
        {"logPrefix", LogPrefix},
        {"user", GetUserForLog()},
        JsonToStructuredMessage(op));
}

TString GetLogPrefix() {
    return TString();
}

}
}

