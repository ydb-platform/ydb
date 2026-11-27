#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <library/cpp/json/json_value.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

namespace NKikimr {
namespace NHive {

class THive;

class TLoggedTransaction {
    ui64 Index;
    TString User;
    TString LogPrefix;

protected:
    TLoggedTransaction(TString user, THive* self);

    void WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op, TStringBuf description);

    TString GetUserForLog() const {
        return User.empty() ? TString("anonymous") : User;
    }
};

TString GetLogPrefix();

template <typename T>
class TTransactionBase : public NKikimr::NTabletFlatExecutor::TTransactionBase<T> {
protected:
    using TSelf = T;
    using TBase = TTransactionBase<T>;

public:
    TTransactionBase(T* self)
        : NKikimr::NTabletFlatExecutor::TTransactionBase<T>(self)
    {}

    TString GetLogPrefix() const {
        return NKikimr::NTabletFlatExecutor::TTransactionBase<T>::Self->GetLogPrefix();
    }
};

}
}

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << stream)
#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { BLOG_ERROR("Failed condition \"" << #cond << "\" " << stream); }

