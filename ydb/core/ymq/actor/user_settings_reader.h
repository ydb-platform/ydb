#pragma once
#include "defs.h"
#include "events.h"
#include "log.h"
#include "serviceid.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSQS {

class TUserSettingsReader : public TActorBootstrapped<TUserSettingsReader> {
public:
    explicit TUserSettingsReader(const TIntrusivePtr<TTransactionCounters>& transactionCounters);
    ~TUserSettingsReader();

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_USER_SETTINGS_READER_ACTOR;
    }

private:
    using TSettings = std::map<TString, TString>; // name -> value.
    using TSettingsPtr = std::shared_ptr<TSettings>;
    using TUserSettings = std::map<TString, TSettingsPtr>; // user -> settings.
    using TUserSettingsPtr = std::shared_ptr<TUserSettings>;

    STATEFN(StateFunc);
    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

    void StartReading();
    void NextRequest();
    void OnUserSettingsRead(const TSqsEvents::TEvExecuted::TRecord& record);
    void CompileRequest();
    void OnRequestCompiled(const TSqsEvents::TEvExecuted::TRecord& record);
    void ScheduleNextUpdate();
    void FinishScan();
    void CompareUserSettings(const TString& userName, const TSettingsPtr& oldSettings, const TSettingsPtr& newSettings);
    void OnRemoveUserSettings(const TString& userName, const TSettingsPtr& oldSettings);
    void OnAddUserSettings(const TString& userName, const TSettingsPtr& currentSettings);

private:
    TString CompiledQuery;

    TIntrusivePtr<TTransactionCounters> TransactionCounters;
    TString CurrentUser;
    TString CurrentName;

    TUserSettingsPtr OldSettings;
    TUserSettingsPtr CurrentSettings = std::make_shared<TUserSettings>();
};

} // namespace NKikimr::NSQS
