#include "user_settings_reader.h"
#include "cfg.h"
#include "executor.h"
#include "events.h"

namespace NKikimr::NSQS {

TUserSettingsReader::TUserSettingsReader(const TIntrusivePtr<TTransactionCounters>& transactionCounters)
    : TransactionCounters(transactionCounters)
{
}

TUserSettingsReader::~TUserSettingsReader() {
}

void TUserSettingsReader::Bootstrap() {
    Become(&TUserSettingsReader::StateFunc);
    StartReading();
}

STATEFN(TUserSettingsReader::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvWakeup, HandleWakeup);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS user settings reader actor: " << ev->Type << " (" << ev->ToString() << "), sender: " << ev->Sender);
    }
}

void TUserSettingsReader::HandleWakeup(TEvWakeup::TPtr& ev) {
    Y_UNUSED(ev);
    StartReading();
}

void TUserSettingsReader::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TUserSettingsReader::StartReading() {
    CurrentUser = TString();
    CurrentName = TString();
    OldSettings = std::move(CurrentSettings);
    CurrentSettings = std::make_shared<TUserSettings>();
    if (CompiledQuery) {
        NextRequest();
    } else {
        CompileRequest();
    }
}

void TUserSettingsReader::CompileRequest() {
    TExecutorBuilder(SelfId(), "")
        .Mode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE)
        .QueryId(GET_USER_SETTINGS_ID)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnRequestCompiled(ev); })
        .Counters(TransactionCounters)
        .Start();
}

void TUserSettingsReader::OnRequestCompiled(const TSqsEvents::TEvExecuted::TRecord& record) {
    LOG_SQS_TRACE("Handle compiled user settings query: " << record);
    if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        CompiledQuery = record.GetMiniKQLCompileResults().GetCompiledProgram();
        NextRequest();
    } else {
        LOG_SQS_WARN("Get user setting request compilation failed: " << record);
        CurrentSettings = OldSettings;
        OldSettings = nullptr;
        ScheduleNextUpdate();
    }
}

void TUserSettingsReader::NextRequest() {
    TExecutorBuilder(SelfId(), "")
        .QueryId(GET_USER_SETTINGS_ID)
        .Bin(CompiledQuery)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnUserSettingsRead(ev); })
        .Counters(TransactionCounters)
        .Params()
            .Utf8("FROM_USER", CurrentUser)
            .Utf8("FROM_NAME", CurrentName)
            .Uint64("BATCH_SIZE", Cfg().GetUserSettingsReadBatchSize())
        .ParentBuilder().Start();
}

void TUserSettingsReader::OnUserSettingsRead(const TSqsEvents::TEvExecuted::TRecord& record) {
    LOG_SQS_TRACE("Handle user settings: " << record);
    if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        const TValue settingsVal(val["settings"]);
        const bool truncated = val["truncated"];
        for (size_t i = 0; i < settingsVal.Size(); ++i) {
            const TValue row = settingsVal[i];
            TString user = row["Account"];
            TString name = row["Name"];
            TString value = row["Value"];
            auto& settings = (*CurrentSettings)[user];
            if (!settings) {
                settings = std::make_shared<TSettings>();
            }
            settings->emplace(std::move(name), std::move(value));
        }

        const bool scanCompleted = !truncated || settingsVal.Size() == 0;
        if (scanCompleted) {
            FinishScan();
            ScheduleNextUpdate();
        } else {
            CurrentUser = TString(settingsVal[settingsVal.Size() - 1]["Account"]);
            CurrentName = TString(settingsVal[settingsVal.Size() - 1]["Name"]);
            NextRequest();
        }
    } else {
        LOG_SQS_WARN("Get user setting request failed: " << record);
        CurrentSettings = OldSettings;
        OldSettings = nullptr;
        ScheduleNextUpdate();
    }
}

void TUserSettingsReader::FinishScan() {
    auto oldIt = OldSettings->begin();
    auto newIt = CurrentSettings->begin();
    while (oldIt != OldSettings->end() && newIt != CurrentSettings->end()) {
        if (oldIt->first == newIt->first) {
            CompareUserSettings(oldIt->first, oldIt->second, newIt->second);
            ++oldIt;
            ++newIt;
        } else if (oldIt->first < newIt->first) {
            OnRemoveUserSettings(oldIt->first, oldIt->second);
            ++oldIt;
        } else {
            OnAddUserSettings(newIt->first, newIt->second);
            ++newIt;
        }
    }
    while (oldIt != OldSettings->end()) {
        OnRemoveUserSettings(oldIt->first, oldIt->second);
        ++oldIt;
    }
    while (newIt != CurrentSettings->end()) {
        OnAddUserSettings(newIt->first, newIt->second);
        ++newIt;
    }
}

void TUserSettingsReader::CompareUserSettings(const TString& userName, const TSettingsPtr& oldSettings, const TSettingsPtr& newSettings) {
    std::shared_ptr<std::set<TString>> diff = std::make_shared<std::set<TString>>();
    auto oldIt = oldSettings->begin();
    auto newIt = newSettings->begin();
    while (oldIt != oldSettings->end() && newIt != newSettings->end()) {
        if (oldIt->first == newIt->first) {
            if (oldIt->second != newIt->second) {
                diff->emplace(oldIt->first);
            }
            ++oldIt;
            ++newIt;
        } else if (oldIt->first < newIt->first) {
            diff->emplace(oldIt->first);
            ++oldIt;
        } else {
            diff->emplace(newIt->first);
            ++newIt;
        }
    }
    while (oldIt != oldSettings->end()) {
        diff->emplace(oldIt->first);
        ++oldIt;
    }
    while (newIt != newSettings->end()) {
        diff->emplace(newIt->first);
        ++newIt;
    }
    if (!diff->empty()) {
        Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvUserSettingsChanged(userName, newSettings, std::move(diff)));
    }
}

void TUserSettingsReader::OnRemoveUserSettings(const TString& userName, const TSettingsPtr& oldSettings) {
    auto diff = std::make_shared<std::set<TString>>();
    for (const auto& [name, value] : *oldSettings) {
        diff->insert(name);
    }
    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvUserSettingsChanged(userName, std::make_shared<const std::map<TString, TString>>(), std::move(diff)));
}

void TUserSettingsReader::OnAddUserSettings(const TString& userName, const TSettingsPtr& currentSettings) {
    auto diff = std::make_shared<std::set<TString>>();
    for (const auto& [name, value] : *currentSettings) {
        diff->insert(name);
    }
    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvUserSettingsChanged(userName, currentSettings, std::move(diff)));
}

void TUserSettingsReader::ScheduleNextUpdate() {
    const ui64 period = Cfg().GetUserSettingsUpdateTimeMs();
    const TDuration randomTime = TDuration::MilliSeconds(period + RandomNumber(period / 2));
    Schedule(randomTime, new TEvWakeup());
}

} // namespace NKikimr::NSQS
