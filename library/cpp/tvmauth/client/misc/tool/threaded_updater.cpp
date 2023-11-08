#include "threaded_updater.h"

#include <library/cpp/tvmauth/client/misc/checker.h>
#include <library/cpp/tvmauth/client/misc/default_uid_checker.h>
#include <library/cpp/tvmauth/client/misc/getter.h>
#include <library/cpp/tvmauth/client/misc/src_checker.h>
#include <library/cpp/tvmauth/client/misc/utils.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NTvmAuth::NTvmTool {
    TAsyncUpdaterPtr TThreadedUpdater::Create(const TClientSettings& settings, TLoggerPtr logger) {
        Y_ENSURE_EX(logger, TNonRetriableException() << "Logger is required");
        THolder<TThreadedUpdater> p(new TThreadedUpdater(settings, std::move(logger)));

        try {
            p->Init(settings);
        } catch (const TRetriableException& e) {
            if (!settings.EnableLazyInitialization) {
                throw e;
            }
        }

        p->StartWorker();
        return p.Release();
    }

    TThreadedUpdater::~TThreadedUpdater() {
        StopWorker(); // Required here to avoid using of deleted members
    }

    TClientStatus TThreadedUpdater::GetStatus() const {
        const TClientStatus::ECode state = GetState();
        return TClientStatus(state, GetLastError(state == TClientStatus::Ok));
    }

    TString TThreadedUpdater::GetServiceTicketFor(const TClientSettings::TAlias& dst) const {
        Y_ENSURE_EX(IsInited(), TNotInitializedException() << "Client is not initialized");

        if (!MetaInfo_.GetConfig()->AreTicketsRequired()) {
            throw TBrokenTvmClientSettings() << "Need to enable ServiceTickets fetching";
        }
        auto c = GetCachedServiceTickets();
        return TServiceTicketGetter::GetTicket(dst, c);
    }

    TString TThreadedUpdater::GetServiceTicketFor(const TTvmId dst) const {
        Y_ENSURE_EX(IsInited(), TNotInitializedException() << "Client is not initialized");

        if (!MetaInfo_.GetConfig()->AreTicketsRequired()) {
            throw TBrokenTvmClientSettings() << "Need to enable ServiceTickets fetching";
        }
        auto c = GetCachedServiceTickets();
        return TServiceTicketGetter::GetTicket(dst, c);
    }

    TCheckedServiceTicket TThreadedUpdater::CheckServiceTicket(TStringBuf ticket, const TServiceContext::TCheckFlags& flags) const {
        Y_ENSURE_EX(IsInited(), TNotInitializedException() << "Client is not initialized");

        TServiceContextPtr c = GetCachedServiceContext();
        TCheckedServiceTicket res = TServiceTicketChecker::Check(ticket, c, flags);
        if (Settings_.ShouldCheckSrc && RolesFetcher_ && res) {
            NRoles::TRolesPtr roles = GetRoles();
            return TSrcChecker::Check(std::move(res), roles);
        }
        return res;
    }

    TCheckedUserTicket TThreadedUpdater::CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overridenEnv) const {
        Y_ENSURE_EX(IsInited(), TNotInitializedException() << "Client is not initialized");

        auto c = GetCachedUserContext(overridenEnv);
        TCheckedUserTicket res = TUserTicketChecker::Check(ticket, c);
        if (Settings_.ShouldCheckDefaultUid && RolesFetcher_ && res && res.GetEnv() == EBlackboxEnv::ProdYateam) {
            NRoles::TRolesPtr roles = GetRoles();
            return TDefaultUidChecker::Check(std::move(res), roles);
        }
        return res;
    }

    NRoles::TRolesPtr TThreadedUpdater::GetRoles() const {
        Y_ENSURE_EX(IsInited(), TNotInitializedException() << "Client is not initialized");

        Y_ENSURE_EX(RolesFetcher_,
                    TBrokenTvmClientSettings() << "Roles were not configured in settings");

        return RolesFetcher_->GetCurrentRoles();
    }

    TClientStatus::ECode TThreadedUpdater::GetState() const {
        const TInstant now = TInstant::Now();

        if (!IsInited()) {
            return TClientStatus::NotInitialized;
        }

        const TMetaInfo::TConfigPtr config = MetaInfo_.GetConfig();

        if ((config->AreTicketsRequired() && AreServiceTicketsInvalid(now)) || ArePublicKeysInvalid(now)) {
            return TClientStatus::Error;
        }

        if (config->AreTicketsRequired()) {
            if (!GetCachedServiceTickets() || config->DstAliases.size() > GetCachedServiceTickets()->TicketsByAlias.size()) {
                return TClientStatus::Error;
            }
        }

        const TDuration st = now - GetUpdateTimeOfServiceTickets();
        const TDuration pk = now - GetUpdateTimeOfPublicKeys();

        if ((config->AreTicketsRequired() && st > ServiceTicketsDurations_.Expiring) || pk > PublicKeysDurations_.Expiring) {
            return TClientStatus::Warning;
        }

        if (RolesFetcher_ && RolesFetcher_->ShouldWarn(now - GetUpdateTimeOfRoles())) {
            return TClientStatus::Warning;
        }

        if (IsConfigWarnTime()) {
            return TClientStatus::Warning;
        }

        return TClientStatus::Ok;
    }

    TThreadedUpdater::TThreadedUpdater(const TClientSettings& settings, TLoggerPtr logger)
        : TThreadedUpdaterBase(TDuration::Seconds(5), logger, settings.GetHostname(), settings.GetPort(), settings.GetSocketTimeout(), settings.GetConnectTimeout())
        , MetaInfo_(logger)
        , ConfigWarnDelay_(TDuration::Seconds(30))
        , Settings_(settings)
    {
        ServiceTicketsDurations_.RefreshPeriod = TDuration::Minutes(10);
        PublicKeysDurations_.RefreshPeriod = TDuration::Minutes(10);
    }

    void TThreadedUpdater::Init(const TClientSettings& settings) {
        const TMetaInfo::TConfigPtr config = MetaInfo_.Init(GetClient(), settings);
        LastVisitForConfig_ = TInstant::Now();

        SetBbEnv(config->BbEnv, settings.GetOverridedBlackboxEnv());
        if (settings.GetOverridedBlackboxEnv()) {
            LogInfo(TStringBuilder()
                    << "Meta: override blackbox env: " << config->BbEnv
                    << "->" << *settings.GetOverridedBlackboxEnv());
        }

        if (config->IdmSlug) {
            RolesFetcher_ = std::make_unique<TRolesFetcher>(
                TRolesFetcherSettings{
                    .SelfAlias = settings.GetSelfAlias(),
                },
                Logger_);
        }

        ui8 tries = 3;
        do {
            UpdateState();
        } while (!IsEverythingOk(*config) && --tries > 0);

        if (!IsEverythingOk(*config)) {
            ThrowLastError();
        }
        SetInited(true);
    }

    void TThreadedUpdater::UpdateState() {
        bool wasUpdated = false;
        try {
            wasUpdated = MetaInfo_.TryUpdateConfig(GetClient());
            LastVisitForConfig_ = TInstant::Now();
            ClearError(EScope::TvmtoolConfig);
        } catch (const std::exception& e) {
            ProcessError(EType::Retriable, EScope::TvmtoolConfig, e.what());
            LogWarning(TStringBuilder() << "Error while fetching of tvmtool config: " << e.what());
        }
        if (IsConfigWarnTime()) {
            LogError(TStringBuilder() << "Tvmtool config have not been refreshed for too long period");
        }

        TMetaInfo::TConfigPtr config = MetaInfo_.GetConfig();

        if (wasUpdated || IsTimeToUpdateServiceTickets(*config, LastVisitForServiceTickets_)) {
            try {
                const TInstant updateTime = UpdateServiceTickets(*config);
                SetUpdateTimeOfServiceTickets(updateTime);
                LastVisitForServiceTickets_ = TInstant::Now();

                if (AreServiceTicketsOk(*config)) {
                    ClearError(EScope::ServiceTickets);
                }
                LogDebug(TStringBuilder() << "Tickets fetched from tvmtool: " << updateTime);
            } catch (const std::exception& e) {
                ProcessError(EType::Retriable, EScope::ServiceTickets, e.what());
                LogWarning(TStringBuilder() << "Error while fetching of tickets: " << e.what());
            }

            if (TInstant::Now() - GetUpdateTimeOfServiceTickets() > ServiceTicketsDurations_.Expiring) {
                LogError("Service tickets have not been refreshed for too long period");
            }
        }

        if (wasUpdated || IsTimeToUpdatePublicKeys(LastVisitForPublicKeys_)) {
            try {
                const TInstant updateTime = UpdateKeys(*config);
                SetUpdateTimeOfPublicKeys(updateTime);
                LastVisitForPublicKeys_ = TInstant::Now();

                if (ArePublicKeysOk()) {
                    ClearError(EScope::PublicKeys);
                }
                LogDebug(TStringBuilder() << "Public keys fetched from tvmtool: " << updateTime);
            } catch (const std::exception& e) {
                ProcessError(EType::Retriable, EScope::PublicKeys, e.what());
                LogWarning(TStringBuilder() << "Error while fetching of public keys: " << e.what());
            }

            if (TInstant::Now() - GetUpdateTimeOfPublicKeys() > PublicKeysDurations_.Expiring) {
                LogError("Public keys have not been refreshed for too long period");
            }
        }

        if (RolesFetcher_ && (wasUpdated || RolesFetcher_->IsTimeToUpdate(TInstant::Now() - GetUpdateTimeOfRoles()))) {
            try {
                RolesFetcher_->Update(RolesFetcher_->FetchActualRoles(MetaInfo_.GetAuthHeader(), GetClient()));
                SetUpdateTimeOfRoles(TInstant::Now());

                if (RolesFetcher_->AreRolesOk()) {
                    ClearError(EScope::Roles);
                }
            } catch (const std::exception& e) {
                ProcessError(EType::Retriable, EScope::Roles, e.what());
                LogWarning(TStringBuilder() << "Failed to update roles: " << e.what());
            }

            if (RolesFetcher_->ShouldWarn(TInstant::Now() - GetUpdateTimeOfRoles())) {
                LogError("Roles have not been refreshed for too long period");
            }
        }
    }

    TInstant TThreadedUpdater::UpdateServiceTickets(const TMetaInfo::TConfig& config) {
        const std::pair<TString, TInstant> tickets = FetchServiceTickets(config);

        if (TInstant::Now() - tickets.second >= ServiceTicketsDurations_.Invalid) {
            throw yexception() << "Service tickets are too old: " << tickets.second;
        }

        TPairTicketsErrors p = ParseFetchTicketsResponse(tickets.first, config.DstAliases);
        SetServiceTickets(MakeIntrusiveConst<TServiceTickets>(std::move(p.Tickets),
                                                              std::move(p.Errors),
                                                              config.DstAliases));
        return tickets.second;
    }

    std::pair<TString, TInstant> TThreadedUpdater::FetchServiceTickets(const TMetaInfo::TConfig& config) const {
        TStringStream s;
        THttpHeaders headers;

        const TString request = TMetaInfo::GetRequestForTickets(config);
        auto code = GetClient().DoGet(request, &s, MetaInfo_.GetAuthHeader(), &headers);
        Y_ENSURE(code == 200, ProcessHttpError(EScope::ServiceTickets, request, code, s.Str()));

        return {s.Str(), GetBirthTimeFromResponse(headers, "tickets")};
    }

    static THashSet<TTvmId> GetAllTvmIds(const TMetaInfo::TDstAliases& dsts) {
        THashSet<TTvmId> res;
        res.reserve(dsts.size());

        for (const auto& pair : dsts) {
            res.insert(pair.second);
        }

        return res;
    }

    TAsyncUpdaterBase::TPairTicketsErrors TThreadedUpdater::ParseFetchTicketsResponse(const TString& resp,
                                                                                      const TMetaInfo::TDstAliases& dsts) const {
        const THashSet<TTvmId> allTvmIds = GetAllTvmIds(dsts);

        TServiceTickets::TMapIdStr tickets;
        TServiceTickets::TMapIdStr errors;

        auto procErr = [this](const TString& msg) {
            ProcessError(EType::NonRetriable, EScope::ServiceTickets, msg);
            LogError(msg);
        };

        NJson::TJsonValue doc;
        Y_ENSURE(NJson::ReadJsonTree(resp, &doc), "Invalid json from tvmtool: " << resp);

        for (const auto& pair : doc.GetMap()) {
            NJson::TJsonValue tvmId;
            unsigned long long tvmIdNum = 0;

            if (!pair.second.GetValue("tvm_id", &tvmId) ||
                !tvmId.GetUInteger(&tvmIdNum)) {
                procErr(TStringBuilder()
                        << "Failed to get 'tvm_id' from key, should never happend '"
                        << pair.first << "': " << resp);
                continue;
            }

            if (!allTvmIds.contains(tvmIdNum)) {
                continue;
            }

            NJson::TJsonValue val;
            if (!pair.second.GetValue("ticket", &val)) {
                TString err;
                if (pair.second.GetValue("error", &val)) {
                    err = val.GetString();
                } else {
                    err = "Failed to get 'ticket' and 'error', should never happend: " + pair.first;
                }

                procErr(TStringBuilder()
                        << "Failed to get ServiceTicket for " << pair.first
                        << " (" << tvmIdNum << "): " << err);

                errors.insert({tvmIdNum, std::move(err)});
                continue;
            }

            tickets.insert({tvmIdNum, val.GetString()});
        }

        // This work-around is required because of bug in old verions of tvmtool: PASSP-24829
        for (const auto& pair : dsts) {
            if (!tickets.contains(pair.second) && !errors.contains(pair.second)) {
                TString err = "Missing tvm_id in response, should never happend: " + pair.first;

                procErr(TStringBuilder()
                        << "Failed to get ServiceTicket for " << pair.first
                        << " (" << pair.second << "): " << err);

                errors.emplace(pair.second, std::move(err));
            }
        }

        return {std::move(tickets), std::move(errors)};
    }

    TInstant TThreadedUpdater::UpdateKeys(const TMetaInfo::TConfig& config) {
        const std::pair<TString, TInstant> keys = FetchPublicKeys();

        if (TInstant::Now() - keys.second >= PublicKeysDurations_.Invalid) {
            throw yexception() << "Public keys are too old: " << keys.second;
        }

        SetServiceContext(MakeIntrusiveConst<TServiceContext>(
            TServiceContext::CheckingFactory(config.SelfTvmId, keys.first)));
        SetUserContext(keys.first);

        return keys.second;
    }

    std::pair<TString, TInstant> TThreadedUpdater::FetchPublicKeys() const {
        TStringStream s;
        THttpHeaders headers;

        auto code = GetClient().DoGet("/tvm/keys", &s, MetaInfo_.GetAuthHeader(), &headers);
        Y_ENSURE(code == 200, ProcessHttpError(EScope::PublicKeys, "/tvm/keys", code, s.Str()));

        return {s.Str(), GetBirthTimeFromResponse(headers, "public keys")};
    }

    TInstant TThreadedUpdater::GetBirthTimeFromResponse(const THttpHeaders& headers, TStringBuf errMsg) {
        auto it = std::find_if(headers.begin(),
                               headers.end(),
                               [](const THttpInputHeader& h) {
                                   return AsciiEqualsIgnoreCase(h.Name(), "X-Ya-Tvmtool-Data-Birthtime");
                               });
        Y_ENSURE(it != headers.end(), "Failed to fetch bithtime of " << errMsg << " from tvmtool");

        ui64 time = 0;
        Y_ENSURE(TryIntFromString<10>(it->Value(), time),
                 "Bithtime of " << errMsg << " from tvmtool must be unixtime. Got: " << it->Value());

        return TInstant::Seconds(time);
    }

    bool TThreadedUpdater::IsTimeToUpdateServiceTickets(const TMetaInfo::TConfig& config,
                                                        TInstant lastUpdate) const {
        return config.AreTicketsRequired() &&
               TInstant::Now() - lastUpdate > ServiceTicketsDurations_.RefreshPeriod;
    }

    bool TThreadedUpdater::IsTimeToUpdatePublicKeys(TInstant lastUpdate) const {
        return TInstant::Now() - lastUpdate > PublicKeysDurations_.RefreshPeriod;
    }

    bool TThreadedUpdater::IsEverythingOk(const TMetaInfo::TConfig& config) const {
        if (RolesFetcher_ && !RolesFetcher_->AreRolesOk()) {
            return false;
        }
        return AreServiceTicketsOk(config) && ArePublicKeysOk();
    }

    bool TThreadedUpdater::AreServiceTicketsOk(const TMetaInfo::TConfig& config) const {
        return AreServiceTicketsOk(config.DstAliases.size());
    }

    bool TThreadedUpdater::AreServiceTicketsOk(size_t requiredCount) const {
        if (requiredCount == 0) {
            return true;
        }

        auto c = GetCachedServiceTickets();
        return c && c->TicketsByAlias.size() == requiredCount;
    }

    bool TThreadedUpdater::ArePublicKeysOk() const {
        return GetCachedServiceContext() && GetCachedUserContext();
    }

    bool TThreadedUpdater::IsConfigWarnTime() const {
        return LastVisitForConfig_ + ConfigWarnDelay_ < TInstant::Now();
    }

    void TThreadedUpdater::Worker() {
        if (IsInited()) {
            UpdateState();
        } else {
            try {
                Init(Settings_);
            } catch (const TRetriableException& e) {
                // Still not initialized
            } catch (const std::exception& e) {
                // Can't retry, so we mark client as initialized and now GetStatus() will return TClientStatus::Error
                SetInited(true);
            }
        }
    }
}
