#include "threaded_updater.h"

#include <library/cpp/tvmauth/client/misc/disk_cache.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/retry_settings/v1/settings.pb.h>

#include <library/cpp/tvmauth/client/logger.h>

#include <library/cpp/json/json_reader.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/thread.h>

namespace NTvmAuth::NTvmApi {
    static TString CreatePublicKeysUrl(const TClientSettings& settings,
                                       const NUtils::TProcInfo& procInfo) {
        TStringStream s;
        s << "/2/keys";
        s << "?";
        procInfo.AddToRequest(s);

        s << "&get_retry_settings=yes";

        if (settings.GetSelfTvmId() != 0) {
            s << "&src=" << settings.GetSelfTvmId();
        }

        if (settings.IsUserTicketCheckingRequired()) {
            s << "&env=" << static_cast<int>(settings.GetEnvForUserTickets());
        }

        return s.Str();
    }

    TAsyncUpdaterPtr TThreadedUpdater::Create(const TClientSettings& settings, TLoggerPtr logger) {
        Y_ENSURE_EX(logger, TNonRetriableException() << "Logger is required");
        THolder<TThreadedUpdater> p(new TThreadedUpdater(settings, std::move(logger)));
        p->Init();
        p->StartWorker();
        return p.Release();
    }

    TThreadedUpdater::~TThreadedUpdater() {
        ExpBackoff_.SetEnabled(false);
        ExpBackoff_.Interrupt();
        StopWorker(); // Required here to avoid using of deleted members
    }

    TClientStatus TThreadedUpdater::GetStatus() const {
        const TClientStatus::ECode state = GetState();
        return TClientStatus(state, GetLastError(state == TClientStatus::Ok || state == TClientStatus::IncompleteTicketsSet)); 
    }

    NRoles::TRolesPtr TThreadedUpdater::GetRoles() const {
        Y_ENSURE_EX(RolesFetcher_,
                    TBrokenTvmClientSettings() << "Roles were not configured in settings");
        return RolesFetcher_->GetCurrentRoles();
    }

    TClientStatus::ECode TThreadedUpdater::GetState() const {
        const TInstant now = TInstant::Now();

        if (Settings_.IsServiceTicketFetchingRequired()) {
            if (AreServiceTicketsInvalid(now)) {
                return TClientStatus::Error;
            }
            auto tickets = GetCachedServiceTickets();
            if (!tickets) {
                return TClientStatus::Error;
            }
            if (tickets->TicketsById.size() < Destinations_.size()) {
                if (Settings_.IsIncompleteTicketsSetAnError) {
                    return TClientStatus::Error; 
                } else { 
                    return TClientStatus::IncompleteTicketsSet; 
                } 
            }
        }
        if ((Settings_.IsServiceTicketCheckingRequired() || Settings_.IsUserTicketCheckingRequired()) && ArePublicKeysInvalid(now)) {
            return TClientStatus::Error;
        }

        const TDuration sincePublicKeysUpdate = now - GetUpdateTimeOfPublicKeys();
        const TDuration sinceServiceTicketsUpdate = now - GetUpdateTimeOfServiceTickets();
        const TDuration sinceRolesUpdate = now - GetUpdateTimeOfRoles();

        if (Settings_.IsServiceTicketFetchingRequired() && sinceServiceTicketsUpdate > ServiceTicketsDurations_.Expiring) {
            return TClientStatus::Warning;
        }
        if ((Settings_.IsServiceTicketCheckingRequired() || Settings_.IsUserTicketCheckingRequired()) &&
            sincePublicKeysUpdate > PublicKeysDurations_.Expiring)
        {
            return TClientStatus::Warning;
        }
        if (RolesFetcher_ && TRolesFetcher::ShouldWarn(RetrySettings_, sinceRolesUpdate)) {
            return TClientStatus::Warning;
        }

        return TClientStatus::Ok;
    }

    TThreadedUpdater::TThreadedUpdater(const TClientSettings& settings, TLoggerPtr logger)
        : TThreadedUpdaterBase(
              TRetrySettings{}.WorkerAwakingPeriod,
              std::move(logger),
              settings.GetTvmHost(),
              settings.GetTvmPort(),
              settings.TvmSocketTimeout,
              settings.TvmConnectTimeout)
        , ExpBackoff_(RetrySettings_.BackoffSettings)
        , Settings_(settings.CloneNormalized())
        , ProcInfo_(NUtils::TProcInfo::Create(Settings_.GetLibVersionPrefix()))
        , PublicKeysUrl_(CreatePublicKeysUrl(Settings_, ProcInfo_))
        , DstAliases_(MakeAliasMap(Settings_))
        , Headers_({{"Content-Type", "application/x-www-form-urlencoded"}})
        , Random_(TInstant::Now().MicroSeconds())
    {
        if (Settings_.IsServiceTicketFetchingRequired()) {
            SigningContext_ = TServiceContext::SigningFactory(Settings_.GetSelfSecret());
        }

        if (Settings_.IsServiceTicketFetchingRequired()) {
            Destinations_ = {Settings_.GetDestinations().begin(), Settings_.GetDestinations().end()};
        }

        PublicKeysDurations_.RefreshPeriod = TDuration::Days(1);
        ServiceTicketsDurations_.RefreshPeriod = TDuration::Hours(1);

        if (Settings_.IsUserTicketCheckingRequired()) {
            SetBbEnv(Settings_.GetEnvForUserTickets());
        }

        if (Settings_.IsRolesFetchingEnabled()) {
            RolesFetcher_ = std::make_unique<TRolesFetcher>(
                TRolesFetcherSettings{
                    Settings_.GetTiroleHost(),
                    Settings_.GetTirolePort(),
                    Settings_.GetDiskCacheDir(),
                    ProcInfo_,
                    Settings_.GetSelfTvmId(),
                    Settings_.GetIdmSystemSlug(),
                },
                Logger_);
        }

        if (Settings_.IsDiskCacheUsed()) {
            TString path = Settings_.GetDiskCacheDir();
            if (path.back() != '/') {
                path.push_back('/');
            }

            if (Settings_.IsServiceTicketFetchingRequired()) {
                ServiceTicketsFilepath_ = path;
                ServiceTicketsFilepath_.append("service_tickets");
            }

            if (Settings_.IsServiceTicketCheckingRequired() || Settings_.IsUserTicketCheckingRequired()) {
                PublicKeysFilepath_ = path;
                PublicKeysFilepath_.append("public_keys");
            }

            RetrySettingsFilepath_ = path + "retry_settings";
        } else {
            LogInfo("Disk cache disabled. Please set disk cache directory in settings for best reliability");
        }
    }

    void TThreadedUpdater::Init() {
        ReadStateFromDisk();
        ClearErrors();
        ExpBackoff_.SetEnabled(false);

        // First of all try to get tickets: there are a lot of reasons to fail this request.
        // As far as disk cache usually disabled, client will not fetch keys before fail on every ctor call.
        UpdateServiceTickets();
        if (!AreServicesTicketsOk()) {
            ThrowLastError();
        }

        UpdatePublicKeys();
        if (!IsServiceContextOk() || !IsUserContextOk()) {
            ThrowLastError();
        }

        UpdateRoles();
        if (RolesFetcher_ && !RolesFetcher_->AreRolesOk()) {
            ThrowLastError();
        }

        Inited_ = true;
        ExpBackoff_.SetEnabled(true);
    }

    void TThreadedUpdater::UpdateServiceTickets() {
        if (!Settings_.IsServiceTicketFetchingRequired()) {
            return;
        }

        TInstant stut = GetUpdateTimeOfServiceTickets();
        try {
            if (IsTimeToUpdateServiceTickets(stut)) {
                UpdateAllServiceTickets();
                NeedFetchMissingServiceTickets_ = false;
            } else if (NeedFetchMissingServiceTickets_ && GetCachedServiceTickets()->TicketsById.size() < Destinations_.size()) {
                UpdateMissingServiceTickets(Destinations_);
                NeedFetchMissingServiceTickets_ = false;
            }
            if (AreServicesTicketsOk()) {
                ClearError(EScope::ServiceTickets);
            }
        } catch (const std::exception& e) {
            ProcessError(EType::Retriable, EScope::ServiceTickets, e.what());
            LogWarning(TStringBuilder() << "Failed to update service tickets: " << e.what());
            if (TInstant::Now() - stut > ServiceTicketsDurations_.Expiring) {
                LogError("Service tickets have not been refreshed for too long period");
            }
        }
    }

    void TThreadedUpdater::UpdateAllServiceTickets() {
        THttpResult st = GetServiceTicketsFromHttp(Destinations_, RetrySettings_.DstsLimit);

        auto oldCache = GetCachedServiceTickets();
        if (oldCache) {
            for (const auto& pair : oldCache->ErrorsById) {
                st.TicketsWithErrors.Errors.insert(pair);
            }
        }

        UpdateServiceTicketsCache(std::move(st.TicketsWithErrors), TInstant::Now());
        if (ServiceTicketsFilepath_) {
            DiskCacheServiceTickets_ = CreateJsonArray(st.Responses);
            TDiskWriter w(ServiceTicketsFilepath_, Logger_.Get());
            w.Write(PrepareTicketsForDisk(DiskCacheServiceTickets_, Settings_.GetSelfTvmId()));
        }
    }

    TServiceTicketsPtr TThreadedUpdater::UpdateMissingServiceTickets(const TDstSet& required) {
        TServiceTicketsPtr cache = GetCachedServiceTickets();
        TClientSettings::TDstVector dsts = FindMissingDsts(cache, required);

        if (dsts.empty()) {
            return cache;
        }

        THttpResult st = GetServiceTicketsFromHttp(dsts, RetrySettings_.DstsLimit);

        size_t gotTickets = st.TicketsWithErrors.Tickets.size();

        for (const auto& pair : cache->TicketsById) {
            st.TicketsWithErrors.Tickets.insert(pair);
        }
        for (const auto& pair : cache->ErrorsById) {
            st.TicketsWithErrors.Errors.insert(pair);
        }
        for (const auto& pair : st.TicketsWithErrors.Tickets) {
            st.TicketsWithErrors.Errors.erase(pair.first);
        }

        TServiceTicketsPtr c = UpdateServiceTicketsCachePartly(
            std::move(st.TicketsWithErrors),
            gotTickets);
        if (!c) {
            LogWarning("UpdateMissingServiceTickets: new cache is NULL. BUG?");
            c = cache;
        }

        if (!ServiceTicketsFilepath_) {
            return c;
        }

        DiskCacheServiceTickets_ = AppendToJsonArray(DiskCacheServiceTickets_, st.Responses);

        TDiskWriter w(ServiceTicketsFilepath_, Logger_.Get());
        w.Write(PrepareTicketsForDisk(DiskCacheServiceTickets_, Settings_.GetSelfTvmId()));

        return c;
    }

    void TThreadedUpdater::UpdatePublicKeys() {
        if (!Settings_.IsServiceTicketCheckingRequired() && !Settings_.IsUserTicketCheckingRequired()) {
            return;
        }

        TInstant pkut = GetUpdateTimeOfPublicKeys();
        if (!IsTimeToUpdatePublicKeys(pkut)) {
            return;
        }

        try {
            TString publicKeys = GetPublicKeysFromHttp();

            UpdatePublicKeysCache(publicKeys, TInstant::Now());
            if (PublicKeysFilepath_) {
                TDiskWriter w(PublicKeysFilepath_, Logger_.Get());
                w.Write(publicKeys);
            }
            if (IsServiceContextOk() && IsUserContextOk()) {
                ClearError(EScope::PublicKeys);
            }
        } catch (const std::exception& e) {
            ProcessError(EType::Retriable, EScope::PublicKeys, e.what());
            LogWarning(TStringBuilder() << "Failed to update public keys: " << e.what());
            if (TInstant::Now() - pkut > PublicKeysDurations_.Expiring) {
                LogError("Public keys have not been refreshed for too long period");
            }
        }
    }

    void TThreadedUpdater::UpdateRoles() {
        if (!RolesFetcher_) {
            return;
        }

        TInstant rut = GetUpdateTimeOfRoles();
        if (!TRolesFetcher::IsTimeToUpdate(RetrySettings_, TInstant::Now() - rut)) {
            return;
        }

        struct TCloser {
            TRolesFetcher* Fetcher;
            ~TCloser() {
                Fetcher->ResetConnection();
            }
        } closer{RolesFetcher_.get()};

        try {
            TServiceTicketsPtr st = GetCachedServiceTickets();
            Y_ENSURE(st, "No one service ticket in memory: how it possible?");
            auto it = st->TicketsById.find(Settings_.GetTiroleTvmId());
            Y_ENSURE(it != st->TicketsById.end(),
                     "Missing tvmid for tirole in cache: " << Settings_.GetTiroleTvmId());

            RolesFetcher_->Update(
                FetchWithRetries(
                    [&]() { return RolesFetcher_->FetchActualRoles(it->second); },
                    EScope::Roles));
            SetUpdateTimeOfRoles(TInstant::Now());

            if (RolesFetcher_->AreRolesOk()) {
                ClearError(EScope::Roles);
            }
        } catch (const std::exception& e) {
            ProcessError(EType::Retriable, EScope::Roles, e.what());
            LogWarning(TStringBuilder() << "Failed to update roles: " << e.what());
            if (TRolesFetcher::ShouldWarn(RetrySettings_, TInstant::Now() - rut)) {
                LogError("Roles have not been refreshed for too long period");
            }
        }
    }

    TServiceTicketsPtr TThreadedUpdater::UpdateServiceTicketsCachePartly(
        TAsyncUpdaterBase::TPairTicketsErrors&& tickets,
        size_t got) {
        size_t count = tickets.Tickets.size();
        TServiceTicketsPtr c = MakeIntrusiveConst<TServiceTickets>(std::move(tickets.Tickets),
                                                                   std::move(tickets.Errors),
                                                                   DstAliases_);
        SetServiceTickets(c);

        LogInfo(TStringBuilder()
                << "Cache was partly updated with " << got
                << " service ticket(s). total: " << count);

        return c;
    }

    void TThreadedUpdater::UpdateServiceTicketsCache(TPairTicketsErrors&& tickets, TInstant time) {
        size_t count = tickets.Tickets.size();
        SetServiceTickets(MakeIntrusiveConst<TServiceTickets>(std::move(tickets.Tickets),
                                                              std::move(tickets.Errors),
                                                              DstAliases_));

        SetUpdateTimeOfServiceTickets(time);

        if (count > 0) { 
            LogInfo(TStringBuilder() << "Cache was updated with " << count << " service ticket(s): " << time); 
        } 
    }

    void TThreadedUpdater::UpdatePublicKeysCache(const TString& publicKeys, TInstant time) {
        if (publicKeys.empty()) {
            return;
        }

        if (Settings_.IsServiceTicketCheckingRequired()) {
            SetServiceContext(MakeIntrusiveConst<TServiceContext>(
                TServiceContext::CheckingFactory(Settings_.GetSelfTvmId(),
                                                 publicKeys)));
        }

        if (Settings_.IsUserTicketCheckingRequired()) {
            SetUserContext(publicKeys);
        }

        SetUpdateTimeOfPublicKeys(time);

        LogInfo(TStringBuilder() << "Cache was updated with public keys: " << time);
    }

    void TThreadedUpdater::ReadStateFromDisk() {
        try {
            TServiceTicketsFromDisk st = ReadServiceTicketsFromDisk();
            UpdateServiceTicketsCache(std::move(st.TicketsWithErrors), st.BornDate);
            DiskCacheServiceTickets_ = st.FileBody;
        } catch (const std::exception& e) {
            LogWarning(TStringBuilder() << "Failed to read service tickets from disk: " << e.what());
        }

        try {
            std::pair<TString, TInstant> pk = ReadPublicKeysFromDisk();
            UpdatePublicKeysCache(pk.first, pk.second);
        } catch (const std::exception& e) {
            LogWarning(TStringBuilder() << "Failed to read public keys from disk: " << e.what());
        }

        try {
            TString rs = ReadRetrySettingsFromDisk();
            UpdateRetrySettings(rs);
        } catch (const std::exception& e) {
            LogWarning(TStringBuilder() << "Failed to read retry settings from disk: " << e.what());
        }

        try {
            if (RolesFetcher_) {
                SetUpdateTimeOfRoles(RolesFetcher_->ReadFromDisk());
            }
        } catch (const std::exception& e) {
            LogWarning(TStringBuilder() << "Failed to read roles from disk: " << e.what());
        }
    }

    TThreadedUpdater::TServiceTicketsFromDisk TThreadedUpdater::ReadServiceTicketsFromDisk() const {
        if (!ServiceTicketsFilepath_) {
            return {};
        }

        TDiskReader r(ServiceTicketsFilepath_, Logger_.Get());
        if (!r.Read()) {
            return {};
        }

        std::pair<TStringBuf, TTvmId> data = ParseTicketsFromDisk(r.Data());
        if (data.second != Settings_.GetSelfTvmId()) {
            TStringStream s;
            s << "Disk cache is for another tvmId (" << data.second << "). ";
            s << "Self=" << Settings_.GetSelfTvmId();
            LogWarning(s.Str());
            return {};
        }

        TPairTicketsErrors res;
        ParseTicketsFromResponse(data.first, Destinations_, res);
        if (IsInvalid(TServiceTickets::GetInvalidationTime(res.Tickets), TInstant::Now())) {
            LogWarning("Disk cache (service tickets) is too old");
            return {};
        }

        LogInfo(TStringBuilder() << "Got " << res.Tickets.size() << " service ticket(s) from disk");
        return {std::move(res), r.Time(), TString(data.first)};
    }

    std::pair<TString, TInstant> TThreadedUpdater::ReadPublicKeysFromDisk() const {
        if (!PublicKeysFilepath_) {
            return {};
        }

        TDiskReader r(PublicKeysFilepath_, Logger_.Get());
        if (!r.Read()) {
            return {};
        }

        if (TInstant::Now() - r.Time() > PublicKeysDurations_.Invalid) {
            LogWarning("Disk cache (public keys) is too old");
            return {};
        }

        return {r.Data(), r.Time()};
    }

    TString TThreadedUpdater::ReadRetrySettingsFromDisk() const {
        if (!RetrySettingsFilepath_) {
            return {};
        }

        TDiskReader r(RetrySettingsFilepath_, Logger_.Get());
        if (!r.Read()) {
            return {};
        }

        return r.Data();
    }

    template <class Dsts>
    TThreadedUpdater::THttpResult TThreadedUpdater::GetServiceTicketsFromHttp(const Dsts& dsts, const size_t dstLimit) const {
        Y_ENSURE(SigningContext_, "Internal error");

        TClientSettings::TDstVector part;
        part.reserve(dstLimit);
        THttpResult res;
        res.TicketsWithErrors.Tickets.reserve(dsts.size());
        res.Responses.reserve(dsts.size() / dstLimit + 1);

        for (auto it = dsts.begin(); it != dsts.end();) {
            part.clear();
            for (size_t count = 0; it != dsts.end() && count < dstLimit; ++count, ++it) {
                part.push_back(*it);
            }

            TString response =
                FetchWithRetries(
                    [this, &part]() {
                        // create request here to keep 'ts' actual
                        return FetchServiceTicketsFromHttp(PrepareRequestForServiceTickets(
                            Settings_.GetSelfTvmId(),
                            *SigningContext_,
                            part,
                            ProcInfo_));
                    },
                    EScope::ServiceTickets)
                    .Response;
            ParseTicketsFromResponse(response, part, res.TicketsWithErrors);
            LogDebug(TStringBuilder()
                     << "Response with service tickets for " << part.size()
                     << " destination(s) was successfully fetched from " << TvmUrl_);

            res.Responses.push_back(response);
        }

        LogDebug(TStringBuilder()
                 << "Got responses with service tickets with " << res.Responses.size() << " pages for "
                 << dsts.size() << " destination(s)");
        for (const auto& p : res.TicketsWithErrors.Errors) {
            LogError(TStringBuilder()
                     << "Failed to get service ticket for dst=" << p.first << ": " << p.second);
        }

        return res;
    }

    TString TThreadedUpdater::GetPublicKeysFromHttp() const {
        TString publicKeys =
            FetchWithRetries(
                [this]() { return FetchPublicKeysFromHttp(); },
                EScope::PublicKeys)
                .Response;

        LogDebug("Public keys were successfully fetched from " + TvmUrl_);

        return publicKeys;
    }

    NUtils::TFetchResult TThreadedUpdater::FetchServiceTicketsFromHttp(const TString& body) const {
        TStringStream s;

        THttpHeaders outHeaders;
        TKeepAliveHttpClient::THttpCode code = GetClient().DoPost("/2/ticket", body, &s, Headers_, &outHeaders);

        const THttpInputHeader* settings = outHeaders.FindHeader("X-Ya-Retry-Settings");

        return {code, {}, "/2/ticket", s.Str(), settings ? settings->Value() : ""};
    }

    NUtils::TFetchResult TThreadedUpdater::FetchPublicKeysFromHttp() const {
        TStringStream s;

        THttpHeaders outHeaders;
        TKeepAliveHttpClient::THttpCode code = GetClient().DoGet(PublicKeysUrl_, &s, {}, &outHeaders);

        const THttpInputHeader* settings = outHeaders.FindHeader("X-Ya-Retry-Settings");

        return {code, {}, "/2/keys", s.Str(), settings ? settings->Value() : ""};
    }

    bool TThreadedUpdater::UpdateRetrySettings(const TString& header) const {
        if (header.empty()) {
            // Probably it is some kind of test?
            return false;
        }

        try {
            TString raw = NUtils::Base64url2bin(header);
            Y_ENSURE(raw, "Invalid base64url in settings");

            retry_settings::v1::Settings proto;
            Y_ENSURE(proto.ParseFromString(raw), "Invalid proto");

            // This ugly hack helps to process these settings in any case
            TThreadedUpdater& this_ = *const_cast<TThreadedUpdater*>(this);
            TRetrySettings& res = this_.RetrySettings_;

            TStringStream diff;
            auto update = [&diff](auto& l, const auto& r, TStringBuf desc) {
                if (l != r) {
                    diff << desc << ":" << l << "->" << r << ";";
                    l = r;
                }
            };

            if (proto.has_exponential_backoff_min_sec()) {
                update(res.BackoffSettings.Min,
                       TDuration::Seconds(proto.exponential_backoff_min_sec()),
                       "exponential_backoff_min");
            }
            if (proto.has_exponential_backoff_max_sec()) {
                update(res.BackoffSettings.Max,
                       TDuration::Seconds(proto.exponential_backoff_max_sec()),
                       "exponential_backoff_max");
            }
            if (proto.has_exponential_backoff_factor()) {
                update(res.BackoffSettings.Factor,
                       proto.exponential_backoff_factor(),
                       "exponential_backoff_factor");
            }
            if (proto.has_exponential_backoff_jitter()) {
                update(res.BackoffSettings.Jitter,
                       proto.exponential_backoff_jitter(),
                       "exponential_backoff_jitter");
            }
            this_.ExpBackoff_.UpdateSettings(res.BackoffSettings);

            if (proto.has_max_random_sleep_default()) {
                update(res.MaxRandomSleepDefault,
                       TDuration::MilliSeconds(proto.max_random_sleep_default()),
                       "max_random_sleep_default");
            }
            if (proto.has_max_random_sleep_when_ok()) {
                update(res.MaxRandomSleepWhenOk,
                       TDuration::MilliSeconds(proto.max_random_sleep_when_ok()),
                       "max_random_sleep_when_ok");
            }
            if (proto.has_retries_on_start()) {
                Y_ENSURE(proto.retries_on_start(), "retries_on_start==0");
                update(res.RetriesOnStart,
                       proto.retries_on_start(),
                       "retries_on_start");
            }
            if (proto.has_retries_in_background()) {
                Y_ENSURE(proto.retries_in_background(), "retries_in_background==0");
                update(res.RetriesInBackground,
                       proto.retries_in_background(),
                       "retries_in_background");
            }
            if (proto.has_worker_awaking_period_sec()) {
                update(res.WorkerAwakingPeriod,
                       TDuration::Seconds(proto.worker_awaking_period_sec()),
                       "worker_awaking_period");
                this_.WorkerAwakingPeriod_ = res.WorkerAwakingPeriod;
            }
            if (proto.has_dsts_limit()) {
                Y_ENSURE(proto.dsts_limit(), "dsts_limit==0");
                update(res.DstsLimit,
                       proto.dsts_limit(),
                       "dsts_limit");
            }

            if (proto.has_roles_update_period_sec()) {
                Y_ENSURE(proto.roles_update_period_sec(), "roles_update_period==0");
                update(res.RolesUpdatePeriod,
                       TDuration::Seconds(proto.roles_update_period_sec()),
                       "roles_update_period_sec");
            }
            if (proto.has_roles_warn_period_sec()) {
                Y_ENSURE(proto.roles_warn_period_sec(), "roles_warn_period_sec==0");
                update(res.RolesWarnPeriod,
                       TDuration::Seconds(proto.roles_warn_period_sec()),
                       "roles_warn_period_sec");
            }

            if (diff.empty()) {
                return false;
            }

            LogDebug("Retry settings were updated: " + diff.Str());
            return true;
        } catch (const std::exception& e) {
            LogWarning(TStringBuilder()
                       << "Failed to update retry settings from server, header '"
                       << header << "': "
                       << e.what());
        }

        return false;
    }

    template <typename Func>
    NUtils::TFetchResult TThreadedUpdater::FetchWithRetries(Func func, EScope scope) const {
        const ui32 tries = Inited_ ? RetrySettings_.RetriesInBackground
                                   : RetrySettings_.RetriesOnStart;

        for (size_t idx = 1;; ++idx) {
            RandomSleep();

            try {
                NUtils::TFetchResult result = func();

                if (UpdateRetrySettings(result.RetrySettings) && RetrySettingsFilepath_) {
                    TDiskWriter w(RetrySettingsFilepath_, Logger_.Get());
                    w.Write(result.RetrySettings);
                }

                if (400 <= result.Code && result.Code <= 499) {
                    throw TNonRetriableException() << ProcessHttpError(scope, result.Path, result.Code, result.Response);
                }
                if (result.Code < 200 || result.Code >= 399) {
                    throw yexception() << ProcessHttpError(scope, result.Path, result.Code, result.Response);
                }

                ExpBackoff_.Decrease();
                return result;
            } catch (const TNonRetriableException& e) {
                LogWarning(TStringBuilder() << "Failed to get " << scope << ": " << e.what());
                ExpBackoff_.Increase();
                throw;
            } catch (const std::exception& e) {
                LogWarning(TStringBuilder() << "Failed to get " << scope << ": " << e.what());
                ExpBackoff_.Increase();
                if (idx >= tries) {
                    throw;
                }
            }
        }

        throw yexception() << "unreachable";
    }

    void TThreadedUpdater::RandomSleep() const {
        const TDuration maxSleep = TClientStatus::ECode::Ok == GetState()
                                       ? RetrySettings_.MaxRandomSleepWhenOk
                                       : RetrySettings_.MaxRandomSleepDefault;

        if (maxSleep) {
            ui32 toSleep = Random_.GenRand() % maxSleep.MilliSeconds();
            ExpBackoff_.Sleep(TDuration::MilliSeconds(toSleep));
        }
    }

    TString TThreadedUpdater::PrepareRequestForServiceTickets(TTvmId src,
                                                              const TServiceContext& ctx,
                                                              const TClientSettings::TDstVector& dsts,
                                                              const NUtils::TProcInfo& procInfo,
                                                              time_t now) {
        TStringStream s;

        const TString ts = IntToString<10>(now);
        TStringStream dst;
        dst.Reserve(10 * dsts.size());
        for (const TClientSettings::TDst& d : dsts) {
            if (dst.Str()) {
                dst << ',';
            }
            dst << d.Id;
        }

        s << "grant_type=client_credentials";
        s << "&src=" << src;
        s << "&dst=" << dst.Str();
        s << "&ts=" << ts;
        s << "&sign=" << ctx.SignCgiParamsForTvm(ts, dst.Str());
        s << "&get_retry_settings=yes";

        s << "&";
        procInfo.AddToRequest(s);

        return s.Str();
    }

    template <class Dsts>
    void TThreadedUpdater::ParseTicketsFromResponse(TStringBuf resp,
                                                    const Dsts& dsts,
                                                    TPairTicketsErrors& out) const {
        NJson::TJsonValue doc;
        Y_ENSURE(NJson::ReadJsonTree(resp, &doc), "Invalid json from tvm-api: " << resp);

        const NJson::TJsonValue* currentResp = doc.IsMap() ? &doc : nullptr;
        auto find = [&currentResp, &doc](TTvmId id, NJson::TJsonValue& obj) -> bool {
            const TString idStr = IntToString<10>(id);
            if (currentResp && currentResp->GetValue(idStr, &obj)) {
                return true;
            }

            for (const NJson::TJsonValue& val : doc.GetArray()) {
                currentResp = &val;
                if (currentResp->GetValue(idStr, &obj)) {
                    return true;
                }
            }

            return false;
        };

        for (const TClientSettings::TDst& d : dsts) {
            NJson::TJsonValue obj;
            NJson::TJsonValue val;

            if (!find(d.Id, obj) || !obj.GetValue("ticket", &val)) {
                TString err;
                if (obj.GetValue("error", &val)) {
                    err = val.GetString();
                } else {
                    err = "Missing tvm_id in response, should never happend: " + IntToString<10>(d.Id);
                }

                TStringStream s;
                s << "Failed to get ServiceTicket for " << d.Id << ": " << err;
                ProcessError(EType::NonRetriable, EScope::ServiceTickets, s.Str());

                out.Errors.insert({d.Id, std::move(err)});
                continue;
            }

            out.Tickets.insert({d.Id, val.GetString()});
        }
    }

    static const char DELIMETER = '\t';
    TString TThreadedUpdater::PrepareTicketsForDisk(TStringBuf tvmResponse, TTvmId selfId) {
        TStringStream s;
        s << tvmResponse << DELIMETER << selfId;
        return s.Str();
    }

    std::pair<TStringBuf, TTvmId> TThreadedUpdater::ParseTicketsFromDisk(TStringBuf data) {
        TStringBuf tvmId = data.RNextTok(DELIMETER);
        return {data, IntFromString<TTvmId, 10>(tvmId)};
    }

    const TDstSet& TThreadedUpdater::GetDsts() const {
        return Destinations_;
    }

    void TThreadedUpdater::AddDstToSettings(const TClientSettings::TDst& dst) {
        Destinations_.insert(dst);
    }

    bool TThreadedUpdater::IsTimeToUpdateServiceTickets(TInstant lastUpdate) const {
        return TInstant::Now() - lastUpdate > ServiceTicketsDurations_.RefreshPeriod;
    }

    bool TThreadedUpdater::IsTimeToUpdatePublicKeys(TInstant lastUpdate) const {
        return TInstant::Now() - lastUpdate > PublicKeysDurations_.RefreshPeriod;
    }

    bool TThreadedUpdater::AreServicesTicketsOk() const {
        if (!Settings_.IsServiceTicketFetchingRequired()) {
            return true;
        }
        auto c = GetCachedServiceTickets();
        return c && (!Settings_.IsIncompleteTicketsSetAnError || c->TicketsById.size() == Destinations_.size());
    }

    bool TThreadedUpdater::IsServiceContextOk() const {
        if (!Settings_.IsServiceTicketCheckingRequired()) {
            return true;
        }

        return bool(GetCachedServiceContext());
    }

    bool TThreadedUpdater::IsUserContextOk() const {
        if (!Settings_.IsUserTicketCheckingRequired()) {
            return true;
        }
        return bool(GetCachedUserContext());
    }

    void TThreadedUpdater::Worker() {
        UpdateServiceTickets();
        UpdatePublicKeys();
        UpdateRoles();
    }

    TServiceTickets::TMapAliasId TThreadedUpdater::MakeAliasMap(const TClientSettings& settings) {
        TServiceTickets::TMapAliasId res;

        if (settings.HasDstAliases()) {
            for (const auto& pair : settings.GetDstAliases()) {
                res.insert({pair.first, pair.second.Id});
            }
        }

        return res;
    }

    TClientSettings::TDstVector TThreadedUpdater::FindMissingDsts(TServiceTicketsPtr available, const TDstSet& required) {
        Y_ENSURE(available);
        TDstSet set;
        // available->TicketsById is not sorted
        for (const auto& pair : available->TicketsById) {
            set.insert(pair.first);
        }
        return FindMissingDsts(set, required);
    }

    TClientSettings::TDstVector TThreadedUpdater::FindMissingDsts(const TDstSet& available, const TDstSet& required) {
        TClientSettings::TDstVector res;
        std::set_difference(required.begin(), required.end(),
                            available.begin(), available.end(),
                            std::inserter(res, res.begin()));
        return res;
    }

    TString TThreadedUpdater::CreateJsonArray(const TSmallVec<TString>& responses) {
        if (responses.empty()) {
            return "[]";
        }

        size_t size = 0;
        for (const TString& r : responses) {
            size += r.size() + 1;
        }

        TString res;
        res.reserve(size + 2);

        res.push_back('[');
        for (const TString& r : responses) {
            res.append(r).push_back(',');
        }
        res.back() = ']';

        return res;
    }

    TString TThreadedUpdater::AppendToJsonArray(const TString& json, const TSmallVec<TString>& responses) {
        Y_ENSURE(json, "previous body required");

        size_t size = 0;
        for (const TString& r : responses) {
            size += r.size() + 1;
        }

        TString res;
        res.reserve(size + 2 + json.size());

        res.push_back('[');
        if (json.StartsWith('[')) {
            Y_ENSURE(json.EndsWith(']'), "array is broken:" << json);
            res.append(TStringBuf(json).Chop(1).Skip(1));
        } else {
            res.append(json);
        }

        res.push_back(',');
        for (const TString& r : responses) {
            res.append(r).push_back(',');
        }
        res.back() = ']';

        return res;
    }
}
