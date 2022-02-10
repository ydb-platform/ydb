#pragma once

#include <library/cpp/tvmauth/client/misc/settings.h>

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/type.h>

#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NTvmAuth::NTvmApi {
    /**
     * Settings for TVM client. Uses https://tvm-api.yandex.net to get state.
     * At least one of them is required:
     *     FetchServiceTicketsForDsts_/FetchServiceTicketsForDstsWithAliases_
     *     CheckServiceTickets_
     *     CheckUserTicketsWithBbEnv_
     */
    class TClientSettings: public NTvmAuth::TClientSettings {
    public:
        class TDst;

        /**
         * Alias is an internal name for destinations within your code.
         *  You can associate a name with an tvm_id once in your code and use the name as an alias for
         *  tvm_id to each calling point. Useful for several environments: prod/test/etc.
         * @example:
         *      // init
         *      static const TString MY_BACKEND = "my backend";
         *      TDstMap map = {{MY_BACKEND, TDst(config.get("my_back_tvm_id"))}};
         *      ...
         *      // per request
         *      TString t = tvmClient.GetServiceTicket(MY_BACKEND);
         */
        using TDstMap = THashMap<TAlias, TDst>;
        using TDstVector = TVector<TDst>;

    public:
        /*!
         * NOTE: Please use this option: it provides the best reliability
         * NOTE: Client requires read/write permissions
         * WARNING: The same directory can be used only:
         *            - for TVM clients with the same settings
         *          OR
         *            - for new client replacing previous - with another config.
         *          System user must be the same for processes with these clients inside.
         *          Implementation doesn't provide other scenarios.
         */
        TString DiskCacheDir;

        // Required for Service Ticket fetching or checking
        TTvmId SelfTvmId = 0;

        // Options for Service Tickets fetching
        NSecretString::TSecretString Secret;
        /*!
         * Client will process both attrs:
         *   FetchServiceTicketsForDsts_, FetchServiceTicketsForDstsWithAliases_
         * WARNING: It is not way to provide authorization for incoming ServiceTickets!
         *          It is way only to send your ServiceTickets to your backend!
         */
        TDstVector FetchServiceTicketsForDsts;
        TDstMap FetchServiceTicketsForDstsWithAliases;
        bool IsIncompleteTicketsSetAnError = true;

        // Options for Service Tickets checking
        bool CheckServiceTickets = false;

        // Options for User Tickets checking
        TMaybe<EBlackboxEnv> CheckUserTicketsWithBbEnv;

        // Options for roles fetching
        TString FetchRolesForIdmSystemSlug;
        /*!
         * By default client checks src from ServiceTicket or default uid from UserTicket -
         *   to prevent you from forgetting to check it yourself.
         * It does binary checks only:
         *   ticket gets status NoRoles, if there is no role for src or default uid.
         * You need to check roles on your own if you have a non-binary role system or
         *     you have disabled ShouldCheckSrc/ShouldCheckDefaultUid
         *
         * You may need to disable this check in the following cases:
         *   - You use GetRoles() to provide verbose message (with revision).
         *     Double check may be inconsistent:
         *       binary check inside client uses revision of roles X - i.e. src 100500 has no role,
         *       exact check in your code uses revision of roles Y -  i.e. src 100500 has some roles.
         */
        bool ShouldCheckSrc = true;
        bool ShouldCheckDefaultUid = true;

        // Options for tests
        TString TvmHost = "https://tvm-api.yandex.net";
        ui16 TvmPort = 443;
        TString TiroleHost = "https://tirole-api.yandex.net";
        TDuration TvmSocketTimeout = TDuration::Seconds(5);
        TDuration TvmConnectTimeout = TDuration::Seconds(30);
        ui16 TirolePort = 443;
        TTvmId TiroleTvmId = TIROLE_TVMID;

        // for debug purposes
        TString LibVersionPrefix;

        void CheckValid() const;
        TClientSettings CloneNormalized() const;

        static inline const TTvmId TIROLE_TVMID = 2028120;
        static inline const TTvmId TIROLE_TVMID_TEST = 2026536;

        // DEPRECATED API
        // TODO: get rid of it: PASSP-35377
    public:
        // Deprecated: set attributes directly
        void SetSelfTvmId(TTvmId selfTvmId) {
            SelfTvmId = selfTvmId;
        }

        // Deprecated: set attributes directly
        void EnableServiceTicketChecking() {
            CheckServiceTickets = true;
        }

        // Deprecated: set attributes directly
        void EnableUserTicketChecking(EBlackboxEnv env) {
            CheckUserTicketsWithBbEnv = env;
        }

        // Deprecated: set attributes directly
        void SetTvmHostPort(const TString& host, ui16 port) {
            TvmHost = host;
            TvmPort = port;
        } 
 
        // Deprecated: set attributes directly
        void SetTiroleHostPort(const TString& host, ui16 port) {
            TiroleHost = host;
            TirolePort = port;
        }

        // Deprecated: set attributes directly
        void EnableRolesFetching(const TString& systemSlug, TTvmId tiroleTvmId = TIROLE_TVMID) {
            TiroleTvmId = tiroleTvmId;
            FetchRolesForIdmSystemSlug = systemSlug;
        }

        // Deprecated: set attributes directly
        void DoNotCheckSrcByDefault() {
            ShouldCheckSrc = false;
        }

        // Deprecated: set attributes directly
        void DoNotCheckDefaultUidByDefault() {
            ShouldCheckDefaultUid = false;
        }

        // Deprecated: set attributes directly
        void SetDiskCacheDir(const TString& dir) {
            DiskCacheDir = dir;
        }

        // Deprecated: set attributes directly
        void EnableServiceTicketsFetchOptions(const TStringBuf selfSecret,
                                              TDstMap&& dsts,
                                              const bool considerIncompleteTicketsSetAsError = true) {
            IsIncompleteTicketsSetAnError = considerIncompleteTicketsSetAsError;
            Secret = selfSecret;

            FetchServiceTicketsForDsts = TDstVector{};
            FetchServiceTicketsForDsts.reserve(dsts.size());
            for (const auto& pair : dsts) {
                FetchServiceTicketsForDsts.push_back(pair.second);
            }

            FetchServiceTicketsForDstsWithAliases = std::move(dsts);
        }

        // Deprecated: set attributes directly
        void EnableServiceTicketsFetchOptions(const TStringBuf selfSecret,
                                              TDstVector&& dsts,
                                              const bool considerIncompleteTicketsSetAsError = true) {
            IsIncompleteTicketsSetAnError = considerIncompleteTicketsSetAsError;
            Secret = selfSecret;
            FetchServiceTicketsForDsts = std::move(dsts);
        }

    public:
        bool IsServiceTicketFetchingRequired() const {
            return bool(Secret.Value());
        }

        const TStringBuf GetSelfSecret() const {
            return Secret;
        }

        bool HasDstAliases() const {
            return !FetchServiceTicketsForDstsWithAliases.empty();
        }

        const TDstMap& GetDstAliases() const {
            return FetchServiceTicketsForDstsWithAliases;
        }

        const TDstVector& GetDestinations() const {
            return FetchServiceTicketsForDsts;
        }

        bool IsUserTicketCheckingRequired() const {
            return bool(CheckUserTicketsWithBbEnv);
        }

        EBlackboxEnv GetEnvForUserTickets() const {
            return *CheckUserTicketsWithBbEnv;
        }

        bool IsServiceTicketCheckingRequired() const {
            return CheckServiceTickets;
        }

        bool IsDiskCacheUsed() const {
            return bool(DiskCacheDir);
        }

        TString GetDiskCacheDir() const {
            return DiskCacheDir;
        }

        TTvmId GetSelfTvmId() const {
            return SelfTvmId;
        }

        const TString& GetLibVersionPrefix() const {
            return LibVersionPrefix;
        }

        const TString& GetTvmHost() const {
            return TvmHost;
        } 
 
        ui16 GetTvmPort() const {
            return TvmPort;
        } 
 
        bool IsRolesFetchingEnabled() const {
            return bool(FetchRolesForIdmSystemSlug);
        }

        TTvmId GetTiroleTvmId() const {
            return TiroleTvmId;
        }

        const TString& GetIdmSystemSlug() const {
            return FetchRolesForIdmSystemSlug;
        }

        const TString& GetTiroleHost() const {
            return TiroleHost;
        }

        ui16 GetTirolePort() const {
            return TirolePort;
        }

        bool NeedServiceTicketsFetching() const {
            return !FetchServiceTicketsForDsts.empty() ||
                   !FetchServiceTicketsForDstsWithAliases.empty() ||
                   FetchRolesForIdmSystemSlug;
        }

        // TODO: get rid of TDst: PASSP-35377
        class TDst {
        public:
            TDst(TTvmId id)
                : Id(id)
            {
                Y_ENSURE_EX(id != 0, TBrokenTvmClientSettings() << "TvmId cannot be 0");
            }

            TTvmId Id;

            bool operator==(const TDst& o) const {
                return Id == o.Id;
            }

            bool operator<(const TDst& o) const {
                return Id < o.Id;
            }

        public: // for python binding
            TDst()
                : Id(0)
            {
            }
        };

    public:
        static void CheckPermissions(const TString& dir);
    };
}
