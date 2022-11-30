#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_HIGH_LVL_WRAPPER_H_
#define _TVM_AUTH_HIGH_LVL_WRAPPER_H_

#include "high_lvl_client.h"
#include "tvmauth_wrapper.h"

#include <map>

namespace NTvmAuthWrapper {
    /*!
     * Uses local http-interface to get state: http://localhost/tvm/.
     * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
     * See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.
     */
    class TTvmToolClientSettings {
    public:
        /*!
         * Create settings struct for tvmtool client
         * Sets default values:
         * - hostname: "localhost"
         * - port detected with env["DEPLOY_TVM_TOOL_URL"] (provided with Yandex.Deploy),
         *      otherwise port == 1 (it is ok for Qloud)
         * - authToken: env["TVMTOOL_LOCAL_AUTHTOKEN"] (provided with Yandex.Deploy),
         *      otherwise env["QLOUD_TVM_TOKEN"] (provided with Qloud)
         *
         * AuthToken is protection from SSRF.
         *
         * @param selfAias - alias for your TVM client, which you specified in tvmtool or YD interface
         */
        TTvmToolClientSettings(const std::string& selfAlias)
            : Ptr(nullptr, TA_TvmToolClientSettings_Delete)
        {
            TA_TTvmToolClientSettings* rawPtr;
            ThrowIfFatal(TA_TvmToolClientSettings_Create(selfAlias.data(), selfAlias.size(), &rawPtr));
            Ptr.reset(rawPtr);
        }

        /*!
         * Look at comment for ctor
         */
        void SetPort(uint16_t port) {
            ThrowIfFatal(TA_TvmToolClientSettings_SetPort(Ptr.get(), port));
        }

        /*!
         * Default value: hostname == "localhost"
         */
        void SetHostname(const std::string& hostname) {
            ThrowIfFatal(TA_TvmToolClientSettings_SetHostname(Ptr.get(), hostname.data(), hostname.size()));
        }

        /*!
         * Look at comment for ctor
         */
        void SetAuthtoken(const std::string& authtoken) {
            ThrowIfFatal(TA_TvmToolClientSettings_SetAuthToken(Ptr.get(), authtoken.data(), authtoken.size()));
        }

        /*!
          * Blackbox environmet is provided by tvmtool for client.
          * You can override it for your purpose with limitations:
          *   (env from tvmtool) -> (override)
          *  - Prod/ProdYateam -> Prod/ProdYateam
          *  - Test/TestYateam -> Test/TestYateam
          *  - Stress -> Stress
          *
          * You can contact tvm-dev@yandex-team.ru if limitations are too strict
          * @param[in] env
          */
        void OverrideBlackboxEnv(TA_EBlackboxEnv env) {
            ThrowIfFatal(TA_TvmToolClientSettings_OverrideBlackboxEnv(Ptr.get(), env));
        }

    private:
        friend class TTvmClient;
        std::unique_ptr<TA_TTvmToolClientSettings, decltype(&TA_TvmToolClientSettings_Delete)> Ptr;
    };

    /*!
     * Uses general way to get state: https://tvm-api.yandex.net.
     * It is not recomended for Qloud/YP.
     */
    class TTvmApiClientSettings {
    public:
        /**
         * Settings for TVM client
         * At least one of them is required: EnableServiceTicketChecking(), EnableUserTicketChecking()
         */
        TTvmApiClientSettings()
            : Ptr(nullptr, TA_TvmApiClientSettings_Delete)
        {
            TA_TTvmApiClientSettings* rawPtr;
            ThrowIfFatal(TA_TvmApiClientSettings_Create(&rawPtr));
            Ptr.reset(rawPtr);
        }

        void SetSelfTvmId(TTvmId selfTvmId) {
            if (selfTvmId == 0) {
                throw std::runtime_error("selfTvmId cannot be 0");
            }
            ThrowIfFatal(TA_TvmApiClientSettings_SetSelfTvmId(Ptr.get(), selfTvmId));
        }

        /*!
         * Prerequieres SetSelfTvmId()
         */
        void EnableServiceTicketChecking() {
            ThrowIfFatal(TA_TvmApiClientSettings_EnableServiceTicketChecking(Ptr.get()));
        }

        void EnableUserTicketChecking(TA_EBlackboxEnv env) {
            ThrowIfFatal(TA_TvmApiClientSettings_EnableUserTicketChecking(Ptr.get(), env));
        }

        class TDst {
        public:
            TDst(TTvmId id)
                : Id_(id)
            {
                if (id == 0) {
                    throw std::runtime_error("TvmId cannot be 0");
                }
            }

            const TTvmId Id_;
        };
        using TAlias = std::string;
        using TDstMap = std::map<TAlias, TDst>;
        using TDstVector = std::vector<TDst>;

        /**
         * Alias is internal name of destination in your code. It allowes not to bring destination's
         *  tvm_id to each calling point. Useful for several environments: prod/test/etc.
         * Overrides result of any other call of EnableServiceTicketsFetchOptions()
         * @example:
         *      // init
         *      static const TString MY_BACKEND = "my backend";
         *      TDstMap map = {{MY_BACKEND, TDst(config.get("my_back_tvm_id"))}};
         *      ...
         *      // per request
         *      TString t = tvmClient.GetServiceTicket(MY_BACKEND);
         */
        void EnableServiceTicketsFetchOptions(const std::string& selfSecret,
                                              const TDstMap& dsts) {
            std::string d;
            for (const auto& p : dsts) {
                d.append(p.first).push_back(':');
                d.append(std::to_string(p.second.Id_)).push_back(';');
            }

            ThrowIfFatal(TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases(
                             Ptr.get(),
                             selfSecret.data(),
                             selfSecret.size(),
                             d.data(),
                             d.size()));
        }

        void EnableServiceTicketsFetchOptions(const std::string& selfSecret,
                                              const TDstVector& dsts) {
            std::string d;
            for (const TDst& dst : dsts) {
                d.append(std::to_string(dst.Id_)).push_back(';');
            }

            ThrowIfFatal(TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds(
                             Ptr.get(),
                             selfSecret.data(),
                             selfSecret.size(),
                             d.data(),
                             d.size()));
        }

        /*!
         * Set path to directory for disk cache
         * Requires read/write permissions. Checks permissions
         * WARNING: The same directory can be used only:
         *            - for TVM clients with the same settings
         *          OR
         *            - for new client replacing previous - with another config.
         *          System user must be the same for processes with these clients inside.
         *          Implementation doesn't provide other scenarios.
         * @param[in] dir
         */
        void SetDiskCacheDir(const std::string& dir) {
            ThrowIfFatal(TA_TvmApiClientSettings_SetDiskCacheDir(Ptr.get(), dir.data(), dir.size()));
        }

    private:
        friend class TTvmClient;
        std::unique_ptr<TA_TTvmApiClientSettings, decltype(&TA_TvmApiClientSettings_Delete)> Ptr;
    };

    struct TClientStatus {
        TA_ETvmClientStatusCode Code = TA_TCSC_OK;
        std::string LastError;
    };

    /**
     * In 99% cases TvmClient shoud be created at service startup and live for the whole process lifetime.
     * @brief Long lived thread-safe object for interacting with TVM.
     */
    class TTvmClient {
    public:
        /*!
         * Create client for tvmtool. Starts thread for updating of cache in background
         * @param[in] settings
         * @param[in] logger is usefull for monitoring and debuging
         */
        TTvmClient(const TTvmToolClientSettings& settings, TA_TLoggerFunc logger)
            : Ptr(nullptr, TA_TvmClient_Delete) {
            TA_TTvmClient* rawPtr;
            ThrowIfFatal(TA_TvmClient_CreateForTvmtool(settings.Ptr.get(), logger, &rawPtr));
            Ptr.reset(rawPtr);
        }

        /*!
         * Starts thread for updating of in-memory cache in background
         * Reads cache from disk if specified
         * @param[in] settings
         * @param[in] logger is usefull for monitoring and debuging
         */
        TTvmClient(const TTvmApiClientSettings& settings, TA_TLoggerFunc logger)
            : Ptr(nullptr, TA_TvmClient_Delete) {
            TA_TTvmClient* rawPtr;
            ThrowIfFatal(TA_TvmClient_Create(settings.Ptr.get(), logger, &rawPtr));
            Ptr.reset(rawPtr);
        }

        TTvmClient(TTvmClient&&) = default;
        TTvmClient& operator=(TTvmClient&&) = default;

        TClientStatus GetStatus() const {
            TA_TTvmClientStatus* s = nullptr;
            ThrowIfFatal(TA_TvmClient_GetStatus(Ptr.get(), &s));

            std::unique_ptr<TA_TTvmClientStatus, decltype(&TA_TvmClient_DeleteStatus)> ptr(
                s, TA_TvmClient_DeleteStatus);

            TClientStatus res;
            ThrowIfFatal(TA_TvmClient_Status_GetCode(ptr.get(), &res.Code));

            const char* msg = nullptr;
            size_t size = 0;
            ThrowIfFatal(TA_TvmClient_Status_GetLastError(ptr.get(), &msg, &size));
            res.LastError = std::string(msg, size);

            return res;
        }

        /*!
         * Chekcing must be enabled in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param[in] ticket
         */
        TCheckedServiceTicket CheckServiceTicket(const std::string& ticket) const {
            TA_TCheckedServiceTicket* ticketPtr = nullptr;
            TA_EErrorCode resultCode = TA_TvmClient_CheckServiceTicket(Ptr.get(), ticket.data(), ticket.size(), &ticketPtr);
            TCheckedServiceTicket t(ticketPtr, resultCode);
            ThrowIfFatal(resultCode);
            return t;
        }

        /*!
         * Blackbox enviroment must be cofingured in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param[in] ticket
         */
        TCheckedUserTicket CheckUserTicket(const std::string& ticket) const {
            TA_TCheckedUserTicket* ticketPtr = nullptr;
            TA_EErrorCode resultCode = TA_TvmClient_CheckUserTicket(Ptr.get(), ticket.data(), ticket.size(), &ticketPtr);
            TCheckedUserTicket t(ticketPtr, resultCode);
            ThrowIfFatal(resultCode);
            return t;
        }

        /*!
         * Blackbox enviroment must be cofingured in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param[in] ticket
         * @param[in] env - allowes to overwrite env from settings
         */
        TCheckedUserTicket CheckUserTicket(const std::string& ticket, TA_EBlackboxEnv env) const {
            TA_TCheckedUserTicket* ticketPtr = nullptr;
            TA_EErrorCode resultCode = TA_TvmClient_CheckUserTicketWithOverridedEnv(Ptr.get(), ticket.data(), ticket.size(), env, &ticketPtr);
            TCheckedUserTicket t(ticketPtr, resultCode);
            ThrowIfFatal(resultCode);
            return t;
        }

        /*!
         * Requires fetchinig options (from TClientSettings or Qloud/YP/tvmtool settings)
         * Can throw exception if cache is invalid or wrong config
         * @param[in] dst
         */
        std::string GetServiceTicketFor(const TTvmApiClientSettings::TAlias& dst) {
            char buffer[512];
            size_t realSize = 0;
            TA_EErrorCode code = TA_TvmClient_GetServiceTicketForAlias(Ptr.get(), dst.data(), dst.size(), sizeof(buffer), buffer, &realSize);
            if (code == TA_EC_SMALL_BUFFER) {
                std::string res(realSize, 0);
                ThrowIfFatal(TA_TvmClient_GetServiceTicketForAlias(Ptr.get(), dst.data(), dst.size(), realSize, (char*)res.data(), &realSize));
                return res;
            }
            ThrowIfFatal(code);
            return std::string(buffer, realSize);
        }

        /*!
         * Requires fetchinig options (from TClientSettings or Qloud/YP/tvmtool settings)
         * Can throw exception if cache is invalid or wrong config
         * @param[in] dst
         */
        std::string GetServiceTicketFor(TTvmId dst) {
            char buffer[512];
            size_t realSize = 0;
            TA_EErrorCode code = TA_TvmClient_GetServiceTicketForTvmId(Ptr.get(), dst, sizeof(buffer), buffer, &realSize);
            if (code == TA_EC_SMALL_BUFFER) {
                std::string res(realSize, 0);
                ThrowIfFatal(TA_TvmClient_GetServiceTicketForTvmId(Ptr.get(), dst, realSize, (char*)res.data(), &realSize));
                return res;
            }
            ThrowIfFatal(code);
            return std::string(buffer, realSize);
        }

    private:
        std::unique_ptr<TA_TTvmClient, decltype(&TA_TvmClient_Delete)> Ptr;
    };
}

#endif
