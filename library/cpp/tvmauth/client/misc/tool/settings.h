#pragma once

#include <library/cpp/tvmauth/client/misc/settings.h>

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_user_ticket.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NTvmAuth::NTvmTool {
    /**
     * Uses local http-interface to get state: http://localhost/tvm/.
     * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
     * See more: https://wiki.yandex-team.ru/passport/tvm2/qloud/.
     *
     * Most part of settings will be fetched from tvmtool on start of client.
     * You need to use aliases for TVM-clients (src and dst) which you specified in tvmtool or Qloud/YP interface
     */
    class TClientSettings: public NTvmAuth::TClientSettings {
    public:
        /*!
         * Sets default values:
         * - hostname == "localhost"
         * - port detected with env["DEPLOY_TVM_TOOL_URL"] (provided with Yandex.Deploy),
         *      otherwise port == 1 (it is ok for Qloud)
         * - authToken: env["TVMTOOL_LOCAL_AUTHTOKEN"] (provided with Yandex.Deploy),
         *      otherwise env["QLOUD_TVM_TOKEN"] (provided with Qloud)
         *
         * AuthToken is protection from SSRF.
         *
         * @param selfAias - alias for your TVM client, which you specified in tvmtool or YD interface
         */
        TClientSettings(const TAlias& selfAias);

        /*!
         * Look at comment for ctor
         * @param port
         */
        TClientSettings& SetPort(ui16 port) {
            Port_ = port;
            return *this;
        }

        /*!
         * Default value: hostname == "localhost"
         * @param hostname
         */
        TClientSettings& SetHostname(const TString& hostname) {
            Y_ENSURE_EX(hostname, TBrokenTvmClientSettings() << "Hostname cannot be empty");
            Hostname_ = hostname;
            return *this;
        }

        TClientSettings& SetSocketTimeout(TDuration socketTimeout) {
            SocketTimeout_ = socketTimeout;
            return *this;
        }

        TClientSettings& SetConnectTimeout(TDuration connectTimeout) {
            ConnectTimeout_ = connectTimeout;
            return *this;
        }

        /*!
         * Look at comment for ctor
         * @param token
         */
        TClientSettings& SetAuthToken(TStringBuf token) {
            FixSpaces(token);
            Y_ENSURE_EX(token, TBrokenTvmClientSettings() << "Auth token cannot be empty");
            AuthToken_ = token;
            return *this;
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
          * @param env
          */
        TClientSettings& OverrideBlackboxEnv(EBlackboxEnv env) {
            BbEnv_ = env;
            return *this;
        }

    public: // for TAsyncUpdaterBase
        const TAlias& GetSelfAlias() const {
            return SelfAias_;
        }

        const TString& GetHostname() const {
            return Hostname_;
        }

        ui16 GetPort() const {
            return Port_;
        }

        TDuration GetSocketTimeout() const {
            return SocketTimeout_;
        }

        TDuration GetConnectTimeout() const {
            return ConnectTimeout_;
        }

        const TString& GetAuthToken() const {
            Y_ENSURE_EX(AuthToken_, TBrokenTvmClientSettings()
                                        << "Auth token cannot be empty. "
                                        << "Env 'TVMTOOL_LOCAL_AUTHTOKEN' and 'QLOUD_TVM_TOKEN' are empty.");
            return AuthToken_;
        }

        TMaybe<EBlackboxEnv> GetOverridedBlackboxEnv() const {
            return BbEnv_;
        }

    private:
        void FixSpaces(TStringBuf& str);

    private:
        TAlias SelfAias_;
        TString Hostname_;
        ui16 Port_;
        TDuration SocketTimeout_;
        TDuration ConnectTimeout_;
        TString AuthToken_;
        TMaybe<EBlackboxEnv> BbEnv_;
    };
}
