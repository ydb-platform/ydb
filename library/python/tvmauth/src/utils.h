#pragma once

#include <library/cpp/tvmauth/version.h>
#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>
#include <library/cpp/tvmauth/client/misc/tool/settings.h>

#include <util/system/getpid.h>

namespace NTvmAuthPy {
    class TPidCheckedClient: public NTvmAuth::TTvmClient {
    public:
        using TTvmClient::TTvmClient;

        TString GetServiceTicketFor(const NTvmAuth::TClientSettings::TAlias& dst) const {
            pid_.check();
            return TTvmClient::GetServiceTicketFor(dst);
        }

        TString GetServiceTicketFor(const NTvmAuth::TTvmId dst) const {
            pid_.check();
            return TTvmClient::GetServiceTicketFor(dst);
        }

        NTvmAuth::TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) const {
            pid_.check();
            return TTvmClient::CheckServiceTicket(ticket);
        }

        NTvmAuth::TCheckedUserTicket CheckUserTicket(TStringBuf ticket) const {
            pid_.check();
            return TTvmClient::CheckUserTicket(ticket);
        }

        NTvmAuth::TCheckedUserTicket CheckUserTicketWithOveridedEnv(TStringBuf ticket, NTvmAuth::EBlackboxEnv env) const {
            pid_.check();
            return TTvmClient::CheckUserTicket(ticket, env);
        }

        NTvmAuth::NRoles::TRolesPtr GetRoles() const {
            pid_.check();
            return TTvmClient::GetRoles();
        }

    private:
        struct TPidCheck {
            TPidCheck()
                : pid_(GetPID())
            {
            }

            void check() const {
                const TProcessId pid = GetPID();
                Y_ENSURE_EX(pid == pid_,
                            NTvmAuth::TNonRetriableException()
                                << "Creating TvmClient is forbidden before fork. Original pid: " << pid_
                                << ". Current pid: " << pid);
            }

        private:
            const TProcessId pid_;
        } const pid_;
    };

    template <typename T>
    T&& Move(T& d) {
        return std::move(d);
    }

    template <typename T>
    THolder<T> ToHeap(T& t) {
        return MakeHolder<T>(std::move(t));
    }

    THolder<NTvmAuth::TServiceContext> CheckingFactory(NTvmAuth::TTvmId selfTvmId, TStringBuf tvmKeysResponse) {
        return MakeHolder<NTvmAuth::TServiceContext>(
            NTvmAuth::TServiceContext::CheckingFactory(selfTvmId, tvmKeysResponse));
    }

    THolder<NTvmAuth::TServiceContext> SigningFactory(TStringBuf secretBase64) {
        return MakeHolder<NTvmAuth::TServiceContext>(
            NTvmAuth::TServiceContext::SigningFactory(secretBase64));
    }

    TString GetServiceTicketForId(const TPidCheckedClient& cl, NTvmAuth::TTvmId dst) {
        return cl.GetServiceTicketFor(dst);
    }

    TPidCheckedClient* CreateTvmApiClient(NTvmAuth::NTvmApi::TClientSettings& s, NTvmAuth::TLoggerPtr logger) {
        s.LibVersionPrefix = "py_";
        return new TPidCheckedClient(s, logger);
    }

    class TTvmToolClientSettings: public NTvmAuth::NTvmTool::TClientSettings {
    public:
        using TClientSettings::TClientSettings;
    };

    TPidCheckedClient* CreateTvmToolClient(const TTvmToolClientSettings& s, NTvmAuth::TLoggerPtr logger) {
        return new TPidCheckedClient(s, logger);
    }

    TString GetPyVersion() {
        return TString("py_") + NTvmAuth::LibVersion();
    }

    using TOptUid = std::optional<NTvmAuth::TUid>;
}
