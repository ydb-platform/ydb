#pragma once

#include <library/cpp/tvmauth/version.h>
#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>
#include <library/cpp/tvmauth/client/misc/tool/settings.h>

#include <util/system/getpid.h>

namespace NTvmAuth {
    class TPidCheckedClient: public TTvmClient {
    public:
        using TTvmClient::TTvmClient;

        TString GetServiceTicketFor(const TClientSettings::TAlias& dst) const {
            pid_.check();
            return TTvmClient::GetServiceTicketFor(dst);
        }

        TString GetServiceTicketFor(const TTvmId dst) const {
            pid_.check();
            return TTvmClient::GetServiceTicketFor(dst);
        }

        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) const {
            pid_.check();
            return TTvmClient::CheckServiceTicket(ticket);
        }

        TCheckedUserTicket CheckUserTicket(TStringBuf ticket) const {
            pid_.check();
            return TTvmClient::CheckUserTicket(ticket);
        }

        TCheckedUserTicket CheckUserTicketWithOveridedEnv(TStringBuf ticket, EBlackboxEnv env) const {
            pid_.check();
            return TTvmClient::CheckUserTicket(ticket, env);
        }

        static TStringBuf StatusToString(TClientStatus::ECode s) {
            switch (s) {
                case TClientStatus::Ok:
                    return "TvmClient cache is ok";
                case TClientStatus::Warning:
                    return "Normal operation of TvmClient is still possible but there are problems with refreshing cache "
                           "so it is expiring; "
                           "is tvm-api.yandex.net accessible? "
                           "have you changed your TVM-secret or your backend (dst) deleted its TVM-client?";
                case TClientStatus::Error:
                    return "TvmClient cache is already invalid (expired) or soon will be: "
                           "you can't check valid ServiceTicket or be authenticated by your backends (dsts)";
                case TClientStatus::IncompleteTicketsSet:
                    return "TvmClient cant fetch some of your tickets, this should not happen. ";
            }

            return "impossible case";
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
                            TNonRetriableException()
                                << "Creating TvmClient is forbidden before fork. Original pid: " << pid_
                                << ". Current pid: " << pid);
            }

        private:
            const TProcessId pid_;
        } const pid_;
    };

    TString GetServiceTicketForId(const TPidCheckedClient& cl, TTvmId dst) {
        return cl.GetServiceTicketFor(dst);
    }

    class TCustomUpdater: public NTvmApi::TThreadedUpdater {
    public:
        TCustomUpdater(const NTvmApi::TClientSettings& settings, TLoggerPtr logger)
            : TThreadedUpdater(settings, logger)
        {
            WorkerAwakingPeriod_ = TDuration::MilliSeconds(100);
            PublicKeysDurations_.RefreshPeriod = TDuration::MilliSeconds(100);
            Init();
            StartWorker();
        }
    };

    TPidCheckedClient* CreateTvmApiClient(NTvmApi::TClientSettings& s, TLoggerPtr logger) {
        s.LibVersionPrefix = "py_";
        Y_ENSURE(s.IsIncompleteTicketsSetAnError, "incomplete tickets set is not supported in ticket_parser2");
        return new TPidCheckedClient(s, logger);
    }

    class TTvmToolClientSettings: public NTvmTool::TClientSettings {
    public:
        using TClientSettings::TClientSettings;
    };

    TPidCheckedClient* CreateTvmToolClient(const TTvmToolClientSettings& s, TLoggerPtr logger) {
        // We need to disable roles logic: client doesn't allow to use it correctly
        NTvmTool::TClientSettings settingsCopy = s;
        settingsCopy.ShouldCheckSrc = false;
        settingsCopy.ShouldCheckDefaultUid = false;

        return new TPidCheckedClient(settingsCopy, logger);
    }

    TString GetPyVersion() {
        return TString("py_") + LibVersion();
    }

    void StartTvmClientStopping(TPidCheckedClient* cl) {
        NInternal::TClientCaningKnife::StartTvmClientStopping(cl);
    }

    bool IsTvmClientStopped(TPidCheckedClient* cl) {
        return NInternal::TClientCaningKnife::IsTvmClientStopped(cl);
    }

    void DestroyTvmClient(TPidCheckedClient* cl) {
        delete cl;
    }
}
