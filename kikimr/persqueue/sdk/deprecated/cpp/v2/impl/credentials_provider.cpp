#include <kikimr/persqueue/sdk/deprecated/cpp/v2/credentials_provider.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include "internals.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/logger/priority.h>
#include <library/cpp/tvmauth/client/facade.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/mutex.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <kikimr/public/sdk/cpp/client/iam/iam.h>

namespace NPersQueue {

TString GetToken(ICredentialsProvider* credentials) {
    NPersQueue::TCredentials auth;
    credentials->FillAuthInfo(&auth);
    TString token;
    switch (auth.GetCredentialsCase()) {
    case NPersQueue::TCredentials::kTvmServiceTicket:
        token = auth.GetTvmServiceTicket();
        break;
    case NPersQueue::TCredentials::kOauthToken:
        token = auth.GetOauthToken();
        break;
    case NPersQueue::TCredentials::CREDENTIALS_NOT_SET:
        break;
    default:
        Y_VERIFY(true, "Unknown Credentials case.");
    }
    return token;
}

const TString& GetTvmPqServiceName() {
    static const TString PQ_SERVICE_NAME = "pq";
    return PQ_SERVICE_NAME;
}

class TInsecureCredentialsProvider : public ICredentialsProvider {
    void FillAuthInfo(NPersQueue::TCredentials*) const override {}
};

class TLogBridge: public NTvmAuth::ILogger {
public:
    TLogBridge(TIntrusivePtr<NPersQueue::ILogger> logger = nullptr)
        : Logger(logger)
    {}

    void Log(int lvl, const TString& msg) override {
        if (Logger) {
            Logger->Log(msg, "", "", lvl);
        }
    }

private:
    TIntrusivePtr<NPersQueue::ILogger> Logger;
};

class TTVMCredentialsProvider : public ICredentialsProvider {
public:
    TTVMCredentialsProvider(const NTvmAuth::NTvmApi::TClientSettings& settings, const TString& alias, TIntrusivePtr<ILogger> logger = nullptr)
        : Alias(alias)
    {
       if (!settings.FetchServiceTicketsForDstsWithAliases.contains(Alias)) {
           ythrow yexception() << "alias for `" << Alias << "` must be set";
       }
       Logger = MakeIntrusive<TLogBridge>(std::move(logger));
       TvmClient = std::make_shared<NTvmAuth::TTvmClient>(settings, Logger);
    }

    TTVMCredentialsProvider(const TString& secret, const ui32 srcClientId, const ui32 dstClientId, const TString& alias, TIntrusivePtr<ILogger> logger = nullptr)
        : TTVMCredentialsProvider(CreateSettings(secret, srcClientId, dstClientId, alias), alias, logger)
    {}

    TTVMCredentialsProvider(std::shared_ptr<NTvmAuth::TTvmClient> tvmClient, const TString& alias, TIntrusivePtr<ILogger> logger = nullptr)
        : TvmClient(tvmClient)
        , Alias(alias)
        , Logger(MakeIntrusive<TLogBridge>(std::move(logger)))
    {}

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const override {
        try {
            auto status = TvmClient->GetStatus();
            if (status == NTvmAuth::TClientStatus::Ok) {
                authInfo->SetTvmServiceTicket(TvmClient->GetServiceTicketFor(Alias));
            } else {
                Logger->Error(TStringBuilder() << "Can't get ticket: " << status.GetLastError() << "\n");
            }
        } catch (...) {
            Logger->Error(TStringBuilder() << "Can't get ticket: " << CurrentExceptionMessage() << "\n");
        }
    }

private:
    std::shared_ptr<NTvmAuth::TTvmClient> TvmClient;
    TString Alias;
    NTvmAuth::TLoggerPtr Logger;

    static NTvmAuth::NTvmApi::TClientSettings CreateSettings(const TString& secret, const ui32 srcClientId, const ui32 dstClientId, const TString& alias) {
       NTvmAuth::NTvmApi::TClientSettings settings;
       settings.SetSelfTvmId(srcClientId);
       settings.EnableServiceTicketsFetchOptions(secret, {{alias, dstClientId}});
       return settings;
    }
};

class TTVMQloudCredentialsProvider : public ICredentialsProvider {
public:
    TTVMQloudCredentialsProvider(const TString& srcAlias, const TString& dstAlias, const TDuration& refreshPeriod, TIntrusivePtr<ILogger> logger, ui32 port)
        : HttpClient(TSimpleHttpClient("localhost", port))
        , Request(TStringBuilder() << "/tvm/tickets?src=" << CGIEscapeRet(srcAlias) << "&dsts=" << CGIEscapeRet(dstAlias))
        , DstAlias(dstAlias)
        , LastTicketUpdate(TInstant::Zero())
        , RefreshPeriod(refreshPeriod)
        , Logger(std::move(logger))
        , DstId(0)
    {
        GetTicket();
    }

    TTVMQloudCredentialsProvider(const ui32 srcId, const ui32 dstId, const TDuration& refreshPeriod, TIntrusivePtr<ILogger> logger, ui32 port)
            : HttpClient(TSimpleHttpClient("localhost", port))
            , Request(TStringBuilder() << "/tvm/tickets?src=" << srcId << "&dsts=" << dstId)
            , LastTicketUpdate(TInstant::Zero())
            , RefreshPeriod(refreshPeriod)
            , Logger(std::move(logger))
            , DstId(dstId)
    {
        GetTicket();
    }

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const override {
        if (TInstant::Now() > LastTicketUpdate + RefreshPeriod) {
            GetTicket();
        }

        authInfo->SetTvmServiceTicket(Ticket);
    }

private:
    TSimpleHttpClient HttpClient;
    TString Request;
    TString DstAlias;
    mutable TString Ticket;
    mutable TInstant LastTicketUpdate;
    TDuration RefreshPeriod;
    TIntrusivePtr<ILogger> Logger;
    ui32 DstId;

    void GetTicket() const {
        try {
            TStringStream out;
            TSimpleHttpClient::THeaders headers;
            headers["Authorization"] = GetEnv("QLOUD_TVM_TOKEN");
            HttpClient.DoGet(Request, &out, headers);
            NJson::TJsonValue resp;
            NJson::ReadJsonTree(&out, &resp, true);
            TString localDstAlias = DstAlias;
            Y_VERIFY(!DstAlias.empty() || DstId != 0);
            Y_VERIFY(resp.GetMap().size() == 1);
            if (!localDstAlias.empty()) {
                if (!resp.Has(localDstAlias)) {
                    ythrow yexception() << "Result doesn't contain dstAlias `" << localDstAlias << "`";
                }
            }
            else {
                for (const auto &[k, v] : resp.GetMap()) {
                    if (!v.Has("tvm_id") || v["tvm_id"].GetIntegerSafe() != DstId)
                        ythrow yexception() << "Result doesn't contain dstId `" << DstId << "`";
                    localDstAlias = k;
                }
            }
            TString ticket = resp[localDstAlias]["ticket"].GetStringSafe();
            if (ticket.empty()) {
                ythrow yexception() << "Got empty ticket";
            }
            Ticket = ticket;
            LastTicketUpdate = TInstant::Now();
        } catch (...) {
            if (Logger) {
                Logger->Log(TStringBuilder() << "Can't get ticket: " << CurrentExceptionMessage() << "\n", "", "", TLOG_ERR);
            }
        }
    }
};

class TOAuthCredentialsProvider : public ICredentialsProvider {
public:
    TOAuthCredentialsProvider(const TString& token)
        : Token(token)
    {}

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const override {
        authInfo->SetOauthToken(Token);
    }

private:
    TString Token;
};

class TTVMCredentialsForwarder : public ITVMCredentialsForwarder {
public:
    TTVMCredentialsForwarder() = default;

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const override {
        TGuard<TSpinLock> guard(Lock);
        if (!Ticket.empty())
            authInfo->SetTvmServiceTicket(Ticket);
    }

    void SetTVMServiceTicket(const TString& ticket) override {
        TGuard<TSpinLock> guard(Lock);
        Ticket = ticket;
    }


private:
    TString Ticket;
    TSpinLock Lock;
};

class IIAMCredentialsProviderWrapper : public ICredentialsProvider {
public:
    IIAMCredentialsProviderWrapper(std::shared_ptr<NYdb::ICredentialsProviderFactory> factory, TIntrusivePtr<ILogger> logger = nullptr)
        : Provider(factory->CreateProvider())
        , Logger(std::move(logger))
        , Lock()
    {
    }

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const override {
        TString ticket;
        try {
            with_lock(Lock) {
                ticket = Provider->GetAuthInfo();
            }
        } catch (...) {
            if (Logger) {
                Logger->Log(TStringBuilder() << "Can't get ticket: " << CurrentExceptionMessage() << "\n", "", "", TLOG_ERR);
            }
            return;
        }

        authInfo->SetTvmServiceTicket(ticket);
    }

private:
    std::shared_ptr<NYdb::ICredentialsProvider> Provider;
    TIntrusivePtr<ILogger> Logger;
    TMutex Lock;
};

class TIAMCredentialsProviderWrapper : public IIAMCredentialsProviderWrapper {
public:
    TIAMCredentialsProviderWrapper(TIntrusivePtr<ILogger> logger = nullptr)
        : IIAMCredentialsProviderWrapper(
            NYdb::CreateIamCredentialsProviderFactory(),
            std::move(logger)
        )
    {
    }
};

class TIAMJwtFileCredentialsProviderWrapper : public IIAMCredentialsProviderWrapper {
public:
    TIAMJwtFileCredentialsProviderWrapper(
        const TString& jwtKeyFilename, const TString& endpoint, const TDuration& refreshPeriod,
        const TDuration& requestTimeout, TIntrusivePtr<ILogger> logger = nullptr
    )
        : IIAMCredentialsProviderWrapper(
            NYdb::CreateIamJwtFileCredentialsProviderFactory(
                {{endpoint, refreshPeriod, requestTimeout}, jwtKeyFilename}),
            std::move(logger)
        )
    {}
};

class TIAMJwtParamsCredentialsProviderWrapper : public IIAMCredentialsProviderWrapper {
public:
    TIAMJwtParamsCredentialsProviderWrapper(
        const TString& jwtParams, const TString& endpoint, const TDuration& refreshPeriod,
        const TDuration& requestTimeout, TIntrusivePtr<ILogger> logger = nullptr
    )
        : IIAMCredentialsProviderWrapper(
            NYdb::CreateIamJwtParamsCredentialsProviderFactory(
                {{endpoint, refreshPeriod, requestTimeout}, jwtParams}),
            std::move(logger)
        )
    {}
};

std::shared_ptr<ICredentialsProvider> CreateInsecureCredentialsProvider() {
    return std::make_shared<TInsecureCredentialsProvider>();
}

std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(const TString& secret, const ui32 srcClientId, const ui32 dstClientId, TIntrusivePtr<ILogger> logger) {
    return std::make_shared<TTVMCredentialsProvider>(secret, srcClientId, dstClientId, GetTvmPqServiceName(), std::move(logger));
}

std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(const NTvmAuth::NTvmApi::TClientSettings& settings, TIntrusivePtr<ILogger> logger, const TString& alias) {
    return std::make_shared<TTVMCredentialsProvider>(settings, alias, std::move(logger));
}

std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(std::shared_ptr<NTvmAuth::TTvmClient> tvmClient, TIntrusivePtr<ILogger> logger, const TString& alias) {
    return std::make_shared<TTVMCredentialsProvider>(tvmClient, alias, std::move(logger));
}

std::shared_ptr<ICredentialsProvider> CreateTVMQloudCredentialsProvider(const TString& srcAlias, const TString& dstAlias, TIntrusivePtr<ILogger> logger, const TDuration refreshPeriod, ui32 port) {
    return std::make_shared<TTVMQloudCredentialsProvider>(srcAlias, dstAlias, refreshPeriod, std::move(logger), port);
}

std::shared_ptr<ICredentialsProvider> CreateTVMQloudCredentialsProvider(const ui32 srcId, const ui32 dstId, TIntrusivePtr<ILogger> logger, const TDuration refreshPeriod, ui32 port) {
    return std::make_shared<TTVMQloudCredentialsProvider>(srcId, dstId, refreshPeriod, std::move(logger), port);
}

std::shared_ptr<ICredentialsProvider> CreateOAuthCredentialsProvider(const TString& token) {
    return std::make_shared<TOAuthCredentialsProvider>(token);
}

std::shared_ptr<ITVMCredentialsForwarder> CreateTVMCredentialsForwarder() {
    return std::make_shared<TTVMCredentialsForwarder>();
}

std::shared_ptr<ICredentialsProvider> CreateIAMCredentialsForwarder(TIntrusivePtr<ILogger> logger) {
    return std::make_shared<TIAMCredentialsProviderWrapper>(logger);
}

std::shared_ptr<ICredentialsProvider> CreateIAMJwtFileCredentialsForwarder(
        const TString& jwtKeyFilename, TIntrusivePtr<ILogger> logger,
        const TString& endpoint, const TDuration& refreshPeriod, const TDuration& requestTimeout
) {
    return std::make_shared<TIAMJwtFileCredentialsProviderWrapper>(jwtKeyFilename, endpoint,
                                                               refreshPeriod, requestTimeout, logger);
}

std::shared_ptr<ICredentialsProvider> CreateIAMJwtParamsCredentialsForwarder(
        const TString& jwtParams, TIntrusivePtr<ILogger> logger,
        const TString& endpoint, const TDuration& refreshPeriod, const TDuration& requestTimeout
) {
    return std::make_shared<TIAMJwtParamsCredentialsProviderWrapper>(jwtParams, endpoint,
                                                               refreshPeriod, requestTimeout, logger);
}

} // namespace NPersQueue
