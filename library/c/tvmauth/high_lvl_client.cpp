// DO_NOT_STYLE
#include "high_lvl_client.h"

#include "tvmauth.h"
#include "src/exception.h"
#include "src/logger.h"
#include "src/utils.h"

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/settings.h>

using namespace NTvmAuth;
using namespace NTvmAuthC;

void TA_NoopLogger(int, const char*) {
}

TA_EErrorCode TA_TvmToolClientSettings_Create(
    const char* alias,
    size_t aliasSize,
    TA_TTvmToolClientSettings** settings) {
    if (alias == nullptr ||
        aliasSize == 0 ||
        settings == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        *settings = NTvmAuthC::NUtils::Translate(new NTvmTool::TClientSettings(TClientSettings::TAlias(alias, aliasSize)));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmToolClientSettings_Delete(
    TA_TTvmToolClientSettings* settings) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(settings);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmToolClientSettings_SetPort(
    TA_TTvmToolClientSettings* settings,
    uint16_t port) {
    if (settings == nullptr ||
        port == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->SetPort(port);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmToolClientSettings_SetHostname(
    TA_TTvmToolClientSettings* settings,
    const char* hostname,
    size_t hostnameSize) {
    if (settings == nullptr ||
        hostname == nullptr ||
        hostnameSize == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->SetHostname(TString(hostname, hostnameSize));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmToolClientSettings_SetAuthToken(
    TA_TTvmToolClientSettings* settings,
    const char* authtoken,
    size_t authtokenSize) {
    if (settings == nullptr ||
        authtoken == nullptr ||
        authtokenSize == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->SetAuthToken(TString(authtoken, authtokenSize));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmToolClientSettings_OverrideBlackboxEnv(
    TA_TTvmToolClientSettings* settings,
    TA_EBlackboxEnv env) {
    if (settings == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->OverrideBlackboxEnv(NTvmAuth::EBlackboxEnv(int(env)));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_Create(
    TA_TTvmApiClientSettings** settings) {
    if (settings == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        *settings = NTvmAuthC::NUtils::Translate(new NTvmApi::TClientSettings);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_Delete(
    TA_TTvmApiClientSettings* settings) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(settings);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_SetSelfTvmId(
    TA_TTvmApiClientSettings* settings,
    uint32_t selfTvmId) {
    if (settings == nullptr ||
        selfTvmId == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->SelfTvmId = selfTvmId;
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketChecking(
    TA_TTvmApiClientSettings* settings) {
    if (settings == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->CheckServiceTickets = true;
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_EnableUserTicketChecking(
    TA_TTvmApiClientSettings* settings,
    TA_EBlackboxEnv env) {
    if (settings == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->CheckUserTicketsWithBbEnv = NTvmAuth::EBlackboxEnv(int(env));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases(
    TA_TTvmApiClientSettings* settings,
    const char* selfSecret,
    size_t selfSecretSize,
    const char* dsts,
    size_t dstsSize) {
    if (settings == nullptr ||
        selfSecret == nullptr ||
        selfSecretSize == 0 ||
        dsts == nullptr ||
        dstsSize == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmApi::TClientSettings& s = *NTvmAuthC::NUtils::Translate(settings);
        s.Secret = TStringBuf(selfSecret, selfSecretSize);
        s.FetchServiceTicketsForDstsWithAliases = NTvmAuth::NUtils::ParseDstMap(TStringBuf(dsts, dstsSize));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds(
    TA_TTvmApiClientSettings* settings,
    const char* selfSecret,
    size_t selfSecretSize,
    const char* dsts,
    size_t dstsSize) {
    if (settings == nullptr ||
        selfSecret == nullptr ||
        selfSecretSize == 0 ||
        dsts == nullptr ||
        dstsSize == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmApi::TClientSettings& s = *NTvmAuthC::NUtils::Translate(settings);
        s.Secret = TStringBuf(selfSecret, selfSecretSize);
        s.FetchServiceTicketsForDsts = NTvmAuth::NUtils::ParseDstVector(TStringBuf(dsts, dstsSize));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmApiClientSettings_SetDiskCacheDir(
    TA_TTvmApiClientSettings* settings,
    const char* path,
    size_t pathSize) {
    if (settings == nullptr ||
        path == nullptr ||
        pathSize == 0) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        NTvmAuthC::NUtils::Translate(settings)->DiskCacheDir = TString(path, pathSize);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_CreateForTvmtool(
    const TA_TTvmToolClientSettings* settings,
    TA_TLoggerFunc logger,
    TA_TTvmClient** client) {
    if (settings == nullptr ||
        logger == nullptr ||
        client == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        // TODO: drop copy: PASSP-37079
        // We need to disable roles logic: client doesn't allow to use it correctly
        NTvmTool::TClientSettings settingsCopy = *NTvmAuthC::NUtils::Translate(settings);
        settingsCopy.ShouldCheckSrc = false;
        settingsCopy.ShouldCheckDefaultUid = false;

        *client = NTvmAuthC::NUtils::Translate(new TTvmClient(
                                        settingsCopy,
                                        MakeIntrusive<TLoggerC>(logger)));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_Create(
    const TA_TTvmApiClientSettings* settings,
    TA_TLoggerFunc logger,
    TA_TTvmClient** client) {
    if (settings == nullptr ||
        logger == nullptr ||
        client == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        *client = NTvmAuthC::NUtils::Translate(new TTvmClient(
                                        *NTvmAuthC::NUtils::Translate(settings),
                                        MakeIntrusive<TLoggerC>(logger)));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_Delete(
    TA_TTvmClient* client) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(client);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_CheckServiceTicket(
    const TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    TA_TCheckedServiceTicket** serviceTicket) {
    if (client == nullptr ||
        ticket == nullptr ||
        ticketSize == 0 ||
        serviceTicket == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        TCheckedServiceTicket t = NTvmAuthC::NUtils::Translate(client)->CheckServiceTicket(TStringBuf(ticket, ticketSize));
        ETicketStatus s = t.GetStatus();

        *serviceTicket = NTvmAuthC::NUtils::Translate(NTvmAuth::NInternal::TCanningKnife::GetS(t));
        return NTvmAuthC::NUtils::CppErrorCodeToC(s);
    });
}

TA_EErrorCode TA_TvmClient_CheckUserTicket(
    const TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    TA_TCheckedUserTicket** userTicket) {
    if (client == nullptr ||
        ticket == nullptr ||
        ticketSize == 0 ||
        userTicket == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        TCheckedUserTicket t = NTvmAuthC::NUtils::Translate(client)->CheckUserTicket(TStringBuf(ticket, ticketSize));
        ETicketStatus s = t.GetStatus();

        *userTicket = NTvmAuthC::NUtils::Translate(NTvmAuth::NInternal::TCanningKnife::GetU(t));
        return NTvmAuthC::NUtils::CppErrorCodeToC(s);
    });
}

TA_EErrorCode TA_TvmClient_CheckUserTicketWithOverridedEnv(
    const TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    enum TA_EBlackboxEnv env,
    TA_TCheckedUserTicket** userTicket) {
    if (client == nullptr ||
        ticket == nullptr ||
        ticketSize == 0 ||
        userTicket == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        TCheckedUserTicket t = NTvmAuthC::NUtils::Translate(client)->CheckUserTicket(TStringBuf(ticket, ticketSize), NTvmAuth::EBlackboxEnv(int(env)));
        ETicketStatus s = t.GetStatus();

        *userTicket = NTvmAuthC::NUtils::Translate(NTvmAuth::NInternal::TCanningKnife::GetU(t));
        return NTvmAuthC::NUtils::CppErrorCodeToC(s);
    });
}

TA_EErrorCode TA_TvmClient_GetServiceTicketForAlias(
    const TA_TTvmClient* client,
    const char* dst,
    size_t dstSize,
    size_t maxTicketSize,
    char* ticket,
    size_t* ticketSize) {
    if (client == nullptr ||
        dst == nullptr ||
        dstSize == 0 ||
        maxTicketSize == 0 ||
        ticket == nullptr ||
        ticketSize == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        TString t = NTvmAuthC::NUtils::Translate(client)->GetServiceTicketFor(TClientSettings::TAlias(dst, dstSize));

        (*ticketSize) = t.size();
        if (maxTicketSize < *ticketSize) {
            return TA_EC_SMALL_BUFFER;
        }
        strcpy(ticket, t.c_str());

        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_GetServiceTicketForTvmId(
    const TA_TTvmClient* client,
    uint32_t dst,
    size_t maxTicketSize,
    char* ticket,
    size_t* ticketSize) {
    if (client == nullptr ||
        dst == 0 ||
        maxTicketSize == 0 ||
        ticket == nullptr ||
        ticketSize == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        TString t = NTvmAuthC::NUtils::Translate(client)->GetServiceTicketFor(dst);

        (*ticketSize) = t.size();
        if (maxTicketSize < *ticketSize) {
            return TA_EC_SMALL_BUFFER;
        }
        strcpy(ticket, t.c_str());

        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_GetStatus(
    const TA_TTvmClient* client,
    TA_TTvmClientStatus** status) {
    if (client == nullptr ||
        status == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        std::unique_ptr ret = std::make_unique<TClientStatus>(NTvmAuthC::NUtils::Translate(client)->GetStatus());
        *status = NTvmAuthC::NUtils::Translate(ret.release());
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_DeleteStatus(
    TA_TTvmClientStatus* status) {
    if (status == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(status);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_Status_GetCode(
    const TA_TTvmClientStatus* status,
    TA_ETvmClientStatusCode* code) {
    if (status == nullptr ||
        code == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        *code = (TA_ETvmClientStatusCode)NTvmAuthC::NUtils::Translate(status)->GetCode();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_TvmClient_Status_GetLastError(
    const TA_TTvmClientStatus* status,
    const char** lastError,
    size_t* lastErrorSize) {
    if (status == nullptr ||
        lastError == nullptr ||
        lastErrorSize == nullptr) {
        return TA_EC_INVALID_PARAM;
    }

    return CatchExceptions([=]() -> TA_EErrorCode {
        const TString& s = NTvmAuthC::NUtils::Translate(status)->GetLastError();

        *lastError = s.c_str();
        *lastErrorSize = s.size();
        return TA_EC_OK;
    });
}
