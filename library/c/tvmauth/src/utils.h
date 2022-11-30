#pragma once

#include <library/c/tvmauth/high_lvl_client.h>
#include <library/c/tvmauth/tvmauth.h>

#include <library/cpp/tvmauth/ticket_status.h>
#include <library/cpp/tvmauth/client/misc/async_updater.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>
#include <library/cpp/tvmauth/client/misc/tool/threaded_updater.h>
#include <library/cpp/tvmauth/src/utils.h>

#include <string>

namespace NTvmAuth {
    class TTvmClient;
}

namespace NTvmAuthC::NUtils {
    inline TA_EErrorCode CppErrorCodeToC(NTvmAuth::ETicketStatus cppCode) {
        switch (cppCode) {
            case NTvmAuth::ETicketStatus::Ok:
                return TA_EC_OK;
            case NTvmAuth::ETicketStatus::Expired:
                return TA_EC_EXPIRED_TICKET;
            case NTvmAuth::ETicketStatus::InvalidBlackboxEnv:
                return TA_EC_INVALID_BLACKBOX_ENV;
            case NTvmAuth::ETicketStatus::InvalidDst:
                return TA_EC_INVALID_DST;
            case NTvmAuth::ETicketStatus::InvalidTicketType:
                return TA_EC_INVALID_TICKET_TYPE;
            case NTvmAuth::ETicketStatus::Malformed:
                return TA_EC_MALFORMED_TICKET;
            case NTvmAuth::ETicketStatus::MissingKey:
                return TA_EC_MISSING_KEY;
            case NTvmAuth::ETicketStatus::SignBroken:
                return TA_EC_SIGN_BROKEN;
            case NTvmAuth::ETicketStatus::UnsupportedVersion:
                return TA_EC_UNSUPPORTED_VERSION;
            default:
                return TA_EC_UNEXPECTED_ERROR;
        }
    }

    inline NTvmAuth::NTvmTool::TClientSettings* Translate(TA_TTvmToolClientSettings* p) {
        return reinterpret_cast<NTvmAuth::NTvmTool::TClientSettings*>(p);
    }
    inline const NTvmAuth::NTvmTool::TClientSettings* Translate(const TA_TTvmToolClientSettings* p) {
        return reinterpret_cast<const NTvmAuth::NTvmTool::TClientSettings*>(p);
    }

    inline TA_TTvmToolClientSettings* Translate(NTvmAuth::NTvmTool::TClientSettings* p) {
        return reinterpret_cast<TA_TTvmToolClientSettings*>(p);
    }

    inline NTvmAuth::NTvmApi::TClientSettings* Translate(TA_TTvmApiClientSettings* p) {
        return reinterpret_cast<NTvmAuth::NTvmApi::TClientSettings*>(p);
    }
    inline const NTvmAuth::NTvmApi::TClientSettings* Translate(const TA_TTvmApiClientSettings* p) {
        return reinterpret_cast<const NTvmAuth::NTvmApi::TClientSettings*>(p);
    }

    inline TA_TTvmApiClientSettings* Translate(NTvmAuth::NTvmApi::TClientSettings* p) {
        return reinterpret_cast<TA_TTvmApiClientSettings*>(p);
    }

    inline NTvmAuth::TTvmClient* Translate(TA_TTvmClient* p) {
        return reinterpret_cast<NTvmAuth::TTvmClient*>(p);
    }
    inline const NTvmAuth::TTvmClient* Translate(const TA_TTvmClient* p) {
        return reinterpret_cast<const NTvmAuth::TTvmClient*>(p);
    }

    inline TA_TTvmClient* Translate(NTvmAuth::TTvmClient* p) {
        return reinterpret_cast<TA_TTvmClient*>(p);
    }

    inline TA_TCheckedUserTicket* Translate(NTvmAuth::TCheckedUserTicket::TImpl* p) {
        return reinterpret_cast<TA_TCheckedUserTicket*>(p);
    }
    inline NTvmAuth::TCheckedUserTicket::TImpl* Translate(TA_TCheckedUserTicket* p) {
        return reinterpret_cast<NTvmAuth::TCheckedUserTicket::TImpl*>(p);
    }

    inline TA_TCheckedServiceTicket* Translate(NTvmAuth::TCheckedServiceTicket::TImpl* p) {
        return reinterpret_cast<TA_TCheckedServiceTicket*>(p);
    }
    inline NTvmAuth::TCheckedServiceTicket::TImpl* Translate(TA_TCheckedServiceTicket* p) {
        return reinterpret_cast<NTvmAuth::TCheckedServiceTicket::TImpl*>(p);
    }

    inline TA_TTvmClientStatus* Translate(NTvmAuth::TClientStatus* p) {
        return reinterpret_cast<TA_TTvmClientStatus*>(p);
    }
    inline const TA_TTvmClientStatus* Translate(const NTvmAuth::TClientStatus* p) {
        return reinterpret_cast<const TA_TTvmClientStatus*>(p);
    }
    inline NTvmAuth::TClientStatus* Translate(TA_TTvmClientStatus* p) {
        return reinterpret_cast<NTvmAuth::TClientStatus*>(p);
    }
    inline const NTvmAuth::TClientStatus* Translate(const TA_TTvmClientStatus* p) {
        return reinterpret_cast<const NTvmAuth::TClientStatus*>(p);
    }
}
