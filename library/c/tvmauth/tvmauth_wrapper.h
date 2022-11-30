#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_WRAPPER_H_
#define _TVM_AUTH_WRAPPER_H_

#include "tvmauth.h"

#include <memory>
#include <string>
#include <vector>

namespace NTvmAuthWrapper {
    using TTvmId = uint32_t;
    using TScopes = std::vector<std::string>;
    using TUid = uint64_t;
    using TUids = std::vector<TUid>;

    using TA_EErrorCode = TA_EErrorCode;

    namespace NBlackboxTvmId {
        static const char* Prod = "222";
        static const char* Test = "224";
        static const char* ProdYateam = "223";
        static const char* TestYateam = "225";
        static const char* Stress = "226";
        static const char* Mimino = "239";
    }

    class TTvmException: public std::exception {
    private:
        const char* Message;
    public:
        TTvmException(const char* message)
            : Message(message)
        {
        }
        const char* what() const noexcept override {
            return Message;
        }
    };
    class TContextException: public TTvmException {
        using TTvmException::TTvmException;
    };
    class TEmptyTvmKeysException: public TContextException {
        using TContextException::TContextException;
    };
    class TMalformedTvmKeysException: public TContextException {
        using TContextException::TContextException;
    };
    class TMalformedTvmSecretException: public TContextException {
        using TContextException::TContextException;
    };
    class TNotAllowedException: public TTvmException {
        using TTvmException::TTvmException;
    };

    inline std::string LibVersion() {
        return std::string(TA_LibVersion());
    }

    inline void ThrowIfFatal(TA_EErrorCode status) {
        switch (status) {
            case TA_EErrorCode::TA_EC_EMPTY_TVM_KEYS:
                throw TEmptyTvmKeysException("Empty TVM keys");
            case TA_EErrorCode::TA_EC_MALFORMED_TVM_KEYS:
                throw TMalformedTvmKeysException("Malformed TVM keys");
            case TA_EErrorCode::TA_EC_MALFORMED_TVM_SECRET:
                throw TMalformedTvmSecretException("Malformed TVM secret");
            case TA_EErrorCode::TA_EC_NOT_ALLOWED:
                throw TNotAllowedException("Method cannot be used in non-valid ticket");
            case TA_EErrorCode::TA_EC_INVALID_PARAM:
            case TA_EErrorCode::TA_EC_SMALL_BUFFER:
            case TA_EErrorCode::TA_EC_UNEXPECTED_ERROR:
            case TA_EErrorCode::TA_EC_BROKEN_TVM_CLIENT_SETTINGS:
            case TA_EErrorCode::TA_EC_PERMISSION_DENIED_TO_CACHE_DIR:
            case TA_EErrorCode::TA_EC_FAILED_TO_START_TVM_CLIENT:
                throw std::runtime_error(TA_ErrorCodeToString(status));
            default:
                break;
        }
    }

    inline std::string RemoveTicketSignature(const std::string& ticketBody) {
        const char* ticketWithoutSignature;
        size_t realSize;
        ThrowIfFatal(TA_RemoveTicketSignature(ticketBody.c_str(), ticketBody.size(), &ticketWithoutSignature, &realSize));
        return std::string(ticketWithoutSignature, realSize);
    }

    class TCheckedServiceTicket {
        friend class TServiceContext;
        friend class TTvmClient;
    public:
        TCheckedServiceTicket(TCheckedServiceTicket&& o) = default;
        TCheckedServiceTicket& operator=(TCheckedServiceTicket&& o) = default;

        explicit operator bool() const {
            return (GetStatus() == TA_EErrorCode::TA_EC_OK);
        }

        TTvmId GetSrc() const {
            TTvmId src;
            ThrowIfFatal(TA_GetServiceTicketSrc(Ptr.get(), &src));
            return src;
        }

        TA_EErrorCode GetStatus() const {
            return Status;
        }

        std::string DebugInfo() const {
            char buffer[1024];
            size_t realSize;
            TA_EErrorCode resultCode = TA_GetServiceTicketDebugInfo(Ptr.get(), buffer, &realSize, 1024);
            if (resultCode == TA_EErrorCode::TA_EC_SMALL_BUFFER) {
                std::string res(realSize, 0);
                ThrowIfFatal(TA_GetServiceTicketDebugInfo(Ptr.get(), (char*)res.data(), &realSize, realSize));
                return res;
            }
            ThrowIfFatal(resultCode);
            return std::string(buffer, realSize);
        }

        /*!
         * Return uid of developer, who got ServiceTicket with grant_type=sshkey
         * @return uid
         */
        TUid GetIssuerUid() const {
            TUid u = 0;
            ThrowIfFatal(TA_GetServiceTicketIssuerUid(Ptr.get(), &u));
            return u;
        }

    private:
        TCheckedServiceTicket(TA_TCheckedServiceTicket* ptr, TA_EErrorCode status)
            : Ptr(ptr, TA_DeleteServiceTicket)
            , Status(status) {
        }

        std::unique_ptr<TA_TCheckedServiceTicket, decltype(&TA_DeleteServiceTicket)> Ptr;
        TA_EErrorCode Status;
    };

    class TCheckedUserTicket {
        friend class TTvmClient;
        friend class TUserContext;
    public:
        TCheckedUserTicket(TCheckedUserTicket&& o) = default;
        TCheckedUserTicket& operator=(TCheckedUserTicket&& o) = default;

        explicit operator bool() const {
            return (GetStatus() == TA_EErrorCode::TA_EC_OK);
        }

        TUids GetUids() const {
            size_t count;
            TUid scope;
            ThrowIfFatal(TA_GetUserTicketUidsCount(Ptr.get(), &count));

            TUids r(count);
            for (size_t i = 0; i < count; ++i) {
                ThrowIfFatal(TA_GetUserTicketUid(Ptr.get(), i, &scope));
                r[i] = scope;
            }

            return r;
        }

        TUid GetDefaultUid() const {
            TUid defaultUid;
            ThrowIfFatal(TA_GetUserTicketDefaultUid(Ptr.get(), &defaultUid));
            return defaultUid;
        }

        TScopes GetScopes() const {
            size_t count;
            const char* scope;
            ThrowIfFatal(TA_GetUserTicketScopesCount(Ptr.get(), &count));

            TScopes r(count);
            for (size_t i = 0; i < count; ++i) {
                ThrowIfFatal(TA_GetUserTicketScope(Ptr.get(), i, &scope));
                r[i] = std::string(scope);
            }

            return r;
        }

        bool HasScope(const std::string& scopeName) const {
            int result;
            ThrowIfFatal(TA_HasUserTicketScope(Ptr.get(), scopeName.c_str(), scopeName.size(), &result));
            return (result != 0);
        }

        TA_EErrorCode GetStatus() const {
            return Status;
        }

        std::string DebugInfo() const {
            char buffer[1024];
            size_t realSize;
            TA_EErrorCode resultCode = TA_GetUserTicketDebugInfo(Ptr.get(), buffer, &realSize, 1024);
            if (resultCode == TA_EErrorCode::TA_EC_SMALL_BUFFER) {
                std::string res(realSize, 0);
                ThrowIfFatal(TA_GetUserTicketDebugInfo(Ptr.get(), (char*)res.data(), &realSize, realSize));
                return res;
            }
            ThrowIfFatal(resultCode);
            return std::string(buffer, realSize);
        }

    private:
        TCheckedUserTicket(TA_TCheckedUserTicket* ptr, TA_EErrorCode status)
            : Ptr(ptr, TA_DeleteUserTicket)
            , Status(status) {
        }

        std::unique_ptr<TA_TCheckedUserTicket, decltype(&TA_DeleteUserTicket)> Ptr;
        TA_EErrorCode Status;
    };
}

#endif
