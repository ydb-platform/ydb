#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_DEPRECATED_WRAPPER_H_
#define _TVM_AUTH_DEPRECATED_WRAPPER_H_

#include "deprecated.h"
#include "tvmauth_wrapper.h"

#include <memory>
#include <string>
#include <vector>

namespace NTvmAuthWrapper {
    /*!
     * Please do not use thees types: use TvmClient instead
     */

    class TServiceContext {
    public:
        TServiceContext(TTvmId tvmId, const std::string& secretBase64, const std::string& tvmKeysResponse)
            : Ptr(nullptr, TA_DeleteServiceContext) {
            TA_TServiceContext* rawPtr;
            ThrowIfFatal(TA_CreateServiceContext(tvmId, secretBase64.c_str(), secretBase64.size(), tvmKeysResponse.c_str(), tvmKeysResponse.size(), &rawPtr));
            Ptr.reset(rawPtr);
        }

        static TServiceContext CheckingFactory(TTvmId tvmId, const std::string& tvmKeysResponse) {
            return TServiceContext(tvmId, tvmKeysResponse);
        }

        static TServiceContext SigningFactory(const std::string& secretBase64, TTvmId = 0) {
            TServiceContext ins;
            TA_TServiceContext* rawPtr;
            ThrowIfFatal(TA_CreateServiceContext(0, secretBase64.c_str(), secretBase64.size(), nullptr, 0, &rawPtr));
            ins.Ptr.reset(rawPtr);
            return ins;
        }

        TServiceContext(TServiceContext&& o) = default;
        TServiceContext& operator=(TServiceContext&& o) = default;

        TCheckedServiceTicket Check(const std::string& ticketBody) const {
            TA_TCheckedServiceTicket* ticketPtr = nullptr;
            TA_EErrorCode resultCode = TA_CheckServiceTicket(Ptr.get(), ticketBody.c_str(), ticketBody.size(), &ticketPtr);
            return TCheckedServiceTicket(ticketPtr, resultCode);
        }

        std::string SignCgiParamsForTvm(const std::string& ts, const std::string& dst, const std::string& scopes) const {
            char buffer[1024];
            size_t realSize;
            ThrowIfFatal(TA_SignCgiParamsForTvm(Ptr.get(), ts.c_str(), ts.size(), dst.c_str(), dst.size(), scopes.c_str(), scopes.size(), buffer, &realSize, 1024));
            return std::string(buffer, realSize);
        }

    public:
        // Use CheckingFactory()
        TServiceContext(TTvmId tvmId, const std::string& tvmKeysResponse)
            : Ptr(nullptr, TA_DeleteServiceContext) {
            TA_TServiceContext* rawPtr;
            ThrowIfFatal(TA_CreateServiceContext(tvmId, nullptr, 0, tvmKeysResponse.c_str(), tvmKeysResponse.size(), &rawPtr));
            Ptr.reset(rawPtr);
        }

    private:
        TServiceContext()
            : Ptr(nullptr, TA_DeleteServiceContext)
        {
        }

    private:
        std::unique_ptr<TA_TServiceContext, decltype(&TA_DeleteServiceContext)> Ptr;
    };

    class TUserContext {
    public:
        TUserContext(TA_EBlackboxEnv env, const std::string& tvmKeysResponse)
            : Ptr(nullptr, TA_DeleteUserContext) {
            TA_TUserContext* rawPtr;
            ThrowIfFatal(TA_CreateUserContext(env, tvmKeysResponse.c_str(), tvmKeysResponse.size(), &rawPtr));
            Ptr.reset(rawPtr);
        }

        TUserContext(TUserContext&& o) = default;
        TUserContext& operator=(TUserContext&& o) = default;

        TCheckedUserTicket Check(const std::string& ticketBody) const {
            TA_TCheckedUserTicket* ticketPtr = nullptr;
            TA_EErrorCode resultCode = TA_CheckUserTicket(Ptr.get(), ticketBody.c_str(), ticketBody.size(), &ticketPtr);
            return TCheckedUserTicket(ticketPtr, resultCode);
        }

    private:
        std::unique_ptr<TA_TUserContext, decltype(&TA_DeleteUserContext)> Ptr;
    };
}

#endif
