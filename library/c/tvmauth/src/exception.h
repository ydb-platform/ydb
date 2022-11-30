#pragma once

#include <library/c/tvmauth/tvmauth.h>

#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/client/exception.h>

#include <util/generic/yexception.h>

#include <exception>
#include <string>

namespace NTvmAuthC {
    template <class T>
    TA_EErrorCode CatchExceptions(T lambda) {
        using namespace NTvmAuth;

        try {
            return lambda();
        } catch (const TEmptyTvmKeysException&) {
            return TA_EC_EMPTY_TVM_KEYS;
        } catch (const TMalformedTvmKeysException&) {
            return TA_EC_MALFORMED_TVM_KEYS;
        } catch (const TMalformedTvmSecretException&) {
            return TA_EC_MALFORMED_TVM_SECRET;
        } catch (const TNotAllowedException&) {
            return TA_EC_NOT_ALLOWED;
        } catch (const TBrokenTvmClientSettings&) {
            return TA_EC_BROKEN_TVM_CLIENT_SETTINGS;
        } catch (const TPermissionDenied&) {
            return TA_EC_PERMISSION_DENIED_TO_CACHE_DIR;
        } catch (const TMissingServiceTicket&) {
            return TA_EC_UNEXPECTED_ERROR;
        } catch (const TNonRetriableException&) {
            return TA_EC_FAILED_TO_START_TVM_CLIENT;
        } catch (const TRetriableException&) {
            return TA_EC_FAILED_TO_START_TVM_CLIENT;
        } catch (...) {
            return TA_EC_UNEXPECTED_ERROR;
        }
    }
}
