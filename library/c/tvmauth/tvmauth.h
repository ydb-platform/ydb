#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_H_
#define _TVM_AUTH_H_

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

enum TA_EErrorCode {
    TA_EC_OK = 0,
    TA_EC_DEPRECATED,
    TA_EC_EMPTY_TVM_KEYS,
    TA_EC_EXPIRED_TICKET,
    TA_EC_INVALID_BLACKBOX_ENV,
    TA_EC_INVALID_DST,
    TA_EC_INVALID_PARAM,
    TA_EC_INVALID_TICKET_TYPE,
    TA_EC_MALFORMED_TICKET,
    TA_EC_MALFORMED_TVM_KEYS,
    TA_EC_MALFORMED_TVM_SECRET,
    TA_EC_MISSING_KEY,
    TA_EC_NOT_ALLOWED,
    TA_EC_SIGN_BROKEN,
    TA_EC_SMALL_BUFFER,
    TA_EC_UNEXPECTED_ERROR,
    TA_EC_UNSUPPORTED_VERSION,
    TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
    TA_EC_PERMISSION_DENIED_TO_CACHE_DIR,
    TA_EC_FAILED_TO_START_TVM_CLIENT,
};
struct TA_TCheckedServiceTicket;
struct TA_TCheckedUserTicket;

extern const char* TA_BlackboxTvmIdProd;       /* 222 */
extern const char* TA_BlackboxTvmIdTest;       /* 224 */
extern const char* TA_BlackboxTvmIdProdYateam; /* 223 */
extern const char* TA_BlackboxTvmIdTestYateam; /* 225 */
extern const char* TA_BlackboxTvmIdStress;     /* 226 */
extern const char* TA_BlackboxTvmIdMimino;     /* 239 */

enum TA_EBlackboxEnv {
    TA_BE_PROD = 0,
    TA_BE_TEST,
    TA_BE_PROD_YATEAM,
    TA_BE_TEST_YATEAM,
    TA_BE_STRESS
};

const char* TA_ErrorCodeToString(enum TA_EErrorCode code);

/*!
 * Free memory of service ticket.
 * @param[in] ticket
 * @return Error code
 */
enum TA_EErrorCode TA_DeleteServiceTicket(struct TA_TCheckedServiceTicket* ticket);

/*!
 * Source of request, your client.
 * @param[in] ticket Parsed ticket
 * @param[out] srcTvmId Integer identifier of client
 * @return Error code
 */
enum TA_EErrorCode TA_GetServiceTicketSrc(
    const struct TA_TCheckedServiceTicket* ticket,
    uint32_t* srcTvmId);

/*!
 * Return debug info for ticket
 * @param[in] ticket
 * @param[out] debugInfo
 * @param[out] debugInfoSize
 * @param[in] maxDebugInfoSize
 * @return Error code
 */
enum TA_EErrorCode TA_GetServiceTicketDebugInfo(
    const struct TA_TCheckedServiceTicket* ticket,
    char* debugInfo,
    size_t* debugInfoSize,
    size_t maxDebugInfoSize);

/*!
 * Return uid of developer, who got ServiceTicket with grant_type=sshkey
 * uid == 0 if issuer uid is absent
 * @param[in] ticket
 * @param[out] uid
 * @return Error code
 */
enum TA_EErrorCode TA_GetServiceTicketIssuerUid(
    const struct TA_TCheckedServiceTicket* ticket,
    uint64_t* uid);

/*!
 * Return part of ticket which can be safely logged.
 * WARNING: Do not free returned pointer.
 * WARNING: logableTicket is not zero-ended string (it is substring of ticketBody)
 * WARNING: Use logableTicketSize to read valid amount of symbols.
 * @param[in] ticketBody
 * @param[in] ticketBodySize
 * @param[out] logableTicket
 * @param[out] logableTicketSize
 * @return Error code
 */
enum TA_EErrorCode TA_RemoveTicketSignature(
    const char* ticketBody,
    size_t ticketBodySize,
    const char** logableTicket,
    size_t* logableTicketSize);

/*!
 * Free memory of user ticket.
 * @param[in] context
 * @return Error code
 */
enum TA_EErrorCode TA_DeleteUserTicket(struct TA_TCheckedUserTicket* context);

/*!
 * Return number of UIDs in the ticket.
 * @param[in] ticket
 * @param[out] count
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketUidsCount(
    const struct TA_TCheckedUserTicket* ticket,
    size_t* count);

/*!
 * Return UID from ticket by ordinal index
 * @param[in] ticket
 * @param[in] idx
 * @param[out] uid
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketUid(
    const struct TA_TCheckedUserTicket* ticket,
    size_t idx,
    uint64_t* uid);

/*!
 * Return default UID. Default UID is the chosen one which should be considered as primary.
 * @param[in] ticket
 * @param[out] uid
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketDefaultUid(
    const struct TA_TCheckedUserTicket* ticket,
    uint64_t* uid);

/*!
 * Return number of scopes in the ticket.
 * @param[in] ticket
 * @param[out] count
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketScopesCount(
    const struct TA_TCheckedUserTicket* ticket,
    size_t* count);

/*!
 * Return scope by ordinal index.
 * WARNING: Do not free returned pointer.
 * @param[in] ticket
 * @param[in] idx
 * @param[out] scope
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketScope(
    const struct TA_TCheckedUserTicket* ticket,
    size_t idx,
    const char** scope);

/*!
 * Check if user ticket has the scope
 * @param[in] ticket
 * @param[in] scope
 * @param[in] scopeSize Size of string containing scope
 * @param[out] checkingResult Equal to 1 if scope is present in ticket and to 0 otherwise
 * @return Error code
 */
enum TA_EErrorCode TA_HasUserTicketScope(
    const struct TA_TCheckedUserTicket* ticket,
    const char* scope,
    size_t scopeSize,
    int* checkingResult);

/*!
 * Return debug info for ticket
 * @param[in] ticket
 * @param[out] debugInfo
 * @param[out] debugInfoSize
 * @param[in] maxDebugInfoSize
 * @return Error code
 */
enum TA_EErrorCode TA_GetUserTicketDebugInfo(
    const struct TA_TCheckedUserTicket* ticket,
    char* debugInfo,
    size_t* debugInfoSize,
    size_t maxDebugInfoSize);

/*!
 * Return library version.
 * @return version
 */
const char* TA_LibVersion();

#ifdef __cplusplus
}
#endif
#endif
