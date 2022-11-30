#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_DEPRECATED_H_
#define _TVM_AUTH_DEPRECATED_H_

#include "tvmauth.h"

#ifdef __cplusplus
extern "C" {
#endif

/*!
 * Please do not use thees types: use TvmClient instead
 */

struct TA_TServiceContext;
struct TA_TUserContext;

/*!
 * Create service context. Serivce contexts are used to store TVM keys and parse service tickets.
 * @param[in] tvmId Can be 0 if tvmKeysResponse == NULL
 * @param[in] secretBase64 Secret, can be NULL if param 'tvmKeysResponse' is not NULL. Sign attempt hence raises exception
 * @param[in] secretBase64Size Size of string containing secret
 * @param[in] tvmKeysResponse TVM Keys gotten from TVM API, can be NULL if param 'secretBase64' is not NULL. Only for ticket checking
 * @param[in] tvmKeysResponseSize Size of string containing keys
 * @param[out] context
 * @return Error code
 */
enum TA_EErrorCode TA_CreateServiceContext(
    uint32_t tvmId,
    const char* secretBase64,
    size_t secretBase64Size,
    const char* tvmKeysResponse,
    size_t tvmKeysResponseSize,
    struct TA_TServiceContext** context);

/*!
 * Free memory of service context.
 * @param[in] context
 * @return Error code
 */
enum TA_EErrorCode TA_DeleteServiceContext(struct TA_TServiceContext* context);

/*!
 * Parse and validate service ticket body then create TCheckedServiceTicket object.
 * @param[in] context
 * @param[in] ticketBody Service ticket body as string
 * @param[in] ticketBodySize Size of string containing service ticket body
 * @param[out] ticket Service ticket object
 * @return Error code
 */
enum TA_EErrorCode TA_CheckServiceTicket(
    const struct TA_TServiceContext* context,
    const char* ticketBody,
    size_t ticketBodySize,
    struct TA_TCheckedServiceTicket** ticket);

/*!
 * Create signature for selected parameters. Allocate at least 512 byte for signature buffer.
 * @param[in] context
 * @param[in] ts Param 'ts' of request to TVM
 * @param[in] tsSize Size of param 'ts' of request to TVM
 * @param[in] dst Param 'dst' of request to TVM
 * @param[in] dstSize Size of param 'dst' of request to TVM
 * @param[in] scopes Param 'scopes' of request to TVM
 * @param[in] scopesSize Size of param 'scopes' of request to TVM
 * @param[out] signature
 * @param[out] signatureSize
 * @param[in] maxSignatureSize
 * @return Error code
 */
enum TA_EErrorCode TA_SignCgiParamsForTvm(
    const struct TA_TServiceContext* context,
    const char* ts,
    size_t tsSize,
    const char* dst,
    size_t dstSize,
    const char* scopes,
    size_t scopesSize,
    char* signature,
    size_t* signatureSize,
    size_t maxSignatureSize);

/*!
 * Create user context. User contexts are used to store TVM keys and parse user tickets.
 * @param[in] env
 * @param[in] tvmKeysResponse
 * @param[in] tvmKeysResponseSize
 * @param[out] context
 * @return Error code
 */
enum TA_EErrorCode TA_CreateUserContext(
    enum TA_EBlackboxEnv env,
    const char* tvmKeysResponse,
    size_t tvmKeysResponseSize,
    struct TA_TUserContext** context);

/*!
 * Free memory of user context.
 * @param[in] context
 * @return Error code
 */
enum TA_EErrorCode TA_DeleteUserContext(struct TA_TUserContext* context);

/*!
 * Parse and validate user ticket body then create TCheckedUserTicket object.
 * @param[in] context
 * @param[in] ticketBody Service ticket body as string
 * @param[in] ticketBodySize Size of string containing service ticket body
 * @param[out] ticket Service ticket object
 * @return Error code
 */
enum TA_EErrorCode TA_CheckUserTicket(
    const struct TA_TUserContext* context,
    const char* ticketBody,
    size_t ticketBodySize,
    struct TA_TCheckedUserTicket** ticket);

#ifdef __cplusplus
}
#endif
#endif
