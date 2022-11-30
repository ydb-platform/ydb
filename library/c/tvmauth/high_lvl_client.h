#pragma once
// DO_NOT_STYLE

#ifndef _TVM_AUTH_HIGH_LVL_CLIENT_H_
#define _TVM_AUTH_HIGH_LVL_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tvmauth.h"

/*!
 * Long lived thread-safe object for interacting with TVM.
 * In 99% cases TvmClient shoud be created at service startup and live for the whole process lifetime.
 */
struct TA_TTvmClient;

/*!
 * Uses local http-interface to get state: http://localhost/tvm/.
 * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
 * See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.
 */
struct TA_TTvmToolClientSettings;

/*!
 * Uses general way to get state: https://tvm-api.yandex.net.
 * It is not recomended for Qloud/YP.
 */
struct TA_TTvmApiClientSettings;

/*!
 * Contains info about innternal state of client
 */
struct TA_TTvmClientStatus;

enum TA_ETvmClientStatusCode {
    TA_TCSC_OK = 0,
    TA_TCSC_WARNING = 1,
    TA_TCSC_ERROR = 2,
};

/*!
 * Logging callback
 * @param[in] lvl is syslog level: 0(Emergency) ... 7(Debug)
 * @param[in] msg
 */
typedef void (*TA_TLoggerFunc)(int lvl, const char* msg);

/*!
 * Noop logger: it does nothing.
 * Please use it only in tests.
 */
void TA_NoopLogger(int lvl, const char* msg);

/*!
 * Create settings struct for tvmtool client
 * Sets default values:
 * - hostname: "localhost"
 * - port: 1
 * - authToken: environment variable TVMTOOL_LOCAL_AUTHTOKEN (provided with Yandex.Deploy)
 *                                or QLOUD_TVM_TOKEN (provided with Qloud)
 * @param[in] self alias
 * @param[in] self aliasSize
 * @param[out] client settings
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_Create(
    const char* alias,
    size_t aliasSize,
    struct TA_TTvmToolClientSettings** settings);

/*!
 * @param[in] settings
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_Delete(
    struct TA_TTvmToolClientSettings* settings);

/*!
 * Default value: port == 1 - ok for Qloud
 * @param[in] client settings
 * @param[in] port
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_SetPort(
    struct TA_TTvmToolClientSettings* settings,
    uint16_t port);

/*!
 * Default value: hostname == "localhost"
 * @param[in] client settings
 * @param[in] hostname
 * @param[in] hostnameSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_SetHostname(
    struct TA_TTvmToolClientSettings* settings,
    const char* hostname,
    size_t hostnameSize);

/*!
 * Default value: token == environment variable TVMTOOL_LOCAL_AUTHTOKEN - ok for Yandex.Deploy
 *                                           or QLOUD_TVM_TOKEN - ok for Qloud
 * @param[in] client settings
 * @param[in] authtoken
 * @param[in] authtokenSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_SetAuthToken(
    struct TA_TTvmToolClientSettings* settings,
    const char* authtoken,
    size_t authtokenSize);

/*!
 * Blackbox environmet is provided by tvmtool for client.
 * You can override it for your purpose with limitations:
 *   (env from tvmtool) -> (override)
 *  - Prod/ProdYateam -> Prod/ProdYateam
 *  - Test/TestYateam -> Test/TestYateam
 *  - Stress -> Stress
 * You can contact tvm-dev@yandex-team.ru if limitations are too strict
 *
 * @param[in] client settings
 * @param[in] env
 * @return Error code
 */
enum TA_EErrorCode TA_TvmToolClientSettings_OverrideBlackboxEnv(
    struct TA_TTvmToolClientSettings* settings,
    enum TA_EBlackboxEnv env);

/*!
 * Create settings struct for tvm client
 * At least one of them is required:
 *   TA_TvmApiClientSettings_EnableServiceTicketChecking()
 *   TA_TvmApiClientSettings_EnableUserTicketChecking()
 * @param[out] client settings
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_Create(
    struct TA_TTvmApiClientSettings** settings);

/*!
 * @param[in] settings
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_Delete(
    struct TA_TTvmApiClientSettings* settings);

/*!
 * @param[in] client settings
 * @param[in] selfTvmId - cannot be 0
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_SetSelfTvmId(
    struct TA_TTvmApiClientSettings* settings,
    uint32_t selfTvmId);

/*!
 * Requieres TA_TvmApiClientSettings_SetSelfTvmId()
 * @param[in] settings
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketChecking(
    struct TA_TTvmApiClientSettings* settings);

/*!
 * @param[in] settings
 * @param[in] env
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_EnableUserTicketChecking(
    struct TA_TTvmApiClientSettings* settings,
    enum TA_EBlackboxEnv env);

/*!
 * Overrides result of TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds()
 * Prerequires:
 *  TA_TvmApiClientSettings_SetSelfTvmId()
 * @param[in] settings
 * @param[in] selfSecret
 * @param[in] selfSecretSize
 * @param[in] dsts - serialized map of dst-pairs: alias -> tvm_id (delimeters: ":;"). Example: "mpfs:127;blackbox:242"
 * @param[in] dstsSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases(
    struct TA_TTvmApiClientSettings* settings,
    const char* selfSecret,
    size_t selfSecretSize,
    const char* dsts,
    size_t dstsSize);

/*!
 * Overrides result of TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases()
 * Prerequires:
 *  TA_TvmApiClientSettings_SetSelfTvmId()
 * @param[in] settings
 * @param[in] selfSecret
 * @param[in] selfSecretSize
 * @param[in] dsts - serialized list of dst-tvm_id (delimeter is ";"). Example: "13;242"
 * @param[in] dstsSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds(
    struct TA_TTvmApiClientSettings* settings,
    const char* selfSecret,
    size_t selfSecretSize,
    const char* dsts,
    size_t dstsSize);

/*!
 * Set path to directory for disk cache
 * Requires read/write permissions. Checks permissions
 * WARNING: The same directory can be used only:
 *            - for TVM clients with the same settings
 *          OR
 *            - for new client replacing previous - with another config.
 *          System user must be the same for processes with these clients inside.
 *          Implementation doesn't provide other scenarios.
 * @param[in] settings
 * @param[in] path
 * @param[in] pathSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmApiClientSettings_SetDiskCacheDir(
    struct TA_TTvmApiClientSettings* settings,
    const char* path,
    size_t pathSize);

/*!
 * Create client for tvmtool. Starts thread for updating of cache in background
 * @param[in] settings - please, delete by yourself
 * @param[in] logger - may be NULL
 * @param[out] client
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_CreateForTvmtool(
    const struct TA_TTvmToolClientSettings* settings,
    TA_TLoggerFunc logger,
    struct TA_TTvmClient** client);

/*!
 * Create client for tvm-api. Starts thread for updating of cache in background
 * Reads cache from disk if specified
 * @param[in] settings - please, delete by yourself
 * @param[in] logger - may be NULL
 * @param[out] client
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_Create(
    const struct TA_TTvmApiClientSettings* settings,
    TA_TLoggerFunc logger,
    struct TA_TTvmClient** client);

/*!
 * @param[in] client
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_Delete(
    struct TA_TTvmClient* client);

/*!
 * @param[in] client
 * @param[in] ticket
 * @param[in] ticketSize
 * @param[out] serviceTicket
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_CheckServiceTicket(
    const struct TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    struct TA_TCheckedServiceTicket** serviceTicket);

/*!
 * @param[in] client
 * @param[in] ticket
 * @param[in] ticketSize
 * @param[out] userTicket
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_CheckUserTicket(
    const struct TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    struct TA_TCheckedUserTicket** userTicket);

/*!
 * @param[in] client
 * @param[in] ticket
 * @param[in] ticketSize
 * @param[in] env
 * @param[out] userTicket
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_CheckUserTicketWithOverridedEnv(
    const struct TA_TTvmClient* client,
    const char* ticket,
    size_t ticketSize,
    enum TA_EBlackboxEnv env,
    struct TA_TCheckedUserTicket** userTicket);

/*!
 * Allocate at least 512 byte for ticket buffer.
 * Prerequires:
 *  TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases()
 * @param[in] client
 * @param[in] dst - must be specified in TA_TTvmClientSettings
 * @param[in] dstSize
 * @param[out] ticket
 * @param[out] ticketSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_GetServiceTicketForAlias(
    const struct TA_TTvmClient* client,
    const char* dst,
    size_t dstSize,
    size_t maxTicketSize,
    char* ticket,
    size_t* ticketSize);

/*!
 * Allocate at least 512 byte for ticket buffer.
 * Prerequires:
 *  TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds() OR
 *  TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases()
 * @param[in] client
 * @param[in] dst - must be specified in TA_TTvmClientSettings
 * @param[out] ticket
 * @param[out] ticketSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_GetServiceTicketForTvmId(
    const struct TA_TTvmClient* client,
    uint32_t dst,
    size_t maxTicketSize,
    char* ticket,
    size_t* ticketSize);

/*!
 * @param[in] client
 * @param[out] client status
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_GetStatus(
    const struct TA_TTvmClient* client,
    struct TA_TTvmClientStatus** status);

/*!
 * Free memory owned by status
 * @param[in] status
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_DeleteStatus(
    struct TA_TTvmClientStatus* status);

/*!
 * @param[in] status
 * @param[out] code
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_Status_GetCode(
    const struct TA_TTvmClientStatus* status,
    enum TA_ETvmClientStatusCode* code);

/*!
 * Do not free lastError
 * @param[in] status
 * @param[out] lastError
 * @param[out] lastErrorSize
 * @return Error code
 */
enum TA_EErrorCode TA_TvmClient_Status_GetLastError(
    const struct TA_TTvmClientStatus* status,
    const char** lastError,
    size_t* lastErrorSize);

#ifdef __cplusplus
}
#endif
#endif
