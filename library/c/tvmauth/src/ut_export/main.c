// DO_NOT_STYLE
#include <stdio.h>
#include <string.h>

#include <deprecated.h>
#include <high_lvl_client.h>
#include <tvmauth.h>

void foo(int lvl, const char* msg) {
    (void)lvl;
    (void)msg;
    fprintf(stderr, "%s\n", msg);
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    const char tvmKeys[] = "";
    const char clientSecret[] = "";
    const uint32_t tvmId = 123;
    const char srvTicket[] = "";
    const char usrTicket[] = "";

    const char* ts = "100500";
    const char* dst = "456";
    const char* scopes = "";

    /* Service context */
    struct TA_TServiceContext* srvCtx = NULL;
    enum TA_EErrorCode code = TA_CreateServiceContext(
        tvmId,
        clientSecret,
        sizeof(clientSecret),
        tvmKeys,
        sizeof(tvmKeys),
        &srvCtx);

    struct TA_TCheckedServiceTicket* st = NULL;
    code = TA_CheckServiceTicket(srvCtx, srvTicket, sizeof(srvTicket), &st);

    uint32_t src = 0;
    code = TA_GetServiceTicketSrc(st, &src);

    size_t count = 0;
    const char* scope = NULL;
    int check = 0;

    char debugInfo[512];
    code = TA_GetServiceTicketDebugInfo(st, debugInfo, &count, sizeof(debugInfo));

    uint64_t issuer = 0;
    code = TA_GetServiceTicketIssuerUid(st, &issuer);

    const char* sub = NULL;
    code = TA_RemoveTicketSignature(srvTicket, sizeof(srvTicket), &sub, &count);

    char sign[512];
    code = TA_SignCgiParamsForTvm(
        srvCtx,
        ts,
        sizeof(ts),
        dst,
        sizeof(dst),
        scopes,
        sizeof(scopes),
        sign,
        &count,
        sizeof(sign));

    code = TA_DeleteServiceTicket(st);
    code = TA_DeleteServiceContext(srvCtx);

    /* User context */
    struct TA_TUserContext* usrCtx = NULL;
    code = TA_CreateUserContext(
        TA_BE_TEST,
        tvmKeys,
        sizeof(tvmKeys),
        &usrCtx);

    struct TA_TCheckedUserTicket* ut = NULL;
    code = TA_CheckUserTicket(usrCtx, usrTicket, sizeof(usrTicket), &ut);

    code = TA_GetUserTicketUidsCount(ut, &count);

    uint64_t uid = 0;
    code = TA_GetUserTicketUid(ut, 0, &uid);
    code = TA_GetUserTicketDefaultUid(ut, &uid);

    code = TA_GetUserTicketScopesCount(ut, &count);
    code = TA_GetUserTicketScope(ut, 0, &scope);

    code = TA_HasUserTicketScope(ut, "scope", 5, &check);

    code = TA_GetUserTicketDebugInfo(ut, debugInfo, &count, sizeof(debugInfo));

    code = TA_DeleteUserTicket(ut);
    code = TA_DeleteUserContext(usrCtx);

    code = TA_RemoveTicketSignature(usrTicket, sizeof(usrTicket), &sub, &count);

    /* Etc */
    const char* ver = TA_LibVersion();
    (void)ver;
    const char* msg = TA_ErrorCodeToString(TA_EC_DEPRECATED);
    (void)msg;

    printf("%s", TA_BlackboxTvmIdProd);
    printf("%s", TA_BlackboxTvmIdTest);
    printf("%s", TA_BlackboxTvmIdProdYateam);
    printf("%s", TA_BlackboxTvmIdTestYateam);
    printf("%s", TA_BlackboxTvmIdStress);
    printf("%s", TA_BlackboxTvmIdMimino);


    /* TVM client */
    struct TA_TTvmApiClientSettings* settings = NULL;
    code = TA_TvmApiClientSettings_Create(&settings);
    code = TA_TvmApiClientSettings_SetDiskCacheDir(settings, "", 0);

    char ticket[512];
    size_t size = 0;
    struct TA_TTvmClient* client = NULL;
    TA_TLoggerFunc l = &foo;
    code = TA_TvmClient_Create(settings, l, &client);
    code = TA_TvmClient_CheckServiceTicket(client, "", 0, &st);
    code = TA_TvmClient_CheckUserTicket(client, "", 0, &ut);
    code = TA_TvmClient_CheckUserTicketWithOverridedEnv(client, "", 0, TA_BE_TEST, &ut);
    struct TA_TTvmClientStatus* status = NULL;
    code = TA_TvmClient_GetStatus(client, &status);
    enum TA_ETvmClientStatusCode statusCode;
    code = TA_TvmClient_Status_GetCode(status, &statusCode);
    code = TA_TvmClient_Status_GetLastError(status, &msg, &size);
    code = TA_TvmClient_DeleteStatus(status);
    code = TA_TvmClient_GetServiceTicketForAlias(client, "q", 1, 512, ticket, &size);
    code = TA_TvmClient_GetServiceTicketForTvmId(client, 100500, 512, ticket, &size);
    code = TA_TvmClient_Delete(client);
    code = TA_TvmApiClientSettings_Delete(settings);

    settings = NULL;
    code = TA_TvmApiClientSettings_SetSelfTvmId(settings, 100500);
    code = TA_TvmApiClientSettings_EnableServiceTicketChecking(settings);
    code = TA_TvmApiClientSettings_EnableUserTicketChecking(settings, TA_BE_TEST);
    code = TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases(settings, "qwe", 3, ";", 1);
    code = TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds(settings, "qwe", 3, ";", 1);

    struct TA_TTvmToolClientSettings* setts = NULL;
    code = TA_TvmToolClientSettings_Create("me", 2, &setts);
    code = TA_TvmToolClientSettings_SetPort(setts, 1);
    code = TA_TvmToolClientSettings_SetHostname(setts, "localhost", 9);
    code = TA_TvmToolClientSettings_SetAuthToken(setts, "qwe", 3);
    code = TA_TvmToolClientSettings_OverrideBlackboxEnv(setts, TA_BE_TEST);

    client = NULL;
    code = TA_TvmClient_CreateForTvmtool(setts, foo, &client);
    code = TA_TvmClient_Delete(client);
    code = TA_TvmToolClientSettings_Delete(setts);

    code = TA_EC_OK;
    code = TA_EC_DEPRECATED;
    code = TA_EC_EMPTY_TVM_KEYS;
    code = TA_EC_EXPIRED_TICKET;
    code = TA_EC_INVALID_BLACKBOX_ENV;
    code = TA_EC_INVALID_DST;
    code = TA_EC_INVALID_PARAM;
    code = TA_EC_INVALID_TICKET_TYPE;
    code = TA_EC_MALFORMED_TICKET;
    code = TA_EC_MALFORMED_TVM_KEYS;
    code = TA_EC_MALFORMED_TVM_SECRET;
    code = TA_EC_MISSING_KEY;
    code = TA_EC_NOT_ALLOWED;
    code = TA_EC_SIGN_BROKEN;
    code = TA_EC_SMALL_BUFFER;
    code = TA_EC_UNEXPECTED_ERROR;
    code = TA_EC_UNSUPPORTED_VERSION;
    code = TA_EC_BROKEN_TVM_CLIENT_SETTINGS;
    code = TA_EC_PERMISSION_DENIED_TO_CACHE_DIR;
    code = TA_EC_FAILED_TO_START_TVM_CLIENT;

    enum TA_EBlackboxEnv env = TA_BE_PROD;
    env = TA_BE_TEST;
    env = TA_BE_PROD_YATEAM;
    env = TA_BE_TEST_YATEAM;
    env = TA_BE_STRESS;

    return 0;
}
