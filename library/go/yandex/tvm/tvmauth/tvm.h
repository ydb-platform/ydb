#pragma once

#include <util/system/types.h>

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct _TVM_String {
        char* Data;
        int Size;
    } TVM_String;

    // MemPool owns memory allocated by C.
    typedef struct {
        char* ErrorStr;
        void* TicketStr;
        void* RawRolesStr;
        TVM_String* Scopes;
        void* CheckedUserTicket;
        void* CheckedServiceTicket;
        char* DbgInfo;
        char* LogInfo;
        TVM_String LastError;
    } TVM_MemPool;

    void TVM_DestroyMemPool(TVM_MemPool* pool);

    typedef struct {
        int Code;
        int Retriable;

        TVM_String Message;
    } TVM_Error;

    typedef struct {
        int Status;

        ui64 DefaultUid;

        ui64* Uids;
        int UidsSize;

        int Env;

        TVM_String* Scopes;
        int ScopesSize;

        TVM_String DbgInfo;
        TVM_String LogInfo;
    } TVM_UserTicket;

    typedef struct {
        int Status;

        ui32 SrcId;

        ui64 IssuerUid;

        TVM_String DbgInfo;
        TVM_String LogInfo;
    } TVM_ServiceTicket;

    typedef struct {
        ui32 SelfId;

        int EnableServiceTicketChecking;

        int EnableUserTicketChecking;
        int BlackboxEnv;

        unsigned char* SelfSecret;
        int SelfSecretSize;
        unsigned char* DstAliases;
        int DstAliasesSize;

        unsigned char* IdmSystemSlug;
        int IdmSystemSlugSize;
        int DisableSrcCheck;
        int DisableDefaultUIDCheck;

        unsigned char* TVMHost;
        int TVMHostSize;
        int TVMPort;
        unsigned char* TiroleHost;
        int TiroleHostSize;
        int TirolePort;
        ui32 TiroleTvmId;

        unsigned char* DiskCacheDir;
        int DiskCacheDirSize;
    } TVM_ApiSettings;

    typedef struct {
        unsigned char* Alias;
        int AliasSize;

        int Port;

        unsigned char* Hostname;
        int HostnameSize;

        unsigned char* AuthToken;
        int AuthTokenSize;

        int DisableSrcCheck;
        int DisableDefaultUIDCheck;
    } TVM_ToolSettings;

    typedef struct {
        ui32 SelfId;
        int BlackboxEnv;
    } TVM_UnittestSettings;

    typedef struct {
        int Status;
        TVM_String LastError;
    } TVM_ClientStatus;

    // First argument must be passed by value. "Go code may pass a Go pointer to C
    // provided the Go memory to which it points does not contain any Go pointers."
    void TVM_NewApiClient(
        TVM_ApiSettings settings,
        int loggerHandle,
        void** handle,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_NewToolClient(
        TVM_ToolSettings settings,
        int loggerHandle,
        void** handle,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_NewUnittestClient(
        TVM_UnittestSettings settings,
        void** handle,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_DestroyClient(void* handle);

    void TVM_GetStatus(
        void* handle,
        TVM_ClientStatus* status,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_CheckUserTicket(
        void* handle,
        unsigned char* ticketStr, int ticketSize,
        int* env,
        TVM_UserTicket* ticket,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_CheckServiceTicket(
        void* handle,
        unsigned char* ticketStr, int ticketSize,
        TVM_ServiceTicket* ticket,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_GetServiceTicket(
        void* handle,
        ui32 dstId,
        char** ticket,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_GetServiceTicketForAlias(
        void* handle,
        unsigned char* alias, int aliasSize,
        char** ticket,
        TVM_Error* err,
        TVM_MemPool* pool);

    void TVM_GetRoles(
        void* handle,
        unsigned char* currentRevision, int currentRevisionSize,
        char** raw,
        int* rawSize,
        TVM_Error* err,
        TVM_MemPool* pool);

#ifdef __cplusplus
}
#endif
