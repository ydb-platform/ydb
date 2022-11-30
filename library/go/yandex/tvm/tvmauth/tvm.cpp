#include "tvm.h"

#include "_cgo_export.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/logger.h>
#include <library/cpp/tvmauth/client/mocked_updater.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/settings.h>
#include <library/cpp/tvmauth/client/misc/roles/roles.h>

using namespace NTvmAuth;

void TVM_DestroyMemPool(TVM_MemPool* pool) {
    auto freeStr = [](char*& str) {
        if (str != nullptr) {
            free(str);
            str = nullptr;
        }
    };

    freeStr(pool->ErrorStr);

    if (pool->Scopes != nullptr) {
        free(reinterpret_cast<void*>(pool->Scopes));
        pool->Scopes = nullptr;
    }

    if (pool->TicketStr != nullptr) {
        delete reinterpret_cast<TString*>(pool->TicketStr);
        pool->TicketStr = nullptr;
    }
    if (pool->RawRolesStr != nullptr) {
        delete reinterpret_cast<TString*>(pool->RawRolesStr);
        pool->RawRolesStr = nullptr;
    }

    if (pool->CheckedUserTicket != nullptr) {
        delete reinterpret_cast<TCheckedUserTicket*>(pool->CheckedUserTicket);
        pool->CheckedUserTicket = nullptr;
    }

    if (pool->CheckedServiceTicket != nullptr) {
        delete reinterpret_cast<TCheckedServiceTicket*>(pool->CheckedServiceTicket);
        pool->CheckedServiceTicket = nullptr;
    }

    freeStr(pool->DbgInfo);
    freeStr(pool->LogInfo);
    freeStr(pool->LastError.Data);
}

static void PackStr(TStringBuf in, TVM_String* out, char*& poolStr) noexcept {
    out->Data = poolStr = reinterpret_cast<char*>(malloc(in.size()));
    out->Size = in.size();
    memcpy(out->Data, in.data(), in.size());
}

static void UnpackSettings(
    TVM_ApiSettings* in,
    NTvmApi::TClientSettings* out) {
    if (in->SelfId != 0) {
        out->SelfTvmId = in->SelfId;
    }

    if (in->EnableServiceTicketChecking != 0) {
        out->CheckServiceTickets = true;
    }

    if (in->EnableUserTicketChecking != 0) {
        out->CheckUserTicketsWithBbEnv = static_cast<EBlackboxEnv>(in->BlackboxEnv);
    }

    if (in->SelfSecret != nullptr) {
        out->Secret = TString(reinterpret_cast<char*>(in->SelfSecret), in->SelfSecretSize);
    }

    TStringBuf aliases(reinterpret_cast<char*>(in->DstAliases), in->DstAliasesSize);
    if (aliases) {
        NJson::TJsonValue doc;
        Y_ENSURE(NJson::ReadJsonTree(aliases, &doc), "Invalid json: from go part: " << aliases);
        Y_ENSURE(doc.IsMap(), "Dsts is not map: from go part: " << aliases);

        for (const auto& pair : doc.GetMap()) {
            Y_ENSURE(pair.second.IsUInteger(), "dstID must be number");
            out->FetchServiceTicketsForDstsWithAliases.emplace(pair.first, pair.second.GetUInteger());
        }
    }

    if (in->IdmSystemSlug != nullptr) {
        out->FetchRolesForIdmSystemSlug = TString(reinterpret_cast<char*>(in->IdmSystemSlug), in->IdmSystemSlugSize);
        out->ShouldCheckSrc = in->DisableSrcCheck == 0;
        out->ShouldCheckDefaultUid = in->DisableDefaultUIDCheck == 0;
    }

    if (in->TVMHost != nullptr) {
        out->TvmHost = TString(reinterpret_cast<char*>(in->TVMHost), in->TVMHostSize);
        out->TvmPort = in->TVMPort;
    }
    if (in->TiroleHost != nullptr) {
        out->TiroleHost = TString(reinterpret_cast<char*>(in->TiroleHost), in->TiroleHostSize);
        out->TirolePort = in->TirolePort;
    }
    if (in->TiroleTvmId != 0) {
        out->TiroleTvmId = in->TiroleTvmId;
    }

    if (in->DiskCacheDir != nullptr) {
        out->DiskCacheDir = TString(reinterpret_cast<char*>(in->DiskCacheDir), in->DiskCacheDirSize);
    }
}

static void UnpackSettings(
    TVM_ToolSettings* in,
    NTvmTool::TClientSettings* out) {
    if (in->Port != 0) {
        out->SetPort(in->Port);
    }

    if (in->HostnameSize != 0) {
        out->SetHostname(TString(reinterpret_cast<char*>(in->Hostname), in->HostnameSize));
    }

    if (in->AuthTokenSize != 0) {
        out->SetAuthToken(TString(reinterpret_cast<char*>(in->AuthToken), in->AuthTokenSize));
    }

    out->ShouldCheckSrc = in->DisableSrcCheck == 0;
    out->ShouldCheckDefaultUid = in->DisableDefaultUIDCheck == 0;
}

static void UnpackSettings(
    TVM_UnittestSettings* in,
    TMockedUpdater::TSettings* out) {
    out->SelfTvmId = in->SelfId;
    out->UserTicketEnv = static_cast<EBlackboxEnv>(in->BlackboxEnv);
}

template <class TTicket>
static void PackScopes(
    const TScopes& scopes,
    TTicket* ticket,
    TVM_MemPool* pool) {
    if (scopes.empty()) {
        return;
    }

    pool->Scopes = ticket->Scopes = reinterpret_cast<TVM_String*>(malloc(scopes.size() * sizeof(TVM_String)));

    for (size_t i = 0; i < scopes.size(); i++) {
        ticket->Scopes[i].Data = const_cast<char*>(scopes[i].data());
        ticket->Scopes[i].Size = scopes[i].size();
    }
    ticket->ScopesSize = scopes.size();
}

static void PackUserTicket(
    TCheckedUserTicket in,
    TVM_UserTicket* out,
    TVM_MemPool* pool,
    TStringBuf originalStr) noexcept {
    auto copy = new TCheckedUserTicket(std::move(in));
    pool->CheckedUserTicket = reinterpret_cast<void*>(copy);

    PackStr(copy->DebugInfo(), &out->DbgInfo, pool->DbgInfo);
    PackStr(NUtils::RemoveTicketSignature(originalStr), &out->LogInfo, pool->LogInfo);

    out->Status = static_cast<int>(copy->GetStatus());
    if (out->Status != static_cast<int>(ETicketStatus::Ok)) {
        return;
    }

    out->DefaultUid = copy->GetDefaultUid();

    const auto& uids = copy->GetUids();
    if (!uids.empty()) {
        out->Uids = const_cast<TUid*>(uids.data());
        out->UidsSize = uids.size();
    }

    out->Env = static_cast<int>(copy->GetEnv());

    PackScopes(copy->GetScopes(), out, pool);
}

static void PackServiceTicket(
    TCheckedServiceTicket in,
    TVM_ServiceTicket* out,
    TVM_MemPool* pool,
    TStringBuf originalStr) noexcept {
    auto copy = new TCheckedServiceTicket(std::move(in));
    pool->CheckedServiceTicket = reinterpret_cast<void*>(copy);

    PackStr(copy->DebugInfo(), &out->DbgInfo, pool->DbgInfo);
    PackStr(NUtils::RemoveTicketSignature(originalStr), &out->LogInfo, pool->LogInfo);

    out->Status = static_cast<int>(copy->GetStatus());
    if (out->Status != static_cast<int>(ETicketStatus::Ok)) {
        return;
    }

    out->SrcId = copy->GetSrc();

    auto issuer = copy->GetIssuerUid();
    if (issuer) {
        out->IssuerUid = *issuer;
    }
}

template <class F>
static void CatchError(TVM_Error* err, TVM_MemPool* pool, const F& f) {
    try {
        f();
    } catch (const TMalformedTvmSecretException& ex) {
        err->Code = 1;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TMalformedTvmKeysException& ex) {
        err->Code = 2;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TEmptyTvmKeysException& ex) {
        err->Code = 3;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TNotAllowedException& ex) {
        err->Code = 4;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TBrokenTvmClientSettings& ex) {
        err->Code = 5;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TMissingServiceTicket& ex) {
        err->Code = 6;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TPermissionDenied& ex) {
        err->Code = 7;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const TRetriableException& ex) {
        err->Code = 8;
        err->Retriable = 1;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    } catch (const std::exception& ex) {
        err->Code = 8;
        PackStr(ex.what(), &err->Message, pool->ErrorStr);
    }
}

namespace {
    class TGoLogger: public ILogger {
    public:
        TGoLogger(int loggerHandle)
            : LoggerHandle_(loggerHandle)
        {
        }

        void Log(int lvl, const TString& msg) override {
            TVM_WriteToLog(LoggerHandle_, lvl, const_cast<char*>(msg.data()), msg.size());
        }

    private:
        int LoggerHandle_;
    };

}

extern "C" void TVM_NewApiClient(
    TVM_ApiSettings settings,
    int loggerHandle,
    void** handle,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        NTvmApi::TClientSettings realSettings;
        UnpackSettings(&settings, &realSettings);

        realSettings.LibVersionPrefix = "go_";

        auto client = new TTvmClient(realSettings, MakeIntrusive<TGoLogger>(loggerHandle));
        *handle = static_cast<void*>(client);
    });
}

extern "C" void TVM_NewToolClient(
    TVM_ToolSettings settings,
    int loggerHandle,
    void** handle,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        TString alias(reinterpret_cast<char*>(settings.Alias), settings.AliasSize);
        NTvmTool::TClientSettings realSettings(alias);
        UnpackSettings(&settings, &realSettings);

        auto client = new TTvmClient(realSettings, MakeIntrusive<TGoLogger>(loggerHandle));
        *handle = static_cast<void*>(client);
    });
}

extern "C" void TVM_NewUnittestClient(
    TVM_UnittestSettings settings,
    void** handle,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        TMockedUpdater::TSettings realSettings;
        UnpackSettings(&settings, &realSettings);

        auto client = new TTvmClient(MakeIntrusiveConst<TMockedUpdater>(realSettings));
        *handle = static_cast<void*>(client);
    });
}

extern "C" void TVM_DestroyClient(void* handle) {
    delete static_cast<TTvmClient*>(handle);
}

extern "C" void TVM_GetStatus(
    void* handle,
    TVM_ClientStatus* status,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);

        TClientStatus s = client->GetStatus();
        status->Status = static_cast<int>(s.GetCode());

        PackStr(s.GetLastError(), &status->LastError, pool->LastError.Data);
    });
}

extern "C" void TVM_CheckUserTicket(
    void* handle,
    unsigned char* ticketStr, int ticketSize,
    int* env,
    TVM_UserTicket* ticket,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);
        TStringBuf str(reinterpret_cast<char*>(ticketStr), ticketSize);

        TMaybe<EBlackboxEnv> optEnv;
        if (env) {
            optEnv = (EBlackboxEnv)*env;
        }

        auto userTicket = client->CheckUserTicket(str, optEnv);
        PackUserTicket(std::move(userTicket), ticket, pool, str);
    });
}

extern "C" void TVM_CheckServiceTicket(
    void* handle,
    unsigned char* ticketStr, int ticketSize,
    TVM_ServiceTicket* ticket,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);
        TStringBuf str(reinterpret_cast<char*>(ticketStr), ticketSize);
        auto serviceTicket = client->CheckServiceTicket(str);
        PackServiceTicket(std::move(serviceTicket), ticket, pool, str);
    });
}

extern "C" void TVM_GetServiceTicket(
    void* handle,
    ui32 dstId,
    char** ticket,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);
        auto ticketPtr = new TString(client->GetServiceTicketFor(dstId));

        pool->TicketStr = reinterpret_cast<void*>(ticketPtr);
        *ticket = const_cast<char*>(ticketPtr->c_str());
    });
}

extern "C" void TVM_GetServiceTicketForAlias(
    void* handle,
    unsigned char* alias, int aliasSize,
    char** ticket,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);
        auto ticketPtr = new TString(client->GetServiceTicketFor(TString((char*)alias, aliasSize)));

        pool->TicketStr = reinterpret_cast<void*>(ticketPtr);
        *ticket = const_cast<char*>(ticketPtr->c_str());
    });
}

extern "C" void TVM_GetRoles(
    void* handle,
    unsigned char* currentRevision, int currentRevisionSize,
    char** raw,
    int* rawSize,
    TVM_Error* err,
    TVM_MemPool* pool) {
    CatchError(err, pool, [&] {
        auto client = static_cast<TTvmClient*>(handle);
        NTvmAuth::NRoles::TRolesPtr roles = client->GetRoles();

        if (currentRevision &&
            roles->GetMeta().Revision == TStringBuf(reinterpret_cast<char*>(currentRevision), currentRevisionSize)) {
            return;
        }

        auto rawPtr = new TString(roles->GetRaw());

        pool->RawRolesStr = reinterpret_cast<void*>(rawPtr);
        *raw = const_cast<char*>(rawPtr->c_str());
        *rawSize = rawPtr->size();
    });
}
