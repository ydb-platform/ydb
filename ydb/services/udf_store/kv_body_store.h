#pragma once
#include "events.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <library/cpp/digest/md5/md5.h>
#include <util/system/file.h>

namespace NKikimr::NUdfStore {


// Actor that reads a UDF body from a KV tablet via the actor system,
// then saves it to disk and loads it into the function registry.
// Flow:
//   1. Resolves VolumePath via SchemeCache → gets tablet ID from SolomonVolumeInfo
//   2. Opens tablet pipe via NTabletPipe::CreateClient
//   3. Sends TEvKeyValue::TEvRead requests in ReadChunkSize-byte pages until EOF
//   4. Each chunk is appended to a temp file; MD5 is computed incrementally
//   5. On EOF: verifies body size against metadata, MD5, renames tmp→final,
//      loads UDF into FunctionRegistry and checks that modules were registered
//   6. Sends TEvReadBodyResponse(success/failure) to ReplyTo
class TKvBodyReadActor : public NActors::TActorBootstrapped<TKvBodyReadActor> {
private:
    NActors::TActorId ReplyTo;

    TString VolumePath;
    TString Md5Key;
    TString OutputDir;
    ui64 ExpectedSize = 0;

    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;

    ui64 KVTabletId = 0;
    NActors::TActorId PipeClient;

    // Chunked-read state
    // KV tablet hard-caps a single read response at ~25 MiB (TotalSizeLimit).
    // We use 16 MiB chunks to stay well within that limit.
    static constexpr ui64 ReadChunkSize = 16ull << 20; // 16 MiB
    ui64 CurrentOffset = 0;
    TString TmpFilePath;
    THolder<TFile> TmpFile;
    MD5 Md5Ctx;

    void SendNavigateRequest();
    void CreatePipeAndSendRead();  // opens pipe, kicks off first chunk read
    void SendNextChunkRead();      // sends TEvRead for [CurrentOffset, +ReadChunkSize)
    void FinalizeAndSave();        // called when last chunk received
    void CleanupTmpFile();         // closes + removes the tmp file on error
    bool LoadUdfIntoRegistry(const TString& finalPath) const;
    void ReplyError(const TString& message);

public:
    TKvBodyReadActor(const NActors::TActorId& replyTo,
                     const TString& md5Key,
                     const TString& volumePath,
                     const TString& outputDir,
                     TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry,
                     ui64 expectedSize)
        : ReplyTo(replyTo)
        , VolumePath(volumePath)
        , Md5Key(md5Key)
        , OutputDir(outputDir)
        , ExpectedSize(expectedSize)
        , FunctionRegistry(std::move(functionRegistry))
    {}

    void Bootstrap();

    void HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleReadResponse(TEvKeyValue::TEvReadResponse::TPtr& ev);
    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);

    void PassAway() override;

    STFUNC(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigateResult);
            default:
                break;
        }
    }

    STFUNC(StateRead) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKeyValue::TEvReadResponse, HandleReadResponse);
            hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
