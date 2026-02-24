#pragma once

#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/generic/string.h>
#include <util/system/tempfile.h>
#include <util/stream/file.h>

#include <algorithm>
#include <cstring>

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

inline TTempFileHandle MakeTestFile(const TStringBuf content, const TString& name = "test", const TString& extension = "") {
    TTempFileHandle tmpFile = TTempFileHandle::InCurrentDir(name, extension);
    TUnbufferedFileOutput ofs(tmpFile.Name());
    ofs.Write(content);
    ofs.Finish();
    return tmpFile;
}

template <size_t N>
inline NMVP::TMvpStartupOptions MakeOpts(const char* (&argv)[N]) {
    return NMVP::TMvpStartupOptions::Build(N, argv);
}

template <size_t N>
inline NMVP::TMvpStartupOptions MakeOpts(const char* (&&argv)[N]) {
    return NMVP::TMvpStartupOptions::Build(N, argv);
}

class TMvpTestRuntime : public NActors::TTestActorRuntimeBase {
    using NActors::TTestActorRuntimeBase::TTestActorRuntimeBase;

    void InitNodeImpl(TNodeDataBase*, size_t) override;
};
