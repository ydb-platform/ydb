#pragma once

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

template <typename TOpts, size_t N>
TOpts MakeOpts(const char* (&argv)[N]) {
    return TOpts::Build(N, argv);
}

template <typename TOpts, size_t N>
TOpts MakeOpts(const char* (&&argv)[N]) {
    return TOpts::Build(N, argv);
}

inline TTempFileHandle MakeTestFile(const TStringBuf content, const TString& name = "test", const TString& extension = "") {
    TTempFileHandle tmpFile = TTempFileHandle::InCurrentDir(name, extension);
    TUnbufferedFileOutput ofs(tmpFile.Name());
    ofs.Write(content);
    ofs.Finish();
    return tmpFile;
}

class TMvpTestRuntime : public NActors::TTestActorRuntimeBase {
    using NActors::TTestActorRuntimeBase::TTestActorRuntimeBase;

    void InitNodeImpl(TNodeDataBase*, size_t) override;
};
