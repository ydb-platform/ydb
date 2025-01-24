#include "url_meta.h"
#include <library/cpp/http/io/headers.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NYql {

void TUrlMeta::SaveTo(const TString& path) {
    THttpHeaders headers;
    if (ETag) {
        headers.AddHeader(THttpInputHeader("ETag", ETag));
    }

    if (ContentFile) {
        headers.AddHeader(THttpInputHeader("ContentFile", ContentFile));
    }

    if (Md5) {
        headers.AddHeader(THttpInputHeader("Md5", Md5));
    }

    if (LastModified) {
        headers.AddHeader(THttpInputHeader("LastModified", LastModified));
    }


    TFile outFile(path, CreateAlways | ARW | AX);
    TUnbufferedFileOutput out(outFile);
    headers.OutTo(&out);
}

void TUrlMeta::TryReadFrom(const TString& path) {
    ETag.clear();
    LastModified.clear();
    ContentFile.clear();

    TFileHandle fileHandle(path, OpenExisting | RdOnly | Seq);
    if (!fileHandle.IsOpen()) {
        return;
    }

    TFile file(fileHandle.Release(), path);
    TIFStream in(file);
    THttpHeaders headers(&in);
    for (auto it = headers.Begin(); it != headers.End(); ++it) {
        if (it->Name() == TStringBuf("ETag")) {
            ETag = it->Value();
            continue;
        }

        if (it->Name() == TStringBuf("ContentFile")) {
            ContentFile = it->Value();
            continue;
        }

        if (it->Name() == TStringBuf("Md5")) {
            Md5 = it->Value();
            continue;
        }

        if (it->Name() == TStringBuf("LastModified")) {
            LastModified = it->Value();
            continue;
        }
    }
}

} // NYql
