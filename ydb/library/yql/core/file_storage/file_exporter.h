#pragma once

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <library/cpp/uri/http_url.h>

namespace NYql {

class IFileExporter {
public:
    virtual std::pair<ui64, TString> ExportToFile(const TFileStorageConfig& Config,
        const TString& convertedUrl,
        const THttpURL& url,
        const TString& dstFile) = 0;
    virtual ~IFileExporter() {}
};

std::unique_ptr<IFileExporter> CreateFileExporter();

};
