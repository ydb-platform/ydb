#include <ydb/library/yql/core/file_storage/file_exporter.h>
#include <ydb/library/yql/core/file_storage/download_stream.h>
#include <ydb/library/yql/core/file_storage/storage.h>
#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/utils/multi_resource_lock.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/system/shellcommand.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/string/strip.h>
#include <util/folder/dirut.h>

namespace NYql {

class TFileExporterDummy : public IFileExporter {
public:
    virtual std::pair<ui64, TString> ExportToFile(const TFileStorageConfig& Config,
        const TString& convertedUrl,
        const THttpURL& url,
        const TString& dstFile) override
    {
        Y_UNUSED(Config);
        Y_UNUSED(url);

        ythrow yexception() << "VCS is unsupported in current implementation; convertedUrl: " << convertedUrl << ", dstFile: " << dstFile;
    }
};

std::unique_ptr<IFileExporter> CreateFileExporter() {
    return std::make_unique<TFileExporterDummy>();
}

}

