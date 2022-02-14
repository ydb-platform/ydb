#include "download_config.h"

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <google/protobuf/message.h>

namespace NYql {

bool TDownloadConfigBase::FindAndParseConfig(const TFileStorageConfig& cfg, TStringBuf name, ::google::protobuf::Message* msg) {
    if (auto it = cfg.GetDownloaderConfig().find(name); it != cfg.GetDownloaderConfig().end()) {
        try {
            TStringInput in(it->second);
            ParseFromTextFormat(in, *msg, EParseFromTextFormatOption::AllowUnknownField);
            return true;
        } catch (const yexception& e) {
            throw yexception() << "Bad config for \"" << name << "\" downloader" << e;
        }
    }
    return false;
}

}
