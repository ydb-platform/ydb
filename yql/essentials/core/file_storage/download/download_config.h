#pragma once

#include <util/generic/strbuf.h>

#include <vector>

namespace google::protobuf {
    class Message;
}

namespace NYql {

class TFileStorageConfig;

class TDownloadConfigBase {
protected:
    bool FindAndParseConfig(const TFileStorageConfig& cfg, TStringBuf name, ::google::protobuf::Message* msg);
};

template <class TDerived, class TConfig>
class TDownloadConfig: public TDownloadConfigBase {
protected:
    void Configure(const TFileStorageConfig& cfg, TStringBuf name) {
        TConfig downloadCfg;
        if (FindAndParseConfig(cfg, name, &downloadCfg)) {
            static_cast<TDerived*>(this)->DoConfigure(downloadCfg);
        }
    }
};

} // NYql
