#pragma once
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql {

struct TFileQStorageSettings {
    bool BufferUntilCommit = true;
    bool AlwaysFlushIndex = false;
};

IQStoragePtr MakeFileQStorage(const TString& folder = {}, const TFileQStorageSettings& settings = {});

};
