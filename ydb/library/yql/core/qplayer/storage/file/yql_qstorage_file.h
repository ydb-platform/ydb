#pragma once
#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql {

struct TFileQStorageSettings {
    bool BufferUntilCommit = true;
};

IQStoragePtr MakeFileQStorage(const TString& folder = {}, const TFileQStorageSettings& settings = {});

};
