#pragma once
#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql {

IQStoragePtr MakeMemoryQStorage();

};
