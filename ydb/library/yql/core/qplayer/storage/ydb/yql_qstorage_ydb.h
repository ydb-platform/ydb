#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql {

struct TYdbQStorageSettings {
    TString Endpoint;
    TString Database;
    TString Token;
    TString TablesPrefix;
    TString OperationIdPrefix;
    TMaybe<ui64> PartBytes;
    TMaybe<ui32> MaxRetries;
    TMaybe<ui64> MaxBatchSize;
};

IQStoragePtr MakeYdbQStorage(const TYdbQStorageSettings& settings);

}
