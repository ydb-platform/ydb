#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

namespace NYdb::inline Dev {

const Ydb::Import::ListObjectsInS3ExportResult& TProtoAccessor::GetProto(const NYdb::NImport::TListObjectsInS3ExportResult& result) {
    return result.GetProto();
}

}
