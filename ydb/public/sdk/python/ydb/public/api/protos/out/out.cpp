#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, Ydb::Cms::GetDatabaseStatusResult::State, stream, value) {
    stream << Ydb::Cms::GetDatabaseStatusResult::State_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::StatusIds::StatusCode, stream, value) {
    stream << Ydb::StatusIds::StatusCode_Name(value);
}
