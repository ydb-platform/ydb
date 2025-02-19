#pragma once

#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb-cpp-sdk/type_switcher.h>

namespace NYdb::inline V3::NScripting {

class TMockSlyDbProxy : public NYdbProtos::Scripting::V1::ScriptingService::Service
{
public:
    grpc::Status ExecuteYql(
        grpc::ServerContext* context,
        const NYdbProtos::Scripting::ExecuteYqlRequest* request,
        NYdbProtos::Scripting::ExecuteYqlResponse* response) override;
};

}
