#pragma once

#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>
#include "access_service_mock.h"
#include "datastreams_service_mock.h"

class TDataStreamsServiceMock : public Ydb::DataStreams::V1::DataStreamsService::Service {
public:
    virtual grpc::Status PutRecords(grpc::ServerContext*,
        const Ydb::DataStreams::V1::PutRecordsRequest* request,
        Ydb::DataStreams::V1::PutRecordsResponse* response) override
    {
        Y_UNUSED(response);
        for (const auto& record : request->records()) {
            if (record.partition_key() == "Sleep") {
                Sleep(TDuration::Seconds(3));
            } else if (record.partition_key() == "InvalidArgument") {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid argument");
            } else if (record.partition_key() == "Unauthenticated") {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Access denied");
            }
        }
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_id("12345");

        return grpc::Status::OK;
    }

};

