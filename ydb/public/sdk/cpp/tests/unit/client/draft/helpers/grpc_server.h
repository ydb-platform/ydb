#pragma once

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <ydb-cpp-sdk/type_switcher.h>

namespace NYdb::inline Dev {

template<class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(TStringType{address}, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}

}
