#pragma once

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

namespace NYdb::inline V2 {

template<class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const TString& address, TService& service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}

}
