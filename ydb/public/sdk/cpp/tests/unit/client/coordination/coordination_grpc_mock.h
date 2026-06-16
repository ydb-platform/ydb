#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <grpc/impl/channel_arg_names.h>

#include <util/generic/mapfindptr.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace NCoordinationTest {

class TMockDiscoveryService : public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    grpc::Status ListEndpoints(
            grpc::ServerContext* context,
            const Ydb::Discovery::ListEndpointsRequest* request,
            Ydb::Discovery::ListEndpointsResponse* response) override
    {
        Y_UNUSED(context);

        const auto* result = MapFindPtr(MockResults, request->database());
        Y_ABORT_UNLESS(result, "Mock service doesn't have a result for database '%s'", request->database().c_str());

        auto* op = response->mutable_operation();
        op->set_ready(true);
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->mutable_result()->PackFrom(*result);
        return grpc::Status::OK;
    }

    std::unordered_map<std::string, Ydb::Discovery::ListEndpointsResult> MockResults;
};

class TMockCoordinationService : public Ydb::Coordination::V1::CoordinationService::Service {
public:
    std::atomic<bool> FailNextAcquire{false};
    // Zero means unlimited pong responses; set to a positive value to stop responding
    // after N pings (used by SessionPingTimeout).
    std::atomic<size_t> MaxPingResponses{0};

    grpc::Status Session(
            grpc::ServerContext* context,
            grpc::ServerReaderWriter<
                Ydb::Coordination::SessionResponse,
                Ydb::Coordination::SessionRequest>* stream) override
    {
        Y_UNUSED(context);

        Ydb::Coordination::SessionRequest request;

        if (!stream->Read(&request)) {
            return grpc::Status::OK;
        }
        Y_ABORT_UNLESS(request.has_session_start(), "Expected session start");
        auto& start = request.session_start();
        uint64_t sessionId = start.session_id();
        if (!sessionId) {
            sessionId = ++LastSessionId_;
        }
        {
            Ydb::Coordination::SessionResponse response;
            auto* started = response.mutable_session_started();
            started->set_session_id(sessionId);
            started->set_timeout_millis(start.timeout_millis());
            stream->Write(response);
        }
        request.Clear();

        size_t pingsReceived = 0;
        while (stream->Read(&request)) {
            if (request.has_ping()) {
                const size_t maxResponses = MaxPingResponses.load();
                if (!maxResponses || ++pingsReceived <= maxResponses) {
                    Ydb::Coordination::SessionResponse response;
                    auto* pong = response.mutable_pong();
                    pong->set_opaque(request.ping().opaque());
                    stream->Write(response);
                }
            } else if (request.has_acquire_semaphore()) {
                const auto& acquire = request.acquire_semaphore();
                Ydb::Coordination::SessionResponse response;
                auto* result = response.mutable_acquire_semaphore_result();
                result->set_req_id(acquire.req_id());
                if (FailNextAcquire.exchange(false)) {
                    result->set_status(Ydb::StatusIds::BAD_REQUEST);
                    result->set_acquired(false);
                } else {
                    std::lock_guard guard(SemaphoreLock_);
                    auto& sem = Semaphores_[acquire.name()];
                    if (!sem.Held) {
                        sem.Held = true;
                        sem.OwnerSessionId = sessionId;
                        sem.Data = acquire.data();
                        result->set_status(Ydb::StatusIds::SUCCESS);
                        result->set_acquired(true);
                    } else {
                        result->set_status(Ydb::StatusIds::SUCCESS);
                        result->set_acquired(false);
                    }
                }
                stream->Write(response);
            } else if (request.has_release_semaphore()) {
                const auto& release = request.release_semaphore();
                Ydb::Coordination::SessionResponse response;
                auto* result = response.mutable_release_semaphore_result();
                result->set_req_id(release.req_id());
                std::lock_guard guard(SemaphoreLock_);
                auto it = Semaphores_.find(release.name());
                if (it != Semaphores_.end() && it->second.Held && it->second.OwnerSessionId == sessionId) {
                    it->second.Held = false;
                    it->second.OwnerSessionId = 0;
                    it->second.Data.clear();
                    result->set_status(Ydb::StatusIds::SUCCESS);
                    result->set_released(true);
                } else {
                    result->set_status(Ydb::StatusIds::SUCCESS);
                    result->set_released(false);
                }
                stream->Write(response);
            } else if (request.has_describe_semaphore()) {
                const auto& describe = request.describe_semaphore();
                Ydb::Coordination::SessionResponse response;
                auto* result = response.mutable_describe_semaphore_result();
                result->set_req_id(describe.req_id());
                result->set_status(Ydb::StatusIds::SUCCESS);
                auto* desc = result->mutable_semaphore_description();
                desc->set_name(describe.name());
                desc->set_ephemeral(true);
                std::lock_guard guard(SemaphoreLock_);
                if (const auto* sem = MapFindPtr(Semaphores_, describe.name()); sem && sem->Held) {
                    auto* owner = desc->add_owners();
                    owner->set_session_id(sem->OwnerSessionId);
                    owner->set_count(static_cast<uint64_t>(-1));
                    owner->set_data(sem->Data);
                }
                stream->Write(response);
            } else if (request.has_session_stop()) {
                Ydb::Coordination::SessionResponse response;
                response.mutable_session_stopped();
                stream->Write(response);
                return grpc::Status::OK;
            } else {
                Y_ABORT_UNLESS(false, "Unexpected session request: %s", request.ShortDebugString().c_str());
            }
            request.Clear();
        }

        return grpc::Status::OK;
    }

private:
    struct TSemaphoreState {
        bool Held = false;
        uint64_t OwnerSessionId = 0;
        std::string Data;
    };

    std::atomic<uint64_t> LastSessionId_{0};
    std::mutex SemaphoreLock_;
    std::unordered_map<std::string, TSemaphoreState> Semaphores_;
};

template<class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(NYdb::TStringType{address}, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    // Match YDB server defaults: allow client keepalive pings used by the SDK driver.
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 1000);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    return builder.BuildAndStart();
}

} // namespace NCoordinationTest
