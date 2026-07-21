#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

constexpr TStringBuf SupportedJoinGroupProtocol = "consumer";
constexpr TStringBuf AssignStrategyRoundRobin = "roundrobin";
constexpr TStringBuf AssignStrategyServer = "server";

TString ConsumeProtocolName(TFuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
        case 0:
            return TString(AssignStrategyRoundRobin);
        case 1:
            return TString(AssignStrategyServer);
        default:
            return NFuzz::ConsumeString(fdp);
    }
}

TJoinGroupRequestData BuildJoinGroupRequest(TFuzzedDataProvider& fdp, TKafkaVersion version, NFuzz::TBytesStorage& bytesStorage) {
    TJoinGroupRequestData request;
    request.GroupId = NFuzz::ConsumeString(fdp);
    request.SessionTimeoutMs = fdp.ConsumeIntegral<TKafkaInt32>();
    if (version >= 1) {
        request.RebalanceTimeoutMs = fdp.ConsumeIntegral<TKafkaInt32>();
    }
    request.MemberId = NFuzz::ConsumeString(fdp);
    if (version >= 5) {
        request.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
    }
    request.ProtocolType = fdp.ConsumeBool() ? TString(SupportedJoinGroupProtocol) : NFuzz::ConsumeString(fdp);

    const size_t protocolCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));
    for (size_t i = 0; i < protocolCount; ++i) {
        TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
        protocol.Name = ConsumeProtocolName(fdp);
        protocol.Metadata = bytesStorage.Hold(NFuzz::ConsumeBytes(fdp, 96));
        request.Protocols.push_back(std::move(protocol));
    }

    if (version >= 8) {
        request.Reason = NFuzz::ConsumeOptionalString(fdp);
    }

    return request;
}

THeartbeatRequestData BuildHeartbeatRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    THeartbeatRequestData request;
    request.GroupId = NFuzz::ConsumeString(fdp);
    request.GenerationId = fdp.ConsumeIntegral<TKafkaInt32>();
    request.MemberId = NFuzz::ConsumeString(fdp);
    if (version >= 3) {
        request.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
    }
    return request;
}

TLeaveGroupRequestData BuildLeaveGroupRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TLeaveGroupRequestData request;
    request.GroupId = NFuzz::ConsumeString(fdp);

    if (version <= 2) {
        request.MemberId = NFuzz::ConsumeString(fdp);
    } else {
        const size_t memberCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));
        for (size_t i = 0; i < memberCount; ++i) {
            TLeaveGroupRequestData::TMemberIdentity member;
            member.MemberId = NFuzz::ConsumeString(fdp);
            member.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
            if (version >= 5) {
                member.Reason = NFuzz::ConsumeOptionalString(fdp);
            }
            request.Members.push_back(std::move(member));
        }
    }

    return request;
}

TSyncGroupRequestData BuildSyncGroupRequest(TFuzzedDataProvider& fdp, TKafkaVersion version, NFuzz::TBytesStorage& bytesStorage) {
    TSyncGroupRequestData request;
    request.GroupId = NFuzz::ConsumeString(fdp);
    request.GenerationId = fdp.ConsumeIntegral<TKafkaInt32>();
    request.MemberId = NFuzz::ConsumeString(fdp);
    if (version >= 3) {
        request.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
    }
    if (version >= 5) {
        request.ProtocolType = NFuzz::ConsumeOptionalString(fdp);
        request.ProtocolName = NFuzz::ConsumeOptionalString(fdp);
    }

    const size_t assignmentCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));
    for (size_t i = 0; i < assignmentCount; ++i) {
        TSyncGroupRequestData::TSyncGroupRequestAssignment assignment;
        assignment.MemberId = NFuzz::ConsumeString(fdp);
        assignment.Assignment = bytesStorage.Hold(NFuzz::ConsumeBytes(fdp, 96));
        request.Assignments.push_back(std::move(assignment));
    }

    return request;
}

TDescribeGroupsRequestData BuildDescribeGroupsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TDescribeGroupsRequestData request;
    const size_t groupCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));
    for (size_t i = 0; i < groupCount; ++i) {
        request.Groups.push_back(NFuzz::ConsumeString(fdp));
    }
    if (version >= 3) {
        request.IncludeAuthorizedOperations = fdp.ConsumeBool();
    }
    return request;
}

void ParseJoinGroupMetadata(const TJoinGroupRequestData& request) {
    for (const auto& protocol : request.Protocols) {
        try {
            NFuzz::ParseVersionedEnvelope<TConsumerProtocolSubscription>(protocol.Metadata, 0, 3);
        } catch (...) {
        }
    }
}

void ParseSyncAssignments(const TSyncGroupRequestData& request) {
    for (const auto& assignment : request.Assignments) {
        try {
            NFuzz::ParseVersionedEnvelope<TConsumerProtocolAssignment>(assignment.Assignment, 0, 3);
        } catch (...) {
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    NFuzz::TBytesStorage bytesStorage;

    switch (fdp.ConsumeIntegralInRange<int>(0, 4)) {
        case 0: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 9);
            const TJoinGroupRequestData request = BuildJoinGroupRequest(fdp, version, bytesStorage);
            NFuzz::ParseGeneratedRequest(fdp, request, version);
            ParseJoinGroupMetadata(request);
            break;
        }
        case 1: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            NFuzz::ParseGeneratedRequest(fdp, BuildHeartbeatRequest(fdp, version), version);
            break;
        }
        case 2: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 5);
            NFuzz::ParseGeneratedRequest(fdp, BuildLeaveGroupRequest(fdp, version), version);
            break;
        }
        case 3: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 5);
            const TSyncGroupRequestData request = BuildSyncGroupRequest(fdp, version, bytesStorage);
            NFuzz::ParseGeneratedRequest(fdp, request, version);
            ParseSyncAssignments(request);
            break;
        }
        case 4: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 5);
            NFuzz::ParseGeneratedRequest(fdp, BuildDescribeGroupsRequest(fdp, version), version);
            break;
        }
    }

    return 0;
}
