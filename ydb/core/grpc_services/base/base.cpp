#include "base.h"
#include <grpcpp/support/slice.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/stream/str.h>

namespace NKikimr::NGRpcService {

void IRequestProxyCtx::CountRequestPaths() const {
    if (const auto database = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER)) {
        CountDatabasePath(CGIUnescapeRet(*database));
    }
    CountRequestBodyPaths();
}

void IRequestProxyCtx::CountDatabasePath(TStringBuf path) const {
    if (!RelativeDatabaseCounted_ && !path.empty() && !path.StartsWith('/')) {
        if (auto* counters = GetRequestCounters()) {
            counters->CountRelativeDatabase();
            RelativeDatabaseCounted_ = true;
        }
    }
}

void IRequestProxyCtx::CountResourcePath(TStringBuf path) const {
    if (!RelativeResourceCounted_ && !path.empty() && !path.StartsWith('/')) {
        if (auto* counters = GetRequestCounters()) {
            counters->CountRelativeResource();
            RelativeResourceCounted_ = true;
        }
    }
}


void EndGrpcRequestSpanWithStatus(NWilson::TSpan& span, Ydb::StatusIds::StatusCode status) {
    if (!span) {
        return;
    }

    if (status == Ydb::StatusIds::SUCCESS) {
        span.EndOk();
    } else {
        span.Attribute("ydb_status", static_cast<int>(status));
        span.Attribute("ydb_status_name", Ydb::StatusIds_StatusCode_Name(status));
        span.EndError(Ydb::StatusIds_StatusCode_Name(status));
    }
}

grpc::ByteBuffer MakeByteBufferFromSerializedResult(TString&& serializedResult) {
    // res->data() pointer is used inside grpc code.
    // So this object should be destroyed during grpc_slice destroying routine
    auto* res = new TString;
    res->swap(serializedResult);

    static auto freeResult = [](void* p) -> void {
        TString* toDelete = reinterpret_cast<TString*>(p);
        delete toDelete;
    };

    grpc_slice slice = grpc_slice_new_with_user_data(
        const_cast<char*>(res->data()), res->size(), freeResult, res);
    grpc::Slice sl(slice, grpc::Slice::STEAL_REF);
    return grpc::ByteBuffer(&sl, 1);
}

grpc::ByteBuffer MakeByteBufferFromSerializedResult(TRope&& serializedResult) {
    TStackVec<grpc::Slice, 8> slices;
    size_t chunkCount = 0;
    for (auto it = serializedResult.Begin(); it != serializedResult.End(); ++it) {
        ++chunkCount;
    }
    slices.reserve(chunkCount);

    static auto freeChunk = [](void* p) -> void {
        TRcBuf* toDelete = reinterpret_cast<TRcBuf*>(p);
        delete toDelete;
    };

    for (auto it = serializedResult.Begin(); it != serializedResult.End(); ++it) {
        auto* owner = new TRcBuf(it.GetChunk());

        grpc_slice slice = grpc_slice_new_with_user_data(
            const_cast<char*>(it.ContiguousData()), it.ContiguousSize(), freeChunk, owner);
        slices.emplace_back(slice, grpc::Slice::STEAL_REF);
    }
    return grpc::ByteBuffer(slices.data(), slices.size());
}

TActorId CreateGRpcRequestProxyId(int n) {
    if (n == 0) {
        const auto actorId = TActorId(0, "GRpcReqProxy");
        return actorId;
    }

    const auto actorId = TActorId(0, TStringBuilder() << "GRpcReqPro" << n);
    return actorId;
}

const TMaybe<TString> ExtractYdbToken(const TVector<TStringBuf>& authHeadValues) {
    if (authHeadValues.empty()) {
        return {};
    }
    return TString{authHeadValues[0]};
}

const TMaybe<TString> ExtractDatabaseName(const TVector<TStringBuf>& dbHeaderValues) {
    if (dbHeaderValues.empty()) {
        return {};
    }
    return CGIUnescapeRet(dbHeaderValues[0]);
}

TString MakeAuthError(const TString& in, NYql::TIssueManager& issues) {
    TStringStream out;
    out << "unauthenticated"
        << (in ? ", " : "") << in
        << (issues.GetIssues() ? ": " : "");
    issues.GetIssues().PrintTo(out, true /* one line */);
    return out.Str();
}

void TEvHttpRequestAuthAndCheck::ReplyUnauthenticated(const TString& msg)  {
    if (msg) {
        IssueManager.RaiseIssue(NYql::TIssue{msg});
    }
    ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
}

void TEvHttpRequestAuthAndCheck::ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status)  {
    const NActors::TActorContext& ctx = NActors::TActivationContext::AsActorContext();
    FinishSpan();
    if (status == Ydb::StatusIds::SUCCESS) {
        ctx.Send(Sender,
            new TEvRequestAuthAndCheckResult(
                Database,
                YdbToken,
                UserToken,
                GetAuditLogParts(),
                DatabaseAccessVerdict
            )
        );
    } else {
        ctx.Send(Sender,
            new TEvRequestAuthAndCheckResult(
                status,
                IssueManager.GetIssues(),
                GetAuditLogParts(),
                DatabaseAccessVerdict
            )
        );
    }
}

void TEvHttpRequestAuthAndCheck::FinishSpan() {
    if (Span) {
        Span.End();
    }
}

const TString& TEvHttpRequestAuthAndCheck::GetSerializedToken() const {
    if (UserToken) {
        return UserToken->GetSerializedToken();
    }

    return EmptySerializedTokenMessage;
}

} // namespace NKikimr::NGRpcService
