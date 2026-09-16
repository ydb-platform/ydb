#pragma once

#include <util/generic/fwd.h>
#include <ydb/core/path_aliasing/context/path_context.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace google::protobuf {
class Message;
class Arena;
}

namespace NACLib {
class TUserToken;

}
namespace NKikimr {
struct TAppData;

namespace NRpcService {
struct  TRlPath;

}

namespace NGRpcService {

enum class EPathInputOrigin {
    Unspecified,
    Logical,
    Resolved,
};

// Server-owned provenance, never populated from request headers or body fields.
// Database and resource operands may reach a boundary at different stages.
struct TPathRewriteSettings {
    std::shared_ptr<const NPathAliasing::TPathContext> Context;
    EPathInputOrigin Database = EPathInputOrigin::Unspecified;
    EPathInputOrigin Resources = EPathInputOrigin::Unspecified;

    static TPathRewriteSettings UserInput();
    static TPathRewriteSettings Internal();
};

using TAuditLogParts = TVector<std::pair<TString, TString>>;
using TAuditLogHook = std::function<void (ui32 status, const TAuditLogParts&)>;

class IRequestCtxBaseMtSafe {
public:
    // Configure before actor dispatch. An initialized context is immutable.
    void SetPathRewriteSettings(TPathRewriteSettings settings);
    const TPathRewriteSettings& GetPathRewriteSettings() const noexcept;
    TString InitializePathRewriteContext(const TAppData& appData);
    bool HasActivePathRewriting() const noexcept;
    TMaybe<TString> GetLogicalDatabaseName() const;
    TString GetPathRewriteFingerprint() const;
    TMaybe<TString> GetPathResolvedDatabase(TMaybe<TString> originalDatabase) const;
    TConclusion<NPathAliasing::TResolvedSchemaPath> NormalizePath(const TString& completeLogicalCandidate) const;

    virtual TMaybe<TString> GetTraceId() const = 0;
    virtual NWilson::TTraceId GetWilsonTraceId() const = 0;
    // Returns the effective database; GetLogicalDatabaseName retains user input.
    virtual const TMaybe<TString> GetDatabaseName() const = 0;
    // Returns "internal" token (result of ticket parser authentication)
    virtual const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const = 0;
    // Returns internal token as a serialized message.
    virtual const TString& GetSerializedToken() const = 0;
    virtual bool IsClientLost() const = 0;
    // Is this call made from inside YDB?
    virtual bool IsInternalCall() const {
        return false;
    }
    // Meta value from request
    virtual const TMaybe<TString> GetPeerMetaValues(const TString&) const = 0;
    // Return address of the peer
    virtual TString GetPeerName() const = 0;
    virtual const TString& GetRequestName() const = 0;
    // Returns path and resource for rate limiter
    virtual TMaybe<NRpcService::TRlPath> GetRlPath() const = 0;
    // Return deadile of request execution, calculated from client timeout by grpc
    virtual TInstant GetDeadline() const = 0;

private:
    TPathRewriteSettings PathRewrite_;
    bool PathRewriteInitialized_ = false;
};


// Provide methods which can be safely passed though actor system // as part of event
class IRequestCtxMtSafe : public virtual IRequestCtxBaseMtSafe {
public:
    virtual ~IRequestCtxMtSafe() = default;
    virtual const google::protobuf::Message* GetRequest() const = 0;
    virtual const TMaybe<TString> GetRequestType() const = 0;
    // Implementation must be thread safe
    virtual void SetFinishAction(std::function<void()>&& cb) = 0;
    // Allocation is thread safe. https://protobuf.dev/reference/cpp/arenas/#thread-safety
    virtual google::protobuf::Arena* GetArena() = 0;
};

}
}
