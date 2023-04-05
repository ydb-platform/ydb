#include "action.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <google/protobuf/text_format.h>

#include <util/string/ascii.h>
#include <util/string/vector.h>
#include <util/string/cast.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

class TListPermissionsActor
   : public TActionActor<TListPermissionsActor> {
public:
    TListPermissionsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ListPermissions, std::move(cb))
    {
    }

    static constexpr bool NeedExistingQueue() {
        return false;
    }

    static constexpr bool NeedUserSpecified() {
        return false;
    }

private:
    bool DoValidate() override {
        if (!Request().GetPath()) {
            MakeError(Response_.MutableListPermissions(), NErrors::MISSING_PARAMETER, "No Path parameter.");
            return false;
        }

        Path_ = MakeAbsolutePath(Request().GetPath());

        if (IsForbiddenPath(Path_)) {
            MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, Sprintf("Path does not exist: %s.", SanitizeNodePath(Path_).c_str()));
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableListPermissions()->MutableError();
    }

    virtual TString GetCustomACLPath() const override {
        return MakeAbsolutePath(Request().GetPath());
    }

    TString DoGetQueueName() const override {
        return {};
    }

    void RequestSchemeShard(const TString& path) {
        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(path);

        Send(MakeTxProxyID(), navigateRequest.release());
    }

    void DoAction() override {
        Become(&TThis::WaitSchemeShardResponse);
        RequestSchemeShard(Path_);
    }

    STATEFN(WaitSchemeShardResponse) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeShardResponse);
        }
    }

    using TGroupedACE = THashMap<TString, TSet<TStringBuf>>;

    TGroupedACE GroupACEBySID(const NACLibProto::TACL& acl) const {
        TGroupedACE result;
        for (const NACLibProto::TACE& ace : acl.GetACE()) {
            if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Allow) {
                if (ace.GetSID()) {
                    auto& acesForSIDSet = result[ace.GetSID()];
                    const auto aces = GetAccessMatchingACE(ace.GetAccessRight());
                    for (const TStringBuf& aceName : aces) {
                        acesForSIDSet.insert(aceName);
                    }
                }
            }
        }

        return result;
    }

    template<typename TMutablePermissions>
    void ConvertACLToPermissions(const NACLibProto::TACL& acl, TMutablePermissions& nodePermissions) const {
        const auto acesBySID = GroupACEBySID(acl);
        for (auto&& pair : acesBySID) {
            const auto& SID = pair.first;
            const auto& acesSet = pair.second;

            auto* permissions = nodePermissions.Add();

            permissions->SetSubject(SID);
            for (const auto& aceName : acesSet) {
                permissions->MutablePermissionNames()->Add(TString(aceName));
            }
        }
    }

    void HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                const NKikimrSchemeOp::TDirEntry& entry = pathDescription.GetSelf();

                TSecurityObject secObjWithACL(entry.GetOwner(), entry.GetACL(), false);
                TSecurityObject secObjWithEffectiveACL(entry.GetOwner(), entry.GetEffectiveACL(), false);

                auto convertACLToPermissions = [this, &secObjWithACL, &secObjWithEffectiveACL](auto* mutableNodePermissions) {
                    ConvertACLToPermissions(secObjWithACL.GetACL(), *mutableNodePermissions->MutablePermissions());
                    ConvertACLToPermissions(secObjWithEffectiveACL.GetACL(), *mutableNodePermissions->MutableEffectivePermissions());
                };

                const size_t depth = CalculatePathDepth(SanitizeNodePath(Path_));
                if (depth < 2) {
                    convertACLToPermissions(Response_.MutableListPermissions()->MutableAccountPermissions());
                } else {
                    convertACLToPermissions(Response_.MutableListPermissions()->MutableQueuePermissions());
                }

                break;
            }
            case NKikimrScheme::StatusPathDoesNotExist: {
                MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, Sprintf("Path does not exist: %s.", SanitizeNodePath(Path_).c_str()));
                break;
            }
            default: {
                MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
                break;
            }
        }

        SendReplyAndDie();
    }

    const TListPermissionsRequest& Request() const {
        return SourceSqsRequest_.GetListPermissions();
    }

private:
    TString Path_;
};

IActor* CreateListPermissionsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListPermissionsActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
