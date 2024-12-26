#include "client.h"

#include <ydb/public/lib/deprecated/client/grpc_client.h>

#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/system/defaults.h>
#include <util/system/event.h>
#include <util/system/user.h>

using namespace NKikimr::NGRpcProxy;

namespace NKikimr::NSQS {

class TQueueClient::TImpl {
public:
    TImpl(const TClientOptions& options)
        : Options_(options)
        , Client_(TGRpcClientConfig(Join(":", options.Host, options.Port)))
    {
    }

#define METHOD_IMPL(name, hint) \
    Y_CAT(Y_CAT(T, name), Response) name(const Y_CAT(Y_CAT(T, name), Request)& req) { \
        NKikimrClient::TSqsRequest request; \
        Y_CAT(Y_CAT(T, name), Response) resp; \
        request.Y_CAT(Mutable, name)()->CopyFrom(req);  \
        TAutoEvent e; \
        Client_.SqsRequest(request, [e, &resp] (const NGRpcProxy::TGrpcError* error, const NKikimrClient::TSqsResponse& result) mutable \
            { \
                if (error) { \
                    resp.MutableError()->SetStatus(502); \
                    resp.MutableError()->SetMessage(error->first); \
                } else { \
                    resp.CopyFrom(result.Y_CAT(Get, name)());   \
                } \
                e.Signal(); \
            } \
        ); \
        e.WaitI(); \
        if (resp.HasError() && Options_.Throw) { \
            ythrow TQueueException(resp.GetError(), resp.GetRequestId()); \
        } \
        return resp; \
    }

    METHOD_IMPL(CreateUser,              "can't create a user");
    METHOD_IMPL(DeleteUser,              "can't delete user");
    METHOD_IMPL(ListUsers,               "can't list users");
    METHOD_IMPL(ModifyPermissions,       "can't modify permissions");
    METHOD_IMPL(ListPermissions,         "can't list permissions");

#undef METHOD_IMPL

private:
    const TClientOptions Options_;
    TGRpcClient Client_;
};

TQueueClient::TQueueClient(const TClientOptions& options)
    : Impl_(new TImpl(options))
{ }

TQueueClient::~TQueueClient()
{ }

TCreateUserResponse TQueueClient::CreateUser(const TCreateUserRequest& req) {
    return Impl_->CreateUser(req);
}

TDeleteUserResponse TQueueClient::DeleteUser(const TDeleteUserRequest& req) {
    return Impl_->DeleteUser(req);
}

TListUsersResponse TQueueClient::ListUsers(const TListUsersRequest& req) {
    return Impl_->ListUsers(req);
}

TModifyPermissionsResponse TQueueClient::ModifyPermissions(const TModifyPermissionsRequest& req) {
    return Impl_->ModifyPermissions(req);
}

TListPermissionsResponse TQueueClient::ListPermissions(const TListPermissionsRequest& req) {
    return Impl_->ListPermissions(req);
}

} // namespace NKikimr::NSQS
