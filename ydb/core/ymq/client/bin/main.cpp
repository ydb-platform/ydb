#include <util/random/shuffle.h>
#include <ydb/core/ymq/client/cpp/client.h>

#include <library/cpp/getopt/opt.h>
#include <library/cpp/getopt/modchooser.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/user.h>

#include <google/protobuf/text_format.h>

#include <random>

using namespace NLastGetopt;
using namespace NKikimr::NSQS;

class TSqsOptions : public TOpts {
public:
    TSqsOptions(bool addUserOption = true) {
        SetFreeArgsNum(0);
        AddHelpOption('h');
        AddLongOption('s', "server", "sqs host name")
            .Optional()
            .StoreResult(&Host)
            .DefaultValue("localhost")
            .RequiredArgument("HOST");
        AddLongOption('p', "port", "sqs grpc port number")
            .Optional()
            .StoreResult(&Port)
            .DefaultValue("2135")
            .RequiredArgument("PORT");
        if (addUserOption) {
            AddLongOption('u', "user", "name of user who performs an action")
                .Required()
                .StoreResult(&User)
                .RequiredArgument("USER");
        }
        AddLongOption('o', "oauth-token", "oauth token. Can also be set from SQS_OAUTH_TOKEN environment variable")
            .Optional()
            .StoreResult(&OAuthToken);
        AddLongOption('t', "tvm-ticket", "tvm ticket. Can also be set from SQS_TVM_TICKET environment variable")
            .Optional()
            .StoreResult(&TVMTicket);
    }

    template <class TRequestProto>
    void SetCredentials(TRequestProto& req) {
        if (!OAuthToken) {
            OAuthToken = GetEnv("SQS_OAUTH_TOKEN");
        }
        if (!TVMTicket) {
            TVMTicket = GetEnv("SQS_TVM_TICKET");
        }
        if (OAuthToken && TVMTicket) {
            Cerr << "Please specify either OAuth token or TVM ticket, not both." << Endl;
            exit(1);
        }
        if (OAuthToken) {
            req.MutableCredentials()->SetOAuthToken(OAuthToken);
        }
        if (TVMTicket) {
            req.MutableCredentials()->SetTvmTicket(TVMTicket);
        }

    }

    TString Host;
    ui16 Port = 0;
    TString User;
    TString OAuthToken;
    TString TVMTicket;
};


static int HandleUser(int argc, const char* argv[]) {
    bool list = false;
    bool del = false;
    TString name;

    TSqsOptions opts;
    opts.AddLongOption("list", "list configured users")
        .Optional()
        .NoArgument()
        .SetFlag(&list);
    opts.AddLongOption("delete", "delete user")
        .Optional()
        .NoArgument()
        .SetFlag(&del);
    opts.AddLongOption('n', "name", "name of user to create/delete")
        .Optional()
        .RequiredArgument("NAME")
        .StoreResult(&name);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    if (!list && !name) {
        Cerr << "Name parameter is required for creation/deletion." << Endl;
        return 1;
    }

    if (list) {
        TListUsersRequest req;
        req.MutableAuth()->SetUserName(GetUsername());
        opts.SetCredentials(req);

        auto resp = client.ListUsers(req);
        for (size_t i = 0; i < resp.UserNamesSize(); ++i) {
            Cout << resp.GetUserNames(i) << Endl;
        }
    } else if (del) {
        TDeleteUserRequest req;
        req.MutableAuth()->SetUserName(opts.User);
        req.SetUserName(name);
        opts.SetCredentials(req);

        auto resp = client.DeleteUser(req);
        if (resp.HasError()) {
            Cerr << "Got error for user : "
                 << opts.User << " : "
                 << resp.GetError().GetMessage() << Endl;
            return 1;
        } else {
            Cerr << "The user has been deleted"
                 << Endl;
        }
    } else {
        TCreateUserRequest req;
        req.MutableAuth()->SetUserName(opts.User);
        req.SetUserName(name);
        opts.SetCredentials(req);

        auto resp = client.CreateUser(req);
        if (resp.HasError()) {
            Cerr << "Got error for user : "
                 << opts.User << " : "
                 << resp.GetError().GetMessage() << Endl;
            return 1;
        } else {
            Cerr << "The user has been initialized"
                 << Endl;
        }
    }

    return 0;
}

static int HandlePermissions(int argc, const char* argv[]) {
    TString resource;
    TString grantRequest;
    TString revokeRequest;
    TString setRequest;
    bool clearACL;

    TSqsOptions opts(false);
    opts.AddLongOption('q', "resource", "resource path")
        .Required()
        .StoreResult(&resource);
    opts.AddLongOption('g', "grant", "grant permissions to specified user")
        .Optional()
        .StoreResult(&grantRequest);
    opts.AddLongOption('r', "revoke", "revoke permissions from specified user")
        .Optional()
        .StoreResult(&revokeRequest);
    opts.AddLongOption('x', "set", "set permissions for specified user")
        .Optional()
        .StoreResult(&setRequest);
    opts.AddLongOption('c', "clear-acl", "clear all acl for node")
        .Optional()
        .NoArgument()
        .SetFlag(&clearACL);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TModifyPermissionsRequest req;

    auto parseActionsAndSubject = [](auto& action, TStringBuf request) {
        auto subject = request.NextTok(':');
        if (!subject || !request) {
            return false;
        }
        while (auto actionName = request.NextTok(',')) {
            *action.MutablePermissionNames()->Add() = TString(actionName);
        }
        action.SetSubject(TString(subject));
        return true;
    };

    bool valid = true;
    if (grantRequest) {
        valid &= parseActionsAndSubject(*req.MutableActions()->Add()->MutableGrant(), grantRequest);
    }
    if (revokeRequest) {
        valid &= parseActionsAndSubject(*req.MutableActions()->Add()->MutableRevoke(), revokeRequest);
    }
    if (setRequest) {
        valid &= parseActionsAndSubject(*req.MutableActions()->Add()->MutableSet(), setRequest);
    }

    req.SetResource(resource);

    if (!valid || !(grantRequest || revokeRequest || setRequest || clearACL)) {
        Cerr << "Modify permissions command is invalid. Usage example: --grant username@staff:CreateQueue,SendMessage" << Endl;
        return 1;
    }

    opts.SetCredentials(req);

    if (clearACL) {
        req.SetClearACL(true);
    }

    auto resp = client.ModifyPermissions(req);
    if (resp.HasError()) {
        Cerr << "Got error for resource : "
             << resource << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "OK"
             << Endl;
    }

    return 0;
}

static int HandleListPermissions(int argc, const char* argv[]) {
    TString path;

    TSqsOptions opts(false);
    opts.AddLongOption('P', "path", "path to node")
        .Required()
        .StoreResult(&path);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TListPermissionsRequest req;

    req.SetPath(path);
    opts.SetCredentials(req);

    auto resp = client.ListPermissions(req);
    if (resp.HasError()) {
        Cerr << "Got error for path : "
             << path << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cout << resp.Utf8DebugString() << Endl;
    }

    return 0;
}



int main(int argc, const char* argv[]) {
    try {
        TModChooser mods;
        mods.SetDescription("SQS client tool");
        mods.AddMode("user",             HandleUser,               "initialize user");
        mods.AddMode("permissions",      HandlePermissions,        "modify queue permissions");
        mods.AddMode("list-permissions", HandleListPermissions,    "list permissions");
        return mods.Run(argc, argv);
    } catch (const TQueueException& e) {
        Cerr << "Queue Error: "
             << e.Error().GetErrorCode() << " (" << e.Status() << "): " << e.Message() << Endl;
        Cerr << "Request id: " << e.GetRequestId() << Endl;
        return 1;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
