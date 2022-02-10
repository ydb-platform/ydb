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

static int HandleChange(int argc, const char* argv[]) {
    TString name;
    TString receipt;
    ui64 timeout;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of creating queue")
        .Required()
        .StoreResult(&name);
    opts.AddLongOption('r', "receipt", "receipt handle")
        .Required()
        .StoreResult(&receipt);
    opts.AddLongOption('t', "timeout", "visibility timeout")
        .Required()
        .StoreResult(&timeout);

    TOptsParseResult res(&opts, argc, argv);

    TChangeMessageVisibilityRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(name);
    req.SetReceiptHandle(receipt);
    req.SetVisibilityTimeout(timeout);
    opts.SetCredentials(req);
    auto resp = TQueueClient(TClientOptions().SetHost(opts.Host).SetPort(opts.Port)).ChangeMessageVisibility(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << name << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "Visibility timeout has been changed" << Endl;
    }
    return 0;
}

static int HandleCreate(int argc, const char* argv[]) {
    TString queue;
    ui64    shards = 0;
    ui64    partitions = 0;
    ui64    retentionSeconds = 0;
    bool    enableOutOfOrderExecution = false;
    bool    disableOutOfOrderExecution = false;
    bool    contentBasedDeduplication = false;
    bool    enableAutosplit = false;
    bool    disableAutosplit = false;
    ui64    sizeToSplit = 0;

    TCreateQueueRequest req;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of creating queue")
        .Required()
        .StoreResult(&queue);
    opts.AddLongOption("shards", "number of shards")
        .Optional()
        .StoreResult(&shards)
        .DefaultValue(ToString(req.GetShards()));
    opts.AddLongOption("partitions", "number of data partitions")
        .Optional()
        .StoreResult(&partitions)
        .DefaultValue(ToString(req.GetPartitions()));
    opts.AddLongOption("enable-out-of-order-execution", "enable internal tables out of order execution")
        .NoArgument()
        .SetFlag(&enableOutOfOrderExecution)
        .DefaultValue("false");
    opts.AddLongOption("disable-out-of-order-execution", "disable internal tables out of order execution")
        .NoArgument()
        .SetFlag(&disableOutOfOrderExecution)
        .DefaultValue("false");
    opts.AddLongOption("retention-seconds", "retention time in seconds")
        .Optional()
        .StoreResult(&retentionSeconds);
    opts.AddLongOption("content-based-deduplication", "enable content based deduplication")
        .NoArgument()
        .SetFlag(&contentBasedDeduplication)
        .DefaultValue("false");
    opts.AddLongOption("enable-auto-split", "enable autosplit for tables with message data")
        .NoArgument()
        .SetFlag(&enableAutosplit)
        .DefaultValue("false");
    opts.AddLongOption("disable-auto-split", "disable autosplit for tables with message data")
        .NoArgument()
        .SetFlag(&disableAutosplit)
        .DefaultValue("false");
    opts.AddLongOption("size-to-split", "size of message data datashard to split (bytes)")
        .Optional()
        .StoreResult(&sizeToSplit)
        .DefaultValue(ToString(req.GetSizeToSplit()));

    TOptsParseResult res(&opts, argc, argv);

    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queue);
    if (queue.EndsWith(".fifo")) {
        auto fifoAttr = req.AddAttributes();
        fifoAttr->SetName("FifoQueue");
        fifoAttr->SetValue("true");
    }
    if (contentBasedDeduplication) {
        auto dedupAttr = req.AddAttributes();
        dedupAttr->SetName("ContentBasedDeduplication");
        dedupAttr->SetValue("true");
    }
    req.SetShards(shards);
    req.SetPartitions(partitions);
    if (enableOutOfOrderExecution) {
        req.SetEnableOutOfOrderTransactionsExecution(true);
    } else if (disableOutOfOrderExecution) {
        req.SetEnableOutOfOrderTransactionsExecution(false);
    }
    opts.SetCredentials(req);

    if (retentionSeconds) {
        auto* newAttribute = req.mutable_attributes()->Add();
        newAttribute->SetName("MessageRetentionPeriod");
        newAttribute->SetValue(ToString(retentionSeconds));
    }

    if (enableAutosplit) {
        req.SetEnableAutosplit(true); // explicitly
    } else if (disableAutosplit) {
        req.SetEnableAutosplit(false); // explicitly
    }
    if (req.GetEnableAutosplit()) {
        if (sizeToSplit == 0) {
            Cerr << "SizeToSplit can't be zero." << Endl;
            return 1;
        }
        req.SetSizeToSplit(sizeToSplit);
    }

    auto resp = TQueueClient(TClientOptions().SetHost(opts.Host).SetPort(opts.Port)).CreateQueue(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queue << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "New queue has been created : "
             << resp.GetQueueName() << Endl;
        Cout << resp.GetQueueUrl() << Endl;
    }
    return 0;
}

static int HandleDelete(int argc, const char* argv[]) {
    TString queue;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of deleting queue")
        .Required()
        .StoreResult(&queue);

    TOptsParseResult res(&opts, argc, argv);

    TDeleteQueueRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queue);
    opts.SetCredentials(req);

    auto resp = TQueueClient(TClientOptions().SetHost(opts.Host).SetPort(opts.Port)).DeleteQueue(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queue << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "The queue has been deleted"
             << Endl;
    }
    return 0;
}

static int HandleList(int argc, const char* argv[]) {
    TSqsOptions opts;

    TOptsParseResult res(&opts, argc, argv);

    TListQueuesRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    opts.SetCredentials(req);

    auto resp = TQueueClient(TClientOptions().SetHost(opts.Host).SetPort(opts.Port)).ListQueues(req);

    if (resp.HasError()) {
        Cerr << "Got error: "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        for (auto item : resp.queues()) {
            Cout << "Queue : " << item.GetQueueName() << Endl;
        }
    }
    return 0;
}

static int HandleSend(int argc, const char* argv[]) {
    TString queueName;
    TString data;
    TString groupId;
    TString dedup;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of queue")
        .Required()
        .StoreResult(&queueName);
    opts.AddLongOption('d', "data", "message body")
        .Required()
        .StoreResult(&data);
    opts.AddLongOption('g', "group", "message group id")
        .Optional()
        .StoreResult(&groupId);
    opts.AddLongOption("dedup", "deduplication token")
        .Optional()
        .StoreResult(&dedup);

    TOptsParseResult res(&opts, argc, argv);

    TSendMessageRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queueName);
    req.SetMessageBody(data);
    req.SetMessageGroupId(groupId);
    req.SetMessageDeduplicationId(dedup);
    opts.SetCredentials(req);

    auto resp = TQueueClient(TClientOptions().SetHost(opts.Host).SetPort(opts.Port)).SendMessage(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queueName << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "The message has been sent:"
             << " id = " << resp.GetMessageId()
             << " seqno = " << resp.GetSequenceNumber()
             << Endl;
    }
    return 0;
}

static int HandleRead(int argc, const char* argv[]) {
    TString queueName;
    TString attemptId;
    int count = 1;
    ui64 waitTime = 0;
    bool keep = false;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of deleting queue")
        .Required()
        .StoreResult(&queueName);
    opts.AddLongOption('a', "attempt-id", "attempt-id")
        .Optional()
        .StoreResult(&attemptId);
    opts.AddLongOption("count", "read count")
        .Optional()
        .StoreResult(&count)
        .DefaultValue("1");
    opts.AddLongOption("keep", "don't commit readed messages")
        .Optional()
        .NoArgument()
        .SetFlag(&keep);
    opts.AddLongOption("wait", "wait time in seconds")
        .Optional()
        .StoreResult(&waitTime)
        .DefaultValue("0");

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TReceiveMessageRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queueName);
    req.SetMaxNumberOfMessages(count);
    req.SetVisibilityTimeout(60);
    req.SetReceiveRequestAttemptId(attemptId);
    req.SetWaitTimeSeconds(waitTime);
    opts.SetCredentials(req);
    auto resp = client.ReceiveMessage(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queueName << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else if (resp.MessagesSize()) {
        TVector<TString> receipts;

        for (size_t i = 0; i < resp.MessagesSize(); ++i) {
            const auto& msg = resp.GetMessages(i);

            Cout << "The message has been received: data = " << msg.GetData() << "  "
                 << "groupId = " << msg.GetMessageGroupId() << " "
                 << "attemptId = " << resp.GetReceiveRequestAttemptId()
                 << Endl;

            receipts.push_back(msg.GetReceiptHandle());
        }

        if (!keep) {
            Shuffle(receipts.begin(), receipts.end());

            for (auto ri = receipts.begin(); ri != receipts.end(); ++ri) {
                TDeleteMessageRequest d;
                d.MutableAuth()->SetUserName(opts.User);
                d.SetQueueName(queueName);
                d.SetReceiptHandle(*ri);
                auto del = client.DeleteMessage(d);

                if (del.HasError()) {
                    Cerr << "Got error for queue on deletion : "
                         << queueName << " : "
                         << del.GetError().GetMessage() << Endl;
                }
            }
        } else {
            for (auto ri = receipts.begin(); ri != receipts.end(); ++ri) {
                Cout << "Receipt: " << *ri << Endl;
            }
        }
    } else {
        Cout << "No messages" << Endl;
    }
    return 0;
}

static int HandlePurge(int argc, const char* argv[]) {
    TString queueName;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of deleting queue")
        .Required()
        .StoreResult(&queueName);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TPurgeQueueRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queueName);
    opts.SetCredentials(req);

    auto resp = client.PurgeQueue(req);
    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queueName << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cerr << "The queue has been purged"
             << Endl;
    }

    return 0;
}

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



static int HandleGetQueueAttributes(int argc, const char* argv[]) {
    TString queueName;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of deleting queue")
        .Required()
        .StoreResult(&queueName);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TGetQueueAttributesRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queueName);
    req.AddNames("All");
    opts.SetCredentials(req);
    auto resp = client.GetQueueAttributes(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queueName << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cout << resp.Utf8DebugString() << Endl;
    }
    return 0;
}

static int HandleSetQueueAttributes(int argc, const char* argv[]) {
    TString queueName;
    TString name, value;

    TSqsOptions opts;
    opts.AddLongOption('q', "queue-name", "name of deleting queue")
        .Required()
        .StoreResult(&queueName);
    opts.AddLongOption('n', "name", "name of queue attribute")
        .Required()
        .StoreResult(&name);
    opts.AddLongOption('v', "value", "value of queue attribute to set")
        .Required()
        .StoreResult(&value);

    TOptsParseResult res(&opts, argc, argv);

    TQueueClient client(TClientOptions().SetHost(opts.Host).SetPort(opts.Port));

    TSetQueueAttributesRequest req;
    req.MutableAuth()->SetUserName(opts.User);
    req.SetQueueName(queueName);
    opts.SetCredentials(req);
    auto* attr = req.AddAttributes();
    attr->SetName(name);
    attr->SetValue(value);
    auto resp = client.SetQueueAttributes(req);

    if (resp.HasError()) {
        Cerr << "Got error for queue : "
             << queueName << " : "
             << resp.GetError().GetMessage() << Endl;
        return 1;
    } else {
        Cout << "OK" << Endl;
    }
    return 0;
}

int main(int argc, const char* argv[]) {
    try {
        TModChooser mods;
        mods.SetDescription("SQS client tool");
        mods.AddMode("change",           HandleChange,             "change visibility timeout");
        mods.AddMode("create",           HandleCreate,             "create queue");
        mods.AddMode("delete",           HandleDelete,             "delete queue");
        mods.AddMode("list",             HandleList,               "list existing queues");
        mods.AddMode("read",             HandleRead,               "receive and delete message");
        mods.AddMode("send",             HandleSend,               "send message");
        mods.AddMode("purge",            HandlePurge,              "purge queue");
        mods.AddMode("user",             HandleUser,               "initialize user");
        mods.AddMode("permissions",      HandlePermissions,        "modify queue permissions");
        mods.AddMode("get-attributes",   HandleGetQueueAttributes, "get queue attributes");
        mods.AddMode("set-attributes",   HandleSetQueueAttributes, "set queue attributes");
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
