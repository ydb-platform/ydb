#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/tx/schemeshard/schemeshard_user_attr_limits.h>
#include <ydb/core/protos/bind_channel_storage_pool.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>


#include <ydb/library/grpc/client/grpc_client_low.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/hash.h>
#include <util/string/split.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NDriverClient {

void WarnProfilePathSet() {
    Cout << "FYI: profile path is set. You can use short pathnames. Try --help for more info." << Endl;
}

class TClientCommandSchemaMkdir : public TClientCommand {
public:
    TClientCommandSchemaMkdir()
        : TClientCommand("mkdir", {}, "Create directory")
    {}

    TAutoPtr<NKikimrClient::TSchemeOperation> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<NAME>", "Full pathname of a directory (e.g. /ru/home/user/mydb/test1/test2).\n"
            "          Or short pathname if profile path is set (e.g. test1/test2).");
    }

    TString Base;
    TString Name;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        TString pathname = config.ParseResult->GetFreeArgs()[0];
        size_t pos = pathname.rfind('/');
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Base = config.Path;
                Name = pathname;
                return;
            } else {
                WarnProfilePathSet();
            }
        }
        Base = pathname.substr(0, pos);
        Name = pathname.substr(pos + 1);
    }

    virtual int Run(TConfig& config) override {
        auto handler = [this](NClient::TKikimr& kikimr) {
            kikimr.GetSchemaRoot(Base).MakeDirectory(Name);
            return 0;
        };
        return InvokeThroughKikimr(config, std::move(handler));
    }
};

class TClientCommandSchemaDrop : public TClientCommand {
public:
    TClientCommandSchemaDrop()
        : TClientCommand("drop", {}, "Remove schema object")
    {}

    TAutoPtr<NKikimrClient::TSchemeOperation> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<NAME>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "          Or short pathname if profile path is set (e.g. test1/test2).");
    }

    TString Base;
    TString Name;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        TString pathname = config.ParseResult->GetFreeArgs()[0];
        size_t pos = pathname.rfind('/');
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Base = config.Path;
                Name = pathname;
                return;
            } else {
                WarnProfilePathSet();
            }
        }
        Base = pathname.substr(0, pos);
        Name = pathname.substr(pos + 1);
    }

    virtual int Run(TConfig& config) override {
        auto handler = [this](NClient::TKikimr& kikimr) {
            kikimr.GetSchemaRoot(Base).GetChild(Name).Drop();
            return 0;
        };
        return InvokeThroughKikimr(config, std::move(handler));
    }
};

class TClientCommandSchemaExec : public TClientCommandBase {
public:
    TClientCommandSchemaExec()
        : TClientCommandBase("execute", { "exec" }, "Execute schema protobuf")
    {}

    bool ReturnTxId;

    TList<TAutoPtr<NKikimrClient::TSchemeOperation>> Requests;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        ReturnTxId = false;
        config.Opts->AddLongOption('t', "txid", "Print TxId").NoArgument().SetFlag(&ReturnTxId);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<SCHEMA-PROTO>", "Schema protobuf or file with schema protobuf");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        NKikimrSchemeOp::TModifyScript protoScript;
        ParseProtobuf(&protoScript, config.ParseResult->GetFreeArgs()[0]);
        for (const auto& modifyScheme : protoScript.GetModifyScheme()) {
            TAutoPtr<NKikimrClient::TSchemeOperation> request = new NKikimrClient::TSchemeOperation();
            request->MutablePollOptions()->SetTimeout(NClient::TKikimr::POLLING_TIMEOUT);
            request->MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
            Requests.emplace_back(request);
        }
    }

    virtual int Run(TConfig& config) override {
        int result = 0;
        for (const auto& pbRequest : Requests) {
            TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
            request->Record.MergeFrom(*pbRequest);
            result = MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
                [this](const NMsgBusProxy::TBusResponse& response) -> int {
                    if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                        Cerr << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                        return 1;
                    }
                    if (ReturnTxId) {
                        if (response.Record.HasFlatTxId() && response.Record.GetFlatTxId().HasTxId()) {
                            Cout << "TxId: " << response.Record.GetFlatTxId().GetTxId() << Endl;
                        } else {
                            Cout << "TxId: not returned" << Endl;
                        }
                    }
                    return 0;
            });
            if (result != 0) {
                break;
            }
        }
        return result;
    }
};

class TClientCommandSchemaDescribe : public TClientCommand {
public:
    TClientCommandSchemaDescribe()
        : TClientCommand("describe", { "desc" }, "Describe schema object")
    {}

    TAutoPtr<NKikimrClient::TSchemeDescribe> Request;
    TString Path;
    bool Tree;
    bool Details;
    bool AccessRights;
    bool AccessRightsEffective;
    bool BackupInfo;
    bool Protobuf;
    bool PartitionStats;
    bool Boundaries;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        Tree = false;
        Details = false;
        AccessRights = false;
        AccessRightsEffective = false;
        BackupInfo = false;
        Protobuf = false;
        PartitionStats = false;
        Boundaries = false;
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Schema path or pathId (e.g. 72075186232623600/1225)");
        config.Opts->AddLongOption('t', "tree", "Show schema path tree").NoArgument().SetFlag(&Tree);
        config.Opts->AddLongOption('d', "details", "Show detailed information (like columns in a table)").NoArgument().SetFlag(&Details);
        config.Opts->AddLongOption('a', "acl", "Show owner and acl information").NoArgument().SetFlag(&AccessRights);
        config.Opts->AddLongOption('e', "effacl", "Show effective acl information").NoArgument().SetFlag(&AccessRightsEffective);
        config.Opts->AddLongOption('b', "backup", "Show backup information").NoArgument().SetFlag(&BackupInfo);
        config.Opts->AddLongOption('P', "protobuf", "Debug print all info as is").NoArgument().SetFlag(&Protobuf);
        config.Opts->AddLongOption('s', "stats", "Return partition stats").NoArgument().SetFlag(&PartitionStats);
        config.Opts->AddLongOption("boundaries", "Return boundaries").NoArgument().SetFlag(&Boundaries);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Request = new NKikimrClient::TSchemeDescribe();
        Path = config.ParseResult->GetFreeArgs()[0];
        if (Path.StartsWith('/')) {
            Request->SetPath(Path);
        } else {
            // treat path as PathId like '72075186232623600/1225'
            TVector<TString> fields;
            Split(Path, "/", fields);
            if (fields.size() != 2) {
                Cerr << "Invaid path or pathId: " << Path << Endl;
                exit(1);
            }
            ui64 schemeshardId = FromString<ui64>(fields[0]);
            ui32 pathId = FromString<ui32>(fields[1]);
            Request->SetSchemeshardId(schemeshardId);
            Request->SetPathId(pathId);
        }
        auto options = Request->MutableOptions();
        options->SetBackupInfo(BackupInfo);
        options->SetReturnPartitionStats(PartitionStats);
        options->SetReturnBoundaries(Boundaries);
        options->SetReturnPartitioningInfo(!Boundaries);
        options->SetShowPrivateTable(true);

        Protobuf = Protobuf || PartitionStats || Boundaries;
    }

    void PadString(TString& str, size_t size) {
        if (str.size() < size) {
            str += TString(size - str.size(), ' ');
        } else {
            str += ' ';
        }
    }

    void PrintEntry(const NKikimrSchemeOp::TPathDescription& path) {
        const NKikimrSchemeOp::TDirEntry& entry(path.GetSelf());
        TString type;
        switch(entry.GetPathType()) {
        case NKikimrSchemeOp::EPathTypeDir:
            type = "<dir>";
            break;
        case NKikimrSchemeOp::EPathTypeTable:
            type = "<table>";
            break;
        case NKikimrSchemeOp::EPathTypeColumnStore:
            type = "<column store>";
            break;
        case NKikimrSchemeOp::EPathTypeColumnTable:
            type = "<column table>";
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            type = "<sequence>";
            break;
        case NKikimrSchemeOp::EPathTypeReplication:
            type = "<replication>";
            break;
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            type = "<pq group>";
            break;
        default:
            type = "<unknown>";
            break;
        }

        TString id(Sprintf("%lu/%lu", entry.GetSchemeshardId(), entry.GetPathId()));
        TString name(entry.GetName());
        PadString(id, 24);
        PadString(type, 11);
        PadString(name, 26);
        TString owner;
        TString acl;
        if (AccessRights) {
            if (entry.HasOwner()) {
                owner = entry.GetOwner();
                PadString(owner, 20);
            }
            if (AccessRightsEffective) {
                if (entry.HasEffectiveACL()) {
                    acl = NACLib::TACL(entry.GetEffectiveACL()).ToString();
                }
            } else {
                if (entry.HasACL()) {
                    acl = NACLib::TACL(entry.GetACL()).ToString();
                }
            }
        }
        Cout << id << type << name << owner << acl << Endl;
        if (Details) {
            switch(entry.GetPathType()) {
            case NKikimrSchemeOp::EPathTypeTable: {
                const NKikimrSchemeOp::TTableDescription& table(path.GetTable());
                size_t szWidth = id.size() + type.size() + entry.GetName().size();
                size_t szColumns[3] = {0, 0, 0};
                for (const NKikimrSchemeOp::TColumnDescription& column : table.GetColumns()) {
                    szColumns[0] = std::max(szColumns[0], ToString(column.GetId()).size());
                    szColumns[1] = std::max(szColumns[1], column.GetType().size());
                    szColumns[2] = std::max(szColumns[2], column.GetName().size());
                    szWidth = std::max(szWidth, szColumns[0] + szColumns[1] + szColumns[2] + 2 + 2);
                }
                for (size_t i = 0; i < szWidth; ++i) {
                    if (i == szColumns[0] || i == (szColumns[0] + szColumns[1] + 1) || i == (szColumns[0] + szColumns[1] + szColumns[2] + 2)) {
                        Cout << "┬";
                    } else {
                        Cout << "─";
                    }
                }
                Cout << Endl;
                const auto& keyColumnIds(table.GetKeyColumnIds());
                for (const NKikimrSchemeOp::TColumnDescription& column : table.GetColumns()) {
                    TString id(ToString(column.GetId()));
                    TString type(column.GetType());
                    TString name(column.GetName());
                    id = id + TString(szColumns[0] - id.size(), ' ');
                    type = type + TString(szColumns[1] - type.size(), ' ');
                    name = name + TString(szColumns[2] - name.size(), ' ');
                    Cout << id << "│" << type << "│" << name << "│";
                    auto itKey = std::find(keyColumnIds.begin(), keyColumnIds.end(), column.GetId());
                    if (itKey != keyColumnIds.end()) {
                        Cout << "K" << itKey - keyColumnIds.begin(); // 🔑
                    }
                    Cout << Endl;
                }
                break;
            }
            case NKikimrSchemeOp::EPathTypePersQueueGroup: {
                const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroup(path.GetPersQueueGroup());
                for (ui32 pi = 0; pi < pqGroup.PartitionsSize(); ++pi) {
                    const auto& partition = pqGroup.GetPartitions(pi);
                    TString partitionId = Sprintf("  %6" PRIu32 " ", partition.GetPartitionId());
                    Cout << partitionId << "│" << partition.GetTabletId() << Endl;
                }
                break;
            }
            default:
                break;
            }
        }
        if (BackupInfo) {
            if (path.HasBackupProgress()) {
                NKikimrSchemeOp::TBackupProgress backup = path.GetBackupProgress();
                ui32 total = backup.GetTotal();
                ui32 notYet = backup.GetNotCompleteYet();
                Cout << "backup in progress: " << (total - notYet) << "/" << total;
                if (backup.HasTxId()) {
                    Cout << " txId: " << backup.GetTxId();
                }
                Cout << Endl;
            }
            for (const auto& backupResult : path.GetLastBackupResult()) {
                Cout << "backup done: " << backupResult.GetCompleteTimeStamp()
                    << " txId: " << backupResult.GetTxId()
                    << " errors: " << backupResult.GetErrorCount();
                if (backupResult.ErrorsSize()) {
                    Cout << " errorsExplain: { ";
                    bool first = true;
                    for (const auto &shardError : backupResult.GetErrors()) {
                        Cout << shardError.GetExplain().Quote();
                        if (!first) {
                            Cout << ", ";
                        }
                        first = false;
                    }
                    Cout << " }";
                }
                Cout << Endl;
            }
        }
        if (Protobuf) {
            TString debugProto;
            ::google::protobuf::TextFormat::PrintToString(path, &debugProto);
            Cout << debugProto << Endl;
        }
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        if (Request != nullptr)
            request->Record.MergeFrom(*Request);

        TList<NKikimrSchemeOp::TPathDescription> entries;

        for(;;) {
            MessageBusCall<NMsgBusProxy::TBusSchemeDescribe, NMsgBusProxy::TBusResponse>(config, request,
                [&entries](const NMsgBusProxy::TBusResponse& response) -> int {
                    entries.push_front(response.Record.GetPathDescription());
                    return 0;
            });

            if (Tree
                    && entries.front().GetSelf().HasParentPathId()
                    && entries.front().GetSelf().HasSchemeshardId()
                    && entries.front().GetSelf().GetParentPathId() != entries.front().GetSelf().GetPathId()) {
                request = new NMsgBusProxy::TBusSchemeDescribe();
                request->Record.SetSchemeshardId(entries.front().GetSelf().GetSchemeshardId());
                request->Record.SetPathId(entries.front().GetSelf().GetParentPathId());
                continue;
            }
            break;
        }
        int cnt = 0;
        //Cout << Path << Endl;
        for (const NKikimrSchemeOp::TPathDescription& entry : entries) {
            if (cnt > 0) {
                Cout << TString((cnt - 1) * 3, ' ');
                Cout << "└─ ";
            }
            PrintEntry(entry);
            ++cnt;
        }
        return 0;
    }
};

class TClientCommandSchemaLs : public TClientCommand {
public:
    TClientCommandSchemaLs()
        : TClientCommand("ls", {}, "List schema object or path content")
    {}

    TAutoPtr<NKikimrClient::TSchemeDescribe> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Schema path");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Request = new NKikimrClient::TSchemeDescribe();
        Request->SetPath(config.ParseResult->GetFreeArgs()[0]);
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        if (Request != nullptr)
            request->Record.MergeFrom(*Request);

        return MessageBusCall<NMsgBusProxy::TBusSchemeDescribe, NMsgBusProxy::TBusResponse>(config, request,
            [this](const NMsgBusProxy::TBusResponse& response) -> int {
                return PrintResponse(response);
        });
    }

    static void PrintEntry(const NKikimrSchemeOp::TDirEntry& entry) {
        TString type;
        switch(entry.GetPathType()) {
        case NKikimrSchemeOp::EPathTypeDir:
            type = "<dir>";
            break;
        case NKikimrSchemeOp::EPathTypeSubDomain:
            type = "<database>";
            break;
        case NKikimrSchemeOp::EPathTypeTable:
            type = "<table>";
            break;
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            type = "<pq group>";
            break;
        case NKikimrSchemeOp::EPathTypeColumnStore:
            type = "<column store>";
            break;
        case NKikimrSchemeOp::EPathTypeColumnTable:
            type = "<column table>";
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            type = "<sequence>";
            break;
        case NKikimrSchemeOp::EPathTypeReplication:
            type = "<replication>";
            break;
        default:
            type = "<unknown>";
            break;
        }

        Cout << entry.GetSchemeshardId() << '/' << entry.GetPathId() << '\t' << type << '\t' << entry.GetName() << Endl;
    }

    int PrintResponse(const NMsgBusProxy::TBusResponse& response) const {
        const NKikimrClient::TResponse& record(response.Record);
        const auto& description(record.GetPathDescription());
        if (description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeDir
            || description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeSubDomain
            || description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeColumnStore) {
            for (const auto& entry : description.GetChildren()) {
                PrintEntry(entry);
            }
        } else {
            PrintEntry(description.GetSelf());
        }
        return 0;
    }
};

class TClientCommandSchemaInit : public TClientCommand {
public:
    TClientCommandSchemaInit()
        : TClientCommand("init", {}, "Initialize schema root")
    {}

    TAutoPtr<NKikimrClient::TSchemeInitRoot> Request;
    TString Root;
    TString DefaultStoragePool;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("pool", "default storage pool").RequiredArgument("NAME").StoreResult(&DefaultStoragePool);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<ROOT>", "Schema root name");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Request = new NKikimrClient::TSchemeInitRoot();
        Root = config.ParseResult->GetFreeArgs()[0];
        if (Root.StartsWith('/'))
            Root = Root.substr(1);
        Request->SetTagName(Root);
        if (DefaultStoragePool) {
            auto* storagePool = Request->AddStoragePools();
            storagePool->SetName(DefaultStoragePool);
            TStringBuf kind = TStringBuf(DefaultStoragePool).RAfter(':');
            if (kind) {
                storagePool->SetKind(TString(kind));
            }
        }
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeInitRoot> request(new NMsgBusProxy::TBusSchemeInitRoot());
        if (Request != nullptr)
            request->Record.MergeFrom(*Request);
        return MessageBusCall(config, request);
    }
};

class TClientCommandSchemaChown : public TClientCommand {
public:
    TClientCommandSchemaChown()
        : TClientCommand("chown", {}, "Change owner")
    {}

    TAutoPtr<NKikimrClient::TSchemeOperation> Request;

    bool Recursive = false;
    bool Verbose = false;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(2);
        SetFreeArgTitle(0, "<USER>", "User");
        SetFreeArgTitle(1, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "          Or short pathname if profile path is set (e.g. test1/test2).");
        config.Opts->AddLongOption('R', "recursive", "Change owner on schema objects recursively").NoArgument().SetFlag(&Recursive);
        config.Opts->AddLongOption('v', "verbose", "Verbose output").NoArgument().SetFlag(&Verbose);
    }

    TString Owner;
    TString Path;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Owner = config.ParseResult->GetFreeArgs()[0];
        TString pathname = config.ParseResult->GetFreeArgs()[1];
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Path = config.Path + '/' + pathname;
                return;
            } else {
                WarnProfilePathSet();
            }
        }
        Path = pathname;
    }

    int Chown(TConfig& config, const TString& path) {
        size_t pos = path.rfind('/');
        TString base = path.substr(0, pos);
        TString name = path.substr(pos + 1);
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        NKikimrClient::TSchemeOperation& record(request->Record);
        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
        modifyScheme.SetWorkingDir(base);
        auto& modifyAcl = *modifyScheme.MutableModifyACL();
        modifyAcl.SetName(name);
        modifyAcl.SetNewOwner(Owner);
        if (Verbose) {
            Cout << path << Endl;
        }
        int result = MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
            [path](const NMsgBusProxy::TBusResponse& response) -> int {
                if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << path << ' ' << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                    return 1;
                }
                return 0;
        });
        return result;
    }

    TList<TString> Ls(TConfig& config, const TString& path) {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        NKikimrClient::TSchemeDescribe& record(request->Record);
        record.SetPath(path);
        TList<TString> result;
        MessageBusCall<NMsgBusProxy::TBusSchemeDescribe, NMsgBusProxy::TBusResponse>(config, request,
            [&result, path](const NMsgBusProxy::TBusResponse& response) -> int {
                for (const auto& item : response.Record.GetPathDescription().GetChildren()) {
                    result.emplace_back(path + '/' + item.GetName());
                }
                return 0;
        });
        return result;
    }

    virtual int Run(TConfig& config) override {
        TList<TString> paths;
        paths.emplace_back(Path);
        while (!paths.empty()) {
            TString path = paths.front();
            int result = Chown(config, path);
            if (result != 0) {
                return result;
            }
            paths.pop_front();
            if (Recursive) {
                TList<TString> children = Ls(config, path);
                paths.insert(paths.begin(), children.begin(), children.end());
            }
        }
        return 0;
    }
};

class TClientCommandSchemaAccessAdd : public TClientCommand {
public:
    TClientCommandSchemaAccessAdd()
        : TClientCommand("add", {}, "Add access right")
    {}

    TAutoPtr<NKikimrClient::TSchemeOperation> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(2);
        SetFreeArgTitle(0, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "            Or short pathname if profile path is set (e.g. test1/test2).");
        SetFreeArgTitle(1, "<ACCESS>", "ACCESS");
    }

    TString Access;
    TString Base;
    TString Name;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        TString pathname = config.ParseResult->GetFreeArgs()[0];
        size_t pos = pathname.rfind('/');
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Base = config.Path;
                Name = pathname;
            } else {
                WarnProfilePathSet();
                Base = pathname.substr(0, pos);
                Name = pathname.substr(pos + 1);
            }
        } else {
            Base = pathname.substr(0, pos);
            Name = pathname.substr(pos + 1);
        }
        Access = config.ParseResult->GetFreeArgs()[1];
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        NKikimrClient::TSchemeOperation& record(request->Record);
        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
        modifyScheme.SetWorkingDir(Base);
        auto& modifyAcl = *modifyScheme.MutableModifyACL();
        modifyAcl.SetName(Name);
        NACLib::TDiffACL diffAcl;
        {
            NACLibProto::TACE ace;
            NACLib::TACL::FromString(ace, Access);
            if (!ace.HasAccessRight() || !ace.HasAccessType()) {
                throw yexception() << "Invalid access right specified";
            }
            diffAcl.AddAccess(ace);
        }
        modifyAcl.SetDiffACL(diffAcl.SerializeAsString());
        int result = MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
            [](const NMsgBusProxy::TBusResponse& response) -> int {
                if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                    return 1;
                }
                return 0;
        });
        return result;
    }
};

class TClientCommandSchemaAccessRemove : public TClientCommand {
public:
    TClientCommandSchemaAccessRemove()
        : TClientCommand("remove", {}, "Remove access right")
    {}

    TAutoPtr<NKikimrClient::TSchemeOperation> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(2);
        SetFreeArgTitle(0, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "            Or short pathname if profile path is set (e.g. test1/test2).");
        SetFreeArgTitle(1, "<ACCESS>", "ACCESS");
    }

    TString Access;
    TString Base;
    TString Name;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        TString pathname = config.ParseResult->GetFreeArgs()[0];
        size_t pos = pathname.rfind('/');
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Base = config.Path;
                Name = pathname;
            } else {
                WarnProfilePathSet();
                Base = pathname.substr(0, pos);
                Name = pathname.substr(pos + 1);
            }
        } else {
            Base = pathname.substr(0, pos);
            Name = pathname.substr(pos + 1);
        }
        Access = config.ParseResult->GetFreeArgs()[1];
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        NKikimrClient::TSchemeOperation& record(request->Record);
        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
        modifyScheme.SetWorkingDir(Base);
        auto& modifyAcl = *modifyScheme.MutableModifyACL();
        modifyAcl.SetName(Name);
        NACLib::TDiffACL diffAcl;
        {
            NACLibProto::TACE ace;
            NACLib::TACL::FromString(ace, Access);
            diffAcl.RemoveAccess(ace);
        }
        modifyAcl.SetDiffACL(diffAcl.SerializeAsString());
        int result = MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
            [](const NMsgBusProxy::TBusResponse& response) -> int {
                if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                    return 1;
                }
                return 0;
        });
        return result;
    }
};

class TClientCommandSchemaAccess : public TClientCommandTree {
public:
    TClientCommandSchemaAccess()
        : TClientCommandTree("access", { "acl" }, "Access operations")
    {
        AddCommand(std::make_unique<TClientCommandSchemaAccessAdd>());
        AddCommand(std::make_unique<TClientCommandSchemaAccessRemove>());
        //AddCommand(std::make_unique<TClientCommandSchemaAccessGrant>());
        //AddCommand(std::make_unique<TClientCommandSchemaAccessRevoke>());
    }
};

class TClientCommandSchemaTableOptions : public TClientCommand {
public:
    NYdbGrpc::TGRpcClientConfig ClientConfig;

    TClientCommandSchemaTableOptions()
        : TClientCommand("options", {}, "Describe table options")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        ClientConfig.Locator = CommandConfig.ClientConfig.Locator;
        ClientConfig.Timeout = CommandConfig.ClientConfig.Timeout;
        ClientConfig.MaxMessageSize = CommandConfig.ClientConfig.MaxMessageSize;
        ClientConfig.MaxInFlight = CommandConfig.ClientConfig.MaxInFlight;
        ClientConfig.EnableSsl = CommandConfig.ClientConfig.EnableSsl;
        ClientConfig.SslCredentials.pem_root_certs = CommandConfig.ClientConfig.SslCredentials.pem_root_certs;
    }

    template<typename T>
    void PrintLabels(const T &rec) {
        for (auto &pr : rec.labels())
            Cout << "     " << pr.first << ": " << pr.second << Endl;
    }

    template<typename T>
    void PrintPolicies(const T &array) {
        for (auto &policy : array) {
            Cout << " - " << policy.name() << Endl;
            PrintLabels(policy);
        }
    }

    virtual int Run(TConfig& config) override {
        int res = 0;

        if (!ClientConfig.Locator) {
            Cerr << "GRPC call error: GRPC server is not specified (MBus protocol is not supported for this command)." << Endl;
            return -2;
        }

        NYdbGrpc::TCallMeta meta;
        if (config.SecurityToken) {
            meta.Aux.push_back({NYdb::YDB_AUTH_TICKET_HEADER, config.SecurityToken});
        }

        Ydb::Operations::Operation response;
        NYdbGrpc::TResponseCallback<Ydb::Table::DescribeTableOptionsResponse> responseCb =
            [&res, &response](NYdbGrpc::TGrpcStatus &&grpcStatus, Ydb::Table::DescribeTableOptionsResponse &&resp) -> void {
            res = (int)grpcStatus.GRpcStatusCode;
            if (!res) {
                response.CopyFrom(resp.operation());
            } else {
                Cerr << "GRPC call error: " << grpcStatus.Msg << Endl;
            }
        };

        {
            NYdbGrpc::TGRpcClientLow clientLow;
            Ydb::Table::DescribeTableOptionsRequest request;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(ClientConfig);
            connection->DoRequest(request, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncDescribeTableOptions, meta);
        }

        if (!res) {
            Y_ABORT_UNLESS(response.ready());
            if (response.status() == Ydb::StatusIds::SUCCESS) {
                Ydb::Table::DescribeTableOptionsResult result;
                response.result().UnpackTo(&result);
                Cout << "Table profiles" << Endl;
                for (auto &profile : result.table_profile_presets()) {
                    Cout << " - " << profile.name() << Endl
                         << "     Compaction policy: " << profile.default_compaction_policy() << Endl
                         << "     Execution policy: " << profile.default_execution_policy() << Endl
                         << "     Partitioning policy: " << profile.default_partitioning_policy() << Endl
                         << "     Storage policy: " << profile.default_storage_policy() << Endl
                         << "     Replication policy: " << profile.default_replication_policy() << Endl
                         << "     Caching policy: " << profile.default_caching_policy() << Endl
                         << "     Allowed compaction policies: " << JoinSeq(", ", profile.allowed_compaction_policies()) << Endl
                         << "     Allowed execution policies: " << JoinSeq(", ", profile.allowed_execution_policies()) << Endl
                         << "     Allowed partitioning policies: " << JoinSeq(", ", profile.allowed_partitioning_policies()) << Endl
                         << "     Allowed storage policies: " << JoinSeq(", ", profile.allowed_storage_policies()) << Endl
                         << "     Allowed replication policies: " << JoinSeq(", ", profile.allowed_replication_policies()) << Endl
                         << "     Allowed caching policies: " << JoinSeq(", ", profile.allowed_caching_policies()) << Endl;
                    PrintLabels(profile);
                }
                Cout << "Compaction policies" << Endl;
                PrintPolicies(result.compaction_policy_presets());
                Cout << "Execution policies" << Endl;
                PrintPolicies(result.execution_policy_presets());
                Cout << "Partitioning policies" << Endl;
                PrintPolicies(result.partitioning_policy_presets());
                Cout << "Storage policies" << Endl;
                PrintPolicies(result.storage_policy_presets());
                Cout << "Replication policies" << Endl;
                PrintPolicies(result.replication_policy_presets());
                Cout << "Caching policies" << Endl;
                PrintPolicies(result.caching_policy_presets());
            } else {
                Cerr << "ERROR: " << response.status() << Endl;
                for (auto &issue : response.issues())
                    Cerr << issue.message() << Endl;
            }
        }

        return res;
    }
};

class TClientCommandSchemaTableCopy : public TClientCommand {
public:
    NYdbGrpc::TGRpcClientConfig ClientConfig;
    TString DatabaseName;
    TVector<TString> SrcValues;
    TVector<TString> DstValues;

    TClientCommandSchemaTableCopy()
        : TClientCommand("copy", {}, "Copy table")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.SetFreeArgsNum(0);
        config.Opts->AddLongOption("database", "Database name").AddLongName("db").Required().RequiredArgument("DB").StoreResult(&DatabaseName);
        config.Opts->AddLongOption("source", "Source table path").AddLongName("src").RequiredArgument("PATH").AppendTo(&SrcValues);
        config.Opts->AddLongOption("destination", "Destination table path").AddLongName("dst").RequiredArgument("PATH").AppendTo(&DstValues);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        ClientConfig = CommandConfig.ClientConfig;
    }

    virtual int Run(TConfig& config) override {
        if (!ClientConfig.Locator) {
            Cerr << "GRPC call error: GRPC server is not specified (MBus protocol is not supported for this command)." << Endl;
            return -2;
        }

        if (!DatabaseName) {
            Cerr << "Invalid paramets: database name is empty." << Endl;
            return -2;
        }

        if (SrcValues.size() != DstValues.size()) {
            Cerr << "Invalid paramets: sources count is not equal to count destinations count." << Endl;
            return -2;
        }

        if (SrcValues.size() == 0 || DstValues.size() == 0) {
            Cerr << "Invalid paramets: either sources or destinations are not preset." << Endl;
            return -2;
        }

        Y_ABORT_UNLESS(SrcValues.size() == DstValues.size());
        const ui32 itemCount = SrcValues.size();

        TVector<NYdb::NTable::TCopyItem> copyItems;
        copyItems.reserve(itemCount);
        for (ui32 i = 0; i < itemCount; ++i) {
            copyItems.emplace_back(SrcValues[i], DstValues[i]);
        }

        auto driverConfig = NYdb::TDriverConfig()
                                .SetEndpoint(ClientConfig.Locator)
                                .SetDatabase(DatabaseName)
                                .SetAuthToken(config.SecurityToken);

        NYdb::TDriver driver(driverConfig);

        NYdb::NTable::TTableClient client(driver);

        auto status = client.RetryOperationSync([copyItems{std::move(copyItems)}](NYdb::NTable::TSession session) {
            return session.CopyTables(copyItems).GetValueSync();
        });

        if (!status.IsSuccess()) {
            Cerr << "Copying tables failed with status: " << status.GetStatus() << Endl;
            status.GetIssues().PrintTo(Cerr);
            return -1;
        }

        return 0;
    }
};

class TClientCommandSchemaTable : public TClientCommandTree {
public:
    TClientCommandSchemaTable()
        : TClientCommandTree("table", {}, "Table operations")
    {
        AddCommand(std::make_unique<TClientCommandSchemaTableOptions>());
        AddCommand(std::make_unique<TClientCommandSchemaTableCopy>());
    }
};

class TClientCommandSchemaUserAttributeGet: public TClientCommand {
public:
    TClientCommandSchemaUserAttributeGet()
        : TClientCommand("get", {}, "Get user attributes")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "            Or short pathname if profile path is set (e.g. test1/test2).");
    }

    TString Path;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        TString pathname = config.ParseResult->GetFreeArgs()[0];
        if (config.Path) {
            // Profile path is set
            if (!pathname.StartsWith('/')) {
                Path = config.Path + '/' + pathname;
            } else {
                WarnProfilePathSet();
                Path = pathname;
            }
        } else {
            Path = pathname;
        }
    }

    static void Print(const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>& items) {
        for (const auto& item : items) {
            Cout << item.GetKey() << ": " << item.GetValue() << Endl;
        }
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        NKikimrClient::TSchemeDescribe& record(request->Record);

        record.SetPath(Path);

        return MessageBusCall<NMsgBusProxy::TBusSchemeDescribe, NMsgBusProxy::TBusResponse>(config, request,
            [](const NMsgBusProxy::TBusResponse& response) -> int {
                Print(response.Record.GetPathDescription().GetUserAttributes());
                return 0;
            }
        );
    }
};

std::pair<TString, TString> SplitPath(const TClientCommand::TConfig& config, const TString& pathname) {
    std::pair<TString, TString> result;

    size_t pos = pathname.rfind('/');
    if (config.Path) {
        // Profile path is set
        if (!pathname.StartsWith('/')) {
            result.first = config.Path;
            result.second = pathname;
        } else {
            WarnProfilePathSet();
            result.first = pathname.substr(0, pos);
            result.second = pathname.substr(pos + 1);
        }
    } else {
        result.first = pathname.substr(0, pos);
        result.second = pathname.substr(pos + 1);
    }

    return result;
}

class TClientCommandSchemaUserAttributeSet: public TClientCommand {
    using TUserAttributesLimits = NSchemeShard::TUserAttributesLimits;

public:
    TClientCommandSchemaUserAttributeSet()
        : TClientCommand("set", {}, "Set user attribute(s)")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsMin(2);
        SetFreeArgTitle(0, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "            Or short pathname if profile path is set (e.g. test1/test2).");
        SetFreeArgTitle(1, "<ATTRIBUTE>", "NAME=VALUE");
    }

    TString Base;
    TString Name;
    THashMap<TString, TString> Attributes;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        std::tie(Base, Name) = SplitPath(config, config.ParseResult->GetFreeArgs()[0]);

        for (ui32 i = 1; i < config.ParseResult->GetFreeArgCount(); ++i) {
            TString attr = config.ParseResult->GetFreeArgs()[i];
            TVector<TString> items = StringSplitter(attr).Split('=').ToList<TString>();
            if (items.size() != 2) {
                Cerr << "Bad format in attribute '" + attr + "'";
                exit(1);
            }

            const TString& key = items.at(0);
            if (key.size() > TUserAttributesLimits::MaxNameLen) {
                Cerr << "Key '" << key << "' too long"
                     << ", max: " << TUserAttributesLimits::MaxNameLen
                     << ", actual: " << key.size() << Endl;
                exit(1);
            }

            const TString& value = items.at(1);
            if (value.size() > TUserAttributesLimits::MaxValueLen) {
                Cerr << "Value '" << value << "' too long"
                     << ", max: " << TUserAttributesLimits::MaxValueLen
                     << ", actual: " << value.size() << Endl;
                exit(1);
            }

            if (Attributes.contains(key)) {
                Cerr << "Duplicate value for key: " << key << Endl;
            }

            Attributes[key] = value;
        }
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        NKikimrClient::TSchemeOperation& record(request->Record);

        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes);
        modifyScheme.SetWorkingDir(Base);

        auto& alter = *modifyScheme.MutableAlterUserAttributes();
        alter.SetPathName(Name);
        auto& attributes = *alter.MutableUserAttributes();
        attributes.Reserve(Attributes.size());
        for (const auto& kv : Attributes) {
            auto& attribute = *attributes.Add();
            attribute.SetKey(kv.first);
            attribute.SetValue(kv.second);
        }

        return MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
            [](const NMsgBusProxy::TBusResponse& response) -> int {
                if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                    return 1;
                }
                return 0;
            }
        );
    }
};

class TClientCommandSchemaUserAttributeDel: public TClientCommand {
    using TUserAttributesLimits = NSchemeShard::TUserAttributesLimits;

public:
    TClientCommandSchemaUserAttributeDel()
        : TClientCommand("del", {}, "Delete user attribute(s)")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsMin(2);
        SetFreeArgTitle(0, "<PATH>", "Full pathname of an object (e.g. /ru/home/user/mydb/test1/test2).\n"
            "            Or short pathname if profile path is set (e.g. test1/test2).");
        SetFreeArgTitle(1, "<ATTRIBUTE>", "NAME");
    }

    TString Base;
    TString Name;
    TSet<TString> Attributes;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        std::tie(Base, Name) = SplitPath(config, config.ParseResult->GetFreeArgs()[0]);

        for (ui32 i = 1; i < config.ParseResult->GetFreeArgCount(); ++i) {
            TString key = config.ParseResult->GetFreeArgs()[i];

            if (Attributes.contains(key)) {
                Cerr << "Duplicate value for key: " << key << Endl;
            }

            Attributes.insert(key);
        }
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        NKikimrClient::TSchemeOperation& record(request->Record);

        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes);
        modifyScheme.SetWorkingDir(Base);

        auto& alter = *modifyScheme.MutableAlterUserAttributes();
        alter.SetPathName(Name);
        auto& attributes = *alter.MutableUserAttributes();
        attributes.Reserve(Attributes.size());
        for (const auto& key : Attributes) {
            auto& attribute = *attributes.Add();
            attribute.SetKey(key);
        }

        return MessageBusCall<NMsgBusProxy::TBusSchemeOperation, NMsgBusProxy::TBusResponse>(config, request,
            [](const NMsgBusProxy::TBusResponse& response) -> int {
                if (response.Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << ToCString(static_cast<NMsgBusProxy::EResponseStatus>(response.Record.GetStatus())) << " " << response.Record.GetErrorReason() << Endl;
                    return 1;
                }
                return 0;
            }
        );
    }
};

class TClientCommandSchemaUserAttribute: public TClientCommandTree {
public:
    TClientCommandSchemaUserAttribute()
        : TClientCommandTree("user-attribute", { "ua" }, "User attribute operations")
    {
        AddCommand(std::make_unique<TClientCommandSchemaUserAttributeGet>());
        AddCommand(std::make_unique<TClientCommandSchemaUserAttributeSet>());
        AddCommand(std::make_unique<TClientCommandSchemaUserAttributeDel>());
    }
};

TClientCommandSchemaLite::TClientCommandSchemaLite()
    : TClientCommandTree("schema", {}, "Schema operations")
{
    AddCommand(std::make_unique<TClientCommandSchemaExec>());
    AddCommand(std::make_unique<TClientCommandSchemaDescribe>());
    AddCommand(std::make_unique<TClientCommandSchemaLs>());
    AddCommand(std::make_unique<TClientCommandSchemaMkdir>());
    AddCommand(std::make_unique<TClientCommandSchemaDrop>());
    AddCommand(std::make_unique<TClientCommandSchemaChown>());
    AddCommand(std::make_unique<TClientCommandSchemaAccess>());
    AddCommand(std::make_unique<TClientCommandSchemaTable>());
    AddCommand(std::make_unique<TClientCommandSchemaUserAttribute>());
}

class TClientCommandSchema : public TClientCommandSchemaLite {
public:
    TClientCommandSchema()
    {
        AddCommand(std::make_unique<TClientCommandSchemaInit>());
    }
};

class TClientCommandDbExec : public TClientCommandBase {
public:
    TClientCommandDbExec()
        : TClientCommandBase("minikql", { "execute", "exec", "mkql" }, "Execute Mini-KQL query")
    {}

    TString MiniKQL;
    TString Params;
    bool Proto = false;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1, 2);
        SetFreeArgTitle(0, "<MINIKQL>", "Text MiniKQL");
        SetFreeArgTitle(1, "<PARAMS>", "Text MiniKQL parameters");
        config.Opts->AddLongOption('p', "proto", "MiniKQL parameters are in protobuf format").NoArgument().SetFlag(&Proto);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        MiniKQL = GetMiniKQL(config.ParseResult->GetFreeArgs()[0]);
        if (config.ParseResult->GetFreeArgsPos() > 1) {
            auto paramsArg = config.ParseResult->GetFreeArgs()[1];
            Params = Proto
                ? TUnbufferedFileInput(paramsArg).ReadAll()
                : GetMiniKQL(paramsArg);
        }
    }

    virtual int Run(TConfig& config) override {
        auto handler = [this](NClient::TKikimr& kikimr) {
            NClient::TTextQuery query(kikimr.Query(MiniKQL));

            NThreading::TFuture<NClient::TQueryResult> future;
            if (Proto) {
                NKikimrMiniKQL::TParams mkqlParams;
                NClient::TQuery::ParseTextParameters(mkqlParams, Params);
                future = query.AsyncExecute(mkqlParams);
            } else {
                future = query.AsyncExecute(Params);
            }

            return HandleResponse<NClient::TQueryResult>(future, [](const NClient::TQueryResult& result) -> int {
                Cout << result.GetValue().GetValueText<NClient::TFormatJSON>() << '\n';
                return 0;
            });
        };
        return InvokeThroughKikimr(config, std::move(handler));
    }
};

TClientCommandDb::TClientCommandDb()
    : TClientCommandTree("db", {}, "YDB DB operations")
{
    AddCommand(std::make_unique<TClientCommandSchema>());
    AddCommand(std::make_unique<TClientCommandDbExec>());
}

}
}
