#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard__op_traits.h>  // for IsCreatePathOperation

#include <ydb/core/tx/schemeshard/schemeshard_system_names.h>


using namespace NSchemeShardUT_Private;  // for helpers.h *Requests() methods

using SetupFlagsMethod = void (*)(TTestEnvOptions& options);
using SetupObjectsMethod = void (*)(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir);
using CreateRequestMethod = TEvSchemeShard::TEvModifySchemeTransaction* (*)(const TString& workingDir, const TString& path);

struct TCreatePathOp {
    NKikimrSchemeOp::EOperationType Type;
    SetupFlagsMethod SetupFlags = nullptr;
    SetupObjectsMethod SetupObjects = nullptr;
    CreateRequestMethod CreateRequest;
};

// CreatePathOperations must contain an entry for every scheme operation that create path.
//
//
// For any type, .CreateRequest method should return TEvModifySchemeTransaction,
// complete enough to be able to create object of that type, and no more.
// The goal is to create object, not use it, no fancy specs required.
//
// There are useful *Request() methods that build TEvModifySchemeTransaction from
// a description of respective TModifyScheme field formatted in a protobuf text format.
// (*Request() are defined in ydb/core/tx/schemeshard/ut_helpers/helpers.{h,cpp}, mostly by the GENERIC_HELPERS macros).
//
// .SetupFlags -- auxiliary method to set feature flags required to enable object creation of the type
// .SetupObjects -- auxiliary method to create prerequisite objects needed for target object creation
//
const std::vector<TCreatePathOp> CreatePathOperations({
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpMkDir,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            return MkDirRequest(0 /* txId */, workingDir, path);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )",
                path.c_str()
            );
            return CreateTableRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        // This is special as here ESchemeOpCreateIndexedTable is used to
        // test creation of table index object and not table itself.
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            auto parts = SplitPath(path);
            const auto basename = parts.back();
            parts.back() = "table";
            const auto dirname = JoinPath(parts);
            const TString modifyScheme = Sprintf(
                R"(
                    TableDescription {
                        Name: "%s"
                        Columns { Name: "key"   Type: "Uint64" }
                        Columns { Name: "value" Type: "Uint64" }
                        KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                        Name: "%s"
                        KeyColumnNames: ["value"]
                    }
                )",
                dirname.c_str(),
                basename.c_str()
            );
            return CreateIndexedTableRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        // index creation is already tested with ESchemeOpCreateIndexedTable
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex,
        .CreateRequest = nullptr,
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .SetupObjects = [](TTestActorRuntime& runtime, ui64 txId, const TString& workingDir) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "table"
        //             Columns { Name: "key"   Type: "Uint64" }
        //             Columns { Name: "value" Type: "Uint64" }
        //             KeyColumnNames: ["key"]
        //         )"
        //     );
        //     return TestCreateTable(runtime, txId, workingDir, modifyScheme);
        // },
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             TableName: "%s"
        //             StreamDescription: {
        //                 Name: "%s"
        //                 Mode: ECdcStreamModeKeysOnly
        //                 Format: ECdcStreamFormatProto
        //             }
        //         )",
        //         path.c_str()
        //     );
        //     return CreateCdcStreamRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .SetupObjects = XXX,
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "%s"
        //             ColumnShardCount: 1
        //             Schema {
        //                 Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
        //                 Columns { Name: "key1" Type: "Uint32" }
        //                 KeyColumnNames: [ "timestamp" ]
        //             }
        //         )",
        //         path.c_str()
        //     );
        //     return MoveTableRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex,
        //TODO: proper check
        .CreateRequest = nullptr,
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex,
        //TODO: proper check
        .CreateRequest = nullptr,
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    PartitionsCount: 0
                )",
                path.c_str()
            );
            return CreateRtmrVolumeRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateKesusRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    PartitionCount: 1
                )",
                path.c_str()
            );
            return CreateSolomonRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                        }
                    }
                )",
                path.c_str()
            );
            return CreateOlapStoreRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateSubDomainRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateExtSubDomainRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    TotalGroupCount: 1
                    PartitionPerTablet: 1
                    PQTabletConfig: {
                        PartitionConfig {
                            LifetimeSeconds: 10
                        }
                    }
                )",
                path.c_str()
            );
            return CreatePQGroupRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    Config {
                        BlockSize: 4096
                        BlocksCount: 4096
                        ExplicitChannelProfiles {
                            PoolKind: "pool-kind-1"
                        }
                    }
                )",
                path.c_str()
            );
            return CreateFileStoreRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    VolumeConfig {
                        BlockSize: 4096
                        Partitions: [{
                            BlockCount: 1
                        }]
                        ExplicitChannelProfiles {
                            PoolKind: "pool-kind-1"
                        }
                    }
                )",
                path.c_str()
            );
            return CreateBlockStoreVolumeRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                        Columns { Name: "key1" Type: "Uint32" }
                        KeyColumnNames: [ "timestamp" ]
                    }
                )",
                path.c_str()
            );
            return CreateColumnTableRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateSequenceRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        //TODO: proper check
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence,
        // .SetupObjects = [](TTestActorRuntime& runtime, ui64 txId, const TString& workingDir) {
        //     TestCreateSequence(runtime, txId, workingDir, R"(
        //         Name: "prepare-sequence"
        //     )");
        // },
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     return MoveSequenceRequest(0 /* txId */, JoinPath({workingDir, "prepare-sequence"}), JoinPath({workingDir, path}));
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    SourceType: "ObjectStorage"
                    Location: "https://s3.cloud.net/my_bucket"
                    Auth {
                        None {
                        }
                    }
                )",
                path.c_str()
            );
            return CreateExternalDataSourceRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable,
        .SetupObjects = [](TTestActorRuntime& runtime, ui64 txId, const TString& workingDir) {
            TestCreateExternalDataSource(runtime, txId, workingDir, R"(
                Name: "prepare-external-data-source"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )");
        },
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    SourceType: "General"
                    DataSourcePath: "%s/prepare-external-data-source"
                    Location: "/"
                    Columns { Name: "key" Type: "Uint64" }
                )",
                path.c_str(),
                workingDir.c_str()
            );
            return CreateExternalTableRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateView,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                    QueryText: "query"
                )",
                path.c_str()
            );
            return CreateViewRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(
                R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateReplicationRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot,
        //TODO: proper check
        .CreateRequest = nullptr,
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup,
        //TODO: proper check
        .CreateRequest = nullptr,
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "%s"
        //         )",
        //         path.c_str()
        //     );
        //     return CreateResourcePoolRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "%s"
        //         )",
        //         path.c_str()
        //     );
        //     return CreateBackupCollectionRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .SetupFlags = [](TTestEnvOptions& options) {
        //     options.EnableTopicTransfer(true);
        // },
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "%s"
        //         )",
        //         path.c_str()
        //     );
        //     return CreateTransferRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView,
        //TODO: proper check
        .CreateRequest = nullptr,
        // .CreateRequest = [](const TString& workingDir, const TString& path) {
        //     const TString modifyScheme = Sprintf(
        //         R"(
        //             Name: "%s"
        //         )",
        //         path.c_str()
        //     );
        //     return CreateSysViewRequest(0 /* txId */, workingDir, modifyScheme);
        // }
    },
    {
        .Type = NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery,
        .CreateRequest = [](const TString& workingDir, const TString& path) {
            const TString modifyScheme = Sprintf(R"(
                    Name: "%s"
                )",
                path.c_str()
            );
            return CreateStreamingQueryRequest(0 /* txId */, workingDir, modifyScheme);
        }
    },

    //NOTE: ADD NEW ENTRY ABOVE THIS LINE
});


namespace NSchemeShardUT_Private {

const TVector<TString>& GetReservedNames();
const TVector<TString>& GetReservedPrefixes();
const TVector<TString>& GetReservedNamesExceptions();

}  // namespace NSchemeShardUT_Private

// for UNIT_ASSERT_VALUES_EQUAL on NKikimrSchemeOp::EOperationType values
template<>
inline void Out<NKikimrSchemeOp::EOperationType>(IOutputStream& o, NKikimrSchemeOp::EOperationType x) {
    o << NKikimrSchemeOp::EOperationType_Name(x);
}

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

enum class EExpectedResult : bool {
    FORBIDDEN = false,
    ALLOWED = true
};


Y_UNIT_TEST_SUITE(TSchemeShardSysNamesCore) {

    //NOTE: Any update to the name list must be approved by the project committee.
    Y_UNIT_TEST(NameListIsUnchanged) {
        TVector<TString> entries = {
            ".metadata",
            ".sys",
            ".tmp",
            ".backups",
        };
        for (const auto& i : GetReservedNames()) {
            auto found = std::find(entries.begin(), entries.end(), i);
            UNIT_ASSERT_C((found != entries.end()), TStringBuilder()
                << "Unexpected system name: '" << i << "'. Please note that updates to the list of reserved system names MUST be explicitly approved."
            );
        }
    }

    Y_UNIT_TEST(PrefixListIsUnchanged) {
        TVector<TString> entries = {
            ".",
            "__ydb",
        };
        for (const auto& i : GetReservedPrefixes()) {
            auto found = std::find(entries.begin(), entries.end(), i);
            UNIT_ASSERT_C((found != entries.end()), TStringBuilder()
                << "Unexpected system name prefix: '" << i << "'. Please note that updates the list of reserved system prefixes MUST be explicitly approved."
            );
        }
    }

    Y_UNIT_TEST(ExceptionsListIsUnchanged) {
        TVector<TString> entries = {
            ".sys_health",
            ".AtomicCounter",
            ".Events",
            ".FIFO",
            ".Queues",
            ".Quoter",
            ".RemovedQueues",
            ".Settings",
            ".STD",
        };
        for (const auto& i : GetReservedNamesExceptions()) {
            auto found = std::find(entries.begin(), entries.end(), i);
            UNIT_ASSERT_C((found != entries.end()), TStringBuilder()
                << "Unexpected exception from system names: '" << i << "'. Please note that updates the list of exceptions from the reserved system names MUST be explicitly approved."
            );
        }
    }

    void TestSystemNames(const NACLib::TUserToken* userToken, const TVector<TString>& adminSids, EExpectedResult expected) {
        const TString user = (userToken ? userToken->GetUserSID() : "anonymous");
        for (const auto& i : GetReservedNames()) {
            TString reasonAccum;
            bool result = CheckReservedName(i, userToken, adminSids, reasonAccum);
            if (!result) {
                UNIT_ASSERT_STRING_CONTAINS(reasonAccum, "name is reserved by the system");
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result, bool(expected), TStringBuilder()
                << "Wrong result for name '" << i << "', user '" << user << "', error '" << reasonAccum << "'"
            );
        }
    }

    static const TVector<TString> AdminSids = {
        "admin-user@builtin",
        "admin-group@builtin"
    };

    Y_UNIT_TEST(SystemNamesForbiddenForAnonymousUser) {
        TestSystemNames(nullptr, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemNamesForbiddenForOrdinaryUser) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {});
        TestSystemNames(&userToken, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemNamesAllowedForAdminUser) {
        NACLib::TUserToken userToken("admin-user@builtin", /* groups */ {});
        TestSystemNames(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesAllowedForAdminGroup) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {"admin-group@builtin"});
        TestSystemNames(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesAllowedForSystemUser) {
        NACLib::TUserToken userToken("metadata@system", /* groups */ {});
        TestSystemNames(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    // And there is no such thing as a system group

    void TestSystemPrefixes(const NACLib::TUserToken* userToken, const TVector<TString>& adminSids, EExpectedResult expected) {
        const TString user = (userToken ? userToken->GetUserSID() : "anonymous");
        // test use of system prefixes as:
        // - a whole names
        // - as prefixes to longer names
        TVector<TString> names = GetReservedPrefixes();
        for (const auto& i : GetReservedPrefixes()) {
            names.push_back(i + "-user-name");
        }
        for (const auto& i : names) {
            TString error;
            bool result = CheckReservedName(i, userToken, adminSids, error);
            if (!result) {
                UNIT_ASSERT_STRING_CONTAINS(error, "prefix is reserved by the system");
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result, bool(expected), TStringBuilder()
                << "Wrong result for prefix '" << i << "', user '" << user << "', error '" << error << "'"
            );
        }
    }

    Y_UNIT_TEST(SystemPrefixesForbiddenForAnonymousUser) {
        TestSystemPrefixes(nullptr, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemPrefixesForbiddenForOrdinaryUser) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {});
        TestSystemPrefixes(&userToken, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemPrefixesForbiddenForAdminUser) {
        NACLib::TUserToken userToken("admin-user@builtin", /* groups */ {});
        TestSystemPrefixes(&userToken, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemPrefixesForbiddenForAdminGroup) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {"admin-group@builtin"});
        TestSystemPrefixes(&userToken, AdminSids, EExpectedResult::FORBIDDEN);
    }

    Y_UNIT_TEST(SystemPrefixesForbiddenForSystemUser) {
        NACLib::TUserToken userToken("metadata@system", /* groups */ {});
        TestSystemPrefixes(&userToken, AdminSids, EExpectedResult::FORBIDDEN);
    }

    void TestSystemNamesExceptions(const NACLib::TUserToken* userToken, const TVector<TString>& adminSids, EExpectedResult expected) {
        const TString user = (userToken ? userToken->GetUserSID() : "anonymous");
        for (const auto& i : GetReservedNamesExceptions()) {
            TString reasonAccum;
            bool result = CheckReservedName(i, userToken, adminSids, reasonAccum);
            UNIT_ASSERT_VALUES_EQUAL_C(result, bool(expected), TStringBuilder()
                << "Wrong result for name exception'" << i << "', user '" << user << "', error '" << reasonAccum << "'"
            );
        }
    }

    Y_UNIT_TEST(SystemNamesExceptionsAllowedForAnonymousUser) {
        TestSystemNamesExceptions(nullptr, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesExceptionsAllowedForOrdinaryUser) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {});
        TestSystemNamesExceptions(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesExceptionsAllowedForAdminUser) {
        NACLib::TUserToken userToken("admin-user@builtin", /* groups */ {});
        TestSystemNamesExceptions(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesExceptionsAllowedForAdminGroup) {
        NACLib::TUserToken userToken("user1@builtin", /* groups */ {"admin-group@builtin"});
        TestSystemNamesExceptions(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }

    Y_UNIT_TEST(SystemNamesExceptionsAllowedForSystemUser) {
        NACLib::TUserToken userToken("metadata@system", /* groups */ {});
        TestSystemNamesExceptions(&userToken, AdminSids, EExpectedResult::ALLOWED);
    }
}


void ChangeOwner(TTestActorRuntime& runtime, ui64 txId, const TString& path, const TString& targetSid) {
    TestModifyACL(runtime, txId, ToString(ExtractParent(path)), ToString(ExtractBase(path)), "", targetSid);
}

void UpdateCreateOp(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction* ev, ui64 txId, const TString& subject) {
    NKikimrScheme::TEvModifySchemeTransaction& t = ev->Record;
    t.SetTxId(txId);
    t.SetTabletId(TTestTxConfig::SchemeShard);
    if (subject) {
        NACLib::TUserToken userToken(subject, /* groups */ {});
        t.SetUserToken(userToken.SerializeAsString());
    }
}

void TestCreateOp(TTestActorRuntime& runtime, ui64 txId, auto*ev, const TVector<TExpectedResult>& expected) {
    AsyncSend(runtime, TTestTxConfig::SchemeShard, ev);
    TestModificationResults(runtime, txId, expected);
}


Y_UNIT_TEST_SUITE(TSchemeShardSysNames) {

    //NOTE: This test guards against forgetting to add system names protection test here
    // when adding a new scheme operation that create paths
    Y_UNIT_TEST(CreateOpsAreCovered) {
        std::vector<NKikimrSchemeOp::EOperationType> all;
        {
            const auto* d = NKikimrSchemeOp::EOperationType_descriptor();
            for (int i = 0; i < d->value_count(); ++i) {
                auto op = NKikimrSchemeOp::EOperationType(d->value(i)->number());
                if (IsCreatePathOperation(op)) {
                    all.push_back(op);
                }
            }
            std::sort(all.begin(), all.end());
        }
        std::vector<NKikimrSchemeOp::EOperationType> covered;
        {
            for (const auto& op : CreatePathOperations) {
                covered.push_back(op.Type);
            }
            std::sort(covered.begin(), covered.end());
        }

        std::vector<NKikimrSchemeOp::EOperationType> delta;
        std::set_difference(
            all.begin(), all.end(),
            covered.begin(), covered.end(),
            std::back_inserter(delta)
        );

        UNIT_ASSERT_VALUES_EQUAL_C(delta, std::vector<NKikimrSchemeOp::EOperationType>(),
            "No system names protection tests for indicated create operation(s). "
            "Check if new create operation was added without adding support for it here"
            // Look for definition of CreatePathOperations
        );
    }

    enum class EAccessLevel {
        Anonymous,
        User,
        DatabaseAdmin,
        ClusterAdmin,
        SystemUser,
    };

    const TString UserName = "ordinary-user@builtin";
    const TString DatabaseAdminName = "db-admin@builtin";
    const TString ClusterAdminName = "cluster-admin@builtin";
    const TString SystemUserName = "metadata@system";

    TString BuiltinSubjectSid(EAccessLevel level) {
        switch (level) {
            case EAccessLevel::Anonymous:
                return "";
            case EAccessLevel::User:
                return UserName;
            case EAccessLevel::DatabaseAdmin:
                return DatabaseAdminName;
            case EAccessLevel::ClusterAdmin:
                return ClusterAdminName;
            case EAccessLevel::SystemUser:
                return SystemUserName;
        }
    }

    // dimensions:
    // + reserved: whole name or prefix
    // + object: dir, subdomain, table, topic, sequence etc
    // + position in path: leaf or intermediary
    // + operation: ordinary create, index building, restore from backup etc
    // + subject: cluster admin, database admin, ordinary user, anonymous user, system

    void SystemNamesProtect(
            NUnitTest::TTestContext&,
            const TCreatePathOp& op,
            bool EnableSystemNamesProtection,
            bool EnableDatabaseAdmin,
            EAccessLevel level,
            EExpectedResult expectedNameResult,
            EExpectedResult expectedPrefixResult
        ) {
        TTestBasicRuntime runtime;
        auto opts = TTestEnvOptions()
            .EnableSystemNamesProtection(EnableSystemNamesProtection)
            .EnableDatabaseAdmin(EnableDatabaseAdmin)
            .EnableRealSystemViewPaths(false)
        ;
        if (op.SetupFlags) {
            op.SetupFlags(opts);
        }
        TTestEnv env(runtime, opts);

        // Make AdministrationAllowedSIDs not empty to deny all users cluster admin privilege
        runtime.GetAppData().AdministrationAllowedSIDs.push_back("thou-shalt-not-pass");

        const TString rootDir = "/MyRoot";
        const TString subjectSid = BuiltinSubjectSid(level);
        ui64 txId = 100;

        // Make subject a proper cluster admin, if requested
        if (level == EAccessLevel::ClusterAdmin) {
            runtime.GetAppData().AdministrationAllowedSIDs.push_back(subjectSid);
        }

        // Make subject a proper database admin (by transfer the database ownership to them), if requested
        if (level == EAccessLevel::DatabaseAdmin) {
            ChangeOwner(runtime, ++txId, rootDir, subjectSid);
        }

        const TVector<TExpectedResult> resultSuccess({{NKikimrScheme::StatusAccepted}});
        const TVector<TExpectedResult> resultErrorName({{NKikimrScheme::StatusSchemeError, "name is reserved by the system"}});
        const TVector<TExpectedResult> resultErrorPrefix({{NKikimrScheme::StatusSchemeError, "prefix is reserved by the system"}});

        ui32 counter = 0;
        auto unique = [&counter](const TString& name) {
            return name + ToString(counter++);
        };
        auto test = [&](const TCreatePathOp& op, const TString& path, const TVector<TExpectedResult>& expected) {
            const auto& [type, _, setupObjects, createEvent] = op;

            // make unique directory for each test to avoid conflicts
            const TString uniqueName = unique(NKikimrSchemeOp::EOperationType_Name(type));
            TestMkDir(runtime, ++txId, rootDir, uniqueName, resultSuccess);
            env.TestWaitNotification(runtime, txId);

            const TString uniqueWD = JoinPath({rootDir, uniqueName});
            if (setupObjects) {
                setupObjects(runtime, ++txId, uniqueWD);
                env.TestWaitNotification(runtime, txId);
            }
            auto* ev = createEvent(uniqueWD, path);
            UpdateCreateOp(ev, ++txId, subjectSid);
            TestCreateOp(runtime, txId, ev, expected);

            if (expected == resultSuccess) {
                env.TestWaitNotification(runtime, txId);
            }
        };

        const auto expectedResultForName = (expectedNameResult == EExpectedResult::ALLOWED ? resultSuccess : resultErrorName);
        const auto expectedResultForPrefix = (EnableSystemNamesProtection == false ? resultSuccess : resultErrorPrefix);

        Cerr << "TEST: SystemNames, ReservedNames size " << GetReservedNames().size() << Endl;
        Cerr << "TEST: SystemNames, ReservedPrefix size " << GetReservedPrefixes().size() << Endl;

        const auto& typeName = NKikimrSchemeOp::EOperationType_Name(op.Type);

        // Protected cases, ordinary create, whole name
        {
            for (const auto& name : GetReservedNames()) {
                // special case of .sys protection before the times of general system names protection
                auto& expected = (EnableSystemNamesProtection == false && name == ".sys") ? resultErrorName : expectedResultForName;

                Cerr << "TEST: SystemNames, test " << typeName << ", whole name " << name
                    << ", should be " << (expected == resultSuccess ? "ALLOWED" : "FORBIDDEN")
                    << Endl;

                test(op, name, expected);

                test(op, JoinPath({"good", name}), expected);
                test(op, JoinPath({name, "good"}), expected);

                test(op, JoinPath({"good", "good", name}), expected);
                test(op, JoinPath({"good", name, "good"}), expected);
                test(op, JoinPath({name, "good", "good"}), expected);
            }
        }

        // Protected cases, ordinary create, prefix
        {
            auto& expected = expectedResultForPrefix;
            // test system prefixes themselves (except single '.')
            // and user provided names with the system prefixes
            TVector<TString> names;
            for (const auto& i : GetReservedPrefixes()) {
                if (!EnableSystemNamesProtection && i == ".") {
                    continue;
                }
                names.push_back(i);
            }
            for (const auto& i : GetReservedPrefixes()) {
                names.push_back(i + "-user-name");
            }

            for (const auto& name : names) {
                Cerr << "TEST: SystemNames, test type " << typeName << ", name with prefix " << name
                    << ", should be " << (expectedPrefixResult == EExpectedResult::ALLOWED ? "ALLOWED" : "FORBIDDEN")
                    << Endl;

                test(op, name, expected);

                test(op, JoinPath({"good", name}), expected);
                test(op, JoinPath({name, "good"}), expected);

                test(op, JoinPath({"good", "good", name}), expected);
                test(op, JoinPath({"good", name, "good"}), expected);
                test(op, JoinPath({name, "good", "good"}), expected);
            }
        }

        // Positive cases
        {
            const auto prevTxId = txId;
            auto& expected = resultSuccess;
            test(op, "good", expected);
            test(op, JoinPath({"good", "good"}), expected);
            test(op, JoinPath({"good", "good", "good"}), expected);
            env.TestWaitNotification(runtime, xrange(prevTxId + 1, txId));
        }

    }

    struct TProtectTestCase {
        EAccessLevel SubjectLevel;
        bool EnableSystemNamesProtection = false;
        bool EnableDatabaseAdmin = false;
        EExpectedResult ReservedName = EExpectedResult::FORBIDDEN;
        EExpectedResult ReservedPrefix = EExpectedResult::FORBIDDEN;
    };
    static const std::vector<TProtectTestCase> ParameterizedTests = {
        // anonymous is the same as ordinary user
        { .SubjectLevel = EAccessLevel::Anonymous, .EnableSystemNamesProtection = false, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::Anonymous, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::FORBIDDEN },
        { .SubjectLevel = EAccessLevel::Anonymous, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = true, .ReservedName = EExpectedResult::FORBIDDEN },

        // ordinary user forbidden to use any reserved system names|prefixes
        { .SubjectLevel = EAccessLevel::User, .EnableSystemNamesProtection = false, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::User, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::FORBIDDEN },
        { .SubjectLevel = EAccessLevel::User, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = true, .ReservedName = EExpectedResult::FORBIDDEN },

        // database admin have no special power above ordinary user
        { .SubjectLevel = EAccessLevel::DatabaseAdmin, .EnableSystemNamesProtection = false, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::DatabaseAdmin, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::FORBIDDEN },
        { .SubjectLevel = EAccessLevel::DatabaseAdmin, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = true, .ReservedName = EExpectedResult::FORBIDDEN },

        // cluster admin can fix (recreate) system objects with reserved names
        { .SubjectLevel = EAccessLevel::ClusterAdmin, .EnableSystemNamesProtection = false, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::ClusterAdmin, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::ClusterAdmin, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = true, .ReservedName = EExpectedResult::ALLOWED },

        // system can create objects without limitations
        { .SubjectLevel = EAccessLevel::SystemUser, .EnableSystemNamesProtection = false, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED, .ReservedPrefix = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::SystemUser, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = false, .ReservedName = EExpectedResult::ALLOWED, .ReservedPrefix = EExpectedResult::ALLOWED },
        { .SubjectLevel = EAccessLevel::SystemUser, .EnableSystemNamesProtection = true, .EnableDatabaseAdmin = true, .ReservedName = EExpectedResult::ALLOWED, .ReservedPrefix = EExpectedResult::ALLOWED },
    };
    struct TTestRegistrationSystemNamesProtect {
        TTestRegistrationSystemNamesProtect() {
            static std::vector<TString> TestNames;

            const auto subject = [](EAccessLevel level){
                switch (level) {
                    case EAccessLevel::Anonymous:
                        return "anonymous";
                    case EAccessLevel::User:
                        return "ordinaryuser";
                    case EAccessLevel::DatabaseAdmin:
                        return "dbadmin";
                    case EAccessLevel::ClusterAdmin:
                        return "clusteradmin";
                    case EAccessLevel::SystemUser:
                        return "system";
                }
            };

            for (const auto& op : CreatePathOperations) {
                const auto& typeName = NKikimrSchemeOp::EOperationType_Name(op.Type);

                // skip specially marked entries
                if (op.CreateRequest == nullptr) {
                    continue;
                }

                for (const auto& entry : ParameterizedTests) {
                    TestNames.emplace_back(TStringBuilder() << typeName
                        << "-" << (entry.EnableSystemNamesProtection ? "Protect" : "NoProtect")
                        << "-" << (entry.EnableDatabaseAdmin ? "DbAdmin" : "NoDbAdmin")
                        << "-" << subject(entry.SubjectLevel)
                    );

                    TCurrentTest::AddTest(TestNames.back().c_str(), std::bind(
                            SystemNamesProtect,
                            std::placeholders::_1,
                            op,
                            entry.EnableSystemNamesProtection,
                            entry.EnableDatabaseAdmin,
                            entry.SubjectLevel,
                            entry.ReservedName,
                            entry.ReservedPrefix
                        ),
                        /*forceFork*/ false
                    );
                }
            }
        }
    };
    static TTestRegistrationSystemNamesProtect testRegistrationSystemNamesProtect;

}
