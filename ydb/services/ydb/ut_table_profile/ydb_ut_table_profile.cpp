#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ymath.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {


NKikimrCompaction::TCompactionPolicy DEFAULT_COMPACTION_POLICY;
NKikimrCompaction::TCompactionPolicy COMPACTION_POLICY1;
NKikimrCompaction::TCompactionPolicy COMPACTION_POLICY2;
NKikimrSchemeOp::TPipelineConfig PIPELINE_CONFIG1;
NKikimrSchemeOp::TPipelineConfig PIPELINE_CONFIG2;
NKikimrSchemeOp::TStorageConfig STORAGE_CONFIG1;
NKikimrSchemeOp::TStorageConfig STORAGE_CONFIG2;
NKikimrConfig::TExecutionPolicy EXECUTION_POLICY1;
NKikimrConfig::TExecutionPolicy EXECUTION_POLICY2;
NKikimrConfig::TPartitioningPolicy PARTITIONING_POLICY1;
NKikimrConfig::TPartitioningPolicy PARTITIONING_POLICY2;
NKikimrConfig::TStoragePolicy STORAGE_POLICY1;
NKikimrConfig::TStoragePolicy STORAGE_POLICY2;
NKikimrConfig::TReplicationPolicy REPLICATION_POLICY1;
NKikimrConfig::TReplicationPolicy REPLICATION_POLICY2;
NKikimrConfig::TCachingPolicy CACHING_POLICY1;
NKikimrConfig::TCachingPolicy CACHING_POLICY2;

void InitConfigs(TKikimrWithGrpcAndRootSchema &server) {
    {
        TString tenant_name = "ydb_ut_tenant";
        TString tenant = Sprintf("/Root/%s", tenant_name.c_str());

        TClient client(*server.ServerSettings);

        TStoragePools tenant_pools = CreatePoolsForTenant(client, server.ServerSettings->StoragePoolTypes, tenant_name);

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 client.CreateSubdomain("/Root", GetSubDomainDeclarationSetting(tenant_name)));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 client.AlterSubdomain("/Root", GetSubDomainDefaultSetting(tenant_name, tenant_pools), TDuration::MilliSeconds(500)));

        server.Tenants_->Run(tenant);
    }

    {
        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultUserTablePolicy();
        DEFAULT_COMPACTION_POLICY.Clear();
        policy->Serialize(DEFAULT_COMPACTION_POLICY);
    }

    {
        NLocalDb::TCompactionPolicy policy;
        policy.Generations.push_back({0, 8, 8, 128 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        COMPACTION_POLICY1.Clear();
        policy.Serialize(COMPACTION_POLICY1);
    }

    {
        NLocalDb::TCompactionPolicy policy;
        policy.Generations.push_back({0, 8, 8, 128 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        policy.Generations.push_back({40 * 1024 * 1024, 5, 16, 512 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(2), false});
        COMPACTION_POLICY2.Clear();
        policy.Serialize(COMPACTION_POLICY2);
    }

    {
        PIPELINE_CONFIG1.Clear();
        PIPELINE_CONFIG1.SetNumActiveTx(1);
        PIPELINE_CONFIG1.SetEnableOutOfOrder(false);
        PIPELINE_CONFIG1.SetDisableImmediate(false);
        PIPELINE_CONFIG1.SetEnableSoftUpdates(true);
    }

    {
        PIPELINE_CONFIG2.Clear();
        PIPELINE_CONFIG2.SetNumActiveTx(8);
        PIPELINE_CONFIG2.SetEnableOutOfOrder(true);
        PIPELINE_CONFIG2.SetDisableImmediate(true);
        PIPELINE_CONFIG2.SetEnableSoftUpdates(false);
    }

    {
        EXECUTION_POLICY1.Clear();
        EXECUTION_POLICY1.MutablePipelineConfig()->CopyFrom(PIPELINE_CONFIG1);
        EXECUTION_POLICY1.SetResourceProfile("profile1");
        EXECUTION_POLICY1.SetEnableFilterByKey(true);
        EXECUTION_POLICY1.SetExecutorFastLogPolicy(false);
        EXECUTION_POLICY1.SetTxReadSizeLimit(10000000);
    }
    {
        EXECUTION_POLICY2.Clear();
        EXECUTION_POLICY2.MutablePipelineConfig()->CopyFrom(PIPELINE_CONFIG2);
        EXECUTION_POLICY2.SetResourceProfile("profile2");
        EXECUTION_POLICY2.SetEnableFilterByKey(false);
        EXECUTION_POLICY2.SetExecutorFastLogPolicy(true);
        EXECUTION_POLICY2.SetTxReadSizeLimit(20000000);
    }

    {
        PARTITIONING_POLICY1.Clear();
        PARTITIONING_POLICY1.SetUniformPartitionsCount(10);
        PARTITIONING_POLICY1.SetAutoSplit(true);
        PARTITIONING_POLICY1.SetAutoMerge(false);
        PARTITIONING_POLICY1.SetSizeToSplit(123456);
    }

    {
        PARTITIONING_POLICY2.Clear();
        PARTITIONING_POLICY2.SetUniformPartitionsCount(20);
        PARTITIONING_POLICY2.SetAutoSplit(true);
        PARTITIONING_POLICY2.SetAutoMerge(true);
        PARTITIONING_POLICY2.SetSizeToSplit(1000000000);
    }

    {
        STORAGE_CONFIG1.Clear();
        STORAGE_CONFIG1.MutableSysLog()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableLog()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableData()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableExternal()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.SetExternalThreshold(Max<ui32>());
    }

    {
        STORAGE_CONFIG2.Clear();
        STORAGE_CONFIG2.MutableSysLog()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableLog()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableData()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableExternal()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.SetDataThreshold(30000);
    }

    {
        REPLICATION_POLICY1.Clear();
        REPLICATION_POLICY1.SetFollowerCount(1);
        REPLICATION_POLICY1.SetCrossDataCenter(true);
        REPLICATION_POLICY1.SetAllowFollowerPromotion(false);
    }

    {
        REPLICATION_POLICY2.Clear();
        REPLICATION_POLICY2.SetFollowerCount(2);
        REPLICATION_POLICY2.SetCrossDataCenter(false);
        REPLICATION_POLICY2.SetAllowFollowerPromotion(true);
    }

    {
        CACHING_POLICY1.Clear();
        CACHING_POLICY1.SetExecutorCacheSize(10000000);
    }

    {
        CACHING_POLICY2.Clear();
        CACHING_POLICY2.SetExecutorCacheSize(20000000);
    }

    {
        STORAGE_POLICY1.Clear();
        auto &family = *STORAGE_POLICY1.AddColumnFamilies();
        family.SetId(0);
        family.SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
        family.MutableStorageConfig()->CopyFrom(STORAGE_CONFIG1);
    }

    {
        STORAGE_POLICY2.Clear();
        auto &family = *STORAGE_POLICY2.AddColumnFamilies();
        family.SetId(0);
        family.SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
        family.MutableStorageConfig()->CopyFrom(STORAGE_CONFIG2);
    }

    TClient client(*server.ServerSettings);
    TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest());
    auto &item = *request->Record.MutableConfigureRequest()->AddActions()
        ->MutableAddConfigItem()->MutableConfigItem();
    item.SetKind((ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem);
    auto &profiles = *item.MutableConfig()->MutableTableProfilesConfig();
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("compaction1");
        policy.MutableCompactionPolicy()->CopyFrom(COMPACTION_POLICY1);
    }
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("compaction2");
        policy.MutableCompactionPolicy()->CopyFrom(COMPACTION_POLICY2);
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.CopyFrom(EXECUTION_POLICY1);
        policy.SetName("execution1");
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.CopyFrom(EXECUTION_POLICY2);
        policy.SetName("execution2");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.CopyFrom(PARTITIONING_POLICY1);
        policy.SetName("partitioning1");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.CopyFrom(PARTITIONING_POLICY2);
        policy.SetName("partitioning2");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.CopyFrom(STORAGE_POLICY1);
        policy.SetName("storage1");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.CopyFrom(STORAGE_POLICY2);
        policy.SetName("storage2");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.CopyFrom(REPLICATION_POLICY1);
        policy.SetName("replication1");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.CopyFrom(REPLICATION_POLICY2);
        policy.SetName("replication2");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.CopyFrom(CACHING_POLICY1);
        policy.SetName("caching1");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.CopyFrom(CACHING_POLICY2);
        policy.SetName("caching2");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("default");
        profile.SetCompactionPolicy("default");
        profile.SetExecutionPolicy("default");
        profile.SetPartitioningPolicy("default");
        profile.SetStoragePolicy("default");
        profile.SetReplicationPolicy("default");
        profile.SetCachingPolicy("default");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("profile1");
        profile.SetCompactionPolicy("compaction1");
        profile.SetExecutionPolicy("execution1");
        profile.SetPartitioningPolicy("partitioning1");
        profile.SetStoragePolicy("storage1");
        profile.SetReplicationPolicy("replication1");
        profile.SetCachingPolicy("caching1");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("profile2");
        profile.SetCompactionPolicy("compaction2");
        profile.SetExecutionPolicy("execution2");
        profile.SetPartitioningPolicy("partitioning2");
        profile.SetStoragePolicy("storage2");
        profile.SetReplicationPolicy("replication2");
        profile.SetCachingPolicy("caching2");
    }
    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus msgStatus = client.SyncCall(request, reply);
    UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
    auto resp = dynamic_cast<NMsgBusProxy::TBusConsoleResponse*>(reply.Get())->Record;
    UNIT_ASSERT_VALUES_EQUAL(resp.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
}

void CheckTableSettings(const TKikimrWithGrpcAndRootSchema &server,
                        const TString &path,
                        NKikimrSchemeOp::TTableDescription expected)
{
    TClient client(*server.ServerSettings);
    auto desc = client.Ls(path)->Record.GetPathDescription();
    NKikimrSchemeOp::TTableDescription resp = desc.GetTable();
    // Table profiles affect only few fields. Clear other fields to simplify comparison.
    THashSet<ui32> affectedFields = {
        7  // PartitionConfig
    };

    if (expected.HasUniformPartitionsCount()) {
        UNIT_ASSERT_VALUES_EQUAL(desc.TablePartitionsSize(), expected.GetUniformPartitionsCount());
        expected.ClearUniformPartitionsCount();
    } else {
        UNIT_ASSERT_VALUES_EQUAL(desc.TablePartitionsSize(), 1);
    }

    if (resp.GetPartitionConfig().GetPartitioningPolicy().HasMinPartitionsCount() &&
        !expected.GetPartitionConfig().GetPartitioningPolicy().HasMinPartitionsCount())
    {
        // SchemeShard will set some min partitions count when unspecified by the user
        expected.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMinPartitionsCount(
            resp.GetPartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount());
    }

    std::vector<const ::google::protobuf::FieldDescriptor*> fields;
    auto *reflection = resp.GetReflection();
    reflection->ListFields(resp, &fields);
    for (auto field : fields)
        if (!affectedFields.contains(field->number()))
            reflection->ClearField(&resp, field);

    UNIT_ASSERT_VALUES_EQUAL(resp.DebugString(), expected.DebugString());
}

void CheckTablePartitions(const TKikimrWithGrpcAndRootSchema &server,
                          const TString &path,
                          const ::google::protobuf::RepeatedPtrField<Ydb::TypedValue> &points)
{
    TClient client(*server.ServerSettings);
    auto desc = client.Ls(path)->Record.GetPathDescription();

    UNIT_ASSERT_VALUES_EQUAL(points.size(), desc.TablePartitionsSize() - 1);
    for (int i = 0; i < points.size(); ++i) {
        auto &bnd = desc.GetTablePartitions(i);
        auto &vals = points[i].value();
        auto &types = points[i].type();

        TSerializedCellVec vec;
        vec.Parse(bnd.GetEndOfRangeKeyPrefix());
        auto cells = vec.GetCells();

        UNIT_ASSERT_VALUES_EQUAL(cells.size(), vals.items_size());
        UNIT_ASSERT_VALUES_EQUAL(vals.items_size(), types.tuple_type().elements_size());

        for (size_t j = 0; j < cells.size(); ++j) {
            auto cell = cells[j];
            auto &val = vals.items(j);
            auto &type = types.tuple_type().elements(j);
            ui32 typeId = type.optional_type().item().type_id();

            TString cellStr;
            DbgPrintValue(cellStr, cell, NScheme::TTypeInfo(typeId));

            TString valStr;
            switch (typeId) {
            case Ydb::Type::UINT64:
                valStr = ToString(val.uint64_value());
                break;
            case Ydb::Type::UTF8:
                valStr = ToString(val.text_value());
                break;
            case Ydb::Type::STRING:
                valStr = ToString(val.bytes_value());
                break;
            default:
                valStr = "UNKNOWN";
            }

            UNIT_ASSERT_VALUES_EQUAL(cellStr, valStr);
            Cout << cellStr << " " << valStr << Endl;
        }
    }
}

void Apply(const NKikimrCompaction::TCompactionPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    description.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(policy);
}

void Apply(const NKikimrConfig::TExecutionPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasPipelineConfig())
        partition.MutablePipelineConfig()->CopyFrom(policy.GetPipelineConfig());
    else
        partition.ClearPipelineConfig();
    if (policy.HasResourceProfile())
        partition.SetResourceProfile(policy.GetResourceProfile());
    else
        partition.ClearResourceProfile();
    if (policy.HasEnableFilterByKey())
        partition.SetEnableFilterByKey(policy.GetEnableFilterByKey());
    else
        partition.ClearEnableFilterByKey();
    if (policy.HasExecutorFastLogPolicy())
        partition.SetExecutorFastLogPolicy(policy.GetExecutorFastLogPolicy());
    else
        partition.ClearExecutorFastLogPolicy();
    if (policy.HasTxReadSizeLimit())
        partition.SetTxReadSizeLimit(policy.GetTxReadSizeLimit());
    else
        partition.ClearTxReadSizeLimit();
}

void Apply(const NKikimrConfig::TPartitioningPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasUniformPartitionsCount())
        description.SetUniformPartitionsCount(policy.GetUniformPartitionsCount());
    else
        description.ClearUniformPartitionsCount();
    if (policy.GetAutoSplit()) {
        partition.MutablePartitioningPolicy()->SetSizeToSplit(policy.GetSizeToSplit());
        if (policy.HasMaxPartitionsCount())
            partition.MutablePartitioningPolicy()->SetMaxPartitionsCount(policy.GetMaxPartitionsCount());
        else
            partition.MutablePartitioningPolicy()->ClearMaxPartitionsCount();
        if (policy.GetAutoMerge()) {
            partition.MutablePartitioningPolicy()->SetMinPartitionsCount(Max((ui32)1, policy.GetUniformPartitionsCount()));
        } else {
            partition.MutablePartitioningPolicy()->ClearMinPartitionsCount();
        }
    } else {
        partition.ClearPartitioningPolicy();
    }
}

void Apply(const NKikimrConfig::TStoragePolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    partition.ClearColumnFamilies();
    for (auto &family : policy.GetColumnFamilies())
        partition.AddColumnFamilies()->CopyFrom(family);
}

void Apply(const NKikimrConfig::TReplicationPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    partition.ClearFollowerCount();
    partition.ClearCrossDataCenterFollowerCount();
    partition.ClearAllowFollowerPromotion();
    partition.ClearFollowerGroups();
    if (policy.HasFollowerCount()) {
        auto& followerGroup = *partition.AddFollowerGroups();
        followerGroup.SetFollowerCount(policy.GetFollowerCount());
        if (policy.GetCrossDataCenter()) {
            followerGroup.SetRequireAllDataCenters(true);
        } else {
            followerGroup.SetRequireAllDataCenters(false);
        }
        if (policy.HasAllowFollowerPromotion()) {
            followerGroup.SetAllowLeaderPromotion(policy.GetAllowFollowerPromotion());
        }
    }
}

void Apply(const NKikimrConfig::TCachingPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasExecutorCacheSize())
        partition.SetExecutorCacheSize(policy.GetExecutorCacheSize());
    else
        partition.ClearExecutorCacheSize();
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const Ydb::Table::CreateTableRequest &request,
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    std::shared_ptr<grpc::Channel> Channel_;
    Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
    Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
    grpc::ClientContext context;
    Ydb::Table::CreateTableResponse response;
    auto status = Stub_->CreateTable(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready());
    if (deferred.status() != code)
        Cerr << deferred.DebugString();
    UNIT_ASSERT_VALUES_EQUAL(deferred.status(), code);
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const TString &path,
                 const Ydb::Table::CreateTableRequest &extension = Ydb::Table::CreateTableRequest(),
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request = extension;
    request.set_path(path);
    auto &col = *request.add_columns();
    col.set_name("key");
    col.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    request.add_primary_key("key");

    CreateTable(server, request, code);
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const TString &path,
                 const Ydb::Table::TableProfile &profile,
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request;
    request.mutable_profile()->CopyFrom(profile);
    CreateTable(server, path, request, code);
}

void CreateTableComplexKey(TKikimrWithGrpcAndRootSchema &server,
                           const TString &path,
                           const Ydb::Table::CreateTableRequest &extension = Ydb::Table::CreateTableRequest(),
                           Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request = extension;
    request.set_path(path);
    auto &col1 = *request.add_columns();
    col1.set_name("key1");
    col1.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    request.add_primary_key("key1");
    auto &col2 = *request.add_columns();
    col2.set_name("key2");
    col2.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    request.add_primary_key("key2");
    auto &col3 = *request.add_columns();
    col3.set_name("key3");
    col3.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    request.add_primary_key("key3");

    CreateTable(server, request, code);
}

void CreateTableComplexKey(TKikimrWithGrpcAndRootSchema &server,
                           const TString &path,
                           const Ydb::Table::TableProfile &profile,
                           Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request;
    request.mutable_profile()->CopyFrom(profile);
    CreateTableComplexKey(server, path, request, code);
}

} // anonymous namespace


Y_UNIT_TEST_SUITE(TTableProfileTests) {
    Y_UNIT_TEST(UseDefaultProfile) {
        TKikimrWithGrpcAndRootSchema server;
        //server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_CONFIGS, NLog::PRI_TRACE);
        //server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CONFIGS_DISPATCHER, NLog::PRI_TRACE);
        InitConfigs(server);

        NKikimrSchemeOp::TTableDescription defaultDescription;
        defaultDescription.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(DEFAULT_COMPACTION_POLICY);
        //defaultDescription.MutablePartitionConfig()->SetChannelProfileId(0);

        {
            CreateTable(server, "/Root/table-1");
            CheckTableSettings(server, "/Root/table-1", defaultDescription);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("default");
            CreateTable(server, "/Root/table-2", profile);

            CheckTableSettings(server, "/Root/table-2", defaultDescription);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("default");
            profile.mutable_compaction_policy()->set_preset_name("default");
            profile.mutable_execution_policy()->set_preset_name("default");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            profile.mutable_storage_policy()->set_preset_name("default");
            CreateTable(server, "/Root/table-3", profile);

            CheckTableSettings(server, "/Root/table-3", defaultDescription);
        }
    }

    Y_UNIT_TEST(UseTableProfilePreset) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY2, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(UseTableProfilePresetViaSdk) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64);
            tableBuilder.SetPrimaryKeyColumn("Key");
            auto settings = TCreateTableSettings().PresetName("profile1");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-1", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64);
            tableBuilder.SetPrimaryKeyColumn("Key");
            auto settings = TCreateTableSettings().PresetName("profile2");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-2", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY2, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            auto res = session.ExecuteSchemeQuery("CREATE TABLE `/Root/ydb_ut_tenant/table-3` (Key Uint64, Value Utf8, PRIMARY KEY (Key))").ExtractValueSync();
            NKikimrSchemeOp::TTableDescription defaultDescription;
            defaultDescription.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(DEFAULT_COMPACTION_POLICY);
            //defaultDescription.MutablePartitionConfig()->SetChannelProfileId(0);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", defaultDescription);
        }

    }


    Y_UNIT_TEST(OverwriteCompactionPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("compaction2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(DEFAULT_COMPACTION_POLICY, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }


    Y_UNIT_TEST(OverwriteExecutionPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("execution2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(OverwritePartitioningPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("partitioning2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::DISABLED);
            profile.mutable_partitioning_policy()->set_uniform_partitions(5);
            CreateTable(server, "/Root/ydb_ut_tenant/table-3", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = PARTITIONING_POLICY1;
            policy.SetUniformPartitionsCount(5);
            policy.SetAutoSplit(false);
            policy.SetAutoMerge(false);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("partitioning2");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::DISABLED);
            profile.mutable_partitioning_policy()->set_uniform_partitions(5);
            CreateTable(server, "/Root/ydb_ut_tenant/table-4", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = PARTITIONING_POLICY2;
            policy.SetUniformPartitionsCount(5);
            policy.SetAutoSplit(false);
            policy.SetAutoMerge(false);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-4", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::AUTO_SPLIT);
            CreateTable(server, "/Root/ydb_ut_tenant/table-5", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            NKikimrConfig::TPartitioningPolicy policy;
            policy.SetAutoSplit(true);
            policy.SetSizeToSplit(1 << 30);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-5", description);
        }

    }

    Y_UNIT_TEST(DescribeTableWithPartitioningPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Data", EPrimitiveType::String)
                .AddNullableColumn("KeyHash", EPrimitiveType::Uint64)
                .AddNullableColumn("Version", EPrimitiveType::Uint32)
                .AddNullableColumn("Ratio", EPrimitiveType::Float)
                .AddNullableColumn("SubKey", EPrimitiveType::Int32)
                .AddNullableColumn("Key", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumns({"KeyHash", "Key", "SubKey"});
            auto settings = TCreateTableSettings().PresetName("profile1");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-1", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);

            auto descResult = session.DescribeTable(
                    "/Root/ydb_ut_tenant/table-1",
                    TDescribeTableSettings().WithKeyShardBoundary(true)
                ).GetValueSync();

            UNIT_ASSERT(descResult.IsSuccess());
            auto ranges = descResult.GetTableDescription().GetKeyRanges();
            UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 10);

            auto extractValue = [](const TValue& val) {
                auto parser = TValueParser(val);
                parser.OpenTuple();
                UNIT_ASSERT(parser.TryNextElement());
                return parser.GetOptionalUint64().value();
            };

            int n = 0;
            const ui64 expectedRanges[10] = {
                1844674407370955264ul,
                3689348814741910528ul,
                5534023222112865280ul,
                7378697629483821056ul,
                9223372036854775808ul,
                11068046444225730560ul,
                12912720851596685312ul,
                14757395258967642112ul,
                16602069666338596864ul
            };

            for (const auto& range : ranges) {
                if (n == 0) {
                    UNIT_ASSERT(!range.From());
                } else {
                    UNIT_ASSERT(range.From()->IsInclusive());

                    auto left = extractValue(range.From()->GetValue());
                    UNIT_ASSERT_VALUES_EQUAL(left, expectedRanges[n - 1]);
                }

                if (n == 9) {
                    UNIT_ASSERT(!range.To());
                } else {
                    UNIT_ASSERT(!range.To()->IsInclusive());
                    auto right = extractValue(range.To()->GetValue());
                    UNIT_ASSERT_VALUES_EQUAL(right, expectedRanges[n]);
                }

                ++n;
            }

        }
    }

    Y_UNIT_TEST(OverwriteStoragePolicy) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->GetAppData().FeatureFlags.SetEnablePublicApiKeepInMemory(true);
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("storage2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            // TODO: remove this line when storage config is supported in SchemeShard.
            //description.MutablePartitionConfig()->SetChannelProfileId(0);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->mutable_syslog()->set_media("ssd");
            profile.mutable_storage_policy()->mutable_data()->set_media("ssd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::ENABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-3", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = STORAGE_POLICY1;
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableData()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("storage2");
            profile.mutable_storage_policy()->mutable_log()->set_media("hdd");
            profile.mutable_storage_policy()->mutable_external()->set_media("hdd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::DISABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-4", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = STORAGE_POLICY2;
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("hdd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableExternal()->SetPreferredPoolKind("hdd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableExternal()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->ClearColumnCache();
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-4", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("default");
            profile.mutable_storage_policy()->mutable_syslog()->set_media("ssd");
            profile.mutable_storage_policy()->mutable_log()->set_media("ssd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::ENABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-5", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            NKikimrConfig::TStoragePolicy policy;
            policy.AddColumnFamilies()->SetId(0);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-5", description);
        }
    }

    Y_UNIT_TEST(OverwriteCachingPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("caching2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(WrongTableProfile) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy();
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy();
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_replication_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExplicitPartitionsSimple) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_uint64_value(10);
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(20);
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(30);
        points.add_split_points()->CopyFrom(point);
        CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

        CheckTablePartitions(server, "/Root/ydb_ut_tenant/table-1",
                             policy.explicit_partitions().split_points());
    }

    Y_UNIT_TEST(ExplicitPartitionsUnordered) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_uint64_value(10);
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(30);
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(20);
        points.add_split_points()->CopyFrom(point);
        CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ExplicitPartitionsComplex) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_uint64_value(10);
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_text_value("key2");
        keyVal.mutable_items(1)->set_uint64_value(20);
        keyVal.mutable_items(2)->set_bytes_value("value2");
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_text_value("key3");
        keyVal.mutable_items(1)->set_uint64_value(30);
        keyVal.mutable_items(2)->set_bytes_value("value3");
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile);

        CheckTablePartitions(server, "/Root/ydb_ut_tenant/table-1",
                             policy.explicit_partitions().split_points());
    }

    Y_UNIT_TEST(ExplicitPartitionsWrongKeyFormat) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_text_value("10");
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile,
                              Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ExplicitPartitionsWrongKeyType) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_text_value("10");
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::SCHEME_ERROR);
    }

    void CheckRepeated(const ::google::protobuf::RepeatedPtrField<TString> &array,
                       THashSet<TString> expected)
    {
        UNIT_ASSERT_VALUES_EQUAL(array.size(), expected.size());
        for (auto &val : array) {
            UNIT_ASSERT(expected.contains(val));
            expected.erase(val);
        }
    }

    void CheckAllowedPolicies(const Ydb::Table::TableProfileDescription &profile,
                              const THashSet<TString> &storage,
                              const THashSet<TString> &partitioning,
                              const THashSet<TString> &execution,
                              const THashSet<TString> &compaction,
                              const THashSet<TString> &replication,
                              const THashSet<TString> &caching)
    {
        CheckRepeated(profile.allowed_storage_policies(), storage);
        CheckRepeated(profile.allowed_partitioning_policies(), partitioning);
        CheckRepeated(profile.allowed_execution_policies(), execution);
        CheckRepeated(profile.allowed_compaction_policies(), compaction);
        CheckRepeated(profile.allowed_replication_policies(), replication);
        CheckRepeated(profile.allowed_caching_policies(), caching);
    }

    void CheckLabels(const ::google::protobuf::Map<TString, TString> &labels,
                     THashMap<TString, TString> expected)
    {
        UNIT_ASSERT_VALUES_EQUAL(labels.size(), expected.size());
        for (auto &pr : labels) {
            UNIT_ASSERT(expected.contains(pr.first));
            UNIT_ASSERT_VALUES_EQUAL(expected.at(pr.first), pr.second);
            expected.erase(pr.first);
        }
    }

    Y_UNIT_TEST(DescribeTableOptions) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);


        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::DescribeTableOptionsRequest request;
        Ydb::Table::DescribeTableOptionsResponse response;
        auto status = Stub_->DescribeTableOptions(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(deferred.ready());
        UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);

        Ydb::Table::DescribeTableOptionsResult result;
        deferred.result().UnpackTo(&result);

        THashSet<TString> storage = {{TString("default"), TString("storage1"), TString("storage2")}};
        THashSet<TString> partitioning = {{TString("default"), TString("partitioning1"), TString("partitioning2")}};
        THashSet<TString> execution = {{TString("default"), TString("execution1"), TString("execution2")}};
        THashSet<TString> compaction = {{TString("default"), TString("compaction1"), TString("compaction2")}};
        THashSet<TString> replication = {{TString("default"), TString("replication1"), TString("replication2")}};
        THashSet<TString> caching = {{TString("default"), TString("caching1"), TString("caching2")}};
        for (auto &profile: result.table_profile_presets()) {
            if (profile.name() == "default") {
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "default");
            } else if (profile.name() == "profile1") {
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "storage1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "partitioning1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "execution1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "compaction1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "replication1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "caching1");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(profile.name(), "profile2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "storage2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "partitioning2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "execution2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "compaction2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "replication2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "caching2");
            }
            CheckAllowedPolicies(profile, storage, partitioning, execution,
                                 compaction, replication, caching);
        }

        for (auto &description : result.storage_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "storage1") {
                CheckLabels(description.labels(),
                            {{ {TString("syslog"), TString("hdd")},
                               {TString("log"), TString("hdd")},
                               {TString("data"), TString("hdd")},
                               {TString("external"), TString("hdd")},
                               {TString("external_threshold"), ToString(Max<ui32>())},
                               {TString("codec"), TString("lz4")},
                               {TString("in_memory"), TString("false")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "storage2");
                CheckLabels(description.labels(),
                            {{ {TString("syslog"), TString("ssd")},
                               {TString("log"), TString("ssd")},
                               {TString("data"), TString("ssd")},
                               {TString("external"), TString("ssd")},
                               {TString("medium_threshold"), TString("30000")},
                               {TString("codec"), TString("none")},
                               {TString("in_memory"), TString("true")} }});
            }
        }

        for (auto &description : result.partitioning_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("disabled")},
                               {TString("auto_merge"), TString("disabled")} }});
            } else if (description.name() == "partitioning1"){
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("enabled")},
                               {TString("auto_merge"), TString("disabled")},
                               {TString("split_threshold"), TString("123456")},
                               {TString("uniform_parts"), TString("10")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "partitioning2");
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("enabled")},
                               {TString("auto_merge"), TString("enabled")},
                               {TString("split_threshold"), TString("1000000000")},
                               {TString("uniform_parts"), TString("20")} }});
            }
        }

        for (auto &description : result.execution_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("enabled")},
                               {TString("pipeline_width"), TString("8")},
                               {TString("immediate_tx"), TString("enabled")},
                               {TString("bloom_filter"), TString("disabled")} }});
            } else if (description.name() == "execution1"){
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("disabled")},
                               {TString("immediate_tx"), TString("enabled")},
                               {TString("bloom_filter"), TString("enabled")},
                               {TString("tx_read_limit"), TString("10000000")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "execution2");
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("enabled")},
                               {TString("pipeline_width"), TString("8")},
                               {TString("immediate_tx"), TString("disabled")},
                               {TString("bloom_filter"), TString("disabled")},
                               {TString("tx_read_limit"), TString("20000000")} }});
            }
        }

        for (auto &description : result.compaction_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "compaction1"){
                CheckLabels(description.labels(),
                            {{ {TString("generations"), TString("1")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "compaction2");
                CheckLabels(description.labels(),
                            {{ {TString("generations"), TString("2")} }});
            }
        }

        for (auto &description : result.replication_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("disabled")} }});
            } else if (description.name() == "replication1"){
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("1")},
                               {TString("promotion"), TString("disabled")},
                               {TString("per_zone"), TString("true")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "replication2");
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("2")},
                               {TString("promotion"), TString("enabled")},
                               {TString("per_zone"), TString("false")} }});
            }
        }

        for (auto &description : result.caching_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "caching1"){
                CheckLabels(description.labels(),
                            {{ {TString("executor_cache"), TString("10000000")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "caching2");
                CheckLabels(description.labels(),
                            {{ {TString("executor_cache"), TString("20000000")} }});
            }
        }
    }
}

} // namespace NKikimr
