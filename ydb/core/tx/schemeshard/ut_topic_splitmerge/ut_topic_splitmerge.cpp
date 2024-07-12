#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/services/lib/sharding/sharding.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// bounds of key distribution. Max<ui128> devided to 3 equals parts.
// ranges calculated with NDataStreams::V1::RangeFromShardNumber

// 1/4
const unsigned char bound_1_4[]{0x3F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE};
// 1/3
const unsigned char bound_1_3[]{0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
                                0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x54};
// 2/4
const unsigned char bound_2_4[]{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD};
// 1/2
const unsigned char bound_1_2[]{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE};
// 2/3
const unsigned char bound_2_3[]{0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
                                0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xA9};
// 3/4
const unsigned char bound_3_4[]{0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC};

template<typename T>
bool Contains(const ::google::protobuf::RepeatedField<T>& field, T value) {
    for (auto v : field) {
        if (v == value) {
            return true;
        }
    }
    return false;
}

const TString ToHex(const TString& value) {
    return TStringBuilder() << HexText(TBasicStringBuf(value));
}

void CreateSubDomain(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, TString name = "USER_1") {
    TestCreateSubDomain(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
                        Name: ")" << name << R"("
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "pool-kind-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "pool-kind-2"
                        }
                )");
    env.TestWaitNotification(runtime, txId);
}

void CreateExtSubDomain(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, TString name = "USER_1") {
    TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", TStringBuilder() << "Name: \"" << name << "\"");

    TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
                        Name: ")" << name << R"("
                        ExternalSchemeShard: true
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "pool-kind-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "pool-kind-2"
                        }
                )");
    env.TestWaitNotification(runtime, txId);
}

TTestEnv CreateTestEnv(TTestBasicRuntime& runtime) {
    TTestEnvOptions opts;

    opts.EnableTopicSplitMerge(true);
    opts.EnablePQConfigTransactionsAtSchemeShard(true);

    TTestEnv env(runtime, opts);

    return env;
}

void CreateTopic(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const ui32 partitionCount, bool withSplitMerge = true) {
    TString schema = TStringBuilder() << R"(
            Name: "Topic1"
            TotalGroupCount: )" << partitionCount
                                      << R"(
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 3600
                    WriteSpeedInBytesPerSecond : 1024
                }
                PartitionStrategy {
                    PartitionStrategyType: )" << (withSplitMerge ? "CAN_SPLIT_AND_MERGE" : "DISABLED") << R"(
                }
            }
        )";

    TestCreatePQGroup(runtime, ++txId, "/MyRoot/USER_1", schema);
    env.TestWaitNotification(runtime, txId);
}

void ModifyTopic(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, std::function<void(::NKikimrSchemeOp::TPersQueueGroupDescription& scheme)> modificator,
                    const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName("Topic1");
    scheme.MutablePQTabletConfig()->MutablePartitionConfig();

    modificator(scheme);

    TStringBuilder sb;
    sb << scheme;
    TString scheme_ = sb.substr(1, sb.size() - 2);

    Cerr << ">>>>> " << scheme_ << Endl << Flush;

    TestAlterPQGroup(runtime, ++txId, "/MyRoot/USER_1", scheme_, expectedResults);
    env.TestWaitNotification(runtime, txId);
}

void SplitPartition(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const ui32 partition, TString boundary,
                    const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {

    ModifyTopic(runtime, env, txId, [&](auto& scheme) {
        auto* split = scheme.AddSplit();
        split->SetPartition(partition);
        split->SetSplitBoundary(boundary);
    }, expectedResults);
}

void MergePartition(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const ui32 partition,
                    const ui32 adjacentPartition,
                    const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {

    ModifyTopic(runtime, env, txId, [&](auto& scheme) {
        auto* merge = scheme.AddMerge();
        merge->SetPartition(partition);
        merge->SetAdjacentPartition(adjacentPartition);
    }, expectedResults);
}

auto DescribeTopic(TTestBasicRuntime& runtime, TString path = "/MyRoot/USER_1/Topic1", ui64 ss = TTestTxConfig::SchemeShard) {
    {
        TAtomic unused;
        runtime.GetAppData().Icb->SetValue("SchemeShard_FillAllocatePQ", true, unused);
    }

    return DescribePath(runtime, ss, path, true, true, true).GetPathDescription().GetPersQueueGroup();
}

void ValidatePartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition,
                       NKikimrPQ::ETopicPartitionStatus status, TMaybe<TString> fromBound, TMaybe<TString> toBound) {
    const auto id = partition.GetPartitionId();
    UNIT_ASSERT_C(status == partition.GetStatus(), "Partition " << id << " should not be Active after topic creation");
    if (fromBound || toBound) {
        UNIT_ASSERT_C(partition.HasKeyRange(),
                      "Partition " << id
                                   << " should have KeyRange because topic has many partitions and SplitMerge enabled");
    } else {
        UNIT_ASSERT_C(!partition.HasKeyRange(), "Partition " << id << " should not have KeyRange");
    }
    if (fromBound) {
        UNIT_ASSERT_C(partition.GetKeyRange().HasFromBound(), "Partition " << id << " should have KeyRange.FromBound");
        UNIT_ASSERT_EQUAL_C(*fromBound, partition.GetKeyRange().GetFromBound(),
                            "KeyRange.FromBound : " << ToHex(*fromBound)
                                                    << " != " << ToHex(partition.GetKeyRange().GetFromBound()));
    } else {
        UNIT_ASSERT_C(!partition.GetKeyRange().HasFromBound(),
                      "Partition " << id << " should not have KeyRange.FromBound");
    }
    if (toBound) {
        UNIT_ASSERT_C(partition.GetKeyRange().HasToBound(), "Partition " << id << " should have KeyRange.ToBound");
        UNIT_ASSERT_EQUAL_C(*toBound, partition.GetKeyRange().GetToBound(),
                            "KeyRange.ToBound : " << ToHex(*toBound)
                                                  << " != " << ToHex(partition.GetKeyRange().GetToBound()));
    } else {
        UNIT_ASSERT_C(!partition.GetKeyRange().HasToBound(), "Partition " << id << " should not have KeyRange.ToBound");
    }
}

void ValidatePartitionParents(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition,
                              TVector<ui32> parents) {
    const ui64 id = partition.GetPartitionId();
    UNIT_ASSERT_C((ui64)partition.GetParentPartitionIds().size() == parents.size(),
                  "Partition " << id << " should have " << parents.size() << " parents");
    for (const auto parent : parents) {
        UNIT_ASSERT_C(Contains(partition.GetParentPartitionIds(), parent),
                      "Partition " << id << " should have parent with id " << parent);
    }
}

void ValidatePartitionChildren(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition,
                               TVector<ui32> children) {
    const ui64 id = partition.GetPartitionId();
    UNIT_ASSERT_C((ui64)partition.GetChildPartitionIds().size() == children.size(),
                  "Partition " << id << " should have " << children.size() << " childrens");
    for (const auto child : children) {
        UNIT_ASSERT_C(Contains(partition.GetChildPartitionIds(), child),
                      "Partition " << id << " should have child with id " << child);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardTopicSplitMergeTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    } // Y_UNIT_TEST(Boot)

    Y_UNIT_TEST(CreateTopicWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        auto topic = DescribeTopic(runtime);
        auto partition = topic.GetPartitions()[0];

        UNIT_ASSERT_C(NKikimrPQ::ETopicPartitionStatus::Active == partition.GetStatus(),
                      "Partition should not be Active after topic creation");
        UNIT_ASSERT_C(!partition.HasKeyRange(),
                      "Partitions should not have KeyRange because topic has only one partition");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto partitionR = DescribeTopic(runtime).GetPartitions()[0];
        UNIT_ASSERT_C(partitionR.GetStatus() == partition.GetStatus(),
                      "The status should not change after the SchemeShard restart");
        UNIT_ASSERT_C(!partitionR.HasKeyRange(), "The KeyRange should not change after the SchemeShard restart");
    } // Y_UNIT_TEST(CreateTopicWithOnePartition)

    Y_UNIT_TEST(CreateTopicWithManyPartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];

        TString bound0((char*)bound_1_3, sizeof(bound_1_3));
        TString bound1((char*)bound_2_3, sizeof(bound_2_3));

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), bound0);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, bound0, bound1);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, bound1, Nothing());

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), bound0);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, bound0, bound1);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, bound1, Nothing());
    } // Y_UNIT_TEST(CreateTopicWithManyPartition)

    Y_UNIT_TEST(SplitWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        const unsigned char b[] = {0x7F};
        TString boundary((char*)b, sizeof(b));
        SplitPartition(runtime, env, txId, 0, boundary);

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), Nothing());
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), boundary);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, boundary, Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {0});
        ValidatePartitionParents(partition2, {0});

        ValidatePartitionChildren(partition0, {1, 2});
        ValidatePartitionChildren(partition1, {});
        ValidatePartitionChildren(partition2, {});

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), Nothing());
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), boundary);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, boundary, Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {0});
        ValidatePartitionParents(partition2, {0});

        ValidatePartitionChildren(partition0, {1, 2});
        ValidatePartitionChildren(partition1, {});
        ValidatePartitionChildren(partition2, {});
    } // Y_UNIT_TEST(SplitWithOnePartition)

    Y_UNIT_TEST(SplitTwoPartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 2);

        const unsigned char b0[] = {0x3F};
        TString boundary0((char*)b0, sizeof(b0));

        const unsigned char b1[] = {0xBF};
        TString boundary1((char*)b1, sizeof(b1));

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            {
                auto* split = scheme.AddSplit();
                split->SetPartition(0);
                split->SetSplitBoundary(boundary0);
            }
            {
                auto* split = scheme.AddSplit();
                split->SetPartition(1);
                split->SetSplitBoundary(boundary1);
            }
        });

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];
        auto partition5 = topic.GetPartitions()[5];

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {});
        ValidatePartitionParents(partition2, {0});
        ValidatePartitionParents(partition3, {0});
        ValidatePartitionParents(partition4, {1});
        ValidatePartitionParents(partition5, {1});

        ValidatePartitionChildren(partition0, {2, 3});
        ValidatePartitionChildren(partition1, {4, 5});
    } // Y_UNIT_TEST(SplitTwoPartition)

    Y_UNIT_TEST(SplitWithManyPartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        TString bound0((char*)bound_1_3, sizeof(bound_1_3));
        TString bound1((char*)bound_2_3, sizeof(bound_2_3));

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        const unsigned char b[] = {0x7F};
        TString boundary((char*)b, sizeof(b));
        SplitPartition(runtime, env, txId, 1, boundary);

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), bound0);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, bound0, bound1);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, bound1, Nothing());
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, bound0, boundary);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, boundary, bound1);

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {});
        ValidatePartitionParents(partition2, {});
        ValidatePartitionParents(partition3, {1});
        ValidatePartitionParents(partition4, {1});

        ValidatePartitionChildren(partition0, {});
        ValidatePartitionChildren(partition1, {3, 4});
        ValidatePartitionChildren(partition2, {});
        ValidatePartitionChildren(partition3, {});
        ValidatePartitionChildren(partition4, {});

        // Reboot for check Y_ABORT_UNLESS
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
    } // Y_UNIT_TEST(SplitWithManyPartition)

    Y_UNIT_TEST(SplitWithWrongBoundary) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        TString boundary;
        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Split boundary is empty"}});

        boundary = "\001";
        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter,
                         "Split boundary less or equals FromBound of partition"}});

        boundary = TString((char*)bound_1_3, sizeof(bound_1_3));
        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter,
                         "Split boundary less or equals FromBound of partition"}});

        boundary = "\255";
        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter,
                         "Split boundary greate or equals ToBound of partition"}});

        boundary = TString((char*)bound_2_3, sizeof(bound_2_3));
        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter,
                         "Split boundary greate or equals ToBound of partition"}});
    } // Y_UNIT_TEST(SplitWithWrongBoundary)

    Y_UNIT_TEST(SplitWithWrongPartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        TString boundary = "\127";
        ui32 notExists = 7;
        SplitPartition(runtime, env, txId, notExists, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Splitting partition does not exists: 7"}});

    } // Y_UNIT_TEST(SplitWithWrongPartition)

    Y_UNIT_TEST(SplitInactivePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        TString boundary = "\127";
        SplitPartition(runtime, env, txId, 1, boundary);

        SplitPartition(runtime, env, txId, 1, boundary,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Invalid partition status"}});
    } // Y_UNIT_TEST(SplitInactivePartition)

    Y_UNIT_TEST(MargePartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 2);

        MergePartition(runtime, env, txId, 0, 1);

        TString boundary((char*)bound_1_2, sizeof(bound_1_2));

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), boundary);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, boundary, Nothing());
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {});
        ValidatePartitionParents(partition2, {0, 1});

        ValidatePartitionChildren(partition0, {2});
        ValidatePartitionChildren(partition1, {2});
        ValidatePartitionChildren(partition2, {});

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), boundary);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, boundary, Nothing());
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {});
        ValidatePartitionParents(partition2, {0, 1});

        ValidatePartitionChildren(partition0, {2});
        ValidatePartitionChildren(partition1, {2});
        ValidatePartitionChildren(partition2, {});
    } // Y_UNIT_TEST(MargePartitions)

    Y_UNIT_TEST(MargeUnorderedPartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 2);

        MergePartition(runtime, env, txId, 1, 0);

        TString boundary((char*)bound_1_2, sizeof(bound_1_2));

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), boundary);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, boundary, Nothing());
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {});
        ValidatePartitionParents(partition2, {0, 1});

        ValidatePartitionChildren(partition0, {2});
        ValidatePartitionChildren(partition1, {2});
        ValidatePartitionChildren(partition2, {});
    } // Y_UNIT_TEST(MargeUnorderedPartitions)

    Y_UNIT_TEST(MargePartitions2) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 4);

        MergePartition(runtime, env, txId, 1, 2);

        TString boundary0((char*)bound_1_4, sizeof(bound_1_4));
        TString boundary1((char*)bound_2_4, sizeof(bound_2_4));
        TString boundary2((char*)bound_3_4, sizeof(bound_3_4));

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), boundary0);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, boundary0, boundary1);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Inactive, boundary1, boundary2);
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, boundary2, Nothing());
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, boundary0, boundary2);

        ValidatePartitionParents(partition4, {1, 2});

        ValidatePartitionChildren(partition1, {4});
        ValidatePartitionChildren(partition2, {4});
    } // Y_UNIT_TEST(MargePartitions2)

    Y_UNIT_TEST(MargeNotAdjacentRangePartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        MergePartition(runtime, env, txId, 0, 2,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter,
                         "You cannot merge non-contiguous partitions"}});
    } // Y_UNIT_TEST(MargeNotAdjacentRangePartitions)

    Y_UNIT_TEST(MargeInactivePartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 2);
        SplitPartition(runtime, env, txId, 0, "\010");

        runtime.SimulateSleep(TDuration::Seconds(1));

        MergePartition(runtime, env, txId, 0, 1,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Invalid partition status"}});
        MergePartition(runtime, env, txId, 1, 0,
                       {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Invalid adjacent partition status"}});
    } // Y_UNIT_TEST(MargeInactivePartitions)

    Y_UNIT_TEST(DisableSplitMerge) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto topic = DescribeTopic(runtime);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE),
                static_cast<int>(topic.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType()));
        UNIT_ASSERT_VALUES_EQUAL(1, topic.GetPartitions().size());

        SplitPartition(runtime, env, txId, 0, "\010");
        runtime.SimulateSleep(TDuration::Seconds(1));

        topic = DescribeTopic(runtime);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE),
                static_cast<int>(topic.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType()));
        UNIT_ASSERT_VALUES_EQUAL(3, topic.GetPartitions().size());

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            {
                auto* partitionStrategy = scheme.MutablePQTabletConfig()->MutablePartitionStrategy();
                partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
            }
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter}});

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            {
                auto* partitionStrategy = scheme.MutablePQTabletConfig()->MutablePartitionStrategy();
                partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
                partitionStrategy->SetMaxPartitionCount(0);
            }
        });

        topic = DescribeTopic(runtime);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED),
                static_cast<int>(topic.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType()));

        UNIT_ASSERT_VALUES_EQUAL(3, topic.GetPartitions().size());
        for (const auto& p : topic.GetPartitions()) {
            Cerr <<  ">>>>> Verify partition " << p.GetPartitionId() << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::ETopicPartitionStatus::Active), static_cast<int>(p.GetStatus()));
            UNIT_ASSERT(p.GetChildPartitionIds().empty());
            UNIT_ASSERT(p.GetParentPartitionIds().empty());
            UNIT_ASSERT(!p.HasKeyRange());
        }

    } // Y_UNIT_TEST(DisableSplitMerge)

    Y_UNIT_TEST(EnableSplitMerge) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3, false);

        runtime.SimulateSleep(TDuration::Seconds(1));

        auto topic = DescribeTopic(runtime);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED),
                static_cast<int>(topic.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType()));

        UNIT_ASSERT_VALUES_EQUAL(3, topic.GetPartitions().size());
        for (const auto& p : topic.GetPartitions()) {
            Cerr <<  ">>>>> Verify partition " << p.GetPartitionId() << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::ETopicPartitionStatus::Active), static_cast<int>(p.GetStatus()));
            UNIT_ASSERT(p.GetChildPartitionIds().empty());
            UNIT_ASSERT(p.GetParentPartitionIds().empty());
            UNIT_ASSERT(!p.HasKeyRange());
        }

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            {
                scheme.MutablePQTabletConfig()->MutablePartitionStrategy()->SetPartitionStrategyType(
                    ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE);
            }
        });
        runtime.SimulateSleep(TDuration::Seconds(1));

        topic = DescribeTopic(runtime);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE),
                static_cast<int>(topic.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType()));

        TString bound0((char*)bound_1_3, sizeof(bound_1_3));
        TString bound1((char*)bound_2_3, sizeof(bound_2_3));

        UNIT_ASSERT_VALUES_EQUAL(3, topic.GetPartitions().size());
        for (const auto& p : topic.GetPartitions()) {
            Cerr <<  ">>>>> Verify partition " << p.GetPartitionId() << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(::NKikimrPQ::ETopicPartitionStatus::Active), static_cast<int>(p.GetStatus()));
            UNIT_ASSERT(p.GetChildPartitionIds().empty());
            UNIT_ASSERT(p.GetParentPartitionIds().empty());
            UNIT_ASSERT(p.HasKeyRange());

            switch(p.GetPartitionId()) {
                case 0:
                    ValidatePartition(p, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), bound0);
                    break;
                case 1:
                    ValidatePartition(p, NKikimrPQ::ETopicPartitionStatus::Active, bound0,  bound1);
                    break;
                case 2:
                    ValidatePartition(p, NKikimrPQ::ETopicPartitionStatus::Active, bound1, Nothing());
                    break;
                default:
                    UNIT_ASSERT_C(false, "Unexpected partition id " << p.GetPartitionId());
            }
        }
    } // Y_UNIT_TEST(EnableSplitMerge)

} // Y_UNIT_TEST_SUITE(TSchemeShardTopicSplitMergeTest)
