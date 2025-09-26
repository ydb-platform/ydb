#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/services/lib/sharding/sharding.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace std::string_literals;

// bounds of key distribution. Max<ui128> devided to 3 equals parts.
// ranges calculated with NDataStreams::V1::RangeFromShardNumber

const TString BOUND_0_4 = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"s;
const TString BOUND_1_4 = "\x3f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe"s;
const TString BOUND_2_4 = "\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"s;
const TString BOUND_3_4 = "\xbf\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"s;
const TString BOUND_4_4 = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"s;
const TString BOUND_0_6 = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"s;
const TString BOUND_1_6 = "\x2a\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa"s;
const TString BOUND_2_6 = "\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x54"s;
const TString BOUND_3_6 = "\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"s;
const TString BOUND_4_6 = "\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xa9"s;
const TString BOUND_5_6 = "\xd5\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x55\x54"s;
const TString BOUND_6_6 = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"s;


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


 struct TPartitionBoundary {
    ui32 PartitionId;
    TMaybe<TString> FromBound;
    TMaybe<TString> ToBound;
    bool Create;
 };


static NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionBoundary Convert(const TPartitionBoundary& b) {
    NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionBoundary result;
    result.SetPartition(b.PartitionId);
    if (b.FromBound) {
        result.MutableKeyRange()->SetFromBound(*b.FromBound);
    }
    if (b.ToBound) {
        result.MutableKeyRange()->SetToBound(*b.ToBound);
    }
    result.SetCreatePartition(b.Create);
    return result;
}

void SetRootBoundaries(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
                       const TVector<TPartitionBoundary>& boundaries,
                       const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {
    ModifyTopic(runtime, env, txId, [&](auto& scheme) {
        for (const auto& boundary: boundaries) {
            scheme.AddRootPartitionBoundaries()->CopyFrom(Convert(boundary));
        }
    }, expectedResults);
}

void SplitPartitionTo(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const ui32 partition, TString boundary,
                    const TConstArrayRef<ui32> childPartitionIds, bool allowRootLevelSibling,
                    const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {

    ModifyTopic(runtime, env, txId, [&](auto& scheme) {
        auto* split = scheme.AddSplit();
        split->SetPartition(partition);
        split->SetSplitBoundary(boundary);
        for (ui32 childPartitionId: childPartitionIds) {
            split->AddChildPartitionIds(childPartitionId);
        }
        if (allowRootLevelSibling) {
            split->SetCreateRootLevelSibling(allowRootLevelSibling);
        }
    }, expectedResults);
}

void SplitPartition(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const ui32 partition, TString boundary,
                    const TVector<TExpectedResult>& expectedResults = {{TEvSchemeShard::EStatus::StatusAccepted}}) {
    return SplitPartitionTo(runtime, env, txId, partition, boundary, {}, false, expectedResults);
}

auto DescribeTopic(TTestBasicRuntime& runtime, TString path = "/MyRoot/USER_1/Topic1", ui64 ss = TTestTxConfig::SchemeShard) {
    {
        TControlBoard::SetValue(true, runtime.GetAppData().Icb->SchemeShardControls.FillAllocatePQ);
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
                            "Partition " << id << " KeyRange.FromBound : " << ToHex(*fromBound)
                                                    << " != " << ToHex(partition.GetKeyRange().GetFromBound()));
    } else {
        UNIT_ASSERT_C(!partition.GetKeyRange().HasFromBound(),
                      "Partition " << id << " should not have KeyRange.FromBound");
    }
    if (toBound) {
        UNIT_ASSERT_C(partition.GetKeyRange().HasToBound(), "Partition " << id << " should have KeyRange.ToBound");
        UNIT_ASSERT_EQUAL_C(*toBound, partition.GetKeyRange().GetToBound(),
                            "Partition " << id << " KeyRange.ToBound : " << ToHex(*toBound)
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

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), true}
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "less than the first availiable "}});

        auto topic = DescribeTopic(runtime);
        auto partition = topic.GetPartitions()[0];

        UNIT_ASSERT_C(NKikimrPQ::ETopicPartitionStatus::Active == partition.GetStatus(),
                      "Partition should not be Active after topic creation");
        UNIT_ASSERT_C(!partition.HasKeyRange(),
                      "Partitions should not have KeyRange because topic has only one partition");

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), false}
        });

        topic = DescribeTopic(runtime);
        partition = topic.GetPartitions()[0];

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
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, false},
            {2, BOUND_4_6, Nothing(), false},
        });

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());


        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_1_6, false},
            {1, BOUND_1_6, BOUND_5_6, false},
            {2, BOUND_5_6, Nothing(), false},
        });
        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, BOUND_5_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_5_6, Nothing());

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, BOUND_5_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_5_6, Nothing());
    } // Y_UNIT_TEST(CreateTopicWithManyPartition)

    Y_UNIT_TEST(SplitWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        TString boundary = "\x7f"s;
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

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), false}
        });

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

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), false},
            {1, Nothing(), boundary, false},
            {2, boundary, Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "KeyRange must be specified for root partition bounds"}});

        SetRootBoundaries(runtime, env, ++txId, {
            {1, Nothing(), boundary, false},
            {2, boundary, Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Unable to change bounds of non-root partition"}});

    } // Y_UNIT_TEST(SplitWithOnePartition)

    Y_UNIT_TEST(SplitTwoPartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 2);

        const TString boundary0 = "\x3f"s;

        const TString boundary1 = "\xbf"s;

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

        const TString bound0 = BOUND_2_6;
        const TString bound1 = BOUND_4_6;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        const TString boundary = "\x7f"s;
        SplitPartition(runtime, env, txId, 1, boundary);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_1_6, false},
            {1, BOUND_1_6, BOUND_2_6, false},
            {2, BOUND_2_6, Nothing(), false},
        });

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Inactive, BOUND_1_6, BOUND_2_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, Nothing());
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, boundary);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, boundary, BOUND_4_6);

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

    Y_UNIT_TEST(SetBoundsBadRange) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        const TString bound0 = BOUND_2_6;
        const TString bound1 = BOUND_4_6;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Last patrition"}});

         SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, false},
            {2, BOUND_4_6, BOUND_5_6, false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Last patrition"}});

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, false},
            {2, Nothing(), BOUND_4_6, false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Last patrition"}});

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Only 1 root partitions has new bounds, required: 3"}});

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), Nothing(), false},
            {1, Nothing(), Nothing(), false},
            {2, Nothing(), Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "KeyRange must be specified for root partition bounds"}});

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_1_6, false},
            {0, Nothing(), BOUND_1_6, false},
            {1, BOUND_1_6, BOUND_2_6, false},
            {2, BOUND_2_6, Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "have overlapped bounds"}});

    }


    Y_UNIT_TEST(SetBoundWithWrongPartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        const TString boundary = "\x7f"s;
        SplitPartition(runtime, env, txId, 0, boundary);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), boundary, false},
            {1, boundary, Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Unable to change bounds of non-root partition"}});

    } // Y_UNIT_TEST(SplitWithWrongPartition)

    Y_UNIT_TEST(SetOnDisabledSplitMerge) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3, false);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, false},
            {2, BOUND_4_6, Nothing(), false},
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Split and merge operations disabled"}});

    } // Y_UNIT_TEST(DisabledSplitMerge)

    Y_UNIT_TEST(GrowFromTopicWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {1, BOUND_2_6, BOUND_4_6, true},
            {2, BOUND_4_6, Nothing(), true},
        });

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());

    }

    Y_UNIT_TEST(GrowFromTopicWithManyPartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 3);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_1_6, false},
            {1, BOUND_1_6, BOUND_2_6, false},
            {2, BOUND_2_6, BOUND_3_6, false},
            {3, BOUND_3_6, BOUND_4_6, true},
            {4, BOUND_4_6, BOUND_5_6, true},
            {5, BOUND_5_6, Nothing(), true},
        });

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];
        auto partition5 = topic.GetPartitions()[5];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, BOUND_2_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_3_6);
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_3_6, BOUND_4_6);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, BOUND_5_6);
        ValidatePartition(partition5, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_5_6, Nothing());

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        partition3 = topic.GetPartitions()[3];
        partition4 = topic.GetPartitions()[4];
        partition5 = topic.GetPartitions()[5];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, BOUND_2_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_3_6);
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_3_6, BOUND_4_6);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, BOUND_5_6);
        ValidatePartition(partition5, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_5_6, Nothing());
    }

      Y_UNIT_TEST(GrowFromTopicWithSplittedPartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        TString boundary = "\x7f"s;
        SplitPartition(runtime, env, txId, 0, boundary);

        SetRootBoundaries(runtime, env, ++txId, {
            {0, Nothing(), BOUND_2_6, false},
            {3, BOUND_2_6, BOUND_4_6, true},
            {4, BOUND_4_6, Nothing(), true},
        });
        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];

        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), boundary);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, boundary, Nothing());
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());

        ValidatePartitionParents(partition0, {});
        ValidatePartitionParents(partition1, {0});
        ValidatePartitionParents(partition2, {0});
        ValidatePartitionParents(partition3, {});
        ValidatePartitionParents(partition4, {});

        ValidatePartitionChildren(partition0, {1, 2});
        ValidatePartitionChildren(partition1, {});
        ValidatePartitionChildren(partition2, {});
        ValidatePartitionChildren(partition3, {});
        ValidatePartitionChildren(partition4, {});
    }

     Y_UNIT_TEST(SplitAndGrowFromTopicWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            const TVector<TPartitionBoundary>& boundaries{
                {0, Nothing(), BOUND_2_6, false},
                {1, BOUND_2_6, BOUND_4_6, true},
                {2, BOUND_4_6, Nothing(), true},
            };
            for (const auto& boundary: boundaries) {
                scheme.AddRootPartitionBoundaries()->CopyFrom(Convert(boundary));
            }
            auto* split = scheme.AddSplit();
            split->SetPartition(0);
            split->SetSplitBoundary(BOUND_1_6);
            split->AddChildPartitionIds(3);
            split->AddChildPartitionIds(4);
        });

        auto topic = DescribeTopic(runtime);
        auto partition0 = topic.GetPartitions()[0];
        auto partition1 = topic.GetPartitions()[1];
        auto partition2 = topic.GetPartitions()[2];
        auto partition3 = topic.GetPartitions()[3];
        auto partition4 = topic.GetPartitions()[4];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, Nothing()); // Boundary calculated before grow

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        topic = DescribeTopic(runtime);
        partition0 = topic.GetPartitions()[0];
        partition1 = topic.GetPartitions()[1];
        partition2 = topic.GetPartitions()[2];
        partition3 = topic.GetPartitions()[3];
        partition4 = topic.GetPartitions()[4];
        ValidatePartition(partition0, NKikimrPQ::ETopicPartitionStatus::Inactive, Nothing(), BOUND_2_6);
        ValidatePartition(partition1, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_2_6, BOUND_4_6);
        ValidatePartition(partition2, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_4_6, Nothing());
        ValidatePartition(partition3, NKikimrPQ::ETopicPartitionStatus::Active, Nothing(), BOUND_1_6);
        ValidatePartition(partition4, NKikimrPQ::ETopicPartitionStatus::Active, BOUND_1_6, Nothing());

    }


    Y_UNIT_TEST(GrowAndSplitNewFromTopicWithOnePartition) {
        TTestBasicRuntime runtime;
        TTestEnv env = CreateTestEnv(runtime);

        ui64 txId = 100;

        CreateSubDomain(runtime, env, ++txId);
        CreateTopic(runtime, env, ++txId, 1);

        ModifyTopic(runtime, env, txId, [&](auto& scheme) {
            const TVector<TPartitionBoundary>& boundaries{
                {0, Nothing(), BOUND_2_6, false},
                {1, BOUND_2_6, BOUND_4_6, true},
                {2, BOUND_4_6, Nothing(), true},
            };
            for (const auto& boundary: boundaries) {
                scheme.AddRootPartitionBoundaries()->CopyFrom(Convert(boundary));
            }
            auto* split = scheme.AddSplit();
            split->SetPartition(1);
            split->SetSplitBoundary(BOUND_3_6);
            split->AddChildPartitionIds(3);
            split->AddChildPartitionIds(4);
        }, {{TEvSchemeShard::EStatus::StatusInvalidParameter, "Splitting partition does not exists"}});

    }
}
