#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq::NInternal {
namespace {

class TReadState final : public TPqReadState {
public:
    TReadState(ui64 group, ui64 tasks, const TString& cluster, ui64 partitions = 4)
        : TPqReadState(0, group, {}, TString("rescaling"), MakeSource(cluster, partitions), MakeReadParams(group, tasks, partitions), {}, {})
    {}

    TReadState(NPq::NProto::TDqPqTopicSource source, TVector<NPq::NProto::TDqReadTaskParams> readParams)
        : TPqReadState(0, 0, {}, TString("rescaling"), std::move(source), std::move(readParams), {}, {})
    {}

private:
    static NPq::NProto::TDqPqTopicSource MakeSource(const TString& cluster, ui64 partitions) {
        NPq::NProto::TDqPqTopicSource source;
        if (cluster) {
            auto* info = source.AddFederatedClusters();
            info->SetName(cluster);
            info->SetPartitionsCount(partitions);
        }
        return source;
    }

    static TVector<NPq::NProto::TDqReadTaskParams> MakeReadParams(ui64 group, ui64 tasks, ui64 partitions) {
        TVector<NPq::NProto::TDqReadTaskParams> params(1);
        auto* partitioning = params.front().AddPartitioningParams();
        partitioning->SetTopicPartitionsCount(partitions);
        partitioning->SetDqPartitionsCount(tasks);
        partitioning->SetEachTopicPartitionGroupId(group);
        return params;
    }

    void OnConsumerOffsetsInitialized() override {}
};

void CheckRepeatedRescaling(const TString& cluster) {
    TFakeCASetup setup;
    TVector<std::pair<ui64, size_t>> restored;
    setup.Execute([&](TFakeActor&) {
        TReadState initial(0, 1, cluster);
        for (ui64 partition = 0; partition < 4; ++partition) {
            initial.Partitions[TPartitionKey{cluster, partition}].Offset = 0;
        }
        TSourceState firstCheckpoint;
        initial.SaveState(CreateCheckpoint(), firstCheckpoint);

        // Each new reader receives the old state, but advances only its own
        // partitions. Stale offsets must not survive into the next checkpoint.
        TSourceState secondCheckpoint;
        for (ui64 group = 0; group < 2; ++group) {
            TReadState reader(group, 2, cluster);
            reader.LoadState(firstCheckpoint);
            for (ui64 partition = group; partition < 4; partition += 2) {
                reader.Partitions[TPartitionKey{cluster, partition}].Offset = 2;
            }
            reader.SaveState(CreateCheckpoint(1), secondCheckpoint);
        }

        for (ui64 partition = 0; partition < 4; ++partition) {
            TReadState reader(partition, 4, cluster);
            reader.LoadState(secondCheckpoint);
            restored.emplace_back(*reader.Partitions.at(TPartitionKey{cluster, partition}).Offset, reader.Partitions.size());
        }
    });
    for (const auto& [offset, partitionCount] : restored) {
        UNIT_ASSERT_VALUES_EQUAL(offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(partitionCount, 1);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TPqReadStateTest) {
    Y_UNIT_TEST(RepeatedRescaling) {
        CheckRepeatedRescaling("");
    }

    Y_UNIT_TEST(RepeatedRescalingFederatedCluster) {
        CheckRepeatedRescaling("cluster");
    }

    Y_UNIT_TEST(RestoresOnlyAssignedSparsePartitions) {
        TFakeCASetup setup;
        THashMap<TPartitionKey, ui64> offsets;
        TInstant startingMessageTimestamp;
        ui64 ingressBytes = 0;
        setup.Execute([&](TFakeActor&) {
            TReadState source(0, 1, "", 100);
            for (ui64 partition : {0, 7, 42, 99}) {
                source.Partitions[TPartitionKey{"", partition}].Offset = partition * 10;
            }
            source.StartingMessageTimestamp = TInstant::MilliSeconds(12345);
            source.IngressStats.Bytes = 6789;
            TSourceState checkpoint;
            source.SaveState(CreateCheckpoint(), checkpoint);

            TVector<NPq::NProto::TDqReadTaskParams> params(2);
            for (size_t i = 0; i < params.size(); ++i) {
                auto* range = params[i].AddPartitioningParams();
                range->SetTopicPartitionsCount(100);
                range->SetDqPartitionsCount(100);
                range->SetEachTopicPartitionGroupId(i == 0 ? 0 : 42);
            }
            TReadState reader({}, std::move(params));
            reader.LoadState(checkpoint);
            for (const auto& [key, progress] : reader.Partitions) {
                offsets[key] = *progress.Offset;
            }
            startingMessageTimestamp = reader.StartingMessageTimestamp;
            ingressBytes = reader.IngressStats.Bytes;
        });
        UNIT_ASSERT_VALUES_EQUAL(offsets.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"", 0}), 0);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"", 42}), 420);
        // Filtering partition offsets must not change the other fields.
        UNIT_ASSERT_VALUES_EQUAL(startingMessageTimestamp, TInstant::MilliSeconds(12345));
        UNIT_ASSERT_VALUES_EQUAL(ingressBytes, 6789);
    }

    Y_UNIT_TEST(FederatedPartitionCountsAndClusterIdentity) {
        TFakeCASetup setup;
        THashMap<TPartitionKey, ui64> offsets;
        setup.Execute([&](TFakeActor&) {
            NPq::NProto::TDqPqTopicSource sourceParams;
            TReadState source(0, 1, "");
            const TVector<TString> clusters = {"small", "large", "fallback"};
            const TVector<ui64> counts = {2, 6, 0};
            for (size_t i = 0; i < clusters.size(); ++i) {
                auto* cluster = sourceParams.AddFederatedClusters();
                cluster->SetName(clusters[i]);
                cluster->SetPartitionsCount(counts[i]);
                for (ui64 partition = 0; partition < (counts[i] ? counts[i] : 4); ++partition) {
                    source.Partitions[TPartitionKey{clusters[i], partition}].Offset = 100 * i + partition;
                }
            }
            TSourceState checkpoint;
            source.SaveState(CreateCheckpoint(), checkpoint);
            TVector<NPq::NProto::TDqReadTaskParams> params(1);
            auto* range = params.front().AddPartitioningParams();
            range->SetTopicPartitionsCount(4);
            range->SetDqPartitionsCount(2);
            range->SetEachTopicPartitionGroupId(0);
            TReadState reader(std::move(sourceParams), std::move(params));
            reader.LoadState(checkpoint);
            for (const auto& [key, progress] : reader.Partitions) {
                offsets[key] = *progress.Offset;
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(offsets.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"small", 0}), 0);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"large", 0}), 100);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"large", 2}), 102);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"large", 4}), 104);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"fallback", 0}), 200);
        UNIT_ASSERT_VALUES_EQUAL(offsets.at(TPartitionKey{"fallback", 2}), 202);
    }

    Y_UNIT_TEST(OverlappingStatesKeepMinimumOffset) {
        TFakeCASetup setup;
        ui64 offset = 0;
        setup.Execute([&](TFakeActor&) {
            TReadState source(0, 4, "");
            TSourceState checkpoint;
            for (ui64 savedOffset : {9, 5}) {
                source.Partitions[TPartitionKey{"", 0}].Offset = savedOffset;
                source.SaveState(CreateCheckpoint(), checkpoint);
            }
            TReadState reader(0, 4, "");
            reader.LoadState(checkpoint);
            offset = *reader.Partitions.at(TPartitionKey{"", 0}).Offset;
        });
        UNIT_ASSERT_VALUES_EQUAL(offset, 5);
    }
}

} // namespace NYql::NDq::NInternal
