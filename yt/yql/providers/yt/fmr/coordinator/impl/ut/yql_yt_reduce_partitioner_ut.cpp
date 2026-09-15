#include "yql_yt_sorted_partitioner_base_ut.h"

namespace NYql::NFmr::NPartitionerTest {

Y_UNIT_TEST_SUITE(ReducePartitionerTests) {
    Y_UNIT_TEST(ReducePartitioningCases) {
        const TVector<TCase> cases = {
            {
                .Input = {
                    {{"A","B"},  {"C","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B)"},
                    {"TAB0-[B:D]"}
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","C"}, {"C","D"}},
                },
                .Expected = {
                    {"TAB0-[A:D]"},
                },
                .MaxWeight = 1000,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","C"}, {"C","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B)"},
                    {"TAB0-[B:D]"}
                },
                .MaxWeight = 2,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B", "B"}, {"B","C"}, {"C","D"}},
                },
                .MaxWeight = 2,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1,
                .ExpectedError = "cannot partition"
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}, {"C","D"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B)", "TAB1-[A:B)"},
                    {"TAB0-[B:C)", "TAB1-[B:C)"},
                    {"TAB0-[C:D]", "TAB1-[C:D]"},
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","B"}, {"B","C"}},
                    {{"A","D"}},
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1,
                .ExpectedError = "cannot partition"
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","B"}, {"B","C"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:C)", "TAB1-[A:C)"},
                    {"TAB0-[C:D]", "TAB1-[C:D]"},
                },
                .MaxWeight = 4,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 2,
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}},
                    {{"X","Y"}},
                },
                .Expected = {
                    {"TAB0-[A:Y]", "TAB1-[A:Y]"},
                },
                .MaxWeight = 1000,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"A","B"}},
                    {{"X","Y"}},
                },
                .Expected = {
                    {"TAB0-[A:B)"},
                    {"TAB0-[B:Y]", "TAB1-[B:Y]"},
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1,
            },
            {
                .Input = {
                    {{"A","B"}},
                    {{"A","B"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]"},
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
            {
                .Input = {
                    {{"B","B"}, {"B","B"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:D]", "TAB1-[A:D]"},
                },
                .MaxWeight = 1000,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 3
            },
            {
                .Input = {
                    {{"B","B"}, {"B","B"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B)", "TAB1-[A:B)"},
                    {"TAB0-[B:D]", "TAB1-[B:D]"},
                },
                .MaxWeight = 2,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 2
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}},
                    {{"A","C"}, {"C","D"}},
                    {{"A","D"}, {"D","E"}},
                    {{"A","E"}, {"E","F"}, {"F","G"}},
                    {{"A","F"}, {"F","G"}, {"G","H"}},
                },
                .Expected = {
                    {"TAB0-[A:B)", "TAB1-[A:B)", "TAB2-[A:B)", "TAB3-[A:B)", "TAB4-[A:B)"},
                    {"TAB0-[B:C)", "TAB1-[B:C)", "TAB2-[B:C)", "TAB3-[B:C)", "TAB4-[B:C)"},
                    {"TAB0-[C:D)", "TAB1-[C:D)", "TAB2-[C:D)", "TAB3-[C:D)", "TAB4-[C:D)"},
                    {"TAB1-[D:E)", "TAB2-[D:E)", "TAB3-[D:E)", "TAB4-[D:E)"},
                    {"TAB2-[E:F)", "TAB3-[E:F)", "TAB4-[E:F)"},
                    {"TAB3-[F:G)", "TAB4-[F:G)"},
                    {"TAB3-[G:H]", "TAB4-[G:H]"}
                },
                .MaxWeight = 1,
                .PartitionerType = EPartitionerType::Reduce,
                .MaxKeySizePerPart = 1
            },
        };

        CheckPartitionCorrectness(cases);
    }
}

} // namespace NYql::NFmr::NPartitionerTest
