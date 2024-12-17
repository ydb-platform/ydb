#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/topological_ordering.h>

#include <random>

namespace NYT {
namespace {

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TTopologicalOrderingTest
    : public Test
{
protected:
    std::vector<std::pair<int, int>> CurrentEdges_;
    TIncrementalTopologicalOrdering<int> IncrementalOrdering_;

    void AddEdgeAndValidate(int from, int to)
    {
        CurrentEdges_.emplace_back(from, to);
        IncrementalOrdering_.AddEdge(from, to);
        ValidateOrdering();
    }

    void ValidateOrdering()
    {
        const auto& ordering = IncrementalOrdering_.GetOrdering();
        THashMap<int, int> positionInOrdering;
        for (int index = 0; index < std::ssize(ordering); ++index) {
            positionInOrdering[ordering[index]] = index;
        }
        for (const auto& edge : CurrentEdges_) {
            EXPECT_TRUE(positionInOrdering.contains(edge.first));
            EXPECT_TRUE(positionInOrdering.contains(edge.second));
            EXPECT_LT(positionInOrdering[edge.first], positionInOrdering[edge.second]);
        }
    }

    void Clear()
    {
        CurrentEdges_.clear();
        IncrementalOrdering_ = TIncrementalTopologicalOrdering<int>();
    }

    void TearDown() override
    {
        Clear();
    }
};

TEST_F(TTopologicalOrderingTest, Simple)
{
    AddEdgeAndValidate(2, 0);
    EXPECT_THAT(IncrementalOrdering_.GetOrdering(), ElementsAre(2, 0));

    AddEdgeAndValidate(0, 3);
    EXPECT_THAT(IncrementalOrdering_.GetOrdering(), ElementsAre(2, 0, 3));

    // Adding duplicating edge doesn't change anything.
    AddEdgeAndValidate(2, 0);
    EXPECT_THAT(IncrementalOrdering_.GetOrdering(), ElementsAre(2, 0, 3));

    AddEdgeAndValidate(2, 1);
    // We can't be sure if topological ordering is {2, 0, 3, 1}, {2, 0, 1, 3} or {2, 1, 0, 3} now.

    AddEdgeAndValidate(1, 3);
    // Now topological ordering is either {2, 0, 1, 3} or {2, 1, 0, 3}.

    AddEdgeAndValidate(1, 0);
    EXPECT_THAT(IncrementalOrdering_.GetOrdering(), ElementsAre(2, 1, 0, 3));
}

TEST_F(TTopologicalOrderingTest, RandomizedTest)
{
    const int iterationCount = 1000;
    const int maxVertexCount = 15;
    const int maxVertexValue = 1000 * 1000 * 1000;
    std::mt19937 gen;
    for (int iteration = 0; iteration < iterationCount; ++iteration) {
        int vertexCount = std::uniform_int_distribution<>(2, maxVertexCount)(gen);
        // Generate the vertex values.
        THashSet<int> vertices;
        while (std::ssize(vertices) < vertexCount) {
            vertices.insert(std::uniform_int_distribution<>(0, maxVertexValue)(gen));
        }

        // Generate the desired topological ordering. We will extract edges from it and
        // feed to the incremental ordering.
        std::vector<int> desiredTopologicalOrdering(vertices.begin(), vertices.end());
        std::shuffle(desiredTopologicalOrdering.begin(), desiredTopologicalOrdering.end(), gen);

        std::vector<std::pair<int, int>> edges;
        for (int fromIndex = 0; fromIndex < vertexCount; ++fromIndex) {
            for (int toIndex = fromIndex + 1; toIndex < vertexCount; ++toIndex) {
                int from = desiredTopologicalOrdering[fromIndex];
                int to = desiredTopologicalOrdering[toIndex];
                AddEdgeAndValidate(from, to);
            }
        }

        EXPECT_EQ(IncrementalOrdering_.GetOrdering(), desiredTopologicalOrdering);

        Clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
