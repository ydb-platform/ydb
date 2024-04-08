#include "mkql_vector_spiller_adapter.h"
#include "mock_spiller_ut.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

    std::vector<int> CreateSimpleVectorOfSize(size_t size) {
        std::vector<int> v;
        v.reserve(size);

        for (size_t i = 0; i < size; ++i) {
            v.push_back(i);
        }

        return v;
    }

Y_UNIT_TEST_SUITE(TVectorSpillerAdapterTest) {

    Y_UNIT_TEST(Simple) {

        std::vector<int> initialVector = CreateSimpleVectorOfSize(20);

        auto spiller = TVectorSpillerAdapter<int>(CreateMockSpiller(), 20_B);

        std::vector<int> vectorToSpill(initialVector);
        spiller.AddData(std::move(vectorToSpill));

        while (!spiller.IsAcceptingData()) {
            spiller.Update();
        }
        spiller.Finalize();

        while (!spiller.IsAcceptingDataRequests()) {
            spiller.Update();
        }

        spiller.RequestNextVector();
        
        while (!spiller.IsDataReady()) {
            spiller.Update();
        }

        auto vectorFromSpilling = spiller.ExtractVector();

        UNIT_ASSERT_VALUES_EQUAL(vectorFromSpilling, initialVector);
    }

}

} //namespace namespace NKikimr::NMiniKQL
