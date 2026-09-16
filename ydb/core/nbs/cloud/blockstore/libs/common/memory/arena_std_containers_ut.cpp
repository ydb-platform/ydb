#include "arena_std_containers.h"

#include "arena_allocator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////
//  TArenaSet tests
//////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ArenaSetTests)
{
    Y_UNIT_TEST(ShouldConstructSet)
    {
        TArenaAllocatorPool pool(CreateArenaAllocator());
        TArenaSet<TString> set(&pool);
    }

    Y_UNIT_TEST(ShouldConstructMap)
    {
        TArenaAllocatorPool pool(CreateArenaAllocator());
        TArenaMap<TString, ui64> map1(&pool);
    }

    Y_UNIT_TEST(ShouldConstructHashMap)
    {
        TArenaAllocatorPool pool(CreateArenaAllocator());
        TArenaHashMap<TString, ui64> map1(&pool);
        for (ui64 i = 0; i < 1000000; ++i) {
            map1[std::to_string(i)] = i;
        }
    }
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
