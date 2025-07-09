#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/map_subrange.h>

Y_UNIT_TEST_SUITE(CacheEviction) {

template<class K>
struct TKeyRange {
    K Begin;
    bool IncludeBegin;
    K End;
    bool IncludeEnd;
};

template<class K>
TKeyRange<K> OpenOpen(K begin, K end)
{
    return {.Begin=begin, .IncludeBegin=false, .End=end, .IncludeEnd=false};
}

template<class K>
TKeyRange<K> OpenClose(K begin, K end)
{
    return {.Begin=begin, .IncludeBegin=false, .End=end, .IncludeEnd=true};
}

template<class K>
TKeyRange<K> CloseOpen(K begin, K end)
{
    return {.Begin=begin, .IncludeBegin=true, .End=end, .IncludeEnd=false};
}

template<class K>
TKeyRange<K> CloseClose(K begin, K end)
{
    return {.Begin=begin, .IncludeBegin=true, .End=end, .IncludeEnd=true};
}

void CheckDeleteKeys(const TMap<int, int>& map, const TKeyRange<int>& range, const TSet<int>& expectedKeys)
{
    auto [lowerBound, upperBound] =
        NKikimr::NPQ::MapSubrange(map,
                                  range.Begin, range.IncludeBegin,
                                  range.End, range.IncludeEnd);

    TSet<int> remainKeys;
    for (const auto& [key, _] : map) {
        remainKeys.insert(key);
    }

    for (; lowerBound != upperBound; ++lowerBound) {
        remainKeys.erase(lowerBound->first);
    }

    UNIT_ASSERT_VALUES_EQUAL(remainKeys, expectedKeys);
}

Y_UNIT_TEST(DeleteKeys) {
    const TMap<int, int> map{
        {11, 1},
        {13, 1},
        {15, 1},
        {17, 1},
        {19, 1}
    };

    CheckDeleteKeys(map, OpenOpen  ( 2, 10), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose ( 2, 10), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen ( 2, 10), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose( 2, 10), {11, 13, 15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  ( 2, 11), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose ( 2, 11), {    13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen ( 2, 11), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose( 2, 11), {    13, 15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  ( 2, 14), {        15, 17, 19});
    CheckDeleteKeys(map, OpenClose ( 2, 14), {        15, 17, 19});
    CheckDeleteKeys(map, CloseOpen ( 2, 14), {        15, 17, 19});
    CheckDeleteKeys(map, CloseClose( 2, 14), {        15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  ( 2, 15), {        15, 17, 19});
    CheckDeleteKeys(map, OpenClose ( 2, 15), {            17, 19});
    CheckDeleteKeys(map, CloseOpen ( 2, 15), {        15, 17, 19});
    CheckDeleteKeys(map, CloseClose( 2, 15), {            17, 19});

    CheckDeleteKeys(map, OpenOpen  ( 2, 19), {                19});
    CheckDeleteKeys(map, OpenClose ( 2, 19), {                  });
    CheckDeleteKeys(map, CloseOpen ( 2, 19), {                19});
    CheckDeleteKeys(map, CloseClose( 2, 19), {                  });

    CheckDeleteKeys(map, OpenOpen  ( 2, 21), {                  });
    CheckDeleteKeys(map, OpenClose ( 2, 21), {                  });
    CheckDeleteKeys(map, CloseOpen ( 2, 21), {                  });
    CheckDeleteKeys(map, CloseClose( 2, 21), {                  });

    CheckDeleteKeys(map, OpenOpen  (11, 11), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (11, 11), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (11, 11), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(11, 11), {    13, 15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  (11, 14), {11,     15, 17, 19});
    CheckDeleteKeys(map, OpenClose (11, 14), {11,     15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (11, 14), {        15, 17, 19});
    CheckDeleteKeys(map, CloseClose(11, 14), {        15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  (11, 15), {11,     15, 17, 19});
    CheckDeleteKeys(map, OpenClose (11, 15), {11,         17, 19});
    CheckDeleteKeys(map, CloseOpen (11, 15), {        15, 17, 19});
    CheckDeleteKeys(map, CloseClose(11, 15), {            17, 19});

    CheckDeleteKeys(map, OpenOpen  (11, 19), {11,             19});
    CheckDeleteKeys(map, OpenClose (11, 19), {11                });
    CheckDeleteKeys(map, CloseOpen (11, 19), {                19});
    CheckDeleteKeys(map, CloseClose(11, 19), {                  });

    CheckDeleteKeys(map, OpenOpen  (11, 21), {11                });
    CheckDeleteKeys(map, OpenClose (11, 21), {11                });
    CheckDeleteKeys(map, CloseOpen (11, 21), {                  });
    CheckDeleteKeys(map, CloseClose(11, 21), {                  });

    CheckDeleteKeys(map, OpenOpen  (12, 14), {11,     15, 17, 19});
    CheckDeleteKeys(map, OpenClose (12, 14), {11,     15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (12, 14), {11,     15, 17, 19});
    CheckDeleteKeys(map, CloseClose(12, 14), {11,     15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  (14, 14), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (14, 14), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (14, 14), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(14, 14), {11, 13, 15, 17, 19});

    CheckDeleteKeys(map, OpenOpen  (14, 15), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (14, 15), {11, 13,     17, 19});
    CheckDeleteKeys(map, CloseOpen (14, 15), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(14, 15), {11, 13,     17, 19});

    CheckDeleteKeys(map, OpenOpen  (14, 19), {11, 13,         19});
    CheckDeleteKeys(map, OpenClose (14, 19), {11, 13            });
    CheckDeleteKeys(map, CloseOpen (14, 19), {11, 13,         19});
    CheckDeleteKeys(map, CloseClose(14, 19), {11, 13            });

    CheckDeleteKeys(map, OpenOpen  (14, 21), {11, 13            });
    CheckDeleteKeys(map, OpenClose (14, 21), {11, 13            });
    CheckDeleteKeys(map, CloseOpen (14, 21), {11, 13            });
    CheckDeleteKeys(map, CloseClose(14, 21), {11, 13            });

    CheckDeleteKeys(map, OpenOpen  (15, 15), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (15, 15), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (15, 15), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(15, 15), {11, 13,     17, 19});

    CheckDeleteKeys(map, OpenOpen  (15, 19), {11, 13, 15,     19});
    CheckDeleteKeys(map, OpenClose (15, 19), {11, 13, 15,       });
    CheckDeleteKeys(map, CloseOpen (15, 19), {11, 13,         19});
    CheckDeleteKeys(map, CloseClose(15, 19), {11, 13            });

    CheckDeleteKeys(map, OpenOpen  (15, 21), {11, 13, 15        });
    CheckDeleteKeys(map, OpenClose (15, 21), {11, 13, 15        });
    CheckDeleteKeys(map, CloseOpen (15, 21), {11, 13            });
    CheckDeleteKeys(map, CloseClose(15, 21), {11, 13            });

    CheckDeleteKeys(map, OpenOpen  (19, 19), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (19, 19), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (19, 19), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(19, 19), {11, 13, 15, 17    });

    CheckDeleteKeys(map, OpenOpen  (19, 21), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (19, 21), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (19, 21), {11, 13, 15, 17    });
    CheckDeleteKeys(map, CloseClose(19, 21), {11, 13, 15, 17    });

    CheckDeleteKeys(map, OpenOpen  (21, 21), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, OpenClose (21, 21), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseOpen (21, 21), {11, 13, 15, 17, 19});
    CheckDeleteKeys(map, CloseClose(21, 21), {11, 13, 15, 17, 19});
}

}
