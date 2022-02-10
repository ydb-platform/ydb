#include "datashard_ut_common.h"

#include <ydb/core/tx/datashard/range_ops.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(RangeOps) {
    TString MakeValue(ui64 value) {
        TRawTypeValue raw(&value, sizeof(value), NScheme::NTypeIds::Uint64);
        TCell cell(&raw);
        TVector<TCell> vec = {cell};
        TArrayRef<TCell> cells(vec);
        return TSerializedCellVec::Serialize(cells);
    }
    TString MakeLeftInf() {
        TCell cell;
        TVector<TCell> vec = {cell};
        TArrayRef<TCell> cells(vec);
        return TSerializedCellVec::Serialize(cells);
    }

    TString MakeRightInf() {
        return {};
    }

    TSerializedTableRange MakeRange(TString from, bool fromInclusive, TString to, bool toInclusive) {
        return TSerializedTableRange(from, to, fromInclusive, toInclusive);
    }

    void CheckRange(TConstArrayRef<NScheme::TTypeId> types,
                    const TTableRange& first, const TTableRange& second) {
        if (first.IsEmptyRange(types)) {
            Y_ASSERT(second.IsEmptyRange(types));
            return;
        }

        int cmpFF = CompareBorders<false, false>(first.From,
                                                 second.From,
                                                 first.InclusiveFrom,
                                                 second.InclusiveFrom,
                                                 types);
        Y_ASSERT(cmpFF == 0);

        int cmpTT = CompareBorders<true, true>(first.To,
                                               second.To,
                                               first.InclusiveTo,
                                               second.InclusiveTo,
                                               types);
        Y_ASSERT(cmpTT == 0);

    }

    Y_UNIT_TEST(Intersection) {
        auto typeRegistry = MakeHolder<NScheme::TKikimrTypeRegistry>();
        typeRegistry->CalculateMetadataEtag();

        const TVector<NKikimr::NScheme::TTypeId> valueType = {NScheme::NTypeIds::Uint64};

        const auto emptyRange = MakeRange(
            MakeValue(20), true,
            MakeValue(10), true
            );

        {
            //=================
            //-----------[aaa]---
            //----[bbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(5), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----------[aaa]---
            //----[bbbbbb]-------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(10), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }


        {
            //=================
            //-----------[aaa]---
            //----[bbbbbbbb]-----
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(15), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(15), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----------[aaa]---
            //----[bbbbbbbbbb]---
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //---------[aaa]----
            //--[bbbbbbbbbbbbb]-
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(30), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }


        {
            //=================
            //----[aaaaaa]----------
            //----[]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(10), true,
                MakeValue(10), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(10), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //----[aaaaaa]----------
            //----[bbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(10), true,
                MakeValue(15), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(15), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {

            //=================
            //----[aaaaaa]----------
            //----[bbbbbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {

            //=================
            //----[aaaaaa]----------
            //----[bbbbbbbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(10), true,
                MakeValue(30), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaaaaaa]----------
            //-------[bbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(15), true,
                MakeValue(17), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(15), true,
                MakeValue(17), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaaaaaa]----------
            //-------[bbbbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(15), true,
                MakeValue(20), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(15), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaaaaaa]----------
            //-------[bbbbbbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(15), true,
                MakeValue(30), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(15), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaa]----------
            //---------[]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(20), true,
                MakeValue(20), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(20), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaa]----------
            //---------[bbb]----------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(20), true,
                MakeValue(30), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(20), true,
                MakeValue(20), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----[aaa]----------
            //------------[bbb]---
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(25), true,
                MakeValue(30), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }


        {
            //=================
            //-----------(aaa]---
            //----[bbbbbb]-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);


            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----------[aaa]---
            //----[bbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);


            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----------(aaa]---
            //----[bbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);


            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //---------(aaa]---
            //----[bbbbbb]-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(15), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), false,
                MakeValue(15), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //---------(aaa]---
            //----[bbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(15), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), false,
                MakeValue(15), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //---------(aaa]---
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), false,
                MakeValue(20), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //---------(aaa)---
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeValue(20), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), false,
                MakeValue(20), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //(-inf;        +inf)
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeLeftInf(), true,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-------[aaaaa  +inf)
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), true,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), true,
                MakeValue(20), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //--------(aaaaa  +inf)
            //----[bbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            CheckRange(valueType, emptyRange.ToTableRange(), range);
            Cerr << Endl;
        }


        {
            //=================
            //-------(aaaaa  +inf)
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeValue(10), false,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(10), false,
                MakeValue(20), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //(-inf;  aaa]------
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeLeftInf(), true,
                MakeValue(10), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(20), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(1), true,
                MakeValue(10), true
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //(-inf;       aaa]------
            //----[bbbbbbbb)-------
            auto first = MakeRange(
                MakeLeftInf(), true,
                MakeValue(20), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakeValue(1), true,
                MakeValue(10), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakeValue(1), true,
                MakeValue(10), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }
    }
}}
