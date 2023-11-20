#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/tx/datashard/range_ops.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(RangeOps) {
    TString MakePoint(TVector<std::optional<ui64>> values) {
        TVector<TCell> vec;
        for (const auto& val: values) {
            TCell cell = val ? TCell::Make<ui64>(val.value()) : TCell();
            vec.push_back(cell);
        }
        TArrayRef<TCell> cells(vec);
        return TSerializedCellVec::Serialize(cells);
    }

    TString MakeLeftInf(ui32 cellsCount = 1) {
        TVector<TCell> vec = TVector(cellsCount, TCell());
        TArrayRef<TCell> cells(vec);
        return TSerializedCellVec::Serialize(cells);
    }

    TString MakeRightInf() {
        return {};
    }

    TSerializedTableRange MakeRange(TString from, bool fromInclusive, TString to, bool toInclusive) {
        return TSerializedTableRange(from, to, fromInclusive, toInclusive);
    }

    void CheckRange(TConstArrayRef<NScheme::TTypeInfo> types,
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

        auto typeInfoUi64 = NScheme::TTypeInfo(NScheme::NTypeIds::Uint64);
        const TVector<NScheme::TTypeInfo> valueType = {typeInfoUi64};
        const TVector<NScheme::TTypeInfo> pairType = {typeInfoUi64, typeInfoUi64};

        const auto emptyRange = MakeRange(
            MakePoint({20}), true,
            MakePoint({10}), true
            );

        {
            //=================
            //(-inf;        +inf)
            //----[bbbbbbbb]------- // range with lead null

            auto first = MakeRange(
                MakeLeftInf(pairType.size()), true,
                MakeRightInf(), false
                );

            Cerr << "first " << DebugPrintRange(pairType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({std::nullopt, 1}), true,
                MakePoint({20, 20}), true
                );

            Cerr << " second " << DebugPrintRange(pairType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(pairType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(pairType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({std::nullopt, 1}), true,
                MakePoint({20, 20}), true
                );

            Cerr << " correct " << DebugPrintRange(pairType, correct.ToTableRange(), *typeRegistry);

            CheckRange(pairType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            // test for KIKIMR-14517
            //=================
            //------(-inf;    +inf) // -inf is wrongly set up as {null} instead of {null, null}
            //----[bbbbbbbb]------- // range with lead null

            // here is a mistake: MakeLeftInf(valueType.size()) is set up as {null}
            // but -inf shoud be set up as {null, null}
            // however  MakePoint({std::nullopt, 1}) is {null, 1}
            // as a resul {null} = {null, +inf} > {null, 1} > {null, null}

            auto first = MakeRange(
                MakeLeftInf(valueType.size()), true,
                MakeRightInf(), false
                );

            Cerr << "first " << DebugPrintRange(pairType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({std::nullopt, 1}), true,
                MakePoint({20, 20}), true
                );

            Cerr << " second " << DebugPrintRange(pairType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(pairType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(pairType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({std::nullopt}), true, // {null} = {null, +inf} > {null, 1} > {null, null}, so there is {null} instead {null, 1}
                MakePoint({20, 20}), true
                );

            Cerr << " correct " << DebugPrintRange(pairType, correct.ToTableRange(), *typeRegistry);

            CheckRange(pairType, correct.ToTableRange(), range);
            Cerr << Endl;
        }

        {
            //=================
            //-----------[aaa]---
            //----[bbb]----------
            auto first = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({5}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({10}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({15}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({15}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({30}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({10}), true,
                MakePoint({10}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({10}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({10}), true,
                MakePoint({15}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({15}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({10}), true,
                MakePoint({30}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({15}), true,
                MakePoint({17}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({15}), true,
                MakePoint({17}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({15}), true,
                MakePoint({20}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({15}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({15}), true,
                MakePoint({30}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({15}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({20}), true,
                MakePoint({20}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({20}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({20}), true,
                MakePoint({30}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({20}), true,
                MakePoint({20}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({25}), true,
                MakePoint({30}), true
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
                MakePoint({10}), false,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), true
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
                MakePoint({10}), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), false
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
                MakePoint({10}), false,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), false
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
                MakePoint({10}), false,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({15}), true
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), false,
                MakePoint({15}), true
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
                MakePoint({10}), false,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({15}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), false,
                MakePoint({15}), false
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
                MakePoint({10}), false,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), false,
                MakePoint({20}), false
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
                MakePoint({10}), false,
                MakePoint({20}), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), false,
                MakePoint({20}), false
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
                MakeLeftInf(valueType.size()), true,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
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
                MakePoint({10}), true,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), true,
                MakePoint({20}), false
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
                MakePoint({10}), false,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), false
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
                MakePoint({10}), false,
                MakeRightInf(), false
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({10}), false,
                MakePoint({20}), false
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
                MakeLeftInf(valueType.size()), true,
                MakePoint({10}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({20}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), true
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
                MakeLeftInf(valueType.size()), true,
                MakePoint({20}), true
                );
            Cerr << "first " << DebugPrintRange(valueType, first.ToTableRange(), *typeRegistry);
            Y_ASSERT(!first.ToTableRange().IsEmptyRange(valueType));

            auto second = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), false
                );
            Cerr << " second " << DebugPrintRange(valueType, second.ToTableRange(), *typeRegistry);

            auto range = Intersect(valueType, first.ToTableRange(), second.ToTableRange());
            Cerr << " result " << DebugPrintRange(valueType, range, *typeRegistry);

            auto correct = MakeRange(
                MakePoint({1}), true,
                MakePoint({10}), false
                );

            Cerr << " correct " << DebugPrintRange(valueType, correct.ToTableRange(), *typeRegistry);

            CheckRange(valueType, correct.ToTableRange(), range);
            Cerr << Endl;
        }
    }
}}
