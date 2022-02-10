#pragma once 
 
#include "util.h" 
#include "meter.h" 
 
#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
 
namespace NKikimr { 
namespace NTable { 
 
    class TPerfMemUp { 
    public: 
        void DoUpdate(bool insert) 
        { 
            NTest::TLayoutCook lay; 
 
            lay 
                .Col(0, 0,  NScheme::NTypeIds::Uint64) 
                .Col(0, 1,  NScheme::NTypeIds::Double) 
                .Key({ 0 }); 
 
            TMemTable table(lay.RowScheme(), 0);
 
            TVector<TRawTypeValue> key;
            TVector<TUpdateOp> ops;
 
            ui64 keyUi64 = 42; 
            key.emplace_back(&keyUi64, sizeof(keyUi64), NScheme::NTypeIds::Uin> 
 
            double dblVal = 2015.1027; 
            ops.emplace_back(1, ECellOp::Set, TRawTypeValue(&dblVal, sizeof(dblVa>
 
            NTest::TMeter meter(TDuration::Seconds(10)); 
 
            for (;meter.More();) { 
                keyUi64 = meter.Count(); 
                if (!insert) 
                    keyUi64 %= 0x010000; 
 
                table.Update(ERowOp::Upsert, key, ops);
            } 
 
            Cout << "rps " << meter.Report() << Endl; 
        } 
    }; 
 
} 
} 
