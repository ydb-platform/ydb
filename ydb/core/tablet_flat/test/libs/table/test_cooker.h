#pragma once

#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/rows/heap.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <ydb/core/tablet_flat/flat_mem_warm.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TCooker {

        TCooker(const TLayoutCook &lay, TEpoch epoch = TEpoch::Zero())
            : TCooker(lay.RowScheme(), epoch)
        {

        }

        TCooker(TIntrusiveConstPtr<TRowScheme> scheme, TEpoch epoch = TEpoch::Zero())
            : Table(new TMemTable(std::move(scheme), epoch, 0))
            , Tool(*Table->Scheme)
        {

        }

        TIntrusivePtr<TMemTable> operator*() const noexcept
        {
            return Table;
        }

        TIntrusivePtr<TMemTable> Unwrap() const noexcept
        {
            return std::move(const_cast<TIntrusivePtr<TMemTable>&>(Table));
        }

        const TCooker& Add(const TRowsHeap &heap, ERowOp rop) const
        {
            for (auto &row: heap) Add(row, rop);

            return *this;
        }

        const TCooker& Add(const TRow &tagged, ERowOp rop = ERowOp::Upsert) const
        {
            auto pair = Tool.Split(tagged, true, rop != ERowOp::Erase);

            return Table->Update(rop, pair.Key, pair.Ops, { }, /* TODO: rowVersion */ TRowVersion::Min(),
                    /* committed */ nullptr), *this;
        }

    private:
        TIntrusivePtr<TMemTable> Table;
        const NTest::TRowTool Tool;
    };

}
}
}
