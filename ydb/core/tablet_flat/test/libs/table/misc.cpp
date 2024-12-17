#include "test_pretty.h"

#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_row_misc.h>
#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_mem_iter.h>
#include <ydb/core/tablet_flat/flat_part_dump.h>
#include <ydb/core/tablet_flat/flat_table_part.h>
#include <ydb/core/scheme/scheme_type_registry.h>

namespace NKikimr {
namespace NTable {

void TMemTable::DebugDump() const
{
    DebugDump(Cout, *NTest::DbgRegistry());
}

template <class TIterator>
TString PrintRowImpl(const TRemap& remap, const TIterator& it)
{
    TRowState state(remap.Size());

    {
        TDbTupleRef key = it.GetKey();

        for (auto &pin: remap.KeyPins())
            state.Set(pin.Pos, ECellOp::Set, key.Columns[pin.Key]);
    }

    it.Apply(state, /* committed */ nullptr, /* observer */ nullptr);

    {
        TStringStream ss;

        ss << NFmt::TCells(*state, remap, NTest::DbgRegistry());

        return ss.Str();
    }
}

TString PrintRow(const TDbTupleRef& row)
{
    return DbgPrintTuple(row, *NTest::DbgRegistry());
}

TString PrintRow(const TMemIter& it)
{
    return PrintRowImpl(*it.Remap, it);
}

namespace NTest {

    TString DumpPart(const TPartStore &partStore, ui32 depth) noexcept
    {
        TStringStream out;
        TTestEnv env;

        out.Reserve(2030);

        TDump(out, &env, DbgRegistry()).Part(partStore, depth);

        return out.Str();
    }

}

} // namspace NTable
} // namespace NKikimr
