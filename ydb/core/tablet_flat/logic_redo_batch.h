#pragma once

#include "util_basics.h"
#include "flat_exec_commit.h"
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NRedo {

    struct TBatch {

        void Add(TString redo, TArrayRef<const ui32> affects)
        {
            Bytes += redo.size();
            Bodies.emplace_back(std::move(redo));
            Tables.insert(affects.begin(), affects.end());
        }

        TArrayRef<const ui32> Affects()
        {
            Affects_.assign(Tables.begin(), Tables.end());
            return Affects_;
        }

        TString Flush()
        {
            TString out(Reserve(Bytes));

            for (const auto &x : Bodies)
                out.append(x);

            Bytes = 0;
            Bodies.clear();
            Tables.clear();

            return out;
        }

        TAutoPtr<TLogCommit> Commit;
        ui64 Bytes = 0;
        TVector<TString> Bodies;

    private:
        THashSet<ui32> Tables;
        TStackVec<ui32> Affects_;
    };

}
}
}
