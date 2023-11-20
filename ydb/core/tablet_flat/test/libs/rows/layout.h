#pragma once

#include <ydb/core/tablet_flat/flat_row_column.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_row_remap.h>
#include <ydb/core/tablet_flat/flat_table_column.h>
#include <ydb/core/tablet_flat/flat_util_misc.h>

#include <util/generic/vector.h>

#include <initializer_list>

namespace NKikimr {
namespace NTable {
namespace NTest{

    class TLayoutCook {
    public:
        using TTid = NScheme::TTypeId;

        TLayoutCook& Col(ui32 group, TTag tag, TTid type, TCell null = { })
        {
            Tags_.push_back(tag);

            // pg types are not supported
            Cols.emplace_back("", tag, NScheme::TTypeInfo(type), "");
            Cols.back().Family = group;
            Cols.back().SetDefault(null);

            return *this;
        }

        TLayoutCook& Key(std::initializer_list<NTable::TTag> keys)
        {
            Y_ABORT_UNLESS(!Scheme, "Keys are already assigned for layout cook");

            TPos keyOrder = 0;

            for (auto tag: keys) {
                auto pred = [tag](const TColumn &col) {
                    return tag == col.Id;
                };

                auto it = std::find_if(Cols.begin(), Cols.end(), pred);

                if (it == Cols.end()) {
                    Y_ABORT("Not all key tags found in columns registery");
                } else if (it->KeyOrder != Max<NTable::TPos>()) {
                    Y_ABORT("Non-unique key column tags supplied for layout");
                } else {
                    it->KeyOrder = keyOrder++;
                }
            }

            Scheme = TRowScheme::Make(Cols, NUtil::TAsIs());

            return *this;
        }

        const TRowScheme& operator*() const noexcept
        {
            return *Scheme;
        }

        TIntrusiveConstPtr<TRowScheme> RowScheme() const noexcept
        {
            return Scheme;
        }

        TRemap FullRemap() const
        {
            return TRemap(*Scheme, Tags_);
        }

    private:
        TVector<NTable::TColumn> Cols;
        TVector<NTable::TTag> Tags_;
        TIntrusiveConstPtr<TRowScheme> Scheme;
    };
}
}
}
