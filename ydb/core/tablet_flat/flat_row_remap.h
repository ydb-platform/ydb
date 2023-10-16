#pragma once

#include "flat_row_eggs.h"
#include "flat_row_scheme.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NTable {

    class TRemap {
    public:
        struct TPin { TPos Pos, Key; };

        struct THas {
            operator TPos() const noexcept
            {
                return Pos;
            }

            explicit operator bool() const noexcept
            {
                return Pos != Max<TPos>();
            }

            TPos Pos;
        };

        static TRemap Full(const TRowScheme& scheme)
        {
            TVector<ui32> tags(scheme.Cols.size());
            for (auto &col: scheme.Cols)
                tags[col.Pos] = col.Tag;

            return TRemap(scheme, tags);
        }

        TRemap(const TRowScheme& scheme, TTagsRef tags)
            : Tags(tags.begin(), tags.end())
            , Types_(tags.size())
        {
            Tag2Pos.reserve(tags.size());
            CellDefaults_.reserve(tags.size());

            for (TPos on = 0; on < tags.size(); on++) {
                const auto *info = scheme.ColInfo(tags[on]);
                Y_ABORT_UNLESS(info, "Column %" PRIu32 " does not exist", tags[on]);
                Types_[on] = info->TypeInfo;

                CellDefaults_.emplace_back((*scheme.RowCellDefaults)[info->Pos]);

                if (info->IsKey())
                    KeyPins_.push_back({ on, info->Key });

                if (!Tag2Pos.insert(std::make_pair(tags[on], on)).second)
                    Y_ABORT("Duplicated tag found in remap, isn't allowed");
            }
        }

        TArrayRef<const NScheme::TTypeInfo> Types() const noexcept
        {
            return Types_;
        }

        TPos Size() const
        {
            return Tags.size();
        }

        TArrayRef<const TPin> KeyPins() const noexcept
        {
            return KeyPins_;
        }

        TArrayRef<const TCell> CellDefaults() const noexcept
        {
            return CellDefaults_;
        }

        THas Has(TTag tag) const noexcept
        {
            const auto it = Tag2Pos.find(tag);

            return { it == Tag2Pos.end() ? Max<TPos>() : it->second };
        }

    public:
        const TSmallVec<TTag> Tags;
    private:
        TSmallVec<TPin> KeyPins_;
        TSmallVec<NScheme::TTypeInfo> Types_;
        TSmallVec<TCell> CellDefaults_;
        THashMap<TTag, TPos> Tag2Pos;
    };

}}
