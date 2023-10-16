#pragma once

#include "flat_row_column.h"
#include "flat_row_nulls.h"
#include "flat_table_column.h"

#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NTable {

    class TRowScheme : public TAtomicRefCount<TRowScheme> {
        template<class TNullsType, class TType>
        struct TNullsCook {
            TNullsCook(ui32 slots)
                : Types(slots)
                , Cells(slots)
            {

            }

            void Set(ui32 idx, TType type, const TCell &cell)
            {
                Types[idx] = type;
                Cells[idx] = cell;
            }

            TIntrusiveConstPtr<TNullsType> operator*() const noexcept
            {
                return TNullsType::Make(Types, Cells);
            }

            TVector<TType> Types;
            TVector<TCell> Cells;
        };

        TRowScheme(
                TVector<TColInfo> cols,
                TIntrusiveConstPtr<TKeyCellDefaults> keys,
                TIntrusiveConstPtr<TRowCellDefaults> rowDefaults,
                TVector<ui32> families)
            : Cols(std::move(cols))
            , Keys(std::move(keys))
            , RowCellDefaults(std::move(rowDefaults))
            , Families(std::move(families))
        {
            for (const auto &col: Cols)
                ByTag.emplace(col.Tag, col.Pos);
        }

    public:
        template<typename TGet, typename TSeq>
        static TIntrusiveConstPtr<TRowScheme> Make(const TSeq &cols_, TGet)
        {
            size_t keyCount = 0;
            TSet<ui32> familySet;
            TMap<ui32, const TColumn*> cols; /* order by tag */

            for (auto &it : cols_) {
                auto &meta = TGet::Do(it);
                familySet.insert(meta.Family);
                cols[meta.Id] = &meta;
                keyCount += (meta.KeyOrder == Max<TPos>() ? 0 : 1);
            }

            TNullsCook<TKeyCellDefaults, NScheme::TTypeInfoOrder> keys(keyCount);
            TNullsCook<TRowCellDefaults, NScheme::TTypeInfo> vals(cols.size());

            TVector<TColInfo> info;
            info.reserve(cols.size());

            TVector<ui32> families(familySet.begin(), familySet.end());

            for (auto &it: cols) {
                auto &meta = *it.second;
                auto &col = *info.emplace(info.end());

                auto familyIt = std::lower_bound(families.begin(), families.end(), meta.Family);
                Y_ABORT_UNLESS(familyIt != families.end() && *familyIt == meta.Family);

                col.Tag = meta.Id;
                col.TypeInfo = meta.PType;
                col.Key = meta.KeyOrder;
                col.Pos = info.size() - 1;
                col.Group = familyIt - families.begin();

                vals.Set(col.Pos, col.TypeInfo, meta.Null);

                if (col.IsKey())
                    keys.Set(col.Key, col.TypeInfo, meta.Null);
            }

            return new TRowScheme(std::move(info), *keys, *vals, std::move(families));
        }

        static bool HasTag(TArrayRef<const ui32> array, ui32 tag) noexcept
        {
            return std::binary_search(array.begin(), array.end(), tag);
        }

        const NTable::TColInfo* ColInfo(TTag tag) const
        {
            auto ci = ByTag.find(tag);

            return ci == ByTag.end() ? nullptr : &Cols[ci->second];
        }

        TVector<ui32> Tags(bool keysOnly = false) const noexcept
        {
            TVector<ui32> tags; /* ordered by value tags */

            for (auto &col: Cols)
                if (!keysOnly || col.IsKey())
                    tags.push_back(col.Tag);

            return tags;
        }

        void CheckCompatability(const TRowScheme &scheme) const
        {
            for (auto &col: Cols) {
                auto *other = scheme.ColInfo(col.Tag);

                if (other == nullptr && col.IsKey()) {
                    Y_ABORT("Key column dropping ins't supported");
                } else if (other == nullptr) {
                    /* It is ok to drop non-key columns */
                } else if (col.TypeInfo != other->TypeInfo) {
                    Y_ABORT("Column type alteration is not supproted");
                } else if (col.Key != other->Key) {
                    Y_ABORT("Cannot alter keys order or move col to keys");

                    /* Existing string columns can't be altered to keys as
                        they may hold external blobs references which is not
                        supported for keys. Part iterators itself can extend
                        keys only with default values but not by column with
                        data.
                     */

                } else {
                    auto &null = (*scheme.RowCellDefaults)[other->Pos];
                    if (CompareTypedCells(null, (*RowCellDefaults)[col.Pos], col.TypeInfo))
                        Y_ABORT("Cannot alter existing columnt default value");
                }
            }
        }

    public:
        const TVector<TColInfo> Cols;
        const TIntrusiveConstPtr<TKeyCellDefaults> Keys;
        const TIntrusiveConstPtr<TRowCellDefaults> RowCellDefaults;
        const TVector<ui32> Families; // per-group families

    private:
        THashMap<TTag, NTable::TPos> ByTag;
    };

}}
