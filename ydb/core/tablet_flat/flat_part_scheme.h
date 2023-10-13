#pragma once

#include "util_basics.h"
#include "flat_page_iface.h"
#include "flat_row_eggs.h"
#include "flat_row_column.h"
#include "flat_row_nulls.h"
#include "flat_part_pinout.h"

#include <ydb/core/base/defs.h>

#include <util/generic/ptr.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NTable {

    using TPgSize = NPage::TSize;

    class TPartScheme: public TAtomicRefCount<TPartScheme> {
    public:
        struct TColumn : public TColInfo {
            TColumn() = default;

            TColumn(const TColInfo &info)
                : TColInfo(info)
            {

            }

            TPgSize Offset = 0;
            TPgSize FixedSize = 0;
            bool IsFixed = true;
        };

        struct TGroupInfo {
            /* Data page layout settings */
            TPgSize FixedSize;

            TVector<TColumn> Columns;

            /* Data page key layout */
            TVector<TColumn> ColsKeyData;
            TVector<NScheme::TTypeInfo> KeyTypes;

            /* Index page layout settings */
            TPgSize IdxRecFixedSize;
            TVector<TColumn> ColsKeyIdx;
        };

        TPartScheme(const TPartScheme &scheme) = delete;
        explicit TPartScheme(TArrayRef<const TColInfo> cols);
        static TIntrusiveConstPtr<TPartScheme> Parse(TArrayRef<const char>, bool labeled);

        /**
         * Makes a sorted pin mapping for non-key columns
         */
        TPinout MakePinout(const TTagsRef tags, ui32 group = Max<ui32>()) const
        {
            TVector<TPinout::TPin> pins;
            TVector<ui32> groups;

            if (tags) {
                pins.reserve(tags.size());
                groups.reserve(tags.size());

                for (size_t on = 0; on < tags.size(); on++) {
                    const TColumn* col = FindColumnByTag(tags[on]);

                    if (col &&
                        !col->IsKey() &&
                        (group == Max<ui32>() || group == col->Group))
                    {
                        pins.emplace_back(col->Pos, ui32(on));
                        if (col->Group != 0) {
                            groups.push_back(col->Group);
                        }
                    }
                }

                auto byGroupFirst = [this](const TPinout::TPin& a, const TPinout::TPin& b) -> bool {
                    auto aGroup = AllColumns[a.From].Group;
                    auto bGroup = AllColumns[b.From].Group;
                    return aGroup < bGroup || (aGroup == bGroup && a.From < b.From);
                };

                std::sort(pins.begin(), pins.end(), byGroupFirst);

                std::sort(groups.begin(), groups.end());
                groups.erase(std::unique(groups.begin(), groups.end()), groups.end());
            }

            return TPinout(std::move(pins), std::move(groups));
        }

        const TColumn* FindColumnByTag(TTag tag) const
        {
            auto it = Tag2DataInfo.find(tag);
            if (it != Tag2DataInfo.end()) {
                return it->second;
            } else {
                return nullptr;
            }
        }

        const TGroupInfo& GetLayout(NPage::TGroupId groupId) const noexcept
        {
            Y_ABORT_UNLESS(groupId.Index < Groups.size(), "Group is out of range");

            if (groupId.Index == 0) {
                return groupId.Historic ? HistoryGroup : Groups[0];
            } else {
                return Groups[groupId.Index];
            }
        }

        TSharedData Serialize() const;

    private:
        void FillKeySlots();
        void FillHistoricSlots();
        void InitGroup(TGroupInfo& group);
        size_t InitInfo(TVector<TColumn>& cols, TPgSize header);

    public:
        TVector<TGroupInfo> Groups;
        TVector<TColumn> AllColumns;

        TGroupInfo HistoryGroup;
        TVector<TColumn> HistoryColumns;
        TIntrusiveConstPtr<TKeyCellDefaults> HistoryKeys;

    private:
        THashMap<TTag, const TColumn*> Tag2DataInfo;
    };
}
}
