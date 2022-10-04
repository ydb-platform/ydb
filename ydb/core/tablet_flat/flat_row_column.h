#pragma once

#include "flat_row_eggs.h"
#include <ydb/core/scheme_types/scheme_type_info.h>

namespace NKikimr {
namespace NTable {

    struct TColInfo {
        struct TByTag {
            bool operator()(const TColInfo &le, const TColInfo &ri) const
            {
                return le.Tag < ri.Tag;
            }

            bool operator()(const TColInfo *le, const TColInfo *ri) const
            {
                return le->Tag < ri->Tag;
            }
        };

        struct TByKey {
            bool operator()(const TColInfo &le, const TColInfo &ri) const
            {
                return le.Key < ri.Key;
            }
        };

        struct TByPos {
            bool operator()(const TColInfo& a, const TColInfo& b) const
            {
                return a.Pos < b.Pos;
            }
        };

        TColInfo() = default;

        TColInfo(const TColInfo&) = default;
        TColInfo &operator=(const TColInfo& other) = default;

        bool IsKey() const noexcept {
            return Key != Max<TPos>();
        }

        NScheme::TTypeInfo TypeInfo;
        TTag Tag = Max<TTag>();
        TPos Pos = Max<TPos>(); /* Position in physical layout */
        TPos Key = Max<TPos>(); /* key column sequence number */
        TGroup Group = 0; /* Column group */
    };
}
}
