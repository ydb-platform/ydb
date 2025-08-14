#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    struct TFolderEntry {
        static constexpr const char* Folder = "Folder";
        static constexpr const char* Table = "Table";

        static THashSet<TString> KnownTypes;

        TString Type;
        TString Name;

        friend bool operator==(const TFolderEntry& lhs, const TFolderEntry& rhs) = default;
    };

    struct TListFilter {
        TMaybe<THashSet<TString>> Types;
        bool IsUnknownAllowed = false;
    };

    struct TListRequest {
        TString Cluster;

        // `Path` structure is defined by a `System`.
        // Can end with a folder entry name hint.
        // For example, `/local/exa` lists a folder `/local`,
        // but can rank and filter entries by a hint `exa`.
        TString Path;

        TListFilter Filter;
        size_t Limit = 128;
    };

    struct TListResponse {
        size_t NameHintLength = 0;
        TVector<TFolderEntry> Entries;
    };

    struct TDescribeTableRequest {
        TString TableCluster;
        TString TablePath;
        TString ColumnPrefix;
        size_t ColumnsLimit = 128; // TODO: introduce default limit constant
    };

    struct TDescribeTableResponse {
        bool IsExisting = false;
        TVector<TString> Columns;
    };

    class ISchema: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ISchema>;

        virtual NThreading::TFuture<TListResponse> List(const TListRequest& request) const = 0;

        virtual NThreading::TFuture<TDescribeTableResponse>
        Describe(const TDescribeTableRequest& request) const = 0;
    };

} // namespace NSQLComplete
