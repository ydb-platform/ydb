#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    struct TFolderEntry {
        TString Type;
        TString Name;

        friend bool operator==(const TFolderEntry& lhs, const TFolderEntry& rhs) = default;
    };

    struct TListFilter {
        TMaybe<THashSet<TString>> Types;
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

    class ISchemaGateway {
    public:
        using TPtr = THolder<ISchemaGateway>;

        virtual ~ISchemaGateway() = default;
        virtual NThreading::TFuture<TListResponse> List(const TListRequest& request) = 0;
    };

} // namespace NSQLComplete
