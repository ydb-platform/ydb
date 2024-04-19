#pragma once

#include <util/generic/string.h>
#include <util/generic/set.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>


namespace NKikimr::NSchemeBoard {

struct TOpaquePathDescription {
    // Serialized TEvDescribeSchemeResult
    TString DescribeSchemeResultSerialized;

    // Explicit values extracted from original TEvDescribeSchemeResult

    //  TEvDescribeSchemeResult.Status, invalid status value by default
    NKikimrScheme::EStatus Status = NKikimrScheme::EStatus_MAX;

    //  TEvDescribeSchemeResult.(PathOwnerId,PathId) or TEvDescribeSchemeResult.PathDescription.Self.(SchemeshardId,PathId) in order of presence
    TPathId PathId;

    //  TEvDescribeSchemeResult.Path
    TString Path;

    //  TEvDescribeSchemeResult.PathDescription.Self.PathVersion
    ui64 PathVersion = 0;

    //  TEvDescribeSchemeResult.PathDescription.DomainDescription.DomainKey
    TPathId SubdomainPathId;

    //  TEvDescribeSchemeResult.PathDescription.AbandonedTenantsSchemeShards
    TSet<ui64> PathAbandonedTenantsSchemeShards;

    bool IsEmpty() const;

    TString ToString() const;
};

struct TTwoPartDescription;

TOpaquePathDescription MakeOpaquePathDescription(const TString& preSerializedPart, const NKikimrScheme::TEvDescribeSchemeResult& protoPart);
TOpaquePathDescription MakeOpaquePathDescription(const TTwoPartDescription& twoPartDescription);

}  // NKikimr::NSchemeBoard
