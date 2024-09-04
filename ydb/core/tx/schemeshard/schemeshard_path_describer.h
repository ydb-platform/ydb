#pragma once

#include "schemeshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchemeShard {

class TPathDescriber {
    void FillPathDescr(NKikimrSchemeOp::TDirEntry* descr, TPathElement::TPtr pathEl,
        TPathElement::EPathSubType subType = TPathElement::EPathSubType::EPathSubTypeEmpty);
    void FillPathDescr(NKikimrSchemeOp::TDirEntry* descr, const TPath& path);
    void FillChildDescr(NKikimrSchemeOp::TDirEntry* descr, TPathElement::TPtr pathEl);
    TPathElement::EPathSubType CalcPathSubType(const TPath& path);

    void BuildEffectiveACL(NKikimrSchemeOp::TDirEntry* descr, const TPath& pathEl);
    void FillLastExistedPrefixDescr(const TPath& path);

    void DescribeChildren(const TPath& path);
    void DescribeDir(const TPath& path);
    void DescribeTable(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeOlapStore(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeColumnTable(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribePersQueueGroup(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeRtmrVolume(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeTableIndex(const TPath& path);
    void DescribeCdcStream(const TPath& path);
    void DescribeSolomonVolume(TPathId pathId, TPathElement::TPtr pathEl, bool returnChannelsBinding);
    void DescribeUserAttributes(TPathElement::TPtr pathEl);
    void DescribePathVersion(const TPath& path);
    void DescribeDomain(TPathElement::TPtr pathEl);
    void DescribeDomainRoot(TPathElement::TPtr pathEl);
    void DescribeDomainExtra(TPathElement::TPtr pathEl);
    void DescribeRevertedMigrations(TPathElement::TPtr pathEl);

    void DescribeBlockStoreVolume(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeFileStore(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeKesus(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeSequence(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeReplication(TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeBlobDepot(const TPath& path);
    void DescribeExternalTable(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeExternalDataSource(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeView(const TActorContext&, TPathId pathId, TPathElement::TPtr pathEl);
    void DescribeResourcePool(TPathId pathId, TPathElement::TPtr pathEl);

public:
    explicit TPathDescriber(TSchemeShard* self, NKikimrSchemeOp::TDescribePath&& params)
        : Self(self)
        , Params(std::move(params))
    {
    }

    const NKikimrSchemeOp::TDescribePath& GetParams() const {
        return Params;
    }

    THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> Describe(const TActorContext& ctx);

private:
    TSchemeShard* Self;
    NKikimrSchemeOp::TDescribePath Params;

    THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> Result;

}; // TPathDescriber

THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> DescribePath(
    TSchemeShard* self,
    const TActorContext& ctx,
    TPathId pathId,
    const NKikimrSchemeOp::TDescribeOptions& opts
);

THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> DescribePath(
    TSchemeShard* self,
    const TActorContext& ctx,
    TPathId pathId
);

} // NSchemeShard
} // NKikimr
