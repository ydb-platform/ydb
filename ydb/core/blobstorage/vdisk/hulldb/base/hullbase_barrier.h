#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/library/actors/util/named_tuple.h>

namespace NKikimr {

    // Data types for barrier database

    /////////////////////////////////////////////////////////////////////////
    // TKeyBarrier
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TKeyBarrier
        : public TNamedTupleBase<TKeyBarrier>
    {
        ui64 TabletId;
        ui32 Channel : 8;
        ui32 Reserved : 23;
        ui32 Hard : 1;
        ui32 Gen;           // generation of tablet the command was issued
        ui32 GenCounter;    // sequential number of command during tablet lifetime

        static const char *Name() {
            return "Barriers";
        }

        TKeyBarrier()
            : TabletId(0)
            , Channel(0)
            , Reserved(0)
            , Hard(0)
            , Gen(0)
            , GenCounter(0)
        {}

        TKeyBarrier(ui64 tabletId)
            : TabletId(tabletId)
            , Channel(0)
            , Reserved(0)
            , Hard(0)
            , Gen(0)
            , GenCounter(0)
        {}

        TKeyBarrier(ui64 tabletId, ui32 channel, ui32 gen, ui32 genCounter, bool hard)
            : TabletId(tabletId)
            , Channel(channel)
            , Reserved(0)
            , Hard(hard)
            , Gen(gen)
            , GenCounter(genCounter)
        {}

        TKeyBarrier(const NKikimrBlobStorage::TBarrierKey &proto)
            : TabletId(proto.GetTabletId())
            , Channel(proto.GetChannel())
            , Reserved(0)
            , Hard(proto.GetHard())
            , Gen(proto.GetRecordGeneration())
            , GenCounter(proto.GetPerGenerationCounter())
        {}

        TString ToString() const {
            return Sprintf("[%" PRIu64 " %" PRIu32 " %" PRIu32 " %" PRIu32 " %s]",
                TabletId, Channel, Gen, GenCounter, Hard ? "hard" : "soft");
        }

        TLogoBlobID LogoBlobID() const {
            return TLogoBlobID();
        }


        void Serialize(NKikimrBlobStorage::TBarrierKey &proto) const {
            proto.SetTabletId(TabletId);
            proto.SetChannel(Channel);
            proto.SetRecordGeneration(Gen);
            proto.SetPerGenerationCounter(GenCounter);
            proto.SetHard(Hard);
        }

        static TKeyBarrier First() {
            return TKeyBarrier();
        }

        bool IsSameAs(const TKeyBarrier& other) const {
            return TabletId == other.TabletId
                && Channel == other.Channel
                && Gen == other.Gen
                && GenCounter == other.GenCounter
                && Hard == other.Hard;
        }

        static TKeyBarrier Inf() {
            return TKeyBarrier(Max<ui64>(), Max<ui32>(), Max<ui32>(), Max<ui32>(), true);
        }

        static bool Parse(TKeyBarrier &out, const TString &buf, TString &errorExplanation);

        auto ConvertToTuple() const {
            ui64 alignedTabletId = ReadUnaligned<ui64>(&TabletId);
            return std::make_tuple(alignedTabletId, Channel, Hard, Gen, GenCounter);
        }
    };
#pragma pack(pop)

    /////////////////////////////////////////////////////////////////////////
    // PDiskSignatureForHullDbKey
    /////////////////////////////////////////////////////////////////////////
    template <>
    inline TLogSignature PDiskSignatureForHullDbKey<TKeyBarrier>() {
        return TLogSignature::SignatureHullBarriersDB;
    }

    /////////////////////////////////////////////////////////////////////////
    // TMemRecBarrier
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TMemRecBarrier {
        ui32 CollectGen;
        ui32 CollectStep;
        TBarrierIngress Ingress;

        TMemRecBarrier()
            : CollectGen(0)
            , CollectStep(0)
            , Ingress()
        {}

        TMemRecBarrier(ui32 collectGen, ui32 collectStep, const TBarrierIngress &ingress)
            : CollectGen(collectGen)
            , CollectStep(collectStep)
            , Ingress(ingress)
        {}

        void Merge(const TMemRecBarrier& rec, const TKeyBarrier& key) {
            Y_ABORT_UNLESS(CollectGen == rec.CollectGen && CollectStep == rec.CollectStep,
                   "Barriers MUST be equal; CollectGen# %" PRIu32 " CollectStep# %" PRIu32
                   " rec.CollectGen# %" PRIu32 " rec.CollectStep %" PRIu32
                   " key# %s", CollectGen, CollectStep, rec.CollectGen, rec.CollectStep,
                   key.ToString().data());
            TBarrierIngress::Merge(Ingress, rec.Ingress);
        }

        ui32 DataSize() const {
            return 0;
        }

        bool HasData() const {
            return false;
        }

        void SetDiskBlob(const TDiskPart &dataAddr) {
            Y_UNUSED(dataAddr);
            // nothing to do
        }

        void SetHugeBlob(const TDiskPart &) {
            Y_ABORT("Must not be called");
        }

        void SetManyHugeBlobs(ui32, ui32, ui32) {
            Y_ABORT("Must not be called");
        }

        void SetMemBlob(ui64, ui32) {
            Y_ABORT("Must not be called");
        }

        void SetNoBlob() {
        }

        void SetType(TBlobType::EType t) {
            Y_DEBUG_ABORT_UNLESS(t == TBlobType::DiskBlob);
            Y_UNUSED(t);
        }

        TDiskDataExtractor *GetDiskData(TDiskDataExtractor *extr, const TDiskPart *) const {
            extr->Clear();
            return extr;
        }

        TMemPart GetMemData() const {
            Y_ABORT("Must not be called");
        }

        NMatrix::TVectorType GetLocalParts(TBlobStorageGroupType) const {
            return NMatrix::TVectorType();
        }

        void ClearLocalParts(TBlobStorageGroupType)
        {}

        TBlobType::EType GetType() const {
            return TBlobType::DiskBlob;
        }

        TString ToString(const TIngressCache *cache, const TDiskPart *outbound = nullptr) const {
            Y_UNUSED(outbound);

            return Sprintf("{CollectGen: %" PRIu32 " CollectStep: %" PRIu32 " Ingress: %s}",
                           CollectGen, CollectStep, Ingress.ToString(cache).data());
        }

        void Serialize(NKikimrBlobStorage::TBarrierVal &proto, bool showInternals) const {
            proto.SetCollectGen(CollectGen);
            proto.SetCollectStep(CollectStep);
            if (showInternals)
                proto.SetIngress(Ingress.Raw());
        }
    };
#pragma pack(pop)

    template <>
    inline EHullDbType TKeyToEHullDbType<TKeyBarrier>() {
        return EHullDbType::Barriers;
    }

} // NKikimr

Y_DECLARE_PODTYPE(NKikimr::TKeyBarrier);
Y_DECLARE_PODTYPE(NKikimr::TMemRecBarrier);
