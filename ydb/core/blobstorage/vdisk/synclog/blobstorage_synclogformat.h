#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

#include <util/generic/deque.h>
#include <util/generic/queue.h>
#include <util/generic/algorithm.h>
#include <util/string/printf.h>

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOG FORMAT
        ////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 1)
        struct TLogoBlobRec {
            ui64 Raw[3]; // TLogoBlobID
            TIngress Ingress;

            explicit TLogoBlobRec(const TLogoBlobID &id, ui64 ingressRaw)
                :  Ingress(ingressRaw)
            {
                const ui64 *r = id.GetRaw();
                Raw[0] = r[0];
                Raw[1] = r[1];
                Raw[2] = r[2];
            }

            TLogoBlobID LogoBlobID() const {
                return TLogoBlobID(Raw);
            }

            TString ToString() const {
                return Sprintf("[%s %" PRIu64 "]", LogoBlobID().ToString().data(), Ingress.Raw());
            }
        };

        struct TBlockRec {
            ui64 TabletId;
            ui32 Generation;

            TBlockRec(ui64 tabletId = 0, ui32 gen = 0)
                : TabletId(tabletId)
                , Generation(gen)
            {}

            TString ToString() const {
                return Sprintf("[TabletId# %" PRIu64 " Generation# %" PRIu32 "]", TabletId, Generation);
            }
        };

        struct TBlockRecV2 {
            ui64 TabletId;
            ui32 Generation;
            ui64 IssuerGuid;

            TBlockRecV2(ui64 tabletId = 0, ui32 generation = 0, ui64 issuerGuid = 0)
                : TabletId(tabletId)
                , Generation(generation)
                , IssuerGuid(issuerGuid)
            {}

            TString ToString() const {
                return TStringBuilder() << "[TabletId# " << TabletId << " Generation# " << Generation
                     << " IssuerGuid# " << IssuerGuid << "]";
            }
        };

        struct TBarrierRec {
            ui64 TabletId;
            ui32 Channel : 8;
            ui32 Reserved : 23;
            ui32 Hard : 1;
            ui32 Gen;
            ui32 GenCounter;
            ui32 CollectGeneration;
            ui32 CollectStep;
            TBarrierIngress Ingress;

            TBarrierRec(ui64 tabletId = 0, ui32 channel = 0, ui32 gen = 0, ui32 genCounter = 0, ui32 collGen = 0,
                        ui32 collStep = 0, bool hard = false, ui64 ingressRaw = 0)
                : TabletId(tabletId)
                , Channel(channel)
                , Reserved(0)
                , Hard(hard)
                , Gen(gen)
                , GenCounter(genCounter)
                , CollectGeneration(collGen)
                , CollectStep(collStep)
                , Ingress(ingressRaw)
            {}

            TString ToString() const {
                return Sprintf("[TabletId# %" PRIu64 " Channel# %" PRIu32 " %s]", TabletId, Channel, Hard ? "hard" : "soft");
            }
        };

        struct TRecordHdr {
            enum ESyncLogRecType {
                RecLogoBlob,
                RecBlock,
                RecBarrier,
                RecBlockV2,
            };

            ui64 RecType : 2;
            ui64 Lsn : 62;

            TRecordHdr *Next() {
                return (TRecordHdr*)(((char*)this) + GetSize());
            }

            const TRecordHdr *Next() const {
                return (const TRecordHdr*)(((const char*)this) + GetSize());
            }

            size_t GetSize() const {
                size_t payloadSize = 0;
                switch (RecType) {
                    case RecLogoBlob:   payloadSize = sizeof(TLogoBlobRec); break;
                    case RecBlock:      payloadSize = sizeof(TBlockRec); break;
                    case RecBarrier:    payloadSize = sizeof(TBarrierRec); break;
                    case RecBlockV2:    payloadSize = sizeof(TBlockRecV2); break;
                    default: Y_ABORT("Unsupported type: RecType=%" PRIu64 " Lsn=%" PRIu64, (ui64)RecType, Lsn);
                }
                return sizeof(*this) + payloadSize;
            }

            static const char *RecTypeToStr(unsigned recType) {
                switch (recType) {
                    case RecLogoBlob:   return "LogoBlob";
                    case RecBlock:      return "Block";
                    case RecBarrier:    return "Barrier";
                    case RecBlockV2:    return "Block";
                    default:            return "Unknown";
                }
            }

            const TLogoBlobRec *GetLogoBlob() const {
                Y_DEBUG_ABORT_UNLESS(RecType == RecLogoBlob);
                return (const TLogoBlobRec *)(this + 1);
            }

            const TBlockRec *GetBlock() const {
                Y_DEBUG_ABORT_UNLESS(RecType == RecBlock);
                return (const TBlockRec *)(this + 1);
            }

            const TBarrierRec *GetBarrier() const {
                Y_DEBUG_ABORT_UNLESS(RecType == RecBarrier);
                return (const TBarrierRec *)(this + 1);
            }

            const TBlockRecV2 *GetBlockV2() const {
                Y_DEBUG_ABORT_UNLESS(RecType == RecBlockV2);
                return (const TBlockRecV2 *)(this + 1);
            }

            TString ToString() const {
                return Sprintf("{Lsn# %" PRIu64 " Rec# %s %s}", Lsn, RecTypeToStr(RecType), ValueToString().data());
            }

            TString ValueToString() const {
                switch (RecType) {
                    case RecLogoBlob:   return GetLogoBlob()->ToString();
                    case RecBlock:      return GetBlock()->ToString();
                    case RecBarrier:    return GetBarrier()->ToString();
                    case RecBlockV2:    return GetBlockV2()->ToString();
                    default: Y_ABORT("Unsupported type: RecType=%" PRIu64 " Lsn=%" PRIu64, (ui64)RecType, Lsn);
                }
            }
        };
#pragma pack(pop)

        namespace {
            template<typename T> constexpr
            inline T const& ConstexprMax(T const& a, T const& b) {
                return a > b ? a : b;
            }
        }

        static constexpr ui32 MaxRecPayloadSize = ConstexprMax(sizeof(TLogoBlobRec),
                                                               ConstexprMax(sizeof(TBlockRec), sizeof(TBarrierRec)));
        static const ui32 MaxRecFullSize = sizeof(TRecordHdr) + MaxRecPayloadSize;


        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOG SERIALIZATION ROUTINES
        ////////////////////////////////////////////////////////////////////////////
        struct TSerializeRoutines {
            static ui32 SetLogoBlob(const TBlobStorageGroupType &gtype,
                                    char *buf,
                                    ui64 lsn,
                                    const TLogoBlobID &id,
                                    const TIngress &ingress);
            static ui32 SetBlock(char *buf, ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid);
            static ui32 SetBarrier(char *buf,
                                   ui64 lsn,
                                   ui64 tabletId,
                                   ui32 channel,
                                   ui32 gen,
                                   ui32 genCounter,
                                   ui32 collGen,
                                   ui32 collStep,
                                   bool hard,
                                   const TBarrierIngress &ingress);
            static ui32 SetGC(const TBlobStorageGroupType &gtype,
                              char *buf,
                              ui64 lsn,
                              ui64 lastLsnOfIndexRecord,
                              const NKikimrBlobStorage::TEvVCollectGarbage &record,
                              const TBarrierIngress &ingress);
            static ui32 SetGC(const TBlobStorageGroupType &gtype,
                              char *buf,
                              ui64 lsn,
                              const TDeque<TLogoBlobID>& phantoms);
            static bool CheckData(const TString &data, TString &errorString);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSequenceOfRecs
        ////////////////////////////////////////////////////////////////////////////
        class TSequenceOfRecs {
        public:
            TSequenceOfRecs() = default;
            void SetLogoBlob(const TBlobStorageGroupType &gtype,
                             ui64 lsn,
                             const TLogoBlobID &id,
                             const TIngress &ingress);
            void SetBlock(ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid);
            void SetBarrier(ui64 lsn,
                            ui64 tabletId,
                            ui32 channel,
                            ui32 gen,
                            ui32 genCounter,
                            ui32 collGen,
                            ui32 collStep,
                            bool hard,
                            const TBarrierIngress &ingress);
            void SetGC(const TBlobStorageGroupType &gtype,
                       ui64 lsn,
                       ui64 lastLsnOfIndexRecord,
                       const NKikimrBlobStorage::TEvVCollectGarbage &record,
                       const TBarrierIngress &ingress);
            void SetGC(const TBlobStorageGroupType &gtype,
                       ui64 lsn,
                       const TDeque<TLogoBlobID>& phantoms);
            void Output(IOutputStream &str) const;
            TString ToString() const;

            const char *GetData() const {
                return HeapBuf.empty() ? Buf : &(HeapBuf[0]);
            }

            // size in bytes
            ui32 GetSize() const {
                return Size;
            }

            class TIterator;

        private:
            char Buf[NSyncLog::MaxRecFullSize];
            // Size in bytes
            ui32 Size = 0;
            TVector<char> HeapBuf;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSequenceOfRecs
        ////////////////////////////////////////////////////////////////////////////
        class TSequenceOfRecs::TIterator {
        public:
            TIterator(const TSequenceOfRecs *recs)
                : Recs(recs)
            {}

            void SeekToFirst() {
                It = (const TRecordHdr *)Recs->GetData();
            }

            bool Valid() const {
                return Recs && It && ((const char *)It < (Recs->GetData() + Recs->GetSize()));
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                It = It->Next();
            }

            const TRecordHdr *Get() const {
                return It;
            }

        private:
            const TSequenceOfRecs *Recs = nullptr;
            const TRecordHdr *It = nullptr;
        };

    } // NSyncLog

} // NKikimr
