#include "blobstorage_synclogformat.h"

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOG SERIALIZATION ROUTINES
        ////////////////////////////////////////////////////////////////////////////
        // LogoBlob
        ui32 TSerializeRoutines::SetLogoBlob(const TBlobStorageGroupType &gtype,
                                             char *buf,
                                             ui64 lsn,
                                             const TLogoBlobID &id,
                                             const TIngress &ingress) {
            //Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
            TRecordHdr *hdr = (TRecordHdr *)buf;
            TLogoBlobRec *rec = (TLogoBlobRec *)(hdr + 1);
            hdr->RecType = TRecordHdr::RecLogoBlob;
            hdr->Lsn = lsn;
            const ui64 *raw = id.GetRaw();
            rec->Raw[0] = raw[0];
            rec->Raw[1] = raw[1];
            rec->Raw[2] = raw[2];
            rec->Ingress = ingress.CopyWithoutLocal(gtype);

            return sizeof(TRecordHdr) + sizeof(TLogoBlobRec);
        }

        // Block
        ui32 TSerializeRoutines::SetBlock(char *buf, ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid) {
            if (issuerGuid) {
                TRecordHdr *hdr = (TRecordHdr *)buf;
                hdr->RecType = TRecordHdr::RecBlockV2;
                hdr->Lsn = lsn;
                new(hdr + 1) TBlockRecV2(tabletId, gen, issuerGuid);
                return sizeof(TRecordHdr) + sizeof(TBlockRecV2);
            } else {
                TRecordHdr *hdr = (TRecordHdr *)buf;
                TBlockRec *rec = (TBlockRec *)(hdr + 1);
                hdr->RecType = TRecordHdr::RecBlock;
                hdr->Lsn = lsn;
                rec->TabletId = tabletId;
                rec->Generation = gen;
                return sizeof(TRecordHdr) + sizeof(TBlockRec);
            }
        }

        // Barrier
        ui32 TSerializeRoutines::SetBarrier(char *buf,
                                            ui64 lsn,
                                            ui64 tabletId,
                                            ui32 channel,
                                            ui32 gen,
                                            ui32 genCounter,
                                            ui32 collGen,
                                            ui32 collStep,
                                            bool hard,
                                            const TBarrierIngress &ingress) {
            TRecordHdr *hdr = (TRecordHdr *)buf;
            TBarrierRec *rec = (TBarrierRec *)(hdr + 1);
            hdr->RecType = TRecordHdr::RecBarrier;
            hdr->Lsn = lsn;
            rec->TabletId = tabletId;
            rec->Channel = channel;
            rec->Reserved = 0;
            rec->Gen = gen;
            rec->GenCounter = genCounter;
            rec->CollectGeneration = collGen;
            rec->CollectStep = collStep;
            rec->Hard = hard;
            rec->Ingress = ingress;
            return sizeof(TRecordHdr) + sizeof(TBarrierRec);
        }

        // GC
        // lastLsnOfIndexRecord -- don't apply records with lsns <= lastLsnOfIndexRecord
        ui32 TSerializeRoutines::SetGC(const TBlobStorageGroupType &gtype,
                                       char *buf,
                                       ui64 lsn,
                                       ui64 lastLsnOfIndexRecord,
                                       const NKikimrBlobStorage::TEvVCollectGarbage &record,
                                       const TBarrierIngress &ingress) {
            const ui64 tabletId = record.GetTabletId();
            const ui32 channel = record.GetChannel();

            const bool collect = record.HasCollectGeneration();
            const ui32 gen = record.GetRecordGeneration();
            const ui32 genCounter = record.GetPerGenerationCounter();
            const ui32 collectGeneration = record.GetCollectGeneration();
            const ui32 collectStep = record.GetCollectStep();
            const bool hard = record.GetHard();

            ui64 lsnAdvance = !!collect + record.KeepSize() + record.DoNotKeepSize();
            ui64 lsnPos = lsn - lsnAdvance + 1;
            char *pos = buf;

            // set up keep bits
            TIngress ingressKeep;
            ingressKeep.SetKeep(TIngress::IngressMode(gtype), CollectModeKeep);
            for (ui32 i = 0; i < record.KeepSize(); ++i, ++lsnPos) {
                if (lsnPos > lastLsnOfIndexRecord) {
                    TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetKeep(i));
                    Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
                    ui32 size = SetLogoBlob(gtype, pos, lsnPos, id, ingressKeep);
                    pos += size;
                }
            }

            // set up do not keep bits
            TIngress ingressDontKeep;
            ingressDontKeep.SetKeep(TIngress::IngressMode(gtype), CollectModeDoNotKeep);
            for (ui32 i = 0; i < record.DoNotKeepSize(); ++i, ++lsnPos) {
                if (lsnPos > lastLsnOfIndexRecord) {
                    TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetDoNotKeep(i));
                    Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
                    ui32 size = SetLogoBlob(gtype, pos, lsnPos, id, ingressDontKeep);
                    pos += size;
                }
            }

            // set up barrier
            if (collect && (lsnPos > lastLsnOfIndexRecord)) {
                ui32 size = SetBarrier(pos, lsnPos++, tabletId, channel, gen,
                                       genCounter, collectGeneration,
                                       collectStep, hard, ingress);
                pos += size;
            }

            return pos - buf;
        }

        ui32 TSerializeRoutines::SetGC(const TBlobStorageGroupType &gtype,
                                       char *buf,
                                       ui64 lsn,
                                       const TDeque<TLogoBlobID>& phantoms) {
            char *pos = buf;

            TIngress ingressDontKeep;
            ingressDontKeep.SetKeep(TIngress::IngressMode(gtype), CollectModeDoNotKeep);
            for (const TLogoBlobID& id : phantoms) {
                pos += SetLogoBlob(gtype, pos, lsn++, id, ingressDontKeep);
            }

            return pos - buf;
        }

        bool TSerializeRoutines::CheckData(const TString &data, TString &errorString) {
            const TRecordHdr *begin = (const TRecordHdr *)(data.data());
            const TRecordHdr *end = (const TRecordHdr *)(data.data() + data.size());

            for (const TRecordHdr *it = begin; it < end; it = it->Next()) {
                switch (it->RecType) {
                    case TRecordHdr::RecLogoBlob:
                    case TRecordHdr::RecBlock:
                    case TRecordHdr::RecBarrier:
                    case TRecordHdr::RecBlockV2:
                        break;
                    default: {
                        TStringStream str;
                        str << "Unknown RecType; it# " << it->ToString() << " dataSize# " << data.size();
                        errorString = str.Str();
                        return false;
                    }
                }
            }

            return true;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TSequenceOfRecs
        ////////////////////////////////////////////////////////////////////////////
        void TSequenceOfRecs::SetLogoBlob(const TBlobStorageGroupType &gtype,
                                          ui64 lsn,
                                          const TLogoBlobID &id,
                                          const TIngress &ingress) {
            Y_ABORT_UNLESS(Size == 0 && id.PartId() == 0);
            Size = TSerializeRoutines::SetLogoBlob(gtype, Buf, lsn, id, ingress);
        }

        void TSequenceOfRecs::SetBlock(ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid) {
            Y_ABORT_UNLESS(Size == 0);
            Size = TSerializeRoutines::SetBlock(Buf, lsn, tabletId, gen, issuerGuid);
        }

        void TSequenceOfRecs::SetBarrier(ui64 lsn,
                                         ui64 tabletId,
                                         ui32 channel,
                                         ui32 gen,
                                         ui32 genCounter,
                                         ui32 collGen,
                                         ui32 collStep,
                                         bool hard,
                                         const TBarrierIngress &ingress) {
            Y_ABORT_UNLESS(Size == 0);
            Size = TSerializeRoutines::SetBarrier(Buf, lsn, tabletId, channel, gen, genCounter,
                                                  collGen, collStep, hard, ingress);
        }

        void TSequenceOfRecs::SetGC(const TBlobStorageGroupType &gtype,
                                    ui64 lsn,
                                    ui64 lastLsnOfIndexRecord,
                                    const NKikimrBlobStorage::TEvVCollectGarbage &record,
                                    const TBarrierIngress &ingress) {
            Y_ABORT_UNLESS(Size == 0);
            const bool collect = record.HasCollectGeneration();
            ui32 vecItems = !!collect + record.KeepSize() + record.DoNotKeepSize();
            ui32 vecSize = vecItems * NSyncLog::MaxRecFullSize;
            HeapBuf.resize(vecSize);
            Size = NSyncLog::TSerializeRoutines::SetGC(gtype, &(HeapBuf[0]), lsn,
                                                       lastLsnOfIndexRecord, record, ingress);
        }

        void TSequenceOfRecs::SetGC(const TBlobStorageGroupType &gtype,
                                    ui64 lsn,
                                    const TDeque<TLogoBlobID>& phantoms) {
            Y_ABORT_UNLESS(Size == 0);
            size_t size = NSyncLog::MaxRecFullSize * phantoms.size();
            HeapBuf.resize(size);
            Size = NSyncLog::TSerializeRoutines::SetGC(gtype, HeapBuf.data(), lsn, phantoms);
        }

        void TSequenceOfRecs::Output(IOutputStream &str) const {
            TIterator it(this);
            it.SeekToFirst();
            while (it.Valid()) {
                const TRecordHdr *hdr = it.Get();
                str << hdr->ToString();
                it.Next();
            }
        }

        TString TSequenceOfRecs::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }


    } // NSyncLog

} // NKikimr
