#include "blobstorage_synclogdsk.h"
#include "blobstorage_synclog_private_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/mon.h>


namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TDeltaToDiskRecLog
        ////////////////////////////////////////////////////////////////////////////
        ui32 TDeltaToDiskRecLog::TOneAppend::Serialize(IOutputStream &s,
                                                       ui32 indexBulk) const {
            // serialize chunk idx
            TOneChunk(ChunkIdx).Serialize(s);
            // create index from pages and serialize it
            TOneChunkIndex index(Pages, indexBulk);
            return index.Serialize(s);
        }

        void TDeltaToDiskRecLog::TOneAppend::Output(IOutputStream &str) const {
            str << "{ChunkIdx# " << ChunkIdx
                << " PagesNum# " << Pages.size()
                << " FirstLsn# " << Pages.begin()->GetFirstLsn()
                << " LastLsn# " << Pages.back().GetLastLsn()
                << "}";
        }

        TDeltaToDiskRecLog &TDeltaToDiskRecLog::Append(ui32 chunkIdx,
                                                       const TVector<TSyncLogPageSnap> &pages) {
            Y_ABORT_UNLESS(AllAppends.empty() || AllAppends.back().ChunkIdx != chunkIdx);
            AllAppends.emplace_back(chunkIdx, pages);
            return *this;
        }

        void TDeltaToDiskRecLog::Output(IOutputStream &str) const {
            str << "{AllAppends# ";
            FormatList(str, AllAppends);
            str << "}";
        }

        ////////////////////////////////////////////////////////////////////////////
        // TOneChunk
        ////////////////////////////////////////////////////////////////////////////
        TOneChunk::~TOneChunk() {
            if (Notifier) {
                Notifier->Notify([this] () {return new TEvSyncLogFreeChunk(ChunkIdx);});
            }
        }

        std::pair<TOneChunkPtr, const char *> TOneChunk::Construct(const char *serialized) {
            ui32 chunkIdx = *(const ui32 *)serialized;
            serialized += 4;
            return std::pair<TOneChunkPtr, const char *>(new TOneChunk(chunkIdx), serialized);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TOneChunkIndex
        ////////////////////////////////////////////////////////////////////////////
        void TOneChunkIndex::UpdateIndex(const TVector<TSyncLogPageSnap> &pages, ui32 indexBulk) {
            Y_DEBUG_ABORT_UNLESS(!Index.empty() && !pages.empty());
            TSyncLogPageSnap firstPage = pages.front();
            ui64 firstPageFirstLsn = firstPage.GetFirstLsn();
            ui64 firstPageLastLsn = firstPage.GetLastLsn();

            Y_DEBUG_ABORT_UNLESS(LastRealLsn < firstPageFirstLsn ||
                         (firstPageFirstLsn <= LastRealLsn && LastRealLsn < firstPageLastLsn));
            // save freePagePos before updating index
            ui32 freePagePos = FreePagePos();
            if (firstPageFirstLsn <= LastRealLsn) {
                // remove or update last index rec
                TDiskIndexRecord &lastRec = Index.back();
                Y_DEBUG_ABORT_UNLESS(lastRec.PagesNum >= 1);
                if (lastRec.PagesNum > 1) {
                    lastRec.PagesNum--;
                } else {
                    Index.pop_back();
                }
            }

            // add new pages
            AppendPages(pages, indexBulk, freePagePos);
        }

        void TOneChunkIndex::AppendPages(const TVector<TSyncLogPageSnap> &pages,
                                         ui32 indexBulk,
                                         ui32 freePagePos) {
            ui32 s = pages.size();
            Y_DEBUG_ABORT_UNLESS(s > 0);
            LastRealLsn = pages[s - 1].GetLastLsn();
            for (ui32 i = 0; i < s; i += indexBulk) {
                ui64 firstLsn = pages[i].GetFirstLsn();
                ui32 pagesNum = Min(s - i, indexBulk);
                TDiskIndexRecord rec(firstLsn, freePagePos + i, pagesNum);
                Index.push_back(rec);
            }
        }

        void TOneChunkIndex::OutputHtml(IOutputStream &str) const {
            str << "{LastRealLsn: " << LastRealLsn;
            if (!Index.empty()) {
                str << "IndexRecs:";
                for (const auto &i: Index) {
                    str << " " << i.ToShortString();
                }
            }
            str << "}";
        }

        ui32 TOneChunkIndex::Serialize(IOutputStream &s) const {
            s.Write(&LastRealLsn, sizeof(ui64));
            ui32 diskIndexRecsNum = Index.size();
            s.Write(&diskIndexRecsNum, sizeof(ui32));
            for (const auto &rec: Index) {
                s.Write(&rec.FirstLsn, sizeof(ui64));
                ui16 d = rec.OffsetInPages;
                s.Write(&d, sizeof(ui16));
                d = rec.PagesNum;
                s.Write(&d, sizeof(ui16));
            }
            return diskIndexRecsNum;
        }

        ui32 TOneChunkIndex::Serialize(IOutputStream &s,
                                       const TDeltaToDiskRecLog::TOneAppend &append,
                                       ui32 indexBulk) const {
            Y_ABORT_UNLESS(!Index.empty());

            // we implement Serialize with Delta via deep copy
            TOneChunkIndex idx(*this);
            idx.UpdateIndex(append.Pages, indexBulk);
            return idx.Serialize(s);
        }

        std::pair<TOneChunkIndexPtr, const char *> TOneChunkIndex::Construct(const char *serialized) {
            ui64 lastRealLsn;
            memcpy(&lastRealLsn, serialized, sizeof(ui64));
            serialized += sizeof(ui64);
            ui32 recsNum = *(const ui32 *)serialized;
            serialized += sizeof(ui32);

            TDiskIndexRecs index;
            index.reserve(recsNum);
            for (ui32 i = 0; i < recsNum; i++) {
                ui64 firstLsn;
                memcpy(&firstLsn, serialized, sizeof(ui64));
                serialized += sizeof(ui64);
                ui16 offsetInPages = *(const ui16 *)serialized;
                serialized += sizeof(ui16);
                ui16 pagesNum = *(const ui16 *)serialized;
                serialized += sizeof(ui16);
                index.push_back(TDiskIndexRecord(firstLsn, offsetInPages, pagesNum));
            }

            TOneChunkIndexPtr ptr(new TOneChunkIndex(lastRealLsn, index));
            return std::pair<TOneChunkIndexPtr, const char *>(ptr, serialized);
        }

        TOneChunkIndex::TOneChunkIndex(ui64 lastRealLsn, TDiskIndexRecs &index)
            : Index()
            , LastRealLsn(lastRealLsn)
        {
            Index.swap(index);
        }


        ////////////////////////////////////////////////////////////////////////////
        // TIndexedChunk
        ////////////////////////////////////////////////////////////////////////////
        void TIndexedChunk::OutputHtml(IOutputStream &str) const {
            str << "{ChunkIdx: " << ChunkPtr->GetChunkIdx() << " ";
            IndexPtr->OutputHtml(str);
            str << "}";
        }

        std::pair<TIndexedChunkPtr, const char *> TIndexedChunk::Construct(const char *serialized) {
            auto chunkRes = TOneChunk::Construct(serialized);
            auto indexRes = TOneChunkIndex::Construct(chunkRes.second);
            TIndexedChunkPtr ptr(new TIndexedChunk(chunkRes.first, indexRes.first));
            return std::pair<TIndexedChunkPtr, const char *>(ptr, indexRes.second);
        }

        ui32 TIndexedChunk::Serialize(IOutputStream &s) const {
            ChunkPtr->Serialize(s);
            return IndexPtr->Serialize(s);
        }

        ui32 TIndexedChunk::Serialize(IOutputStream &s, const TDeltaToDiskRecLog &delta) const {
            ChunkPtr->Serialize(s, delta);

            if (!delta.Empty() && ChunkPtr->GetChunkIdx() == delta.First().ChunkIdx) {
                // we have mixed old and new indexes in one chunk
                return IndexPtr->Serialize(s, delta.First(), delta.IndexBulk);
            } else {
                return IndexPtr->Serialize(s);
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TDiskRecLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        TDiskRecLogSnapshot::TDiskRecLogSnapshot(ui32 chunkSize,
                                                 ui32 appendBlockSize,
                                                 ui32 pagesInChunk,
                                                 ui32 indexBulk)
            : ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , PagesInChunk(pagesInChunk)
            , IndexBulk(indexBulk)
        {}

        void TDiskRecLogSnapshot::OutputHtml(IOutputStream &str) const {
            HTML(str) {
                DIV_CLASS("well well-sm") {
                    STRONG() {str << "TDiskRecLog<br>";}
                    if (Empty()) {
                        str << "Log is empty<br>";
                    } else {
                        str << "FirstLsn: " << GetFirstLsn() << "<br>";
                        str << "LastLsn: " << GetLastLsn() << "<br>";
                    }
                    str << "ChunksUsed: " << ManyIdxChunks.size() << "<br>";
                    // print out Index
                    PARA() {
                        if (!ManyIdxChunks.empty()) {
                            str << "Index {Lsn, ChunkIdx, OffsetInPages, PagesNum}:";
                            unsigned counter = 0;
                            for (const auto &i: ManyIdxChunks) {
                                if (++counter > 10) {
                                    str << " <was cut>";
                                    break;
                                }
                                str << " ";
                                i->OutputHtml(str);
                            }
                        }
                    }
                }
            }
        }

        TString TDiskRecLogSnapshot::BoundariesToString() const {
            if (Empty())
                return Sprintf("{Dsk: empty}");
            else
                return Sprintf("{Dsk: [%" PRIu64 ", %" PRIu64 "]}", GetFirstLsn(), GetLastLsn());
        }

        ui32 TDiskRecLogSnapshot::Serialize(IOutputStream &s,
                                            const TDeltaToDiskRecLog &delta) const {
            // number of index recs
            ui32 idxRecs = 0;

            // find chunkIdx to skip if any
            TMaybe<ui32> lastMainChunkIdx;
            if (!ManyIdxChunks.empty()) {
                lastMainChunkIdx = ManyIdxChunks.back()->GetChunkIdx();
            }
            TMaybe<ui32> firstDeltaChunkIdx;
            if (!delta.Empty()) {
                firstDeltaChunkIdx = delta.First().ChunkIdx;
            }
            bool skipFirstDeltaChunk = lastMainChunkIdx && firstDeltaChunkIdx && *firstDeltaChunkIdx == *lastMainChunkIdx;

            // calculate number of chunks
            ui32 chunksNum = ManyIdxChunks.size() + delta.AllAppends.size() - (skipFirstDeltaChunk ? 1 : 0);
            s.Write(&chunksNum, sizeof(ui32));

            // serialize index + update from delta for the last chunk
            for (const auto &chunk : ManyIdxChunks) {
                idxRecs += chunk->Serialize(s, delta);
            }

            // serialize delta
            for (ui32 i = 0, size = delta.AllAppends.size(); i < size; ++i) {
                if (i == 0 && skipFirstDeltaChunk) {
                    // skip this append, since it has been already applied;
                    // check that skipChunkIdx may go first only
                } else {
                    idxRecs += delta.AllAppends[i].Serialize(s, delta.IndexBulk);
                }
            }

            return idxRecs;
        }

        ui32 TDiskRecLogSnapshot::LastChunkIdx() const {
            Y_ABORT_UNLESS(!ManyIdxChunks.empty());
            return ManyIdxChunks.back()->GetChunkIdx();
        }

        ui32 TDiskRecLogSnapshot::LastChunkFreePagesNum() const {
            if (ManyIdxChunks.empty())
                return 0;
            TIndexedChunkPtr last = ManyIdxChunks.back();
            return PagesInChunk - last->FreePagePos();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TDiskRecLog
        ////////////////////////////////////////////////////////////////////////////
        TDiskRecLog::TDiskRecLog(ui32 chunkSize,
                                 ui32 appendBlockSize,
                                 ui32 indexBulk,
                                 const char *serializedDataBegin,
                                 const char *serializedDataEnd)
            : ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , PagesInChunk(chunkSize / appendBlockSize)
            , IndexBulk(indexBulk)
            , ManyIdxChunks()
        {
            if (serializedDataBegin) {
                Y_DEBUG_ABORT_UNLESS(serializedDataEnd && serializedDataBegin < serializedDataEnd);
                Deserialize(serializedDataBegin, serializedDataEnd);
            }
        }

        bool TDiskRecLog::Equal(const TDiskRecLog &d) const {
            auto m = Guard(Lock);
            auto dm = Guard(d.Lock);

            if (ManyIdxChunks.size() != d.ManyIdxChunks.size())
                return false;
            for (ui32 i = 0, s = ManyIdxChunks.size(); i < s; ++i) {
                if (!ManyIdxChunks[i]->Equal(*d.ManyIdxChunks[i]))
                    return false;
            }
            return true;
        }

        TDiskRecLogSnapshotPtr TDiskRecLog::GetSnapshot() const {
            auto m = Guard(Lock);
            TDiskRecLogSnapshotPtr ptr(new TDiskRecLogSnapshot(ChunkSize,
                                                               AppendBlockSize,
                                                               PagesInChunk,
                                                               IndexBulk));
            ptr->ManyIdxChunks = ManyIdxChunks;
            if (!ManyIdxChunks.empty()) {
                auto& lastChunk = ptr->ManyIdxChunks.back();
                lastChunk = lastChunk->DeepCopy();
            }
            return ptr;
        }

        ui32 TDiskRecLog::TrimLog(ui64 confirmedCutLsn,
                                  std::shared_ptr<IActorNotify> notifier,
                                  TVector<ui32> &chunks) {
            auto m = Guard(Lock);
            ui32 nchunks = 0;
            while (!ManyIdxChunks.empty()) {
                TIndexedChunkPtr chunkPtr = ManyIdxChunks.front();
                if (chunkPtr->GetLastLsn() <= confirmedCutLsn) {
                    ManyIdxChunks.pop_front();
                    chunkPtr->SetUpNotifier(notifier);
                    chunks.push_back(chunkPtr->GetChunkIdx());
                    nchunks++;
                } else
                    break;
            }
            return nchunks; // number of chunks scheduled for deletion
        }

        // force to delete nchunks (if we don't have space for instance)
        ui64 TDiskRecLog::DeleteChunks(ui32 nchunks,
                                       std::shared_ptr<IActorNotify> notifier,
                                       TVector<ui32> &chunks) {
            auto m = Guard(Lock);
            const ui32 curChunks = ManyIdxChunks.size();
            const ui32 numChunksToDelete = Min(nchunks, curChunks);
            Y_ABORT_UNLESS(nchunks && curChunks, "nchunks# %" PRIu32 " curChunks# %" PRIu32, nchunks, curChunks);
            ui64 newLogStartLsn = 0;
            for (ui32 i = 0; i < numChunksToDelete; i++) {
                TIndexedChunkPtr chunkPtr = ManyIdxChunks.front();
                ManyIdxChunks.pop_front();
                newLogStartLsn = chunkPtr->GetLastLsn() + 1;
                chunkPtr->SetUpNotifier(notifier);
                chunks.push_back(chunkPtr->GetChunkIdx());
            }

            // we return new LogStartLsn
            return newLogStartLsn;
        }

        void TDiskRecLog::UpdateIndex(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages) {
            auto m = Guard(Lock);
            PrivateUpdateIndex(chunkIdx, pages);
        }

        void TDiskRecLog::UpdateIndex(const TDeltaToDiskRecLog &delta) {
            auto m = Guard(Lock);
            for (const auto &append : delta.AllAppends) {
                PrivateUpdateIndex(append.ChunkIdx, append.Pages);
            }
        }

        ui32 TDiskRecLog::Serialize(IOutputStream &s) const {
            auto m = Guard(Lock);
            ui32 chunksNum = ManyIdxChunks.size();
            s.Write(&chunksNum, sizeof(ui32));
            ui32 idxRecs = 0;
            for (const auto &chunk: ManyIdxChunks) {
                idxRecs += chunk->Serialize(s);
            }
            return idxRecs;
        }

        bool TDiskRecLog::CheckEntryPoint(const char *pos, const char *end) {
            // Data Format
            // data ::= [ChunksNum=4b] ChunkDescription*
            // ChunkDescription ::= [ChunkIdx=4b] [LastRealLsn=8b]
            //                      [DiskIndexRecsNum=4b] DiskIndexRec*
            // DiskIndexRec ::= [FirstLsn=8b] [OffsetInPages=2b] [PagesNum=2b]
            Y_DEBUG_ABORT_UNLESS(pos < end);
            if (size_t(end - pos) < 4)
                return false;

            ui32 chunksNum = *(const ui32 *)pos;
            pos += 4;
            for (ui32 i = 0; i < chunksNum; i++) {
                if (size_t(end - pos) < 16)
                    return false;
                pos += 4 + 8;
                ui32 recsNum = *(const ui32 *)pos;
                pos += 4;
                if (size_t(end - pos) < recsNum * sizeof(TDiskIndexRecord))
                    return false;
                pos += recsNum * sizeof(TDiskIndexRecord);
            }

            return (pos == end);
        }

        TString TDiskRecLog::BoundariesToString() const {
            auto m = Guard(Lock);
            if (PrivateEmpty())
                return Sprintf("{Dsk: empty}");
            else
                return Sprintf("{Dsk: [%" PRIu64 ", %" PRIu64 "]}", PrivateGetFirstLsn(), PrivateGetLastLsn());
        }

        TString TDiskRecLog::ToString() const {
            auto m = Guard(Lock);
            TStringStream s;
            if (PrivateEmpty())
                s << "Empty";
            else {
                bool first = true;
                for (const auto &idxChunkPtr: ManyIdxChunks) {
                    if (first)
                        first = false;
                    else
                        s << " ";
                    s << idxChunkPtr->ToString();
                }
            }
            return s.Str();
        }

        void TDiskRecLog::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const TIndexedChunkPtr& chunk : ManyIdxChunks) {
                chunk->GetOwnedChunks(chunks);
            }
        }

        ui32 TDiskRecLog::HowManyChunksAdds(const TMemRecLogSnapshotPtr &swapSnap) const {
            auto m = Guard(Lock);
            // find out how many new chunks swapSnap adds
            ui32 numChunksToAdd = 0;
            if (swapSnap && !swapSnap->Empty()) {
                ui32 swapPagesSize = swapSnap->Size();
                const ui32 lastChunkFreePages = PrivateLastChunkFreePagesNum();
                if (swapPagesSize > lastChunkFreePages) {
                    swapPagesSize -= lastChunkFreePages;
                    Y_DEBUG_ABORT_UNLESS(swapPagesSize > 0);
                    numChunksToAdd = swapPagesSize / PagesInChunk;
                    if (numChunksToAdd * PagesInChunk != swapPagesSize) {
                        ++numChunksToAdd;
                    }
                }
            }
            return numChunksToAdd;
        }

        void TDiskRecLog::Deserialize(const char *pos, const char *end) {
            ui32 chunksNum = ReadUnaligned<ui32>(pos);
            pos += 4;
            while (pos != end) {
                std::pair<TIndexedChunkPtr, const char *> c = TIndexedChunk::Construct(pos);
                ManyIdxChunks.push_back(c.first);
                pos = c.second;
            }
            Y_ABORT_UNLESS(ManyIdxChunks.size() == chunksNum);
        }

        ui32 TDiskRecLog::PrivateLastChunkIdx() const {
            Y_DEBUG_ABORT_UNLESS(!PrivateEmpty());
            return ManyIdxChunks.back()->GetChunkIdx();
        }

        ui32 TDiskRecLog::PrivateLastChunkFreePagesNum() const {
            if (ManyIdxChunks.empty())
                return 0;
            TIndexedChunkPtr last = ManyIdxChunks.back();
            return PagesInChunk - last->FreePagePos();
        }

        void TDiskRecLog::PrivateUpdateIndex(ui32 chunkIdx,
                                             const TVector<TSyncLogPageSnap> &pages) {
            if (!PrivateEmpty() && PrivateLastChunkIdx() == chunkIdx) {
                // update last chunk
                Y_DEBUG_ABORT_UNLESS(pages.size() <= PrivateLastChunkFreePagesNum());
                ManyIdxChunks.back()->UpdateIndex(pages, IndexBulk);
            } else {
                // add new
                Y_DEBUG_ABORT_UNLESS(pages.size() <= PagesInChunk);
                TIndexedChunkPtr ptr(new TIndexedChunk(chunkIdx, pages, IndexBulk));
                ManyIdxChunks.push_back(ptr);
            }
        }

    } // NSyncLog

} // NKikimr
