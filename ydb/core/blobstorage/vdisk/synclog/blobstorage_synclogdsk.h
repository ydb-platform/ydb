#pragma once

#include "defs.h"
#include "blobstorage_synclogmem.h"

#include <ydb/core/util/serializable_access_check.h>
#include <ydb/core/blobstorage/base/utility.h>

#include <util/generic/deque.h>
#include <util/generic/queue.h>
#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/string/printf.h>

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TDiskIndexRecord
        ////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
        struct TDiskIndexRecord {
            // First Lsn of Data the record references to
            ui64 FirstLsn;
            // Offset of Data in pages from beginning of the chunk
            ui32 OffsetInPages : 16;
            // Number of pages the record indexes
            ui32 PagesNum : 16;

            TDiskIndexRecord(ui64 lsn = 0, ui32 offsInPages = 0, ui32 pagesNum = 0)
                : FirstLsn(lsn)
                , OffsetInPages(offsInPages)
                , PagesNum(pagesNum)
            {}

            bool operator == (const TDiskIndexRecord &r) const {
                return FirstLsn == r.FirstLsn &&
                    OffsetInPages == r.OffsetInPages &&
                    PagesNum == r.PagesNum;
            }

            struct TLess {
                bool operator ()(const TDiskIndexRecord &x, const ui64 &lsn) const {
                    return x.FirstLsn < lsn;
                }
            };

            TString ToString() const {
                return Sprintf("{FirstLsn# %" PRIu64 " OffsInPages# %" PRIu32 " PagesNum# %" PRIu32 "}",
                               FirstLsn, OffsetInPages, PagesNum);
            }

            TString ToShortString() const {
                return Sprintf("{%" PRIu64 " %" PRIu32 " %" PRIu32 "}", FirstLsn, OffsetInPages, PagesNum);
            }
        };
#pragma pack(pop)

        typedef TVector<TDiskIndexRecord> TDiskIndexRecs;


        ////////////////////////////////////////////////////////////////////////////
        // TDeltaToDiskRecLog
        // The class stores delta append to DiskRecLog. The Commiter actor of
        // TSynclogKeeper may want to swap some memory data to disk. These writes
        // are stored as an append delta in this class
        ////////////////////////////////////////////////////////////////////////////
        struct TDeltaToDiskRecLog {
            // One write (append) we make to the chunk when swaping mem data to disk
            struct TOneAppend {
                TOneAppend(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages)
                    : ChunkIdx(chunkIdx)
                    , Pages(pages)
                {}
                // returns number of index records
                ui32 Serialize(IOutputStream &s, ui32 indexBulk) const;
                void Output(IOutputStream &str) const;

                ui32 ChunkIdx = 0;
                TVector<TSyncLogPageSnap> Pages;
            };

            const ui32 IndexBulk;
            TVector<TOneAppend> AllAppends;

            TDeltaToDiskRecLog(ui32 indexBulk)
                : IndexBulk(indexBulk)
            {}

            const TOneAppend &First() const { return AllAppends.front(); }
            bool Empty() const { return AllAppends.empty(); }
            TDeltaToDiskRecLog &Append(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages);
            void Output(IOutputStream &str) const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TOneChunk
        ////////////////////////////////////////////////////////////////////////////
        class TOneChunk;
        typedef TIntrusivePtr<TOneChunk> TOneChunkPtr;

        class TOneChunk : public TThrRefBase {
        public:
            TOneChunk(ui32 chunkIdx)
                : ChunkIdx(chunkIdx)
            {}

            ~TOneChunk();

            bool Equal(const TOneChunk &c) const {
                return ChunkIdx == c.ChunkIdx;
            }

            void SetUpNotifier(std::shared_ptr<IActorNotify> n) {
                Notifier = std::move(n);
            }

            ui32 GetChunkIdx() const {
                return ChunkIdx;
            }

            void Serialize(IOutputStream &s) const {
                s.Write(&ChunkIdx, sizeof(ui32));
            }

            void Serialize(IOutputStream &s, const TDeltaToDiskRecLog &) const {
                Serialize(s);
            }

            TString ToString() const {
                return Sprintf("%" PRIu32, ChunkIdx);
            }

            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
                const bool inserted = chunks.insert(ChunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }

            static std::pair<TOneChunkPtr, const char *> Construct(const char *serialized);

        private:
            ui32 ChunkIdx = 0;
            std::shared_ptr<IActorNotify> Notifier;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TOneChunkIndex
        ////////////////////////////////////////////////////////////////////////////
        class TOneChunkIndex;
        typedef TIntrusivePtr<TOneChunkIndex> TOneChunkIndexPtr;

        class TOneChunkIndex : public TThrRefBase {
        public:
            TOneChunkIndex(const TVector<TSyncLogPageSnap> &pages, ui32 indexBulk) {
                AppendPages(pages, indexBulk, 0);
            }

            bool Equal(const TOneChunkIndex &c) const {
                return Index == c.Index && LastRealLsn == c.LastRealLsn;
            }

            ui64 GetFirstLsn() const {
                return Index.front().FirstLsn;
            }

            ui64 GetLastLsn() const {
                return LastRealLsn;
            }

            ui32 FreePagePos() const {
                if (Index.empty())
                    return 0;
                else {
                    const TDiskIndexRecord &rec = Index.back();
                    return rec.OffsetInPages + rec.PagesNum;
                }
            }

            TString ToString() const {
                TStringStream s;
                s << "{";
                for (const auto &rec: Index) {
                    s << rec.ToString() << " ";
                }
                s << "LastRealLsn# "<< LastRealLsn << "}";
                return s.Str();
            }

            TOneChunkIndexPtr DeepCopy() const {
                return new TOneChunkIndex(*this);
            }

            void UpdateIndex(const TVector<TSyncLogPageSnap> &pages, ui32 indexBulk);
            void OutputHtml(IOutputStream &str) const;
            // returns number of index records
            ui32 Serialize(IOutputStream &s) const;
            ui32 Serialize(IOutputStream &s,
                           const TDeltaToDiskRecLog::TOneAppend &append,
                           ui32 indexBulk) const;
            static std::pair<TOneChunkIndexPtr, const char *> Construct(const char *serialized);

            class TIterator;

        private:
            TDiskIndexRecs Index;
            ui64 LastRealLsn;   // for every chunk in index we support last real lsn of this chunk

            TOneChunkIndex(const TOneChunkIndex &) = default;
            TOneChunkIndex(ui64 lastRealLsn, TDiskIndexRecs &index);
            void AppendPages(const TVector<TSyncLogPageSnap> &pages,
                             ui32 indexBulk,
                             ui32 freePagePos);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TOneChunkIndex::TIterator
        ////////////////////////////////////////////////////////////////////////////
        class TOneChunkIndex::TIterator {
        public:
            TIterator()
                : Ptr(nullptr)
                , It()
            {}

            TIterator(TOneChunkIndexPtr ptr)
                : Ptr(ptr)
                , It(Ptr->Index.end())
            {}

            void SeekToFirst() {
                It = Ptr->Index.begin();
            }

            void Seek(ui64 lsn) {
                // compare function
                auto less = [] (const TDiskIndexRecord &x, const ui64 &lsn) {
                    return x.FirstLsn < lsn;
                };

                TDiskIndexRecs::const_iterator begin = Ptr->Index.begin();
                TDiskIndexRecs::const_iterator end = Ptr->Index.end();
                TDiskIndexRecs::const_iterator it = ::LowerBound(begin, end, lsn, less);

                // NOTE: unfortunately we don't have LastLsn for every bulk, so in some cases we put
                //       our start position one bulk ahead
                if (it == end) {
                    if (begin == end) { // empty container
                        It = end;
                        return;
                    } else {
                        --it;
                    }
                } else {
                    // firstLsn >= lsn
                    Y_DEBUG_ABORT_UNLESS(it->FirstLsn >= lsn);
                    if (it->FirstLsn == lsn || it == begin) {
                        // it is good
                    } else {
                        --it;
                    }
                }
                It = it;
                Y_DEBUG_ABORT_UNLESS(Valid());
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                ++It;
            }

            const TDiskIndexRecord *Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return &*It;
            }

            bool Valid() const {
                return Ptr && It != Ptr->Index.end();
            }


        private:
            TOneChunkIndexPtr Ptr;
            TDiskIndexRecs::const_iterator It;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TIndexedChunk
        ////////////////////////////////////////////////////////////////////////////
        class TIndexedChunk;
        typedef TIntrusivePtr<TIndexedChunk> TIndexedChunkPtr;

        class TIndexedChunk : public TThrRefBase {
        public:
            TIndexedChunk(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages, ui32 indexBulk)
                : ChunkPtr(new TOneChunk(chunkIdx))
                , IndexPtr(new TOneChunkIndex(pages, indexBulk))
            {}

            TIndexedChunk(TOneChunkPtr chunkPtr, TOneChunkIndexPtr indexPtr)
                : ChunkPtr(chunkPtr)
                , IndexPtr(indexPtr)
            {}

            ui64 GetFirstLsn() const {
                return IndexPtr->GetFirstLsn();
            }

            ui64 GetLastLsn() const {
                return IndexPtr->GetLastLsn();
            }

            void SetUpNotifier(std::shared_ptr<IActorNotify> n) {
                ChunkPtr->SetUpNotifier(std::move(n));
            }

            ui32 GetChunkIdx() const {
                return ChunkPtr->GetChunkIdx();
            }

            ui32 FreePagePos() const {
                return IndexPtr->FreePagePos();
            }

            void UpdateIndex(const TVector<TSyncLogPageSnap> &pages, ui32 indexBulk) {
                IndexPtr->UpdateIndex(pages, indexBulk);
            }

            bool Equal(const TIndexedChunk &c) const {
                return ChunkPtr->Equal(*c.ChunkPtr) &&
                    IndexPtr->Equal(*c.IndexPtr);
            }

            // returns number of index records
            ui32 Serialize(IOutputStream &s) const;
            ui32 Serialize(IOutputStream &s, const TDeltaToDiskRecLog &delta) const;

            TOneChunkIndex::TIterator GetIndexIterator() const {
                return TOneChunkIndex::TIterator(IndexPtr);
            }

            TIndexedChunkPtr DeepCopy() const {
                return new TIndexedChunk(ChunkPtr, IndexPtr->DeepCopy());
            }

            void OutputHtml(IOutputStream &str) const;
            static std::pair<TIndexedChunkPtr, const char *> Construct(const char *serialized);
            TString ToString() const {
                TStringStream s;
                s << "{" << ChunkPtr->ToString() << " " << IndexPtr->ToString() << "}";
                return s.Str();
            }

            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
                ChunkPtr->GetOwnedChunks(chunks);
            }

        private:
            TOneChunkPtr ChunkPtr;
            TOneChunkIndexPtr IndexPtr;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TManyIdxChunks
        ////////////////////////////////////////////////////////////////////////////
        typedef TDeque<TIndexedChunkPtr> TManyIndexedChunks;

        ////////////////////////////////////////////////////////////////////////////
        // TDiskRecLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        class TDiskRecLogSnapshot : public TThrRefBase {
        public:
            bool Empty() const {
                return ManyIdxChunks.empty();
            }

            ui64 GetFirstLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return ManyIdxChunks.front()->GetFirstLsn();
            }

            ui64 GetLastLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return ManyIdxChunks.back()->GetLastLsn();
            }

            void OutputHtml(IOutputStream &str) const;
            TString BoundariesToString() const;
            ui32 Serialize(IOutputStream &s, const TDeltaToDiskRecLog &delta) const;
            ui32 LastChunkIdx() const;
            ui32 LastChunkFreePagesNum() const;

            class TIndexRecIterator;

        private:
            TManyIndexedChunks ManyIdxChunks;

            TDiskRecLogSnapshot(ui32 chunkSize,
                                ui32 appendBlockSize,
                                ui32 pagesInChunk,
                                ui32 indexBulk);

            friend class TDiskRecLog;

        public:
            const ui32 ChunkSize;
            const ui32 AppendBlockSize;
            const ui32 PagesInChunk;
            const ui32 IndexBulk; // one index record for IndexBulk number of pages
        };

        typedef TIntrusivePtr<TDiskRecLogSnapshot> TDiskRecLogSnapshotPtr;
        typedef TIntrusiveConstPtr<TDiskRecLogSnapshot> TDiskRecLogSnapshotConstPtr;

        ////////////////////////////////////////////////////////////////////////////
        // TDiskRecLogSnapshot::TIndexRecIterator
        ////////////////////////////////////////////////////////////////////////////
        class TDiskRecLogSnapshot::TIndexRecIterator {
        public:
            TIndexRecIterator()
                : SnapPtr(nullptr)
                , ChunkIt()
                , IdxBulkIt()
            {}

            TIndexRecIterator(TDiskRecLogSnapshotConstPtr &snapPtr)
                : SnapPtr(snapPtr)
                , ChunkIt(SnapPtr->ManyIdxChunks.end())
                , IdxBulkIt()
            {}

            void SeekToFirst() {
                ChunkIt = SnapPtr->ManyIdxChunks.begin();
                if (ChunkIt != SnapPtr->ManyIdxChunks.end()) {
                    IdxBulkIt = (*ChunkIt)->GetIndexIterator();
                    Y_DEBUG_ABORT_UNLESS(IdxBulkIt.Valid());
                }
            }

            void Seek(ui64 lsn) {
                // compare function
                auto less = [] (const TIndexedChunkPtr &x, const ui64 &lsn) {
                    return x->GetFirstLsn() < lsn;
                };

                TManyIndexedChunks::const_iterator begin = SnapPtr->ManyIdxChunks.begin();
                TManyIndexedChunks::const_iterator end = SnapPtr->ManyIdxChunks.end();
                TManyIndexedChunks::const_iterator it = ::LowerBound(begin, end, lsn, less);

                if (it == end) {
                    if (begin == end) { // empty container
                        ChunkIt = end;
                        return;
                    } else {
                        --it;
                        if ((*it)->GetLastLsn() < lsn) {
                            ChunkIt = end;
                            return;
                        }
                    }
                } else {
                    // firstLsn >= lsn
                    ui64 firstLsn = (*it)->GetFirstLsn();
                    Y_DEBUG_ABORT_UNLESS(firstLsn >= lsn);
                    if (firstLsn == lsn || it == begin) {
                        // it is good
                    } else {
                        // firstLsn > lsn and it's not a first page
                        TManyIndexedChunks::const_iterator prevIt = it - 1;
                        // NOTE: prevIt is definitely not the last chunk
                        if ((*prevIt)->GetLastLsn() >= lsn)
                            it = prevIt;
                    }
                }

                // iterator to the chunk is finally found
                ChunkIt = it;
                IdxBulkIt = (*ChunkIt)->GetIndexIterator();
                IdxBulkIt.Seek(lsn);
                Y_DEBUG_ABORT_UNLESS(IdxBulkIt.Valid());
                Y_DEBUG_ABORT_UNLESS(Valid());
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                IdxBulkIt.Next();
                if (!IdxBulkIt.Valid()) {
                    ++ChunkIt;
                    if (ChunkIt != SnapPtr->ManyIdxChunks.end()) {
                        IdxBulkIt = (*ChunkIt)->GetIndexIterator();
                        IdxBulkIt.SeekToFirst();
                        Y_DEBUG_ABORT_UNLESS(IdxBulkIt.Valid(), "ChunkIdx# %u", (*ChunkIt)->GetChunkIdx());
                    }
                }
            }

            std::pair<ui32, const TDiskIndexRecord *> Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return std::pair<ui32, const TDiskIndexRecord *>((*ChunkIt)->GetChunkIdx(), IdxBulkIt.Get());
            }

            bool Valid() const {
                return SnapPtr && ChunkIt != SnapPtr->ManyIdxChunks.end();
            }

        private:
            TDiskRecLogSnapshotConstPtr SnapPtr;
            TManyIndexedChunks::const_iterator ChunkIt;
            TOneChunkIndex::TIterator IdxBulkIt;
        };

        ////////////////////////////////////////////////////////////////////////////
        // Forward declaration for TMemRecLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        class TMemRecLogSnapshot;
        using TMemRecLogSnapshotPtr = TIntrusivePtr<TMemRecLogSnapshot>;

        ////////////////////////////////////////////////////////////////////////////
        // TDiskRecLog
        ////////////////////////////////////////////////////////////////////////////
        class TDiskRecLog {
        public:
            TDiskRecLog(ui32 chunkSize,
                        ui32 appendBlockSize,
                        ui32 indexBulk,
                        const char *serializedDataBegin,
                        const char *serializedDataEnd);
            ~TDiskRecLog() = default;
            bool Equal(const TDiskRecLog &) const;

            TDiskRecLogSnapshotPtr GetSnapshot() const;

            bool Empty() const {
                auto m = Guard(Lock);
                return PrivateEmpty();
            }

            ui32 GetSizeInChunks() const {
                auto m = Guard(Lock);
                return ManyIdxChunks.size();
            }

            ui64 GetFirstLsn() const {
                auto m = Guard(Lock);
                return PrivateGetFirstLsn();
            }

            ui64 GetLastLsn() const {
                auto m = Guard(Lock);
                return PrivateGetLastLsn();
            }

            ui32 LastChunkIdx() const {
                auto m = Guard(Lock);
                return PrivateLastChunkIdx();
            }

            ui32 LastChunkFreePagesNum() const {
                auto m = Guard(Lock);
                return PrivateLastChunkFreePagesNum();
            }

            // cut log because we synced
            ui32 TrimLog(ui64 confirmedCutLsn,
                         std::shared_ptr<IActorNotify> notifier,
                         TVector<ui32> &chunks);
            // force to delete nchunks (if we don't have space for instance);
            // returns new LogStartLsn
            ui64 DeleteChunks(ui32 nchunks,
                              std::shared_ptr<IActorNotify> notifier,
                              TVector<ui32> &chunks);
            void UpdateIndex(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages);
            void UpdateIndex(const TDeltaToDiskRecLog &delta);
            // returns number of index records
            ui32 Serialize(IOutputStream &s) const;
            static bool CheckEntryPoint(const char *beg, const char *end);
            TString BoundariesToString() const;
            TString ToString() const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            // calculate how many chunks adds this memory snapshot to current state
            ui32 HowManyChunksAdds(const TMemRecLogSnapshotPtr &swapSnap) const;

        public:
            const ui32 ChunkSize;
            const ui32 AppendBlockSize;
            const ui32 PagesInChunk;
            const ui32 IndexBulk; // one index record for IndexBulk number of pages

        private:
            TManyIndexedChunks ManyIdxChunks;
            TSerializableAccessChecker Lock;

            void Deserialize(const char *begin, const char *end);
            ui32 PrivateLastChunkIdx() const;
            ui32 PrivateLastChunkFreePagesNum() const;
            bool PrivateEmpty() const {
                return ManyIdxChunks.empty();
            }
            ui64 PrivateGetFirstLsn() const {
                Y_DEBUG_ABORT_UNLESS(!PrivateEmpty());
                return ManyIdxChunks.front()->GetFirstLsn();
            }
            ui64 PrivateGetLastLsn() const {
                Y_DEBUG_ABORT_UNLESS(!PrivateEmpty());
                return ManyIdxChunks.back()->GetLastLsn();
            }
            void PrivateUpdateIndex(ui32 chunkIdx, const TVector<TSyncLogPageSnap> &pages);
        };

    } // NSyncLog

} // NKikimr
