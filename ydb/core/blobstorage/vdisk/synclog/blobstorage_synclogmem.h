#pragma once

#include "defs.h"
#include "blobstorage_synclogformat.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>

#include <util/generic/ptr.h>
#include <util/generic/deque.h>
#include <util/generic/queue.h>
#include <util/generic/algorithm.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPage
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogPage;
        class TSyncLogPageDeleter;
        using TSyncLogPagePtr = std::shared_ptr<TSyncLogPage>;

        class TSyncLogPage : public TNonCopyable {
        public:
            // TODO: we may need to add additional index

            struct THeader {
                ui32 FreePos;
                ui32 RecsNum;
                ui64 LastLsn;

                THeader()
                    : FreePos(0)
                    , RecsNum(0)
                    , LastLsn(0)
                {}

                TString ToString() const {
                    return Sprintf("[FreePos# %" PRIu32 " RecsNum# %" PRIu32
                                   " LastLsn# %" PRIu64 "]", FreePos, RecsNum, LastLsn);
                }
            };

            char *Data() {
                return (char *)(this + 1);
            }

            const char *Data() const {
                return (const char *)(this + 1);
            }

            const char *FreeSpace() const {
                return Data() + Header.FreePos;
            }

            bool Empty() const {
                return Header.FreePos == 0;
            }

            ui64 GetFirstLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return ((TRecordHdr *)Data())->Lsn;
            }

            ui64 GetLastLsn() const {
                return Header.LastLsn;
            }

            ui32 GetRecsNum() const {
                return Header.RecsNum;
            }

            bool HaveRoom(ui32 pageSize, ui32 dataSize) const {
                return Header.FreePos + dataSize <= (pageSize - sizeof(*this));
            }

            void Finalize(ui32 pageSize) {
                memset(Data() + Header.FreePos, 0, pageSize - sizeof(*this) - Header.FreePos);
            }

            void Put(ui32 pageSize, const TRecordHdr *rec, ui32 dataSize);
            static TSyncLogPagePtr Create(TSyncLogPageDeleter &d);

        private:
            THeader Header;
            friend class TSyncLogPageSnap;
            friend class TMemRecLog;
            friend class TMemRecLogSnapshot;

            TSyncLogPage()
                : Header()
            {}
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPageDeleter
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogPageDeleter {
        public:
            TSyncLogPageDeleter(TMemoryConsumer &&memBytes, ui32 pageSize)
                : MemBytes(std::move(memBytes))
                , PageSize(pageSize)
            {}

            TSyncLogPageDeleter() = delete;
            TSyncLogPageDeleter(const TSyncLogPageDeleter &) = default;
            TSyncLogPageDeleter(TSyncLogPageDeleter &&) = default;
            TSyncLogPageDeleter &operator=(const TSyncLogPageDeleter &) = default;
            TSyncLogPageDeleter &operator=(TSyncLogPageDeleter &&) = default;

            void operator () (TSyncLogPage *page) noexcept {
                char *ptr = (char *)page;
                delete [] ptr;
                MemBytes.Subtract(PageSize);
            }

            void Allocate() {
                MemBytes.Add(PageSize);
            }

        private:
            TMemoryConsumer MemBytes;
        public:
            const ui32 PageSize;
        };

        typedef TDeque<TSyncLogPagePtr> TSyncLogPages;

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPageROIterator
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogPageROIterator {
        public:
            TSyncLogPageROIterator(const TSyncLogPage *pagePtr)
                : PagePtr(pagePtr)
                , Pos(nullptr)
                , Begin(nullptr)
                , End(nullptr)
            {
                Y_ABORT_UNLESS(pagePtr);
                Begin = (const TRecordHdr *)(PagePtr->Data());
                End = (const TRecordHdr *)(PagePtr->FreeSpace());
            }

            void SeekToFirst() {
                Pos = Begin;
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                Pos = Pos->Next();
            }

            bool Valid() const {
                return PagePtr && Begin <= Pos && Pos < End;
            }

            const TRecordHdr *Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return Pos;
            }
        private:
            const TSyncLogPage *PagePtr;
            const TRecordHdr *Pos;
            const TRecordHdr *Begin;
            const TRecordHdr *End;

            // check bounders, don't call it normally
            void ParanoidCheck() {
                ui32 total = 0;
                ui64 lastLsn = 0;
                for (const TRecordHdr *p = Begin; p != End; p = p->Next()) {
                    total++;
                    lastLsn = p->Lsn;
                }
                Y_ABORT_UNLESS(total > 0 && total == PagePtr->GetRecsNum());
                Y_ABORT_UNLESS(lastLsn == PagePtr->GetLastLsn());
            }
        };



        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPageSnap
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogPageSnap {
        private:
            TSyncLogPage::THeader Header;
            TSyncLogPagePtr Ptr;

        public:
            TSyncLogPageSnap(TSyncLogPagePtr pagePtr)
                : Header(pagePtr->Header)
                , Ptr(pagePtr)
            {}

            TSyncLogPageSnap(const TSyncLogPage::THeader &header, TSyncLogPagePtr pagePtr)
                : Header(header)
                , Ptr(pagePtr)
            {}

            TRecordHdr *Begin() {
                return (TRecordHdr *)(Ptr->Data());
            }

            TRecordHdr *End() {
                return (TRecordHdr *)(Ptr->Data() + Header.FreePos);
            }

            const TRecordHdr *Begin() const {
                return (const TRecordHdr *)(Ptr->Data());
            }

            const TRecordHdr *End() const {
                return (const TRecordHdr *)(Ptr->Data() + Header.FreePos);
            }

            static TRecordHdr *Next(TRecordHdr *hdr) {
                return hdr->Next();
            }

            static const TRecordHdr *Next(const TRecordHdr *hdr) {
                return hdr->Next();
            }

            ui64 GetFirstLsn() const {
                return Begin()->Lsn;
            }

            ui64 GetLastLsn() const {
                return Header.LastLsn;
            }

            std::pair<const void *, size_t> GetFirstRaw() const {
                return std::pair<const void *, size_t>(&Header, sizeof(Header));
            }

            std::pair<const void *, size_t> GetSecondRaw() const {
                return std::pair<const void *, size_t>(Ptr->Data(), Header.FreePos);
            }

            std::pair<const void *, size_t> GetThirdRaw(ui32 pageSize) const {
                size_t s = pageSize - Header.FreePos - sizeof(Header);
                return std::pair<const void *, size_t>(nullptr, s);
            }

            TString ToVerboseString() const;
            TString ToString() const;
            void Output(IOutputStream &str) const;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPagesSnapshot
        // Stores snapshot of pages + last page header
        // This class is for avoiding data races, because usage of snapshot is
        // tricky
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogPagesSnapshot : private TSyncLogPages {
        public:
            using TBase = TSyncLogPages;

            TSyncLogPagesSnapshot() = default;

            TSyncLogPagesSnapshot(TSyncLogPages &&pages, TSyncLogPage::THeader lastPageHeader)
                : TSyncLogPages(std::move(pages))
                , LastPageHeader(lastPageHeader)
            {}

            TSyncLogPageSnap operator[] (size_type n) {
                Y_DEBUG_ABORT_UNLESS(n < size());
                if (n + 1 == size())
                    return TSyncLogPageSnap(LastPageHeader, TBase::operator[](n));
                else
                    return TSyncLogPageSnap(TBase::operator[](n));
            }

            TSyncLogPageSnap operator[] (size_type n) const {
                Y_DEBUG_ABORT_UNLESS(n < size());
                if (n + 1 == size())
                    return TSyncLogPageSnap(LastPageHeader, TBase::operator[](n));
                else
                    return TSyncLogPageSnap(TBase::operator[](n));
            }

            using TBase::empty;
            using TBase::size;

            struct const_iterator {
                const_iterator() = default;
                const_iterator(TSyncLogPages::const_iterator it)
                    : It(it)
                {}

                const_iterator &operator ++() {
                    ++It;
                    return *this;
                }

                const_iterator &operator --() {
                    --It;
                    return *this;
                }

                int operator - (const_iterator i) const {
                    return It - i.It;
                }

                bool operator ==(const_iterator i) const {
                    return It == i.It;
                }

                bool operator <(const_iterator i) const {
                    return It < i.It;
                }

                bool operator >(const_iterator i) const {
                    return It > i.It;
                }

                bool operator >=(const_iterator i) const {
                    return It >= i.It;
                }

                bool operator <=(const_iterator i) const {
                    return It <= i.It;
                }

            private:
                TSyncLogPages::const_iterator It;
            };

            const_iterator begin() const {
                return const_iterator(TBase::begin());
            }

            const_iterator end() const {
                return const_iterator(TBase::end());
            }

            const_iterator Seek(ui64 lsn) const {
                auto less = [this] (const TSyncLogPagePtr &x, const ui64 &lsn) {
                    if (&x == &back()) {
                        return operator[](size() - 1).GetFirstLsn() < lsn;
                    } else {
                        return x->GetFirstLsn() < lsn;
                    }
                };
                return ::LowerBound(TBase::begin(), TBase::end(), lsn, less);
            }

            TSyncLogPageSnap Deref(const_iterator it) const {
                return operator[](it - begin());
            }

        private:
            TSyncLogPage::THeader LastPageHeader;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        class TMemRecLogSnapshot : public TThrRefBase {
        public:
            // iterator via pages
            class TPageIterator;

            // iterator via elements
            class TIterator;

            bool Empty() const {
                return Pages.empty();
            }

            ui32 Size() const {
                return Pages.size();
            }

            ui64 GetFirstLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return Pages[0].GetFirstLsn();
            }

            ui64 GetLastLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return Pages[Size() - 1].GetLastLsn();
            }

            TSyncLogPageSnap operator [](unsigned i) {
                return Pages[i];
            }

            void OutputHtml(IOutputStream &str) const;
            TString BoundariesToString() const;
            void CheckSnapshotConsistency() const;

        private:
            TSyncLogPagesSnapshot Pages;
            const ui32 RecsNum;

            // only TMemRecLog can create snapshot of itself
            TMemRecLogSnapshot(TSyncLogPages &&pages, ui32 recsNum);
            friend class TMemRecLog;
        };

        typedef TIntrusivePtr<TMemRecLogSnapshot> TMemRecLogSnapshotPtr;
        typedef TIntrusiveConstPtr<TMemRecLogSnapshot> TMemRecLogSnapshotConstPtr;


        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLogSnapshot::TPageIterator
        ////////////////////////////////////////////////////////////////////////////
        class TMemRecLogSnapshot::TPageIterator {
        public:
            TPageIterator(const TMemRecLogSnapshotConstPtr &snap)
                : Snap(snap)
                , It()
            {}

            TPageIterator(const TMemRecLogSnapshot *snap)
                : Snap(const_cast<TMemRecLogSnapshot*>(snap))
                , It()
            {}

            void SeekToFirst() {
                Y_DEBUG_ABORT_UNLESS(Snap);
                It = Snap->Pages.begin();
            }

            void Seek(ui64 lsn) {
                It = Snap->Pages.Seek(lsn);
            }

            bool Valid() const {
                return Snap && It >= Snap->Pages.begin() && It < Snap->Pages.end();
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                ++It;
            }

            void Prev() {
                Y_DEBUG_ABORT_UNLESS(Snap && It > Snap->Pages.begin() && It <= Snap->Pages.end());
                --It;
            }

            TSyncLogPageSnap Get() const {
                return Snap->Pages.Deref(It);
            }

            bool operator ==(const TPageIterator &it) const {
                return Snap.Get() == it.Snap.Get() && It == it.It;
            }

            TString ToString() const {
                TStringStream str;
                str << "[pagePos# " << unsigned(It - Snap->Pages.begin()) << "]";
                return str.Str();
            }

            TMemRecLogSnapshotConstPtr GetSnap() const {
                return Snap;
            }

        private:
            TMemRecLogSnapshotConstPtr Snap;
            TSyncLogPagesSnapshot::const_iterator It;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLogSnapshot::TIterator
        ////////////////////////////////////////////////////////////////////////////
        class TMemRecLogSnapshot::TIterator {
        public:
            TIterator(const TMemRecLogSnapshotConstPtr &snap)
                : PagesIt(snap)
                , Hdr(nullptr)
                , HdrEnd(nullptr)
            {}

            TIterator(const TMemRecLogSnapshot *snap)
                : PagesIt(snap)
                , Hdr(nullptr)
                , HdrEnd(nullptr)
            {}

            void SeekToFirst();
            void Seek(ui64 lsn);
            void Next();

            const TRecordHdr *Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return Hdr;
            }

            bool Valid() const {
                return Hdr;
            }

            TString ToString() const {
                return Sprintf("[PagesIt=%s Hdr=%p]", PagesIt.ToString().data(), Hdr);
            }

        private:
            TPageIterator PagesIt;
            TRecordHdr *Hdr;
            TRecordHdr *HdrEnd;

            void SetupHdr();
        };

        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLog
        ////////////////////////////////////////////////////////////////////////////
        class TMemRecLog {
        public:
            typedef TSyncLogPages::iterator TPageIterator;

            ui64 GetFirstLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return Pages.front()->GetFirstLsn();
            }

            ui64 GetLastLsn() const {
                Y_DEBUG_ABORT_UNLESS(!Empty());
                return Pages.back()->GetLastLsn();
            }

            TPageIterator PageItBegin() {
                return Pages.begin();
            }

            TPageIterator PageItEnd() {
                return Pages.end();
            }

            bool Empty() const {
                return Pages.empty();
            }

            ui32 GetNumberOfPages() const {
                return Pages.size();
            }

            ui64 GetDataSize() const {
                return (ui64)GetNumberOfPages() * (ui64)AppendBlockSize;
            }

            TMemRecLog(const ui32 appendBlockSize,
                       const TMemoryConsumer &memBytes =
                            TMemoryConsumer(new NMonitoring::TCounterForPtr(false)));
            TMemRecLogSnapshotPtr GetSnapshot() const;
            void PutOne(const TRecordHdr *rec, ui32 size);
            void PutMany(const void *buf, ui32 size);
            ui32 TrimLog(ui64 confirmedCutLsn);
            ui32 RemoveCachedPages(ui32 pagesMax, ui64 diskLastLsn);
            TMemRecLogSnapshotPtr BuildSwapSnapshot(
                ui64 diskLastLsn,
                ui64 freeUpToLsn, // excluding
                ui32 freeNPages);
            TString BoundariesToString() const;
            void Output(IOutputStream &s) const;
            TString ToString() const;

        private:
            ui32 DiscardPages(ui32 pagesToKeep, ui64 lastUnneededLsn);

        private:
            TSyncLogPages Pages;
            ui32 RecsNum;
            TSyncLogPageDeleter SyncLogPageDeleter;
        public:
            const ui32 AppendBlockSize;
        };

    } // NSyncLog
} // NKikimr
