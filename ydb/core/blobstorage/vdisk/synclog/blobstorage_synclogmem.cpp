#include "blobstorage_synclogmem.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPage
        ////////////////////////////////////////////////////////////////////////////
        void TSyncLogPage::Put(ui32 pageSize, const TRecordHdr *rec, ui32 dataSize) {
            Y_ABORT_UNLESS(HaveRoom(pageSize, dataSize) &&
                   (Header.LastLsn == 0 || rec->Lsn > Header.LastLsn),
                   "Header# %s rec# %s pageSize# %" PRIu32 " dataSize# %" PRIu32,
                   Header.ToString().data(), rec->ToString().data(), pageSize, dataSize);

            memcpy(Data() + Header.FreePos, rec, dataSize);
            Header.FreePos += dataSize;
            Header.RecsNum++;
            Header.LastLsn = rec->Lsn;
        }

        TSyncLogPagePtr TSyncLogPage::Create(TSyncLogPageDeleter &d) {
            char *buf = new char[d.PageSize];
            d.Allocate();
            TSyncLogPagePtr tmp(new (buf) TSyncLogPage, d);
            return tmp;
        }


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogPageSnap
        ////////////////////////////////////////////////////////////////////////////
        TString TSyncLogPageSnap::ToVerboseString() const {
            TStringStream s;
            for (const TRecordHdr *h = Begin(), *e = End(); h != e; h = Next(h))
                s << h->ToString() << " ";
            return s.Str();
        }

        TString TSyncLogPageSnap::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void TSyncLogPageSnap::Output(IOutputStream &str) const {
            str << "{firstLsn# " << GetFirstLsn() << " lastLsn# " << GetLastLsn() << "]";
        }

        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        TMemRecLogSnapshot::TMemRecLogSnapshot(TSyncLogPages &&pages, ui32 recsNum)
            : Pages()
            , RecsNum(recsNum)
        {
            if (!pages.empty()) {
                auto header = pages.back()->Header;
                Pages = TSyncLogPagesSnapshot(std::move(pages), header);
            }
        }

        void TMemRecLogSnapshot::OutputHtml(IOutputStream &str) const {
            HTML(str) {
                DIV_CLASS("well well-sm") {
                    STRONG() {str << "TMemRecLog<br>";}
                    if (Empty()) {
                        str << "Log is empty<br>";
                    } else {
                        str << "FirstLsn: " << GetFirstLsn() << "<br>";
                        str << "LastLsn: " << GetLastLsn() << "<br>";
                    }
                    str << "RecsNum: " << RecsNum << "<br>";
                    str << "PagesNum: " << Size() << "<br>";

                    PARA() {
                        TMemRecLogSnapshot::TIterator it(this);
                        it.SeekToFirst();
                        if (it.Valid()) {
                            str << "Lsns:";
                            unsigned i = 0;
                            while (it.Valid() && i < 100) {
                                str << " " << it.Get()->Lsn;
                                it.Next();
                                i++;
                            }
                            if (it.Valid())
                                str << " <was cut>";
                        }
                    }
                }
            }
        }

        TString TMemRecLogSnapshot::BoundariesToString() const {
            if (Empty())
                return Sprintf("{Mem: empty}");
            else
                return Sprintf("{Mem# [%" PRIu64 ", %" PRIu64 "] RecsNum# %" PRIu32 "}",
                    GetFirstLsn(), GetLastLsn(), RecsNum);
        }

        void TMemRecLogSnapshot::CheckSnapshotConsistency() const {
            if (Empty())
                return;

            TMemRecLogSnapshotConstPtr snapPtr(const_cast<TMemRecLogSnapshot*>(this));
            TIterator it(snapPtr);
            it.SeekToFirst();
            ui64 lsn = 0;
            bool first = true;
            while (it.Valid()) {
                const TRecordHdr *hdr = it.Get();
                if (first) {
                    first = false;
                } else {
                    Y_ABORT_UNLESS(hdr->Lsn > lsn, "lsn# %" PRIu64 " hdrLsn# %" PRIu64, lsn, hdr->Lsn);
                }
                lsn = hdr->Lsn;

                it.Next();
            }
        }


        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLogSnapshot::TIterator
        ////////////////////////////////////////////////////////////////////////////
        void TMemRecLogSnapshot::TIterator::SeekToFirst() {
            if (PagesIt.GetSnap()->Empty()) {
                Hdr = HdrEnd = nullptr;
            } else {
                PagesIt.SeekToFirst();
                SetupHdr();
            }
        }

        void TMemRecLogSnapshot::TIterator::Seek(ui64 lsn) {
            PagesIt.Seek(lsn);

            if (!PagesIt.Valid()) {
                // behind the last page
                if (PagesIt.GetSnap()->Pages.empty()) {
                    // empty list
                    Hdr = HdrEnd = nullptr;
                    return;
                } else {
                    PagesIt.Prev();
                    ui64 lastLsn = PagesIt.Get().GetLastLsn();
                    if (lastLsn < lsn) {
                        // no records satisfying the condition
                        Hdr = HdrEnd = nullptr;
                        return;
                    } else {
                        // we found the page
                    }
                }
            } else {
                // pageFirstLsn >= lsn
                ui64 pageFirstLsn = PagesIt.Get().GetFirstLsn();
                Y_DEBUG_ABORT_UNLESS(pageFirstLsn >= lsn);
                TPageIterator firstIt(PagesIt.GetSnap());
                firstIt.SeekToFirst();
                if (pageFirstLsn == lsn || PagesIt == firstIt) {
                    // we found the page
                } else {
                    // pageFirstLsn > lsn and it's not a first page
                    TPageIterator prevIt = PagesIt;
                    prevIt.Prev();
                    // NOTE: prevIt is definitely not the last page
                    if (prevIt.Get().GetLastLsn() >= lsn)
                        PagesIt = prevIt;
                }
            }

            // we have found the required page and it is PagesIt
            Y_DEBUG_ABORT_UNLESS(PagesIt.Valid());

            // find exact position in the page
            SetupHdr();
            while (Hdr->Lsn < lsn) {
                Hdr = Hdr->Next();
                Y_DEBUG_ABORT_UNLESS(Hdr != HdrEnd);
            }

            Y_DEBUG_ABORT_UNLESS(Valid());
        }

        void TMemRecLogSnapshot::TIterator::Next() {
            Hdr = Hdr->Next();
            if (Hdr == HdrEnd) {
                PagesIt.Next();
                if (PagesIt.Valid()) {
                    SetupHdr();
                } else
                    Hdr = HdrEnd = nullptr;
            }
        }

        inline void TMemRecLogSnapshot::TIterator::SetupHdr() {
            TSyncLogPageSnap p = PagesIt.Get();
            Hdr = p.Begin();
            HdrEnd = p.End();
        }


        ////////////////////////////////////////////////////////////////////////////
        // TMemRecLog
        ////////////////////////////////////////////////////////////////////////////
        TMemRecLog::TMemRecLog(const ui32 appendBlockSize, const TMemoryConsumer &memBytes)
            : Pages()
            , RecsNum(0)
            , SyncLogPageDeleter(TMemoryConsumer(memBytes), appendBlockSize)
            , AppendBlockSize(appendBlockSize)
        {}

        TMemRecLogSnapshotPtr TMemRecLog::GetSnapshot() const {
            TSyncLogPages pages(Pages); // deep copy intentionally
            TMemRecLogSnapshotPtr snap(new TMemRecLogSnapshot(std::move(pages), RecsNum));
            return snap;
        }

        void TMemRecLog::PutOne(const TRecordHdr *rec, ui32 size) {
            // error report function (it dumps MemRecLog state)
            auto errorReport = [&] () {
                TStringStream str;
                Output(str);
                str << "\n" << "Incoming record: " << rec->ToString() << "\n";
                return str.Str();
            };

            Y_ABORT_UNLESS(Pages.empty() || Pages.back()->GetLastLsn() < rec->Lsn,
                   "pagesSize# %" PRIu32 " lastLsn# %" PRIu64 " recLsn# %" PRIu64 " dump#\n %s",
                   ui32(Pages.size()), Pages.back()->GetLastLsn(), rec->Lsn, errorReport().data());


            if (Pages.empty() || !(Pages.back()->HaveRoom(AppendBlockSize, size))) {
                // create empty page
                TSyncLogPagePtr tmp(TSyncLogPage::Create(SyncLogPageDeleter));
                Pages.push_back(tmp);
            }

            Pages.back()->Put(AppendBlockSize, rec, size);
            RecsNum++;
        }

        void TMemRecLog::PutMany(const void *buf, ui32 size) {
            Y_DEBUG_ABORT_UNLESS(size);
            TRecordHdr *rec = (TRecordHdr*)buf;
            ui32 recSize = 0;
            do {
                recSize = rec->GetSize();
                Y_DEBUG_ABORT_UNLESS(recSize <= size);
                PutOne(rec, recSize);
                rec = rec->Next();
                size -= recSize;
            } while (size);
        }

        ui32 TMemRecLog::TrimLog(ui64 confirmedCutLsn) {
            return DiscardPages(0, confirmedCutLsn);
        }

        ui32 TMemRecLog::RemoveCachedPages(ui32 pagesMax, ui64 diskLastLsn) {
            return DiscardPages(pagesMax, diskLastLsn);
        }

        ui32 TMemRecLog::DiscardPages(ui32 pagesToKeep, ui64 lastUnneededLsn) {
            ui32 removed = 0;
            while (Pages.size() > pagesToKeep && Pages.front()->GetLastLsn() <= lastUnneededLsn) {
                RecsNum -= Pages.front()->Header.RecsNum;
                Pages.pop_front();
                removed++;
            }
            return removed;
        }

        TMemRecLogSnapshotPtr TMemRecLog::BuildSwapSnapshot(ui64 diskLastLsn,
                                                            ui64 freeUpToLsn, // excluding
                                                            ui32 freeNPages) {
            TSyncLogPages pages;

            // compare function
            auto less = [] (const TSyncLogPagePtr &x, const ui64 &lsn) {
                return x->GetLastLsn() < lsn;
            };

            // void skip cache pages, i.e. find the first page that is not in cache
            TSyncLogPages::const_iterator it = ::LowerBound(Pages.begin(), Pages.end(), diskLastLsn, less);
            if (it == Pages.end()) {
                Y_ABORT_UNLESS(Pages.empty());
                return TMemRecLogSnapshotPtr();
            }

            ui64 pageLastLsn = (*it)->GetLastLsn();
            Y_ABORT_UNLESS(pageLastLsn >= diskLastLsn);
            if (pageLastLsn == diskLastLsn)
                ++it;

            if (it == Pages.end()) {
                return TMemRecLogSnapshotPtr();
            }

            // now it points to the first page we are going to swap
            ui32 recsNum = 0;
            while (it != Pages.end()) {
                Y_ABORT_UNLESS((*it)->GetLastLsn() > diskLastLsn);
                if (freeNPages > 0 || (*it)->GetFirstLsn() < freeUpToLsn) { // '<', because 'excluding'
                    pages.push_back(*it);
                    recsNum += (*it)->GetRecsNum();
                    if (freeNPages > 0)
                        freeNPages--;
                    ++it;
                } else
                    break;
            }

            TMemRecLogSnapshotPtr snap(new TMemRecLogSnapshot(std::move(pages), recsNum));
            return snap;
        }

        TString TMemRecLog::BoundariesToString() const {
            if (Empty())
                return Sprintf("{Mem: empty}");
            else
                return Sprintf("{Mem: [%" PRIu64 ", %" PRIu64 "]}", GetFirstLsn(), GetLastLsn());
        }

        void TMemRecLog::Output(IOutputStream &s) const {
            TMemRecLogSnapshotPtr snap = GetSnapshot();
            TMemRecLogSnapshot::TIterator it(snap);
            it.SeekToFirst();
            s << "Elements: ";
            while (it.Valid()) {
                s << it.Get()->ToString() << " ";
                it.Next();
            }
            s << "\nPages: ";
            TMemRecLogSnapshot::TPageIterator pit(snap);
            pit.SeekToFirst();
            while (pit.Valid()) {
                s << "Page=" << pit.Get().ToString() << " ";
                pit.Next();
            }
            s << "\n";
        }

        TString TMemRecLog::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

    } // NSyncLog
} // NKikimr
