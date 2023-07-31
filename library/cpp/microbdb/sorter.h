#pragma once

#include <util/ysaveload.h>
#include <util/generic/algorithm.h>
#include <contrib/libs/libc_compat/include/link/link.h>

#include "header.h"
#include "heap.h"
#include "extinfo.h"
#include "input.h"
#include "output.h"

#ifdef TEST_MERGE
#define MBDB_SORT_FUN ::StableSort
#else
#define MBDB_SORT_FUN ::Sort
#endif

template <class TVal, class TCompare, typename TCompress, typename TSieve, typename TOutPageFile, typename TFileTypes>
class TDatSorterImpl;

template <class TVal>
struct TFakeSieve {
    static inline int Sieve(TVal*, const TVal*) noexcept {
        return 0;
    }
};

template <class TSieve>
struct TIsSieveFake {
    static const bool Result = false;
};

template <class T>
struct TIsSieveFake<TFakeSieve<T>> {
    static const bool Result = true;
};

class TDefInterFileTypes {
public:
    typedef TOutputPageFile TOutPageFile;
    typedef TInputPageFile TInPageFile;
};

//class TCompressedInterFileTypes;

template <class TVal, class TCompare, typename TCompress, typename TSieve, typename TOutPageFile = TOutputPageFile, typename TFileTypes = TDefInterFileTypes>
class TDatSorterImplBase: protected  THeapIter<TVal, TInDatFileImpl<TVal, TInputRecordIterator<TVal, TInputPageIterator<typename TFileTypes::TInPageFile>>>, TCompare> {
    typedef TOutputRecordIterator<TVal, TOutputPageIterator<typename TFileTypes::TOutPageFile>, TFakeIndexer, TCompress> TTmpRecIter;
    typedef TInputRecordIterator<TVal, TInputPageIterator<typename TFileTypes::TInPageFile>> TInTmpRecIter;

public:
    typedef TOutDatFileImpl<TVal, TTmpRecIter> TTmpOut;
    typedef TInDatFileImpl<TVal, TInTmpRecIter> TTmpIn;

    typedef TOutDatFileImpl<TVal, TOutputRecordIterator<TVal, TOutputPageIterator<TOutPageFile>, TFakeIndexer, TCompress>> TOut;
    typedef THeapIter<TVal, TTmpIn, TCompare> TMyHeap;
    typedef TVector<const TVal*> TMyVector;
    typedef typename TMyVector::iterator TMyIterator;

    class IPortionSorter {
    public:
        virtual ~IPortionSorter() {
        }

        virtual void Sort(TMyVector&, TTmpOut*) = 0;
    };

    class TDefaultSorter: public IPortionSorter {
    public:
        void Sort(TMyVector& vector, TTmpOut* out) override {
            MBDB_SORT_FUN(vector.begin(), vector.end(), TCompare());

            const typename TMyVector::const_iterator
                end = (TIsSieveFake<TSieve>::Result) ? vector.end() : TDatSorterImplBase::SieveRange(vector.begin(), vector.end());

            for (typename TMyVector::const_iterator it = vector.begin(); it != end; ++it) {
                out->PushWithExtInfo(*it);
            }
        }
    };

    class TSegmentedSorter: public IPortionSorter {
        class TAdaptor {
            typedef typename TMyVector::const_iterator TConstIterator;

        public:
            TAdaptor(TConstIterator b, TConstIterator e)
                : Curr_(b)
                , End_(e)
            {
                --Curr_;
            }

            inline const TVal* Current() const {
                return *Curr_;
            }

            inline const TVal* Next() {
                ++Curr_;

                if (Curr_ == End_) {
                    return nullptr;
                }

                return *Curr_;
            }

        private:
            TConstIterator Curr_;
            TConstIterator End_;
        };

        typedef THeapIter<TVal, TAdaptor, TCompare> TPortionsHeap;

    public:
        void Sort(TMyVector& vector, TTmpOut* out) override {
            TVector<TAdaptor> bounds;
            typename TMyVector::iterator
                it = vector.begin();
            const size_t portions = Max<size_t>(1, (vector.size() * sizeof(TVal)) / (4 << 20));
            const size_t step = vector.size() / portions;

            // Sort segments
            while (it != vector.end()) {
                const typename TMyVector::iterator
                    end = Min(it + step, vector.end());

                MBDB_SORT_FUN(it, end, TCompare());

                bounds.push_back(TAdaptor(it, end));

                it = end;
            }

            //
            // Merge result
            //

            TPortionsHeap heap(bounds);

            if (TIsSieveFake<TSieve>::Result) {
                while (const TVal* val = heap.Next()) {
                    out->PushWithExtInfo(val);
                }
            } else {
                const TVal* val = heap.Next();
                const TVal* prev = out->PushWithExtInfo(val);

                for (val = heap.Next(); val && prev; val = heap.Next()) {
                    if (TSieve::Sieve((TVal*)prev, val)) {
                        continue;
                    }

                    prev = out->PushWithExtInfo(val);
                }

                if (prev) {
                    TSieve::Sieve((TVal*)prev, prev);
                }
            }
        }
    };

public:
    TDatSorterImplBase()
        : Sorter(new TDefaultSorter)
    {
        InFiles = nullptr;
        TempBuf = nullptr;
        Ptr = Vector.end();
        Cur = nullptr;
        Portions = CPortions = Error = 0;
    }

    ~TDatSorterImplBase() {
        Close();
    }

    int Open(const char* templ, size_t pagesize, size_t pages, int pagesOrBytes = 1) {
        Portions = CPortions = Error = 0;
        TempBuf = strdup(templ);
        Pagesize = pagesize;
        if (pagesOrBytes)
            Pages = pages;
        else
            Pages = pages / pagesize;
        Pages = Max(1, Pages);
        return 0;
    }

    void Push(const TVal* v) {
        // Serialized extInfo must follow a record being pushed, therefore, to avoid
        // unintentional misusage (as if when you are adding TExtInfo in your record
        // type: you may forget to check your sorting routines and get a segfault as
        // a result).
        // PushWithExtInfo(v) should be called on records with extInfo.
        static_assert(!TExtInfoType<TVal>::Exists, "expect !TExtInfoType<TVal>::Exists");

        Vector.push_back(v);
    }

    void PushWithExtInfo(const TVal* v) {
        Vector.push_back(v);
    }

    int SortPortion() {
        Ptr = Vector.end();
        Cur = nullptr;
        if (!Vector.size() || Error)
            return Error;

        MBDB_SORT_FUN(Vector.begin(), Vector.end(), TCompare());

        if (!TIsSieveFake<TSieve>::Result) {
            const typename TMyVector::iterator
                end = SieveRange(Vector.begin(), Vector.end());

            Vector.resize(end - Vector.begin());
        }

        Ptr = Vector.begin();
        Cur = nullptr;
        return 0;
    }

    const TVal* Nextp() {
        Cur = Ptr == Vector.end() ? nullptr : *Ptr++;
        return Cur;
    }

    const TVal* Currentp() const {
        return Cur;
    }

    void Closep() {
        Vector.clear();
        Ptr = Vector.end();
        Cur = nullptr;
    }

    int NextPortion(bool direct = false) {
        if (!Vector.size() || Error)
            return Error;

        TTmpOut out;
        int ret, ret1;
        char fname[FILENAME_MAX];

        snprintf(fname, sizeof(fname), TempBuf, Portions++);
        if ((ret = out.Open(fname, Pagesize, Pages, 1, direct)))
            return Error = ret;

        Sorter->Sort(Vector, &out);

        Vector.erase(Vector.begin(), Vector.end());
        ret = out.GetError();
        ret1 = out.Close();
        Error = Error ? Error : ret ? ret : ret1;
        if (Error)
            unlink(fname);
        return Error;
    }

    int SavePortions(const char* mask, bool direct = false) {
        char srcname[PATH_MAX], dstname[PATH_MAX];
        if (Vector.size())
            NextPortion(direct);
        for (int i = 0; i < Portions; i++) {
            char num[10];
            sprintf(num, "%i", i);
            snprintf(srcname, sizeof(srcname), TempBuf, i);
            snprintf(dstname, sizeof(dstname), mask, num);
            int res = rename(srcname, dstname);
            if (res)
                return res;
        }
        snprintf(dstname, sizeof(dstname), mask, "count");
        TOFStream fcount(dstname);
        Save(&fcount, Portions);
        fcount.Finish();
        return 0;
    }

    int RestorePortions(const char* mask) {
        char srcname[PATH_MAX], dstname[PATH_MAX];
        snprintf(srcname, sizeof(srcname), mask, "count");
        TIFStream fcount(srcname);
        Load(&fcount, Portions);
        for (int i = 0; i < Portions; i++) {
            char num[10];
            sprintf(num, "%i", i);
            snprintf(dstname, sizeof(dstname), TempBuf, i);
            snprintf(srcname, sizeof(srcname), mask, num);
            unlink(dstname);
            int res = link(srcname, dstname);
            if (res)
                return res;
        }
        return 0;
    }

    int RestorePortions(const char* mask, ui32 count) {
        char srcname[PATH_MAX], dstname[PATH_MAX];
        ui32 portions;
        TVector<ui32> counts;
        for (ui32 j = 0; j < count; j++) {
            snprintf(srcname, sizeof(srcname), mask, j, "count");
            TIFStream fcount(srcname);
            Load(&fcount, portions);
            counts.push_back(portions);
            Portions += portions;
        }
        ui32 p = 0;
        for (ui32 j = 0; j < count; j++) {
            int cnt = counts[j];
            for (int i = 0; i < cnt; i++, p++) {
                char num[10];
                sprintf(num, "%i", i);
                snprintf(dstname, sizeof(dstname), TempBuf, p);
                snprintf(srcname, sizeof(srcname), mask, j, num);
                unlink(dstname);
                int res = link(srcname, dstname);
                if (res) {
                    fprintf(stderr, "Can not link %s to %s\n", srcname, dstname);
                    return res;
                }
            }
        }
        return 0;
    }

    int Sort(size_t memory, int maxportions = 1000, bool direct = false) {
        int ret, end, beg, i;
        char fname[FILENAME_MAX];

        if (Vector.size())
            NextPortion();

        if (Error)
            return Error;
        if (!Portions) {
            TMyHeap::Init(&DummyFile, 1); // closed file
            HPages = 1;
            return 0;
        }

        Optimize(memory, maxportions);
        if (!(InFiles = new TTmpIn[MPortions]))
            return MBDB_NO_MEMORY;

        for (beg = 0; beg < Portions && !Error; beg = end) {
            end = (int)Min(beg + FPortions, Portions);
            for (i = beg; i < end && !Error; i++) {
                snprintf(fname, sizeof(fname), TempBuf, i);
                if ((ret = InFiles[i - beg].Open(fname, HPages, 1, nullptr, direct)))
                    Error = Error ? Error : ret;
            }
            if (Error)
                return Error;
            TMyHeap::Init(InFiles, end - beg);
            if (end != Portions) {
                TTmpOut out;
                const TVal* v;
                snprintf(fname, sizeof(fname), TempBuf, Portions++);
                if ((ret = out.Open(fname, Pagesize, HPages)))
                    return Error = Error ? Error : ret;
                while ((v = TMyHeap::Next()))
                    out.PushWithExtInfo(v);
                ret = out.GetError();
                Error = Error ? Error : ret;
                ret = out.Close();
                Error = Error ? Error : ret;
                for (i = beg; i < end; i++) {
                    ret = InFiles[i - beg].Close();
                    Error = Error ? Error : ret;
                    snprintf(fname, sizeof(fname), TempBuf, CPortions++);
                    unlink(fname);
                }
            }
            FPortions = MPortions;
        }
        return Error;
    }

    int Close() {
        char fname[FILENAME_MAX];
        delete[] InFiles;
        InFiles = nullptr;
        Closep();
        for (int i = CPortions; i < Portions; i++) {
            snprintf(fname, sizeof(fname), TempBuf, i);
            unlink(fname);
        }
        CPortions = Portions = 0;
        free(TempBuf);
        TempBuf = nullptr;
        return Error;
    }

    void UseSegmentSorter() {
        Sorter.Reset(new TSegmentedSorter);
    }

    inline int GetError() const {
        return Error;
    }

    inline int GetPages() const {
        return Pages;
    }

    inline int GetPageSize() const {
        return Pagesize;
    }

private:
    static TMyIterator SieveRange(const TMyIterator begin, const TMyIterator end) {
        TMyIterator it = begin;
        TMyIterator prev = begin;

        for (++it; it != end; ++it) {
            if (TSieve::Sieve((TVal*)*prev, *it)) {
                continue;
            }

            ++prev;

            if (it != prev) {
                *prev = *it;
            }
        }

        TSieve::Sieve((TVal*)*prev, *prev);

        return ++prev;
    }

protected:
    void Optimize(size_t memory, int maxportions, size_t fbufmax = 256u << 20) {
        maxportions = (int)Min((size_t)maxportions, memory / Pagesize) - 1;
        size_t maxpages = Max((size_t)1u, fbufmax / Pagesize);

        if (maxportions <= 2) {
            FPortions = MPortions = 2;
            HPages = 1;
            return;
        }
        if (maxportions >= Portions) {
            FPortions = MPortions = Portions;
            HPages = (int)Min(memory / ((Portions + 1) * Pagesize), maxpages);
            return;
        }
        if (((Portions + maxportions - 1) / maxportions) <= maxportions) {
            while (((Portions + maxportions - 1) / maxportions) <= maxportions)
                --maxportions;
            MPortions = ++maxportions;
            int total = ((Portions + maxportions - 1) / maxportions) + Portions;
            FPortions = (total % maxportions) ? (total % maxportions) : MPortions;
            HPages = (int)Min(memory / ((MPortions + 1) * Pagesize), maxpages);
            return;
        }
        FPortions = MPortions = maxportions;
        HPages = (int)Min(memory / ((MPortions + 1) * Pagesize), maxpages);
    }

    TMyVector Vector;
    typename TMyVector::iterator Ptr;
    const TVal* Cur;
    TTmpIn *InFiles, DummyFile;
    char* TempBuf;
    int Portions, CPortions, Pagesize, Pages, Error;
    int FPortions, MPortions, HPages;
    THolder<IPortionSorter> Sorter;
};

template <class TVal, class TCompare, typename TCompress>
class TDatSorterImpl<TVal, TCompare, TCompress, TFakeSieve<TVal>, TOutputPageFile, TDefInterFileTypes>
   : public TDatSorterImplBase<TVal, TCompare, TCompress, TFakeSieve<TVal>, TOutputPageFile, TDefInterFileTypes> {
    typedef TDatSorterImplBase<TVal, TCompare, TCompress, TFakeSieve<TVal>, TOutputPageFile, TDefInterFileTypes> TBase;

public:
    int SortToFile(const char* name, size_t memory, int maxportions = 1000) {
        int ret = TBase::Sort(memory, maxportions);
        if (ret)
            return ret;
        typename TBase::TOut out;
        if ((ret = out.Open(name, TBase::Pagesize, TBase::HPages)))
            return ret;
        const TVal* rec;
        while ((rec = Next()))
            out.PushWithExtInfo(rec);
        if ((ret = out.GetError()))
            return ret;
        if ((ret = out.Close()))
            return ret;
        if ((ret = TBase::Close()))
            return ret;
        return 0;
    }

    int SortToStream(TAutoPtr<IOutputStream> output, size_t memory, int maxportions = 1000) {
        int ret = TBase::Sort(memory, maxportions);
        if (ret)
            return ret;
        typename TBase::TOut out;
        if ((ret = out.Open(output, TBase::Pagesize, TBase::HPages)))
            return ret;
        const TVal* rec;
        while ((rec = Next()))
            out.PushWithExtInfo(rec);
        if ((ret = out.GetError()))
            return ret;
        if ((ret = out.Close()))
            return ret;
        if ((ret = TBase::Close()))
            return ret;
        return 0;
    }

    const TVal* Next() {
        return TBase::TMyHeap::Next();
    }

    const TVal* Current() const {
        return TBase::TMyHeap::Current();
    }

    bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const {
        return TBase::TMyHeap::GetExtInfo(extInfo);
    }

    const ui8* GetExtInfoRaw(size_t* len) const {
        return TBase::TMyHeap::GetExtInfoRaw(len);
    }
};

template <class TVal, class TCompare, typename TCompress, typename TSieve,
          typename TOutPageFile = TOutputPageFile, typename TFileTypes = TDefInterFileTypes>
class TDatSorterImpl: public TDatSorterImplBase<TVal, TCompare, TCompress, TSieve, TOutPageFile, TFileTypes> {
    typedef TDatSorterImplBase<TVal, TCompare, TCompress, TSieve, TOutPageFile, TFileTypes> TBase;

public:
    TDatSorterImpl()
        : Cur(nullptr)
        , Prev(nullptr)
    {
    }

    int SortToFile(const char* name, size_t memory, int maxportions = 1000) {
        int ret = Sort(memory, maxportions);
        if (ret)
            return ret;
        typename TBase::TOut out;
        if ((ret = out.Open(name, TBase::Pagesize, TBase::HPages)))
            return ret;
        const TVal* rec;
        while ((rec = Next()))
            out.PushWithExtInfo(rec);
        if ((ret = out.GetError()))
            return ret;
        if ((ret = out.Close()))
            return ret;
        if ((ret = TBase::Close()))
            return ret;
        return 0;
    }

    int SortToStream(TAutoPtr<IOutputStream> output, size_t memory, int maxportions = 1000) {
        int ret = Sort(memory, maxportions);
        if (ret)
            return ret;
        typename TBase::TOut out;
        if ((ret = out.Open(output, TBase::Pagesize, TBase::HPages)))
            return ret;
        const TVal* rec;
        while ((rec = Next()))
            out.PushWithExtInfo(rec);
        if ((ret = out.GetError()))
            return ret;
        if ((ret = out.Close()))
            return ret;
        if ((ret = TBase::Close()))
            return ret;
        return 0;
    }

    int Open(const char* templ, size_t pagesize, size_t pages, int pagesOrBytes = 1) {
        int res = TBase::Open(templ, pagesize, pages, pagesOrBytes);
        Prev = nullptr;
        Cur = nullptr;
        return res;
    }

    int Sort(size_t memory, int maxportions = 1000, bool direct = false) {
        int res = TBase::Sort(memory, maxportions, direct);
        if (!res) {
            const TVal* rec = TBase::TMyHeap::Next();
            if (rec) {
                size_t els, es;
                size_t sz = NMicroBDB::SizeOfExt(rec, &els, &es);
                sz += els + es;
                if (!TExtInfoType<TVal>::Exists)
                    Cur = (TVal*)malloc(sizeof(TVal));
                else
                    Cur = (TVal*)malloc(TBase::Pagesize);
                memcpy(Cur, rec, sz);
            }
        }
        return res;
    }

    // Prev = last returned
    // Cur = current accumlating with TSieve

    const TVal* Next() {
        if (!Cur) {
            if (Prev) {
                free(Prev);
                Prev = nullptr;
            }
            return nullptr;
        }
        const TVal* rec;

        if (TIsSieveFake<TSieve>::Result)
            rec = TBase::TMyHeap::Next();
        else {
            do {
                rec = TBase::TMyHeap::Next();
            } while (rec && TSieve::Sieve((TVal*)Cur, rec));
        }

        if (!Prev) {
            if (!TExtInfoType<TVal>::Exists)
                Prev = (TVal*)malloc(sizeof(TVal));
            else
                Prev = (TVal*)malloc(TBase::Pagesize);
        }
        size_t els, es;
        size_t sz = NMicroBDB::SizeOfExt(Cur, &els, &es);
        sz += els + es;
        memcpy(Prev, Cur, sz);

        if (rec) {
            sz = NMicroBDB::SizeOfExt(rec, &els, &es);
            sz += els + es;
            memcpy(Cur, rec, sz);
        } else {
            TSieve::Sieve((TVal*)Cur, Cur);
            free(Cur);
            Cur = nullptr;
        }
        return Prev;
    }

    const TVal* Current() const {
        return Prev;
    }

    int Close() {
        int res = TBase::Close();
        if (Prev) {
            free(Prev);
            Prev = nullptr;
        }
        if (Cur) {
            free(Cur);
            Cur = nullptr;
        }
        return res;
    }

protected:
    TVal* Cur;
    TVal* Prev;
};
