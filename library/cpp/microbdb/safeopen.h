#pragma once

// util
#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/string/util.h>
#include <util/system/mutex.h>
#include <thread>

#include "microbdb.h"

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4706) /*assignment within conditional expression*/
#pragma warning(disable : 4267) /*conversion from 'size_t' to 'type', possible loss of data*/
#endif

template <typename TVal, typename TPageFile = TInputPageFile, typename TIterator = TInputPageIterator<TPageFile>>
class TInDatFile: protected  TInDatFileImpl<TVal, TInputRecordIterator<TVal, TIterator>> {
public:
    typedef TVal TRec;
    typedef TInDatFileImpl<TVal, TInputRecordIterator<TVal, TIterator>> TBase;

    TInDatFile(const TString& name, size_t pages, int pagesOrBytes = 1)
        : Name(name)
        , Pages(pages)
        , PagesOrBytes(pagesOrBytes)
    {
    }

    ~TInDatFile() {
        Close();
    }

    void Open(const TString& fname, bool direct = false) {
        ui32 gotRecordSig = 0;
        int ret = TBase::Open(fname.data(), Pages, PagesOrBytes, &gotRecordSig, direct);
        if (ret) {
            // XXX: print record type name, not type sig
            ythrow yexception() << ErrorMessage(ret, "Failed to open input file", fname, TVal::RecordSig, gotRecordSig);
        }
        Name = fname;
    }

    void OpenStream(TAutoPtr<IInputStream> input) {
        ui32 gotRecordSig = 0;
        int ret = TBase::Open(input, Pages, PagesOrBytes, &gotRecordSig);
        if (ret) {
            // XXX: print record type name, not type sig
            ythrow yexception() << ErrorMessage(ret, "Failed to open input file", Name, TVal::RecordSig, gotRecordSig);
        }
    }

    void Close() {
        int ret;
        if (IsOpen() && (ret = TBase::GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing input file", Name);
        if ((ret = TBase::Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing input file", Name);
    }

    const char* GetName() const {
        return Name.data();
    }

    using TBase::Current;
    using TBase::Freeze;
    using TBase::GetError;
    using TBase::GetExtInfo;
    using TBase::GetExtInfoRaw;
    using TBase::GetExtSize;
    using TBase::GetLastPage;
    using TBase::GetPageNum;
    using TBase::GetPageSize;
    using TBase::GetRecSize;
    using TBase::GotoLastPage;
    using TBase::GotoPage;
    using TBase::IsEof;
    using TBase::IsOpen;
    using TBase::Next;
    using TBase::Skip;
    using TBase::Unfreeze;

protected:
    TString Name;
    size_t Pages;
    int PagesOrBytes;
};

template <typename TVal>
class TMappedInDatFile: protected  TInDatFileImpl<TVal, TInputRecordIterator<TVal, TMappedInputPageIterator<TMappedInputPageFile>>> {
public:
    typedef TVal TRec;
    typedef TInDatFileImpl<TVal, TInputRecordIterator<TVal, TMappedInputPageIterator<TMappedInputPageFile>>> TBase;

    TMappedInDatFile(const TString& name, size_t /* pages */, int /* pagesOrBytes */)
        : Name(name)
    {
    }

    ~TMappedInDatFile() {
        Close();
    }

    void Open(const TString& fname) {
        int ret = TBase::Open(fname.data());
        if (ret)
            ythrow yexception() << ErrorMessage(ret, "Failed to open mapped file", fname, TVal::RecordSig);
        Name = fname;
    }

    void Close() {
        int ret;
        if (IsOpen() && (ret = TBase::GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing mapped file", Name);
        if ((ret = TBase::Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing mapped file", Name);
    }

    const char* GetName() const {
        return Name.data();
    }

    using TBase::Current;
    using TBase::GetError;
    using TBase::GetExtInfo;
    using TBase::GetExtInfoRaw;
    using TBase::GetLastPage;
    using TBase::GetPageNum;
    using TBase::GetPageSize;
    using TBase::GotoLastPage;
    using TBase::GotoPage;
    using TBase::IsEof;
    using TBase::IsOpen;
    using TBase::Next;
    using TBase::Skip;

protected:
    TString Name;
};

template <typename TVal, typename TCompressor = TFakeCompression, typename TPageFile = TOutputPageFile>
class TOutDatFile: protected  TOutDatFileImpl<TVal, TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TFakeIndexer, TCompressor>> {
public:
    typedef TOutDatFileImpl<TVal, TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TFakeIndexer, TCompressor>> TBase;

    TOutDatFile(const TString& name, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : Name(name)
        , PageSize(pagesize)
        , Pages(pages)
        , PagesOrBytes(pagesOrBytes)
    {
    }

    ~TOutDatFile() {
        Close();
    }

    void Open(const char* fname, bool direct = false) {
        int ret = TBase::Open(fname, PageSize, Pages, PagesOrBytes, direct);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, "Failed to open output file", fname);
        Name = fname;
    }

    void Open(const TString& fname) {
        Open(fname.data());
    }

    void OpenStream(TAutoPtr<IOutputStream> output) {
        int ret = TBase::Open(output, PageSize, Pages, PagesOrBytes);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, "Failed to open output stream", Name);
    }

    void Close() {
        int ret;
        if ((ret = TBase::GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing output file", Name);
        if ((ret = TBase::Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing output file", Name);
    }

    const char* GetName() const {
        return Name.data();
    }

    using TBase::Freeze;
    using TBase::GetError;
    using TBase::GetPageSize;
    using TBase::IsEof;
    using TBase::IsOpen;
    using TBase::Offset;
    using TBase::Push;
    using TBase::PushWithExtInfo;
    using TBase::Reserve;
    using TBase::Unfreeze;

protected:
    TString Name;
    size_t PageSize, Pages;
    int PagesOrBytes;
};

template <typename TVal, typename TCompressor, typename TPageFile>
class TOutDatFileArray;

template <typename TVal, typename TCompressor = TFakeCompression, typename TPageFile = TOutputPageFile>
class TOutDatFileArray {
    typedef TOutDatFile<TVal, TCompressor, TPageFile> TFileType;

public:
    TOutDatFileArray(const TString& name, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : Name(name)
        , PageSize(pagesize)
        , Pages(pages)
        , PagesOrBytes(pagesOrBytes)
        , NumFiles(0)
        , Files(nullptr)
    {
    }

    ~TOutDatFileArray() {
        for (int i = 0; i < NumFiles; ++i) {
            Files[i].Close();
            Files[i].~TFileType();
        }
        free(Files);
        Files = nullptr;
        NumFiles = 0;
    }

    TFileType& operator[](size_t pos) {
        return Files[pos];
    }

    void Open(int n, const TString& fname) {
        char temp[FILENAME_MAX];

        Name = fname;
        NumFiles = CreateDatObjects(n, fname);

        int i;
        try {
            for (i = 0; i < NumFiles; ++i) {
                sprintf(temp, fname.data(), i);
                Files[i].Open(temp);
            }
        } catch (...) {
            while (--i >= 0)
                Files[i].Close();
            throw;
        }
    }

    template <typename TNameBuilder>
    void OpenWithCallback(int n, const TNameBuilder& builder) {
        NumFiles = CreateDatObjects(n, Name);

        for (int i = 0; i < NumFiles; ++i)
            Files[i].Open(builder.GetName(i).data());
    }

    void Close() {
        for (int i = 0; i < NumFiles; ++i)
            Files[i].Close();
    }

    void CloseMT(ui32 threads) {
        int current = 0;
        TMutex mutex;
        TVector<std::thread> thrs;
        thrs.reserve(threads);
        for (ui32 i = 0; i < threads; i++) {
            thrs.emplace_back([this, &current, &mutex]() {
                while (true) {
                    mutex.Acquire();
                    int cur = current++;
                    mutex.Release();
                    if (cur >= NumFiles)
                        break;
                    Files[cur].Close();
                }
            });
        }
        for (auto& thread : thrs) {
            thread.join();
        }
    }

    const char* GetName() const {
        return Name.data();
    }

protected:
    int CreateDatObjects(int n, const TString& fname) {
        if (!(Files = (TFileType*)malloc(n * sizeof(TFileType))))
            ythrow yexception() << "can't alloc \"" << fname << "\" file array: " << LastSystemErrorText();
        int num = 0;
        char temp[FILENAME_MAX];
        for (int i = 0; i < n; ++i, ++num) {
            sprintf(temp, "%s[%d]", fname.data(), i);
            new (Files + i) TFileType(temp, PageSize, Pages, PagesOrBytes);
        }
        return num;
    }

    TString Name;
    size_t PageSize, Pages;
    int PagesOrBytes, NumFiles;
    TFileType* Files;
};

template <typename TVal, typename TKey, typename TCompressor = TFakeCompression, typename TPageFile = TOutputPageFile>
class TOutDirectFile: protected  TOutDirectFileImpl<TVal, TKey, TCompressor, TPageFile> {
    typedef TOutDirectFileImpl<TVal, TKey, TCompressor, TPageFile> TBase;

public:
    TOutDirectFile(const TString& name, size_t pagesize, size_t pages, size_t ipagesize, size_t ipages, int pagesOrBytes)
        : Name(name)
        , PageSize(pagesize)
        , Pages(pages)
        , IdxPageSize(ipagesize)
        , IdxPages(ipages)
        , PagesOrBytes(pagesOrBytes)
    {
    }

    ~TOutDirectFile() {
        Close();
    }

    void Open(const TString& fname) {
        int ret = TBase::Open(fname.data(), PageSize, Pages, IdxPageSize, IdxPages, PagesOrBytes);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, "Failed to open output file", fname);
        Name = fname;
    }

    void Close() {
        int ret;
        if ((ret = TBase::GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing output file", Name);
        if ((ret = TBase::Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing output file", Name);
    }

    const char* GetName() const {
        return Name.data();
    }

    using TBase::Freeze;
    using TBase::Push;
    using TBase::PushWithExtInfo;
    using TBase::Reserve;
    using TBase::Unfreeze;

protected:
    TString Name;
    size_t PageSize, Pages, IdxPageSize, IdxPages;
    int PagesOrBytes;
};

template <
    typename TVal,
    template <typename T> class TComparer,
    typename TCompress = TFakeCompression,
    typename TSieve = TFakeSieve<TVal>,
    typename TPageFile = TOutputPageFile,
    typename TFileTypes = TDefInterFileTypes>
class TDatSorter: protected  TDatSorterImpl<TVal, TComparer<TVal>, TCompress, TSieve, TPageFile, TFileTypes> {
    typedef TDatSorterImpl<TVal, TComparer<TVal>, TCompress, TSieve, TPageFile, TFileTypes> TBase;

public:
    typedef TVal TRec;

public:
    TDatSorter(const TString& name, size_t memory, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : Name(name)
        , Memory(memory)
        , PageSize(pagesize)
        , Pages(pages)
        , PagesOrBytes(pagesOrBytes)
    {
        Templ[0] = 0;
    }

    ~TDatSorter() {
        Close();
        Templ[0] = 0;
    }

    void Open(const TString& dirName) {
        int ret;
        if (ret = MakeSorterTempl(Templ, dirName.data())) {
            Templ[0] = 0;
            ythrow yexception() << ErrorMessage(ret, Name + " sorter: bad tempdir", dirName);
        }
        if ((ret = TBase::Open(Templ, PageSize, Pages, PagesOrBytes)))
            ythrow yexception() << ErrorMessage(ret, Name + " sorter: open error, temp dir", Templ);
    }

    void Sort(bool direct = false) {
        int ret = TBase::Sort(Memory, 1000, direct);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, Name + " sorter: sort error, temp dir", Templ, TVal::RecordSig);
    }

    void SortToFile(const TString& name) {
        int ret = TBase::SortToFile(name.data(), Memory);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, Name + "sorter: error in SortToFile", name, TVal::RecordSig);
    }

    void SortToStream(TAutoPtr<IOutputStream> output) {
        int ret = TBase::SortToStream(output, Memory);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, Name + "sorter: error in SortToStream", "", TVal::RecordSig);
    }

    void Close() {
        int ret1 = TBase::GetError();
        int ret2 = TBase::Close();
        if (Templ[0]) {
            *strrchr(Templ, GetDirectorySeparator()) = 0;
            RemoveDirWithContents(Templ);
            Templ[0] = 0;
        }
        if (ret1)
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret1, Name + "sorter: error before closing");
        if (ret2)
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret2, Name + "sorter: error while closing");
    }

    int Sort(size_t memory, int maxportions, bool direct = false) {
        return TBase::Sort(memory, maxportions, direct);
    }

    const char* GetName() const {
        return Name.data();
    }

    using TBase::GetPageSize;
    using TBase::GetPages;
    using TBase::Next;
    using TBase::NextPortion;
    using TBase::Push;
    using TBase::PushWithExtInfo;
    using TBase::UseSegmentSorter;

protected:
    TString Name;
    size_t Memory, PageSize, Pages;
    int PagesOrBytes;
    char Templ[FILENAME_MAX];
};

template <typename TSorter>
class TSorterArray {
public:
    typedef TSorter TDatSorter;

public:
    TSorterArray(const TString& name, size_t memory, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : Name(name)
        , Memory(memory)
        , PageSize(pagesize)
        , Pages(pages)
        , PagesOrBytes(pagesOrBytes)
        , NumSorters(0)
        , Sorters(nullptr)
    {
    }

    ~TSorterArray() {
        for (int i = 0; i < NumSorters; ++i) {
            Sorters[i].Close();
            Sorters[i].~TSorter();
        }
        free(Sorters);
        Sorters = nullptr;
        NumSorters = 0;
    }

    TSorter& operator[](size_t pos) {
        return Sorters[pos];
    }

    void Open(int n, const TString& fname, size_t memory = 0) {
        if (!(Sorters = (TSorter*)malloc(n * sizeof(TSorter))))
            ythrow yexception() << "can't alloc \"" << fname << "\" sorter array: " << LastSystemErrorText();
        NumSorters = n;
        char temp[FILENAME_MAX];
        if (memory)
            Memory = memory;
        for (int i = 0; i < NumSorters; ++i) {
            sprintf(temp, "%s[%d]", Name.data(), i);
            new (Sorters + i) TSorter(temp, Memory, PageSize, Pages, PagesOrBytes);
        }
        for (int i = 0; i < NumSorters; ++i)
            Sorters[i].Open(fname);
    }

    void Close() {
        for (int i = 0; i < NumSorters; ++i)
            Sorters[i].Close();
    }

    const char* GetName() const {
        return Name.data();
    }

protected:
    TString Name;
    size_t Memory, PageSize, Pages;
    int PagesOrBytes, NumSorters;
    TSorter* Sorters;
};

template <typename TVal, template <typename T> class TCompare, typename TSieve = TFakeSieve<TVal>>
class TDatSorterArray: public TSorterArray<TDatSorter<TVal, TCompare, TSieve>> {
public:
    TDatSorterArray(const char* name, size_t memory, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : TSorterArray<TDatSorter<TVal, TCompare, TSieve>>(name, memory, pagesize, pages, pagesOrBytes)
    {
    }
};

template <typename TVal, template <typename T> class TCompare, typename TCompress = TFakeCompression,
          typename TSieve = TFakeSieve<TVal>, typename TPageFile = TOutputPageFile, typename TFileTypes = TDefInterFileTypes>
class TDatSorterMemo: public TDatSorter<TVal, TCompare, TCompress, TSieve, TPageFile, TFileTypes> {
    typedef TDatSorter<TVal, TCompare, TCompress, TSieve, TPageFile, TFileTypes> TSorter;

public:
    TOutDatFile<TVal> Memo;
    TString Home;
    bool OpenReq;
    bool Opened;
    bool UseDirectWrite;

public:
    TDatSorterMemo(const char* name, size_t memory, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : TSorter(name, memory, pagesize, pages, pagesOrBytes)
        , Memo(name, pagesize, memory, 0)
    {
        OpenReq = false;
        Opened = false;
        UseDirectWrite = false;
    }

    void Open(const TString& home) {
        OpenReq = true;
        //        TSorter::Open(home);
        Home = home;
        Memo.Open(nullptr);
        Memo.Freeze();
    }

    void Reopen(const char* home) {
        Close();
        Open(home);
    }

    void Open() {
        if (!OpenReq) {
            OpenReq = true;
            Memo.Open(nullptr);
            Memo.Freeze();
        }
    }

    void OpenIfNeeded() {
        if (OpenReq && !Opened) {
            if (!Home)
                ythrow yexception() << "Temp directory not specified, call Open(char*) first : " << TSorter::Name;
            TSorter::Open(Home);
            Opened = true;
        }
    }

    TVal* Reserve(size_t len) {
        if (TExtInfoType<TVal>::Exists)
            return ReserveWithExt(len, 0);

        TVal* u = Memo.Reserve(len);
        if (!u) {
            OpenIfNeeded();
            TSorter::NextPortion(UseDirectWrite);
            Memo.Freeze();
            u = Memo.Reserve(len);
        }
        TSorter::PushWithExtInfo(u);
        return u;
    }

    TVal* ReserveWithExt(size_t len, size_t extSize) {
        size_t fullLen = len + len_long((i64)extSize) + extSize;
        TVal* u = Memo.Reserve(fullLen);
        if (!u) {
            OpenIfNeeded();
            TSorter::NextPortion(UseDirectWrite);
            Memo.Freeze();
            u = Memo.Reserve(fullLen);
            if (!u) {
                if (fullLen > Memo.GetPageSize()) {
                    ythrow yexception() << "Size of element and " << len << " size of extInfo " << extSize
                                        << " is larger than page size " << Memo.GetPageSize();
                }
                ythrow yexception() << "going to insert a null pointer. Bad.";
            }
        }
        out_long((i64)extSize, (char*)u + len);
        TSorter::PushWithExtInfo(u);
        return u;
    }

    char* GetReservedExt(TVal* rec, size_t len, size_t extSize) {
        return (char*)rec + len + len_long((i64)extSize);
    }

    const TVal* Push(const TVal* v, const typename TExtInfoType<TVal>::TResult* extInfo = nullptr) {
        const TVal* u = Memo.Push(v, extInfo);
        if (!u) {
            OpenIfNeeded();
            TSorter::NextPortion(UseDirectWrite);
            Memo.Freeze();
            u = Memo.Push(v, extInfo);
            if (!u) {
                if (SizeOf(v) > Memo.GetPageSize()) {
                    ythrow yexception() << "Size of element " << SizeOf(v)
                                        << " is larger than page size " << Memo.GetPageSize();
                }
                ythrow yexception() << "going to insert a null pointer. Bad.";
            }
        }
        TSorter::PushWithExtInfo(u);
        return u;
    }

    const TVal* Push(const TVal* v, const ui8* extInfoRaw, size_t extLen) {
        const TVal* u = Memo.Push(v, extInfoRaw, extLen);
        if (!u) {
            OpenIfNeeded();
            TSorter::NextPortion(UseDirectWrite);
            Memo.Freeze();
            u = Memo.Push(v, extInfoRaw, extLen);
            if (!u) {
                if (SizeOf(v) > Memo.GetPageSize()) {
                    ythrow yexception() << "Size of element " << SizeOf(v)
                                        << " is larger than page size " << Memo.GetPageSize();
                }
                ythrow yexception() << "going to insert a null pointer. Bad..";
            }
        }
        TSorter::PushWithExtInfo(u);
        return u;
    }

    const TVal* PushWithExtInfo(const TVal* v) {
        const TVal* u = Memo.PushWithExtInfo(v);
        if (!u) {
            OpenIfNeeded();
            TSorter::NextPortion(UseDirectWrite);
            Memo.Freeze();
            u = Memo.PushWithExtInfo(v);
            if (!u) {
                if (SizeOf(v) > Memo.GetPageSize()) {
                    ythrow yexception() << "Size of element " << SizeOf(v)
                                        << " is larger than page size " << Memo.GetPageSize();
                }
                ythrow yexception() << "going to insert a null pointer. Bad...";
            }
        }
        TSorter::PushWithExtInfo(u);
        return u;
    }

    void Sort(bool direct = false) {
        if (Opened) {
            TSorter::NextPortion(UseDirectWrite);
            Memo.Close();
            OpenReq = false;
            TSorter::Sort(direct);
        } else {
            TSorter::SortPortion();
        }
    }

    const TVal* Next() {
        return Opened ? TSorter::Next() : TSorter::Nextp();
    }

    bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const {
        return NMicroBDB::GetExtInfo(Current(), extInfo);
    }

    const ui8* GetExtInfoRaw(size_t* len) const {
        return NMicroBDB::GetExtInfoRaw(Current(), len);
    }

    const TVal* Current() const {
        return Opened ? TSorter::Current() : TSorter::Currentp();
    }

    int NextPortion() {
        OpenIfNeeded();
        return TSorter::NextPortion(UseDirectWrite);
    }

    void SortToFile(const char* name) {
        OpenIfNeeded();
        TSorter::NextPortion(UseDirectWrite);
        Memo.Close();
        OpenReq = false;
        TSorter::SortToFile(name);
    }

    void SortToStream(TAutoPtr<IOutputStream> output) {
        OpenIfNeeded();
        TSorter::NextPortion(UseDirectWrite);
        Memo.Close();
        OpenReq = false;
        TSorter::SortToStream(output);
    }

    template <typename TKey, typename TOutCompress>
    void SortToDirectFile(const char* name, size_t ipagesize, size_t ipages) {
        Sort();
        TOutDirectFile<TVal, TKey, TOutCompress> out(TSorter::Name, TSorter::PageSize, TSorter::Pages, ipagesize, ipages, TSorter::PagesOrBytes);
        out.Open(name);
        while (const TVal* rec = Next())
            out.PushWithExtInfo(rec);
        out.Close();
    }

    template <typename TKey>
    void SortToDirectFile(const char* name, size_t ipagesize, size_t ipages) {
        SortToDirectFile<TKey, TCompress>(name, ipagesize, ipages);
    }

    void CloseSorter() {
        if (Opened)
            TSorter::Close();
        else
            TSorter::Closep();
        Memo.Freeze();
        Opened = false;
    }

    void Close() {
        if (Opened)
            TSorter::Close();
        else
            TSorter::Closep();
        Memo.Close();
        OpenReq = false;
        Opened = false;
    }

    int SavePortions(const char* mask) {
        return TSorter::SavePortions(mask, UseDirectWrite);
    }

public:
    using TSorter::RestorePortions;
};

template <typename TVal, template <typename T> class TCompare, typename TCompress = TFakeCompression,
          typename TSieve = TFakeSieve<TVal>, class TPageFile = TOutputPageFile, class TFileTypes = TDefInterFileTypes>
class TDatSorterMemoArray: public TSorterArray<TDatSorterMemo<TVal, TCompare, TCompress, TSieve, TPageFile, TFileTypes>> {
public:
    typedef TSorterArray<TDatSorterMemo<TVal, TCompare, TCompress, TSieve, TPageFile, TFileTypes>> TBase;

    TDatSorterMemoArray(const char* name, size_t memory, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : TBase(name, memory, pagesize, pages, pagesOrBytes)
    {
    }
};

#if defined(_MSC_VER)
#pragma warning(pop)
#endif
