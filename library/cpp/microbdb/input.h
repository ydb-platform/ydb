#pragma once

#include "header.h"
#include "file.h"
#include "reader.h"

#include <util/system/maxlen.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <thread>

#include <sys/uio.h>

#include <errno.h>

template <class TFileManip>
inline ssize_t Readv(TFileManip& fileManip, const struct iovec* iov, int iovcnt) {
    ssize_t read_count = 0;
    for (int n = 0; n < iovcnt; n++) {
        ssize_t last_read = fileManip.Read(iov[n].iov_base, iov[n].iov_len);
        if (last_read < 0)
            return -1;
        read_count += last_read;
    }
    return read_count;
}

template <class TVal, typename TBasePageIter>
class TInputRecordIterator: public TBasePageIter {
    typedef THolder<NMicroBDB::IBasePageReader<TVal>> TReaderHolder;

public:
    typedef TBasePageIter TPageIter;

    TInputRecordIterator() {
        Init();
    }

    ~TInputRecordIterator() {
        Term();
    }

    const TVal* Current() const {
        return Rec;
    }

    bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const {
        if (!Rec)
            return false;
        return Reader->GetExtInfo(extInfo);
    }

    const ui8* GetExtInfoRaw(size_t* len) const {
        if (!Rec)
            return nullptr;
        return Reader->GetExtInfoRaw(len);
    }

    size_t GetRecSize() const {
        return Reader->GetRecSize();
    }

    size_t GetExtSize() const {
        return Reader->GetExtSize();
    }

    const TVal* Next() {
        if (RecNum)
            --RecNum;
        else {
            TDatPage* page = TPageIter::Next();
            if (!page) {
                if (TPageIter::IsFrozen() && Reader.Get())
                    Reader->SetClearFlag();
                return Rec = nullptr;
            } else if (!!SelectReader())
                return Rec = nullptr;
            RecNum = TPageIter::Current()->RecNum - 1;
        }
        return Rec = Reader->Next();
    }

    // Skip(0) == Current(); Skip(1) == Next()
    const TVal* Skip(int& num) {
        // Y_ASSERT(num >= 0); ? otherwise it gets into infinite loop
        while (num > RecNum) {
            num -= RecNum + 1;
            if (!TPageIter::Next() || !!SelectReader()) {
                RecNum = 0;
                return Rec = nullptr;
            }
            RecNum = TPageIter::Current()->RecNum - 1;
            Rec = Reader->Next();
        }
        ++num;
        while (--num)
            Next();
        return Rec;
    }

    // begin reading from next page
    void Reset() {
        Rec = NULL;
        RecNum = 0;
        if (Reader.Get())
            Reader->Reset();
    }

protected:
    int Init() {
        Rec = nullptr;
        RecNum = 0;
        Format = MBDB_FORMAT_NULL;
        return 0;
    }

    int Term() {
        Reader.Reset(nullptr);
        Format = MBDB_FORMAT_NULL;
        Rec = nullptr;
        RecNum = 0;
        return 0;
    }

    const TVal* GotoPage(int pageno) {
        if (!TPageIter::GotoPage(pageno) || !!SelectReader())
            return Rec = nullptr;
        RecNum = TPageIter::Current()->RecNum - 1;
        return Rec = Reader->Next();
    }

    int SelectReader() {
        if (!TPageIter::Current())
            return MBDB_UNEXPECTED_EOF;
        if (ui32(Format) != TPageIter::Current()->Format) {
            switch (TPageIter::Current()->Format) {
                case MBDB_FORMAT_RAW:
                    Reader.Reset(new NMicroBDB::TRawPageReader<TVal, TPageIter>(this));
                    break;
                case MBDB_FORMAT_COMPRESSED:
                    Reader.Reset(new NMicroBDB::TCompressedReader<TVal, TPageIter>(this));
                    break;
                default:
                    return MBDB_NOT_SUPPORTED;
            }
            Format = EPageFormat(TPageIter::Current()->Format);
        } else {
            Y_ASSERT(Reader.Get() != nullptr);
            Reader->Reset();
        }
        return 0;
    }

    const TVal* Rec;
    TReaderHolder Reader;
    int RecNum; //!< number of records on the current page after the current record
    EPageFormat Format;
};

template <class TBaseReader>
class TInputPageIterator: public TBaseReader {
public:
    typedef TBaseReader TReader;

    TInputPageIterator()
        : Buf(nullptr)
    {
        Term();
    }

    ~TInputPageIterator() {
        Term();
    }

    TDatPage* Current() {
        return CurPage;
    }

    int Freeze() {
        return (Frozen = (PageNum == -1) ? 0 : PageNum);
    }

    void Unfreeze() {
        Frozen = -1;
    }

    inline int IsFrozen() const {
        return Frozen + 1;
    }

    inline size_t GetPageSize() const {
        return TReader::GetPageSize();
    }

    inline int GetPageNum() const {
        return PageNum;
    }

    inline int IsEof() const {
        return Eof;
    }

    TDatPage* Next() {
        if (PageNum >= Maxpage && ReadBuf()) {
            Eof = Eof ? Eof : TReader::IsEof();
            return CurPage = nullptr;
        }
        return CurPage = (TDatPage*)(Buf + ((++PageNum) % Bufpages) * GetPageSize());
    }

    TDatPage* GotoPage(int pageno) {
        if (pageno <= Maxpage && pageno >= (Maxpage - Pages + 1)) {
            PageNum = pageno;
            return CurPage = (TDatPage*)(Buf + (PageNum % Bufpages) * GetPageSize());
        }
        if (IsFrozen() || TReader::GotoPage(pageno))
            return nullptr;
        Maxpage = PageNum = pageno - 1;
        Eof = 0;
        return Next();
    }

protected:
    int Init(size_t pages, int pagesOrBytes) {
        Term();
        if (pagesOrBytes == -1)
            Bufpages = TReader::GetLastPage();
        else if (pagesOrBytes)
            Bufpages = pages;
        else
            Bufpages = pages / GetPageSize();
        if (!TReader::GetLastPage()) {
            Bufpages = 0;
            assert(Eof == 1);
            return 0;
        }
        int lastPage = TReader::GetLastPage();
        if (lastPage >= 0)
            Bufpages = (int)Min(lastPage, Bufpages);
        Bufpages = Max(2, Bufpages);
        Eof = 0;
        ABuf.Alloc(Bufpages * GetPageSize());
        return (Buf = ABuf.Begin()) ? 0 : ENOMEM;
        //        return (Buf = (char*)malloc(Bufpages * GetPageSize())) ? 0 : ENOMEM;
    }

    int Term() {
        //        free(Buf);
        ABuf.Dealloc();
        Buf = nullptr;
        Maxpage = PageNum = Frozen = -1;
        Bufpages = 0;
        Pages = 0;
        Eof = 1;
        CurPage = nullptr;
        return 0;
    }

    int ReadBuf() {
        int nvec;
        iovec vec[2];
        int maxpage = (Frozen == -1 ? Maxpage + 1 : Frozen) + Bufpages - 1;
        int minpage = Maxpage + 1;
        if (maxpage < minpage)
            return EAGAIN;
        minpage %= Bufpages;
        maxpage %= Bufpages;
        if (maxpage < minpage) {
            vec[0].iov_base = Buf + GetPageSize() * minpage;
            vec[0].iov_len = GetPageSize() * (Bufpages - minpage);
            vec[1].iov_base = Buf;
            vec[1].iov_len = GetPageSize() * (maxpage + 1);
            nvec = 2;
        } else {
            vec[0].iov_base = Buf + GetPageSize() * minpage;
            vec[0].iov_len = GetPageSize() * (maxpage - minpage + 1);
            nvec = 1;
        }
        TReader::ReadPages(vec, nvec, &Pages);
        Maxpage += Pages;
        return !Pages;
    }

    int Maxpage, PageNum, Frozen, Bufpages, Eof, Pages;
    TDatPage* CurPage;
    //    TMappedArray<char> ABuf;
    TMappedAllocation ABuf;
    char* Buf;
};

template <class TBaseReader>
class TInputPageIteratorMT: public TBaseReader {
public:
    typedef TBaseReader TReader;

    TInputPageIteratorMT()
        : CurBuf(0)
        , CurReadBuf(0)
        , Buf(nullptr)
    {
        Term();
    }

    ~TInputPageIteratorMT() {
        Term();
    }

    TDatPage* Current() {
        return CurPage;
    }

    int Freeze() {
        return (Frozen = (PageNum == -1) ? 0 : PageNum);
    }

    void Unfreeze() {
        Frozen = -1;
    }

    inline int IsFrozen() const {
        return Frozen + 1;
    }

    inline size_t GetPageSize() const {
        return TReader::GetPageSize();
    }

    inline int GetPageNum() const {
        return PageNum;
    }

    inline int IsEof() const {
        return Eof;
    }

    TDatPage* Next() {
        if (Eof)
            return CurPage = nullptr;
        if (PageNum >= Maxpage && ReadBuf()) {
            Eof = Eof ? Eof : TReader::IsEof();
            return CurPage = nullptr;
        }
        return CurPage = (TDatPage*)(Buf + ((++PageNum) % Bufpages) * GetPageSize());
    }

    TDatPage* GotoPage(int pageno) {
        if (pageno <= Maxpage && pageno >= (Maxpage - Pages + 1)) {
            PageNum = pageno;
            return CurPage = (TDatPage*)(Buf + (PageNum % Bufpages) * GetPageSize());
        }
        if (IsFrozen() || TReader::GotoPage(pageno))
            return nullptr;
        Maxpage = PageNum = pageno - 1;
        Eof = 0;
        return Next();
    }

    void ReadPages() {
        //        fprintf(stderr, "ReadPages started\n");
        bool eof = false;
        while (!eof) {
            QEvent[CurBuf].Wait();
            if (Finish)
                return;
            int pages = ReadCurBuf(Bufs[CurBuf]);
            PagesM[CurBuf] = pages;
            eof = !pages;
            AEvent[CurBuf].Signal();
            CurBuf ^= 1;
        }
    }

protected:
    int Init(size_t pages, int pagesOrBytes) {
        Term();
        if (pagesOrBytes == -1)
            Bufpages = TReader::GetLastPage();
        else if (pagesOrBytes)
            Bufpages = pages;
        else
            Bufpages = pages / GetPageSize();
        if (!TReader::GetLastPage()) {
            Bufpages = 0;
            assert(Eof == 1);
            return 0;
        }
        int lastPage = TReader::GetLastPage();
        if (lastPage >= 0)
            Bufpages = (int)Min(lastPage, Bufpages);
        Bufpages = Max(2, Bufpages);
        Eof = 0;
        ABuf.Alloc(Bufpages * GetPageSize() * 2);
        Bufs[0] = ABuf.Begin();
        Bufs[1] = Bufs[0] + Bufpages * GetPageSize();
        //        return (Buf = (char*)malloc(Bufpages * GetPageSize())) ? 0 : ENOMEM;
        Finish = false;
        ReadThread = std::thread([this]() {
            TThread::SetCurrentThreadName("DatReader");
            ReadPages();
        });
        QEvent[0].Signal();
        return Bufs[0] ? 0 : ENOMEM;
    }

    void StopThread() {
        Finish = true;
        QEvent[0].Signal();
        QEvent[1].Signal();
        ReadThread.join();
    }

    int Term() {
        //        free(Buf);
        if (ReadThread.joinable())
            StopThread();
        ABuf.Dealloc();
        Buf = nullptr;
        Bufs[0] = nullptr;
        Bufs[1] = nullptr;
        Maxpage = MaxpageR = PageNum = Frozen = -1;
        Bufpages = 0;
        Pages = 0;
        Eof = 1;
        CurPage = nullptr;
        return 0;
    }

    int ReadCurBuf(char* buf) {
        int nvec;
        iovec vec[2];
        int maxpage = (Frozen == -1 ? MaxpageR + 1 : Frozen) + Bufpages - 1;
        int minpage = MaxpageR + 1;
        if (maxpage < minpage)
            return EAGAIN;
        minpage %= Bufpages;
        maxpage %= Bufpages;
        if (maxpage < minpage) {
            vec[0].iov_base = buf + GetPageSize() * minpage;
            vec[0].iov_len = GetPageSize() * (Bufpages - minpage);
            vec[1].iov_base = buf;
            vec[1].iov_len = GetPageSize() * (maxpage + 1);
            nvec = 2;
        } else {
            vec[0].iov_base = buf + GetPageSize() * minpage;
            vec[0].iov_len = GetPageSize() * (maxpage - minpage + 1);
            nvec = 1;
        }
        int pages;
        TReader::ReadPages(vec, nvec, &pages);
        MaxpageR += pages;
        return pages;
    }

    int ReadBuf() {
        QEvent[CurReadBuf ^ 1].Signal();
        AEvent[CurReadBuf].Wait();
        Buf = Bufs[CurReadBuf];
        Maxpage += (Pages = PagesM[CurReadBuf]);
        CurReadBuf ^= 1;
        return !Pages;
    }

    int Maxpage, MaxpageR, PageNum, Frozen, Bufpages, Eof, Pages;
    TDatPage* CurPage;
    //    TMappedArray<char> ABuf;
    ui32 CurBuf;
    ui32 CurReadBuf;
    TMappedAllocation ABuf;
    char* Buf;
    char* Bufs[2];
    ui32 PagesM[2];
    TAutoEvent QEvent[2];
    TAutoEvent AEvent[2];
    std::thread ReadThread;
    bool Finish;
};

template <typename TFileManip>
class TInputPageFileImpl: private TNonCopyable {
protected:
    TFileManip FileManip;

public:
    TInputPageFileImpl()
        : Pagesize(0)
        , Fd(-1)
        , Eof(1)
        , Error(0)
        , Pagenum(0)
        , Recordsig(0)
    {
        Term();
    }

    ~TInputPageFileImpl() {
        Term();
    }

    inline int IsEof() const {
        return Eof;
    }

    inline int GetError() const {
        return Error;
    }

    inline size_t GetPageSize() const {
        return Pagesize;
    }

    inline int GetLastPage() const {
        return Pagenum;
    }

    inline ui32 GetRecordSig() const {
        return Recordsig;
    }

    inline bool IsOpen() const {
        return FileManip.IsOpen();
    }

protected:
    int Init(const char* fname, ui32 recsig, ui32* gotrecsig = nullptr, bool direct = false) {
        Error = FileManip.Open(fname, direct);
        return Error ? Error : Init(TFile(), recsig, gotrecsig);
    }

    int Init(const TFile& file, ui32 recsig, ui32* gotrecsig = nullptr) {
        if (!file.IsOpen() && !FileManip.IsOpen())
            return MBDB_NOT_INITIALIZED;
        if (file.IsOpen() && FileManip.IsOpen())
            return MBDB_ALREADY_INITIALIZED;
        if (file.IsOpen()) {
            Error = FileManip.Init(file);
            if (Error)
                return Error;
        }

        //        TArrayHolder<ui8> buf(new ui8[METASIZE + FS_BLOCK_SIZE]);
        //        ui8* ptr = (buf.Get() + FS_BLOCK_SIZE - ((ui64)buf.Get() & (FS_BLOCK_SIZE - 1)));
        TMappedArray<ui8> buf;
        buf.Create(METASIZE);
        ui8* ptr = &buf[0];
        TDatMetaPage* meta = (TDatMetaPage*)ptr;
        ssize_t size = METASIZE;
        ssize_t ret;
        while (size && (ret = FileManip.Read(ptr, (unsigned)size)) > 0) {
            Y_ASSERT(ret <= size);
            size -= ret;
            ptr += ret;
        }
        if (size) {
            FileManip.Close();
            return Error = MBDB_BAD_METAPAGE;
        }
        if (gotrecsig)
            *gotrecsig = meta->RecordSig;
        return Init(TFile(), meta, recsig);
    }

    int Init(TAutoPtr<IInputStream> input, ui32 recsig, ui32* gotrecsig = nullptr) {
        if (!input && !FileManip.IsOpen())
            return MBDB_NOT_INITIALIZED;
        if (FileManip.IsOpen())
            return MBDB_ALREADY_INITIALIZED;

        Error = FileManip.Open(input);
        if (Error)
            return Error;

        TArrayHolder<ui8> buf(new ui8[METASIZE]);
        ui8* ptr = buf.Get();
        ssize_t size = METASIZE;
        ssize_t ret;
        while (size && (ret = FileManip.Read(ptr, (unsigned)size)) > 0) {
            Y_ASSERT(ret <= size);
            size -= ret;
            ptr += ret;
        }
        if (size) {
            FileManip.Close();
            return Error = MBDB_BAD_METAPAGE;
        }
        TDatMetaPage* meta = (TDatMetaPage*)buf.Get();
        if (gotrecsig)
            *gotrecsig = meta->RecordSig;
        return Init(TFile(), meta, recsig);
    }

    int Init(const TFile& file, const TDatMetaPage* meta, ui32 recsig) {
        if (!file.IsOpen() && !FileManip.IsOpen())
            return MBDB_NOT_INITIALIZED;
        if (file.IsOpen() && FileManip.IsOpen())
            return MBDB_ALREADY_INITIALIZED;
        if (file.IsOpen()) {
            Error = FileManip.Init(file);
            if (Error)
                return Error;
        }

        if (meta->MetaSig != METASIG)
            Error = MBDB_BAD_METAPAGE;
        else if (meta->RecordSig != recsig)
            Error = MBDB_BAD_RECORDSIG;

        if (Error) {
            FileManip.Close();
            return Error;
        }

        i64 flength = FileManip.GetLength();
        if (flength >= 0) {
            i64 fsize = flength;
            fsize -= METASIZE;
            if (fsize % meta->PageSize)
                return Error = MBDB_BAD_FILE_SIZE;
            Pagenum = (int)(fsize / meta->PageSize);
        } else {
            Pagenum = -1;
        }
        Pagesize = meta->PageSize;
        Recordsig = meta->RecordSig;
        Error = Eof = 0;
        return Error;
    }

    int ReadPages(iovec* vec, int nvec, int* pages) {
        *pages = 0;

        if (Eof || Error)
            return Error;

        ssize_t size = 0, delta = 0, total = 0;
        iovec* pvec = vec;
        int vsize = nvec;

        while (vsize && (size = Readv(FileManip, pvec, (int)Min(vsize, 16))) > 0) {
            total += size;
            if (delta) {
                size += delta;
                pvec->iov_len += delta;
                pvec->iov_base = (char*)pvec->iov_base - delta;
                delta = 0;
            }
            while (size) {
                if ((size_t)size >= pvec->iov_len) {
                    size -= pvec->iov_len;
                    ++pvec;
                    --vsize;
                } else {
                    delta = size;
                    pvec->iov_len -= size;
                    pvec->iov_base = (char*)pvec->iov_base + size;
                    size = 0;
                }
            }
        }
        if (delta) {
            pvec->iov_len += delta;
            pvec->iov_base = (char*)pvec->iov_base - delta;
        }
        if (size < 0)
            return Error = errno ? errno : MBDB_READ_ERROR;
        if (total % Pagesize)
            return Error = MBDB_BAD_FILE_SIZE;
        if (vsize)
            Eof = 1;
        *pages = total / Pagesize; // it would be better to assign it after the for-loops
        for (; total; ++vec, total -= size)
            for (size = 0; size < total && (size_t)size < vec->iov_len; size += Pagesize)
                if (((TDatPage*)((char*)vec->iov_base + size))->PageSig != PAGESIG)
                    return Error = MBDB_BAD_PAGESIG;
        return Error;
    }

    int GotoPage(int page) {
        if (Error)
            return Error;
        Eof = 0;
        i64 offset = (i64)page * Pagesize + METASIZE;
        if (offset != FileManip.Seek(offset, SEEK_SET))
            Error = MBDB_BAD_FILE_SIZE;
        return Error;
    }

    int Term() {
        return FileManip.Close();
    }

    size_t Pagesize;
    int Fd;
    int Eof;
    int Error;
    int Pagenum; //!< number of pages in this file
    ui32 Recordsig;
};

template <class TBaseReader>
class TMappedInputPageIterator: public TBaseReader {
public:
    typedef TBaseReader TReader;

    TMappedInputPageIterator() {
        Term();
    }

    ~TMappedInputPageIterator() {
        Term();
    }

    TDatPage* Current() {
        return CurPage;
    }

    inline size_t GetPageSize() const {
        return TReader::GetPageSize();
    }

    inline int GetPageNum() const {
        return PageNum;
    }

    inline int IsEof() const {
        return Eof;
    }

    inline int IsFrozen() const {
        return 0;
    }

    TDatPage* Next() {
        i64 pos = (i64)(++PageNum) * GetPageSize() + METASIZE;
        if (pos < 0 || pos >= (i64)TReader::GetSize()) {
            Eof = 1;
            return CurPage = nullptr;
        }
        return CurPage = (TDatPage*)((char*)TReader::GetData() + pos);
    }

protected:
    int Init(size_t /*pages*/, int /*pagesOrBytes*/) {
        Term();
        Eof = 0;
        return 0;
    }

    int Term() {
        PageNum = -1;
        Eof = 1;
        CurPage = nullptr;
        return 0;
    }

    TDatPage* GotoPage(int pageno) {
        PageNum = pageno - 1;
        Eof = 0;
        return Next();
    }

    int PageNum, Eof, Pages, Pagenum;
    TDatPage* CurPage;
};

using TInputPageFile = TInputPageFileImpl<TInputFileManip>;

template <class TVal,
          typename TBaseRecIter = TInputRecordIterator<TVal, TInputPageIterator<TInputPageFile>>>
class TInDatFileImpl: public TBaseRecIter {
public:
    typedef TBaseRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TPageIter::TReader TReader;
    using TRecIter::GotoPage;

    int Open(const char* fname, size_t pages = 1, int pagesOrBytes = 1, ui32* gotRecordSig = nullptr, bool direct = false) {
        int ret = TReader::Init(fname, TVal::RecordSig, gotRecordSig, direct);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Open(const TFile& file, size_t pages = 1, int pagesOrBytes = 1, ui32* gotRecordSig = nullptr) {
        int ret = TReader::Init(file, TVal::RecordSig, gotRecordSig);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Open(TAutoPtr<IInputStream> input, size_t pages = 1, int pagesOrBytes = 1, ui32* gotRecordSig = nullptr) {
        int ret = TReader::Init(input, TVal::RecordSig, gotRecordSig);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Open(const TFile& file, const TDatMetaPage* meta, size_t pages = 1, int pagesOrBytes = 1) {
        int ret = TReader::Init(file, meta, TVal::RecordSig);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Close() {
        int ret1 = TRecIter::Term();
        int ret2 = TPageIter::Term();
        int ret3 = TReader::Term();
        return ret1 ? ret1 : ret2 ? ret2 : ret3;
    }

    const TVal* GotoLastPage() {
        return TReader::GetLastPage() <= 0 ? nullptr : TRecIter::GotoPage(TReader::GetLastPage() - 1);
    }

private:
    int Open2(size_t pages, int pagesOrBytes) {
        int ret = TPageIter::Init(pages, pagesOrBytes);
        if (!ret)
            ret = TRecIter::Init();
        if (ret)
            Close();
        return ret;
    }
};

template <class TVal>
class TInIndexFile: protected  TInDatFileImpl<TVal> {
    typedef TInDatFileImpl<TVal> TDatFile;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TExtInfoType<TVal>::TResult TExtInfo;

public:
    using TDatFile::IsOpen;

    TInIndexFile()
        : Index0(nullptr)
    {
    }

    int Open(const char* fname, size_t pages = 2, int pagesOrBytes = 1, ui32* gotRecordSig = nullptr) {
        int ret = TDatFile::Open(fname, pages, pagesOrBytes, gotRecordSig);
        if (ret)
            return ret;
        if (!(Index0 = (TDatPage*)malloc(TPageIter::GetPageSize()))) {
            TDatFile::Close();
            return MBDB_NO_MEMORY;
        }
        if (!TExtInfoType<TVal>::Exists && SizeOf((TVal*)nullptr))
            RecsOnPage = (TPageIter::GetPageSize() - sizeof(TDatPage)) / DatCeil(SizeOf((TVal*)nullptr));
        TDatFile::Next();
        memcpy(Index0, TPageIter::Current(), TPageIter::GetPageSize());
        return 0;
    }

    int Close() {
        free(Index0);
        Index0 = nullptr;
        return TDatFile::Close();
    }

    inline int GetError() const {
        return TDatFile::GetError();
    }

    int FindKey(const TVal* akey, const TExtInfo* extInfo = nullptr) {
        assert(IsOpen());
        if (TExtInfoType<TVal>::Exists || !SizeOf((TVal*)nullptr))
            return FindVszKey(akey, extInfo);
        int num = FindKeyOnPage(Index0, akey);
        TDatPage* page = TPageIter::GotoPage(num + 1);
        if (!page)
            return 0;
        num = FindKeyOnPage(page, akey);
        num += (TPageIter::GetPageNum() - 1) * RecsOnPage;
        return num;
    }

    int FindVszKey(const TVal* akey, const TExtInfo* extInfo = NULL) {
        int num = FindVszKeyOnPage(Index0, akey, extInfo);
        int num_add = 0;
        for (int p = 0; p < num; p++) {
            TDatPage* page = TPageIter::GotoPage(p + 1);
            if (!page)
                return 0;
            num_add += page->RecNum;
        }
        TDatPage* page = TPageIter::GotoPage(num + 1);
        if (!page)
            return 0;
        num = FindVszKeyOnPage(page, akey, extInfo);
        num += num_add;
        return num;
    }

protected:
    int FindKeyOnPage(TDatPage* page, const TVal* key) {
        int left = 0;
        int right = page->RecNum - 1;
        int recsize = DatCeil(SizeOf((TVal*)nullptr));
        while (left < right) {
            int middle = (left + right) >> 1;
            if (*((TVal*)((char*)page + sizeof(TDatPage) + middle * recsize)) < *key)
                left = middle + 1;
            else
                right = middle;
        }
        //borders check (left and right)
        return (left == 0 || *((TVal*)((char*)page + sizeof(TDatPage) + left * recsize)) < *key) ? left : left - 1;
    }

    // will deserialize rawExtinfoA to extInfoA only if necessery
    inline bool KeyLess_(const TVal* a, const TVal* b,
                         TExtInfo* extInfoA, const TExtInfo* extInfoB,
                         const ui8* rawExtInfoA, size_t rawLen) {
        if (*a < *b) {
            return true;
        } else if (!extInfoB || *b < *a) {
            return false;
        } else {
            // *a == *b && extInfoB
            Y_PROTOBUF_SUPPRESS_NODISCARD extInfoA->ParseFromArray(rawExtInfoA, rawLen);
            return (*extInfoA < *extInfoB);
        }
    }

    int FindVszKeyOnPage(TDatPage* page, const TVal* key, const TExtInfo* extInfo) {
        TVal* cur = (TVal*)((char*)page + sizeof(TDatPage));
        ui32 recnum = page->RecNum;
        if (!TExtInfoType<TVal>::Exists) {
            for (; recnum > 0 && *cur < *key; --recnum)
                cur = (TVal*)((char*)cur + DatCeil(SizeOf(cur)));
        } else {
            size_t ll;
            size_t l;
            size_t sz = NMicroBDB::SizeOfExt(cur, &ll, &l);
            TExtInfo ei;
            for (; recnum > 0 && KeyLess_(cur, key, &ei, extInfo, (ui8*)cur + sz + ll, l); --recnum) {
                cur = (TVal*)((ui8*)cur + DatCeil(sz + ll + l));
                sz = NMicroBDB::SizeOfExt(cur, &ll, &l);
            }
        }

        int idx = page->RecNum - recnum - 1;
        return (idx >= 0) ? idx : 0;
    }

    TDatPage* Index0;
    int RecsOnPage;
};

template <class TVal, class TKey, class TPageIterator = TInputPageIterator<TInputPageFile>>
class TKeyFileMixin: public TInDatFileImpl<TVal, TInputRecordIterator<TVal, TPageIterator>> {
protected:
    TInIndexFile<TKey> KeyFile;
};

template <class TVal, class TKey, class TBase = TKeyFileMixin<TVal, TKey>>
class TDirectInDatFile: public TBase {
    typedef TBase TDatFile;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TDatFile::TPageIter TPageIter;

public:
    void Open(const char* path, size_t pages = 1, size_t keypages = 1, int pagesOrBytes = 1) {
        int ret;
        ui32 gotRecordSig = 0;

        ret = TDatFile::Open(path, pages, pagesOrBytes, &gotRecordSig);
        if (ret) {
            ythrow yexception() << ErrorMessage(ret, "Failed to open input file", path, TVal::RecordSig, gotRecordSig);
        }
        char KeyName[PATH_MAX + 1];
        if (DatNameToIdx(KeyName, path)) {
            ythrow yexception() << ErrorMessage(MBDB_BAD_FILENAME, "Failed to open input file", path);
        }
        gotRecordSig = 0;
        ret = KeyFile.Open(KeyName, keypages, 1, &gotRecordSig);
        if (ret) {
            ythrow yexception() << ErrorMessage(ret, "Failed to open input keyfile", KeyName, TKey::RecordSig, gotRecordSig);
        }
    }

    void Close() {
        int ret;

        if (TDatFile::IsOpen() && (ret = TDatFile::GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing input file");
        if ((ret = TDatFile::Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing input file");

        if (KeyFile.IsOpen() && (ret = KeyFile.GetError()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error before closing input keyfile");
        if ((ret = KeyFile.Close()))
            if (!std::uncaught_exception())
                ythrow yexception() << ErrorMessage(ret, "Error while closing input keyfile");
    }

    const TVal* FindRecord(const TKey* key, const typename TExtInfoType<TKey>::TResult* extInfo = nullptr) {
        int page = KeyFile.FindKey(key, extInfo);
        const TVal* val = TRecIter::GotoPage(page);
        if (!TExtInfoType<TVal>::Exists || !extInfo) {
            TKey k;
            while (val) {
                TMakeExtKey<TVal, TKey>::Make(&k, nullptr, val, nullptr);
                if (!(k < *key))
                    break;
                val = TRecIter::Next();
            }
        } else {
            typename TExtInfoType<TVal>::TResult valExt;
            TKey k;
            typename TExtInfoType<TKey>::TResult kExt;
            while (val) {
                TRecIter::GetExtInfo(&valExt);
                TMakeExtKey<TVal, TKey>::Make(&k, &kExt, val, &valExt);
                if (*key < k || !(k < *key) && !(kExt < *extInfo)) //  k > *key || k == *key && kExt >= *extInfo
                    break;
                val = TRecIter::Next();
            }
        }
        return val;
    }

    int FindPagesNo(const TKey* key, const typename TExtInfoType<TVal>::TResult* extInfo = NULL) {
        return KeyFile.FindKey(key, extInfo);
    }

protected:
    using TBase::KeyFile;
};
