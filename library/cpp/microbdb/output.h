#pragma once

#include "header.h"
#include "file.h"

#include <util/generic/buffer.h>
#include <util/memory/tempbuf.h>

#include <sys/uio.h>

template <class TFileManip>
inline ssize_t Writev(TFileManip& fileManip, const struct iovec* iov, int iovcnt) {
    ssize_t written_count = 0;
    for (int n = 0; n < iovcnt; n++) {
        ssize_t last_write = fileManip.Write(iov[n].iov_base, iov[n].iov_len);
        if (last_write < 0)
            return -1;
        written_count += last_write;
    }
    return written_count;
}

//*********************************************************************
struct TFakeIndexer {
    inline void NextPage(TDatPage*) noexcept {
    }
};

struct TCallbackIndexer {
    typedef void (*TCallback)(void* This, const TDatPage* page);

    TCallbackIndexer() {
        Callback = nullptr;
    }

    void SetCallback(void* t, TCallback c) {
        This = t;
        Callback = c;
    }

    void NextPage(TDatPage* dat) {
        Callback(This, dat);
    }

    TCallback Callback;
    void* This;
};

template <class TVal, typename TBasePageIter, typename TBaseIndexer = TFakeIndexer, typename TCompressor = TFakeCompression>
class TOutputRecordIterator;

template <class TVal, typename TBasePageIter, typename TBaseIndexer>
class TOutputRecordIterator<TVal, TBasePageIter, TBaseIndexer, TFakeCompression>
   : public TBasePageIter, public TBaseIndexer {
public:
    enum EOffset {
        WrongOffset = size_t(-1)
    };

    typedef TBasePageIter TPageIter;
    typedef TBaseIndexer TIndexer;

    TOutputRecordIterator() {
        Clear();
    }

    ~TOutputRecordIterator() {
        Term();
    }

    inline const TVal* Current() const {
        return Rec;
    }

    const TVal* Push(const TVal* v, const typename TExtInfoType<TVal>::TResult* extInfo = nullptr) {
        NMicroBDB::AssertValid(v);
        size_t len = SizeOf(v);
        if (!TExtInfoType<TVal>::Exists)
            return (Reserve(len)) ? (TVal*)memcpy(Rec, v, len) : nullptr;
        else if (extInfo) {
            size_t extSize = extInfo->ByteSize();
            size_t extLenSize = len_long((i64)extSize);
            if (!Reserve(len + extLenSize + extSize))
                return nullptr;
            memcpy(Rec, v, len);
            out_long((i64)extSize, (char*)Rec + len);
            extInfo->SerializeWithCachedSizesToArray((ui8*)Rec + len + extLenSize);
            return Rec;
        } else {
            size_t extLenSize = len_long((i64)0);
            if (!Reserve(len + extLenSize))
                return nullptr;
            memcpy(Rec, v, len);
            out_long((i64)0, (char*)Rec + len);
            return Rec;
        }
    }

    const TVal* Push(const TVal* v, const ui8* extInfoRaw, size_t extLen) {
        NMicroBDB::AssertValid(v);
        size_t sz = SizeOf(v);
        if (!Reserve(sz + extLen))
            return nullptr;
        memcpy(Rec, v, sz);
        memcpy((ui8*)Rec + sz, extInfoRaw, extLen);
        return Rec;
    }

    // use values stored in microbdb readers/writers internal buffer only.
    // method expects serialized extInfo after this record
    const TVal* PushWithExtInfo(const TVal* v) {
        NMicroBDB::AssertValid(v);
        size_t extSize;
        size_t extLenSize;
        size_t sz = NMicroBDB::SizeOfExt(v, &extLenSize, &extSize);
        sz += extLenSize + extSize;
        if (!Reserve(sz))
            return nullptr;
        memcpy(Rec, v, sz);
        return Rec;
    }

    TVal* Reserve(size_t len) {
        if (CurLen + DatCeil(len) > TPageIter::GetPageSize()) {
            if (sizeof(TDatPage) + DatCeil(len) > TPageIter::GetPageSize())
                return Rec = nullptr;
            if (TPageIter::Current() && RecNum) {
                TPageIter::Current()->RecNum = RecNum;
                TPageIter::Current()->Format = MBDB_FORMAT_RAW;
                memset((char*)TPageIter::Current() + CurLen, 0, TPageIter::GetPageSize() - CurLen);
                TIndexer::NextPage(TPageIter::Current());
                RecNum = 0;
            }
            if (!TPageIter::Next()) {
                CurLen = TPageIter::GetPageSize();
                return Rec = nullptr;
            }
            CurLen = sizeof(TDatPage);
        }
        LenForOffset = CurLen;
        Rec = (TVal*)((char*)TPageIter::Current() + CurLen);
        DatSet(Rec, len);

        CurLen += DatCeil(len);

        ++RecNum;
        return Rec;
    }

    void Flush() {
        TPageIter::Current()->RecNum = RecNum;
        TPageIter::Current()->Format = MBDB_FORMAT_RAW;
    }

    size_t Offset() const {
        return Rec ? TPageIter::Offset() + LenForOffset : WrongOffset;
    }

    void ResetDat() {
        CurLen = (char*)Rec - (char*)TPageIter::Current();
        size_t len;
        if (!TExtInfoType<TVal>::Exists) {
            len = SizeOf(Rec);
        } else {
            size_t ll;
            size_t l;
            len = NMicroBDB::SizeOfExt(Rec, &ll, &l);
            len += ll + l;
        }
        CurLen += DatCeil(len);
    }

protected:
    void Clear() {
        Rec = nullptr;
        RecNum = 0;
        CurLen = 0;
        LenForOffset = 0;
    }

    int Init() {
        Clear();
        CurLen = TPageIter::GetPageSize();
        return 0;
    }

    int Term() {
        if (TPageIter::Current()) {
            TPageIter::Current()->RecNum = RecNum;
            TPageIter::Current()->Format = MBDB_FORMAT_RAW;
            memset((char*)TPageIter::Current() + CurLen, 0, TPageIter::GetPageSize() - CurLen);
            RecNum = 0;
        }
        int ret = !TPageIter::Current() && RecNum;
        Clear();
        return ret;
    }

    int GotoPage(int pageno) {
        if (TPageIter::Current()) {
            TPageIter::Current()->RecNum = RecNum;
            TPageIter::Current()->Format = MBDB_FORMAT_RAW;
            memset((char*)TPageIter::Current() + CurLen, 0, TPageIter::GetPageSize() - CurLen);
        }
        int ret = TPageIter::GotoPage(pageno);
        if (!ret) {
            RecNum = 0;
            CurLen = sizeof(TDatPage);
        }
        return ret;
    }

    TVal* Rec;
    int RecNum;
    size_t CurLen;
    size_t LenForOffset;
};

template <class TVal, typename TBasePageIter, typename TBaseIndexer, typename TAlgorithm>
class TOutputRecordIterator
   : public TBasePageIter,
      public TBaseIndexer,
      private TAlgorithm {
    class TPageBuffer {
    public:
        void Init(size_t page) {
            Pos = 0;
            RecNum = 0;
            Size = Min(page / 2, size_t(64 << 10));
            Data.Reset(new ui8[Size]);
        }

        void Clear() {
            Pos = 0;
            RecNum = 0;
        }

        inline bool Empty() const {
            return RecNum == 0;
        }

    public:
        size_t Size;
        size_t Pos;
        int RecNum;
        TArrayHolder<ui8> Data;
    };

public:
    typedef TBasePageIter TPageIter;
    typedef TBaseIndexer TIndexer;

    TOutputRecordIterator()
        : Rec(nullptr)
        , RecNum(0)
    {
    }

    ~TOutputRecordIterator() {
        Term();
    }

    const TVal* Current() const {
        return Rec;
    }

    const TVal* Push(const TVal* v, const typename TExtInfoType<TVal>::TResult* extInfo = nullptr) {
        NMicroBDB::AssertValid(v);
        size_t len = SizeOf(v);
        if (!TExtInfoType<TVal>::Exists)
            return (Reserve(len)) ? (TVal*)memcpy((TVal*)Rec, v, len) : nullptr;
        else if (extInfo) {
            size_t extSize = extInfo->ByteSize();
            size_t extLenSize = len_long((i64)extSize);
            if (!Reserve(len + extLenSize + extSize))
                return nullptr;
            memcpy(Rec, v, len);
            out_long((i64)extSize, (char*)Rec + len);
            extInfo->SerializeWithCachedSizesToArray((ui8*)Rec + len + extLenSize);
            return Rec;
        } else {
            size_t extLenSize = len_long((i64)0);
            if (!Reserve(len + extLenSize))
                return nullptr;
            memcpy(Rec, v, len);
            out_long((i64)0, (char*)Rec + len);
            return Rec;
        }
    }

    const TVal* Push(const TVal* v, const ui8* extInfoRaw, size_t extLen) {
        NMicroBDB::AssertValid(v);
        size_t sz = SizeOf(v);
        if (!Reserve(sz + extLen))
            return NULL;
        memcpy(Rec, v, sz);
        memcpy((ui8*)Rec + sz, extInfoRaw, extLen);
        return Rec;
    }

    // use values stored in microbdb readers/writers internal buffer only.
    // method expects serialized extInfo after this record
    const TVal* PushWithExtInfo(const TVal* v) {
        NMicroBDB::AssertValid(v);
        size_t extSize;
        size_t extLenSize;
        size_t sz = NMicroBDB::SizeOfExt(v, &extLenSize, &extSize);
        sz += extLenSize + extSize;
        if (!Reserve(sz))
            return nullptr;
        memcpy(Rec, v, sz);
        return Rec;
    }

    TVal* Reserve(const size_t len) {
        const size_t aligned = DatCeil(len);

        if (!TPageIter::Current()) { // Allocate fist page
            if (!TPageIter::Next()) {
                CurLen = TPageIter::GetPageSize();
                return Rec = nullptr;
            }
            CurLen = sizeof(TDatPage) + sizeof(TCompressedPage);
        }

        if (Buffer.Pos + aligned > Buffer.Size) {
            if (Buffer.Pos == 0)
                return Rec = nullptr;
            if (FlushBuffer())
                return Rec = nullptr;
            if (Buffer.Pos + aligned + sizeof(TDatPage) + sizeof(TCompressedPage) > Buffer.Size)
                return Rec = nullptr;
        }

        Rec = (TVal*)((char*)Buffer.Data.Get() + Buffer.Pos);
        DatSet(Rec, len); // len is correct because DatSet set align tail to zero

        Buffer.RecNum++;
        Buffer.Pos += aligned;
        ++RecNum;
        return Rec;
    }

    void Flush() {
        if (!Buffer.Empty()) {
            FlushBuffer();
            TPageIter::Current()->RecNum = RecNum;
            TPageIter::Current()->Format = MBDB_FORMAT_COMPRESSED;
        }
    }

    size_t Offset() const {
        // According to vadya@ there is no evil to return 0 all the time
        return 0;
    }

    void ResetDat() {
        Buffer.Pos = (char*)Rec - (char*)Buffer.Data.Get();
        size_t len = SizeOf(Rec);
        Buffer.Pos += DatCeil(len);
    }

protected:
    void Clear() {
        RecNum = 0;
        Rec = nullptr;
        Count = 0;
        CurLen = sizeof(TDatPage) + sizeof(TCompressedPage);
        Buffer.Clear();
    }

    int Init() {
        Clear();
        Buffer.Init(TPageIter::GetPageSize());
        TAlgorithm::Init();
        return 0;
    }

    int Term() {
        if (TPageIter::Current())
            Commit();
        int ret = !TPageIter::Current() && RecNum;
        Clear();
        TAlgorithm::Term();
        return ret;
    }

    int GotoPage(int pageno) {
        if (TPageIter::Current())
            Commit();
        int ret = TPageIter::GotoPage(pageno);
        if (!ret)
            Reset();
        return ret;
    }

private:
    void Commit() {
        Flush();
        TPageIter::Current()->RecNum = RecNum;
        TPageIter::Current()->Format = MBDB_FORMAT_COMPRESSED;
        SetCompressedPageHeader();

        memset((char*)TPageIter::Current() + CurLen, 0, TPageIter::GetPageSize() - CurLen);
        RecNum = 0;
        Count = 0;
    }

    inline void SetCompressedPageHeader() {
        TCompressedPage* const hdr = (TCompressedPage*)((ui8*)TPageIter::Current() + sizeof(TDatPage));

        hdr->BlockCount = Count;
        hdr->Algorithm = TAlgorithm::Code;
        hdr->Version = 0;
        hdr->Reserved = 0;
    }

    inline void Reset() {
        RecNum = 0;
        CurLen = sizeof(TDatPage) + sizeof(TCompressedPage);
        Count = 0;
        Buffer.Clear();
    }

    int FlushBuffer() {
        TArrayHolder<ui8> data;
        const ui8* const buf = Buffer.Data.Get();
        size_t first = 0;

        if (!TExtInfoType<TVal>::Exists)
            first = DatCeil(SizeOf((TVal*)buf));
        else {
            size_t ll;
            size_t l;
            first = NMicroBDB::SizeOfExt((const TVal*)buf, &ll, &l);
            first = DatCeil(first + ll + l);
        }

        size_t total = sizeof(NMicroBDB::TCompressedHeader) + first + ((Buffer.RecNum == 1) ? 0 : TAlgorithm::CompressBound(Buffer.Pos - first));
        size_t real = total;

        {
            ui8* p = nullptr;
            NMicroBDB::TCompressedHeader* hdr = nullptr;

            // 1. Choose data destination (temporary buffer or dat-page)
            if (CurLen + total > TPageIter::GetPageSize()) {
                data.Reset(new ui8[total]);

                hdr = (NMicroBDB::TCompressedHeader*)data.Get();
                p = data.Get() + sizeof(NMicroBDB::TCompressedHeader);
            } else {
                p = (ui8*)TPageIter::Current() + CurLen;
                hdr = (NMicroBDB::TCompressedHeader*)p;
                p += sizeof(NMicroBDB::TCompressedHeader);
            }

            // 2. Compress data

            // Fill header and first record
            hdr->Original = Buffer.Pos;
            hdr->Compressed = 0;
            hdr->Count = Buffer.RecNum;
            hdr->Reserved = 0;
            memcpy(p, Buffer.Data.Get(), first);
            // Fill compressed part
            if (Buffer.RecNum > 1) {
                size_t size = TAlgorithm::CompressBound(Buffer.Pos - first);

                p += first;
                TAlgorithm::Compress(p, size, buf + first, Buffer.Pos - first);

                hdr->Compressed = size;

                real = sizeof(NMicroBDB::TCompressedHeader) + first + size;
            }
        }

        Y_ASSERT(sizeof(TDatPage) + sizeof(TCompressedPage) + real <= TPageIter::GetPageSize());

        // 3. Check page capacity

        if (CurLen + real > TPageIter::GetPageSize()) {
            Y_ASSERT(data.Get() != nullptr);

            if (TPageIter::Current() && RecNum) {
                RecNum = RecNum - Buffer.RecNum;
                TPageIter::Current()->RecNum = RecNum;
                TPageIter::Current()->Format = MBDB_FORMAT_COMPRESSED;
                SetCompressedPageHeader();
                memset((char*)TPageIter::Current() + CurLen, 0, TPageIter::GetPageSize() - CurLen);
                TIndexer::NextPage(TPageIter::Current());
                RecNum = Buffer.RecNum;
                Count = 0;
            }
            if (!TPageIter::Next()) {
                CurLen = TPageIter::GetPageSize();
                return MBDB_NO_MEMORY;
            }
            CurLen = sizeof(TDatPage) + sizeof(TCompressedPage);
        }

        // 4. Flush data and reset buffer state

        if (data.Get())
            memcpy((ui8*)TPageIter::Current() + CurLen, data.Get(), real);
        CurLen += real;
        ++Count;
        Buffer.Clear();
        return 0;
    }

private:
    size_t CurLen;
    TPageBuffer Buffer;
    TVal* Rec;
    ui32 Count; //! < count of compressed blocks on page
public:
    int RecNum;
};

template <typename TBaseWriter>
class TOutputPageIterator: public TBaseWriter {
public:
    typedef TBaseWriter TWriter;

    TOutputPageIterator()
        : Buf(nullptr)
    {
        Clear();
    }

    ~TOutputPageIterator() {
        Term();
    }

    TDatPage* Current() {
        return CurPage;
    }

    size_t Offset() const {
        //Cout << "PS = " << TWriter::GetPageSize() << "; PN = " << PageNum << "; MS = " << METASIZE << Endl;
        return TWriter::GetPageSize() * PageNum + METASIZE;
    }

    int Freeze() {
        return (Frozen = (PageNum == -1) ? 0 : (int)PageNum);
    }

    void Unfreeze() {
        Frozen = -1;
    }

    inline int IsFrozen() const {
        return Frozen + 1;
    }

    inline size_t GetPageSize() const {
        return TWriter::GetPageSize();
    }

    inline int GetPageNum() const {
        return (int)PageNum;
    }

    TDatPage* Next() {
        if (PageNum >= Maxpage && WriteBuf())
            return CurPage = nullptr;
        CurPage = (TDatPage*)(Buf + ((++PageNum) % Bufpages) * GetPageSize());
        memset(CurPage, 0, sizeof(TDatPage));
        return CurPage;
    }

protected:
    int Init(size_t pages, int pagesOrBytes) {
        Term();
        if (pagesOrBytes)
            Bufpages = pages;
        else
            Bufpages = pages / GetPageSize();
        Bufpages = Max<size_t>(1, Bufpages);
        Maxpage = Bufpages - 1;
        //        if (!(Buf = (char*)malloc(Bufpages * GetPageSize())))
        //            return ENOMEM;
        ABuf.Alloc(Bufpages * GetPageSize());
        Buf = ABuf.Begin();
        if (TWriter::Memo)
            Freeze();
        return 0;
    }

    int Term() {
        Unfreeze();
        int ret = (PageNum < 0) ? 0 : WriteBuf();
        Clear();
        return ret;
    }

    int GotoPage(int pageno) {
        int ret = EAGAIN;
        if (IsFrozen() || PageNum >= 0 && ((ret = WriteBuf())) || ((ret = TWriter::GotoPage(pageno))))
            return ret;
        PageNum = pageno;
        Maxpage = Bufpages - 1 + pageno;
        CurPage = (TDatPage*)(Buf + (PageNum % Bufpages) * GetPageSize());
        memset(CurPage, 0, sizeof(TDatPage));
        return 0;
    }

    void Clear() {
        ABuf.Dealloc();
        Buf = nullptr;
        Maxpage = PageNum = Frozen = -1;
        Bufpages = 0;
        CurPage = nullptr;
    }

    int WriteBuf() {
        int nvec;
        iovec vec[2];
        ssize_t minpage = Maxpage - Bufpages + 1;
        ssize_t maxpage = Frozen == -1 ? PageNum : Frozen - 1;
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
        if (TWriter::WritePages(vec, nvec))
            return EIO;
        Maxpage += (maxpage < minpage) ? (Bufpages - minpage + maxpage + 1) : (maxpage - minpage + 1);
        return 0;
    }

    ssize_t Maxpage;
    ssize_t Bufpages;
    ssize_t PageNum;
    int Frozen;
    TDatPage* CurPage;
    char* Buf;
    TMappedAllocation ABuf;
};

template <class TFileManip>
class TOutputPageFileImpl: private TNonCopyable {
public:
    TOutputPageFileImpl()
        : Pagesize(0)
        , Eof(1)
        , Error(0)
        , Memo(0)
        , Recordsig(0)
    {
    }

    ~TOutputPageFileImpl() {
        Term();
    }

    inline int IsEof() const {
        return Eof;
    }

    inline int GetError() const {
        return Error;
    }

    inline bool IsOpen() const {
        return FileManip.IsOpen();
    }

    inline size_t GetPageSize() const {
        return Pagesize;
    }

    inline ui32 GetRecordSig() const {
        return Recordsig;
    }

    int Init(const char* fname, size_t pagesize, ui32 recsig, bool direct = false) {
        Memo = 0;
        if (FileManip.IsOpen())
            return MBDB_ALREADY_INITIALIZED;

        if (!fname) {
            Eof = Error = 0;
            Pagesize = pagesize;
            Recordsig = recsig;
            Memo = 1;
            return 0;
        }

        Error = FileManip.Open(fname, WrOnly | CreateAlways | ARW | AWOther | (direct ? DirectAligned : EOpenMode()));
        if (Error)
            return Error;
        Error = Init(TFile(), pagesize, recsig);
        if (Error) {
            FileManip.Close();
            unlink(fname);
        }
        return Error;
    }

    int Init(TAutoPtr<IOutputStream> output, size_t pagesize, ui32 recsig) {
        Memo = 0;
        if (FileManip.IsOpen()) {
            return MBDB_ALREADY_INITIALIZED;
        }

        if (!output) {
            Eof = Error = 0;
            Pagesize = pagesize;
            Recordsig = recsig;
            Memo = 1;
            return 0;
        }

        Error = FileManip.Open(output);
        if (Error)
            return Error;
        Error = Init(TFile(), pagesize, recsig);
        if (Error) {
            FileManip.Close();
        }
        return Error;
    }

    int Init(const TFile& file, size_t pagesize, ui32 recsig) {
        Memo = 0;
        if (!file.IsOpen() && !FileManip.IsOpen())
            return MBDB_NOT_INITIALIZED;
        if (file.IsOpen() && FileManip.IsOpen())
            return MBDB_ALREADY_INITIALIZED;
        if (file.IsOpen()) {
            Error = FileManip.Init(file);
            if (Error)
                return Error;
        }

        Eof = 1;
        TTempBuf buf(METASIZE + FS_BLOCK_SIZE);
        const char* ptr = (buf.Data() + FS_BLOCK_SIZE - ((ui64)buf.Data() & (FS_BLOCK_SIZE - 1)));
        TDatMetaPage* meta = (TDatMetaPage*)ptr;

        memset(buf.Data(), 0, buf.Size());
        meta->MetaSig = METASIG;
        meta->PageSize = Pagesize = pagesize;
        meta->RecordSig = Recordsig = recsig;

        ssize_t size = METASIZE, ret = 0;
        while (size && (ret = FileManip.Write(ptr, (unsigned)size)) > 0) {
            size -= ret;
            ptr += ret;
        }
        if (size || ret <= 0) {
            Term();
            return Error = errno ? errno : MBDB_WRITE_ERROR;
        }

        Error = Eof = 0;
        return Error;
    }

protected:
    int WritePages(iovec* vec, int nvec) {
        if (Error || Memo)
            return Error;

        ssize_t size, delta;
        iovec* pvec;
        int vsize;

        for (vsize = 0, pvec = vec; vsize < nvec; vsize++, pvec++)
            for (size = 0; (size_t)size < pvec->iov_len; size += Pagesize)
                ((TDatPage*)((char*)pvec->iov_base + size))->PageSig = PAGESIG;

        delta = size = 0;
        pvec = vec;
        vsize = nvec;
        while (vsize && (size = Writev(FileManip, pvec, (int)Min(vsize, 16))) > 0) {
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
        return Error = (!size && !vsize) ? 0 : errno ? errno : MBDB_WRITE_ERROR;
    }

    i64 Tell() {
        return FileManip.RealSeek(0, SEEK_CUR);
    }

    int GotoPage(int pageno) {
        if (Error || Memo)
            return Error;
        Eof = 0;
        i64 offset = (i64)pageno * Pagesize + METASIZE;
        if (offset != FileManip.Seek(offset, SEEK_SET))
            Error = MBDB_BAD_FILE_SIZE;
        return Error;
    }

    int Term() {
        int ret = FileManip.Close();
        Eof = 1;
        Memo = 0;
        if (!Error)
            Error = ret;
        return Error;
    }

    size_t Pagesize;
    int Eof;
    int Error;
    int Memo;
    ui32 Recordsig;

private:
    TFileManip FileManip;
};

using TOutputPageFile = TOutputPageFileImpl<TOutputFileManip>;

template <class TVal,
          typename TBaseRecIter = TOutputRecordIterator<TVal, TOutputPageIterator<TOutputPageFile>>>
class TOutDatFileImpl: public TBaseRecIter {
public:
    typedef TBaseRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TPageIter::TWriter TWriter;

    int Open(const char* fname, size_t pagesize, size_t pages = 1, int pagesOrBytes = 1, bool direct = false) {
        int ret = TWriter::Init(fname, pagesize, TVal::RecordSig, direct);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Open(const TFile& file, size_t pagesize, size_t pages = 1, int pagesOrBytes = 1) {
        int ret = TWriter::Init(file, pagesize, TVal::RecordSig);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Open(TAutoPtr<IOutputStream> output, size_t pagesize, size_t pages = 1, int pagesOrBytes = 1) {
        int ret = TWriter::Init(output, pagesize, TVal::RecordSig);
        return ret ? ret : Open2(pages, pagesOrBytes);
    }

    int Close() {
        int ret1 = TRecIter::Term();
        int ret2 = TPageIter::Term();
        int ret3 = TWriter::Term();
        return ret1 ? ret1 : ret2 ? ret2 : ret3;
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
class TOutIndexFile: public TOutDatFileImpl<
                          TVal,
                          TOutputRecordIterator<TVal, TOutputPageIterator<TOutputPageFile>, TCallbackIndexer, TFakeCompression>> {
    typedef TOutDatFileImpl<
        TVal,
        TOutputRecordIterator<TVal, TOutputPageIterator<TOutputPageFile>, TCallbackIndexer, TFakeCompression>>
        TDatFile;
    typedef TOutIndexFile<TVal> TMyType;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TIndexer TIndexer;

public:
    TOutIndexFile() {
        TIndexer::SetCallback(this, DispatchCallback);
    }

    int Open(const char* fname, size_t pagesize, size_t pages, int pagesOrBytes = 1) {
        int ret = TDatFile::Open(fname, pagesize, pages, pagesOrBytes);
        if (ret)
            return ret;
        if ((ret = TRecIter::GotoPage(1))) {
            TDatFile::Close();
            return ret;
        }
        Index0.Clear();
        return ret;
    }

    int Close() {
        TPageIter::Unfreeze();
        if (TRecIter::RecNum) {
            TRecIter::Flush();
            NextPage(TPageIter::Current());
        }
        int ret = 0;
        if (Index0.Size() && !(ret = TRecIter::GotoPage(0))) {
            const char* ptr = Index0.Begin();
            size_t recSize;
            while (ptr < Index0.End()) {
                Y_ASSERT((size_t)(Index0.End() - ptr) >= sizeof(size_t));
                memcpy(&recSize, ptr, sizeof(size_t));
                ptr += sizeof(size_t);
                Y_ASSERT((size_t)(Index0.End() - ptr) >= recSize);
                ui8* buf = (ui8*)TRecIter::Reserve(recSize);
                if (!buf) {
                    ret = MBDB_PAGE_OVERFLOW;
                    break;
                }
                memcpy(buf, ptr, recSize);
                TRecIter::ResetDat();
                ptr += recSize;
            }
            Index0.Clear();
            ret = (TPageIter::GetPageNum() != 0) ? MBDB_PAGE_OVERFLOW : TPageIter::GetError();
        }
        int ret1 = TDatFile::Close();
        return ret ? ret : ret1;
    }

protected:
    TBuffer Index0;

    void NextPage(const TDatPage* page) {
        const TVal* first = (const TVal*)NMicroBDB::GetFirstRecord(page);
        size_t sz;
        if (!TExtInfoType<TVal>::Exists) {
            sz = SizeOf(first);
        } else {
            size_t ll;
            size_t l;
            sz = NMicroBDB::SizeOfExt(first, &ll, &l);
            sz += ll + l;
        }
        Index0.Append((const char*)&sz, sizeof(size_t));
        Index0.Append((const char*)first, sz);
    }

    static void DispatchCallback(void* This, const TDatPage* page) {
        ((TMyType*)This)->NextPage(page);
    }
};

template <class TVal, class TKey, typename TCompressor = TFakeCompression, class TPageFile = TOutputPageFile>
class TOutDirectFileImpl: public TOutDatFileImpl<
                               TVal,
                               TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TCallbackIndexer, TCompressor>> {
    typedef TOutDatFileImpl<
        TVal,
        TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TCallbackIndexer, TCompressor>>
        TDatFile;
    typedef TOutDirectFileImpl<TVal, TKey, TCompressor, TPageFile> TMyType;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TIndexer TIndexer;
    typedef TOutIndexFile<TKey> TKeyFile;

public:
    TOutDirectFileImpl() {
        TIndexer::SetCallback(this, DispatchCallback);
    }

    int Open(const char* fname, size_t pagesize, int pages = 1, size_t ipagesize = 0, size_t ipages = 1, int pagesOrBytes = 1) {
        char iname[FILENAME_MAX];
        int ret;
        if (ipagesize == 0)
            ipagesize = pagesize;
        ret = TDatFile::Open(fname, pagesize, pages, pagesOrBytes);
        ret = ret ? ret : DatNameToIdx(iname, fname);
        ret = ret ? ret : KeyFile.Open(iname, ipagesize, ipages, pagesOrBytes);
        if (ret)
            TDatFile::Close();
        return ret;
    }

    int Close() {
        if (TRecIter::RecNum) {
            TRecIter::Flush();
            NextPage(TPageIter::Current());
        }
        int ret = KeyFile.Close();
        int ret1 = TDatFile::Close();
        return ret1 ? ret1 : ret;
    }

    int GetError() const {
        return TDatFile::GetError() ? TDatFile::GetError() : KeyFile.GetError();
    }

protected:
    TKeyFile KeyFile;

    void NextPage(const TDatPage* page) {
        typedef TMakeExtKey<TVal, TKey> TMakeExtKey;

        TVal* val = (TVal*)NMicroBDB::GetFirstRecord(page);
        TKey key;
        if (!TMakeExtKey::Exists) {
            TMakeExtKey::Make(&key, nullptr, val, nullptr);
            KeyFile.Push(&key);
        } else {
            size_t ll;
            size_t l;
            size_t sz = NMicroBDB::SizeOfExt(val, &ll, &l);
            typename TExtInfoType<TVal>::TResult valExt;
            if (TExtInfoType<TVal>::Exists)
                Y_PROTOBUF_SUPPRESS_NODISCARD valExt.ParseFromArray((ui8*)val + sz + ll, l);
            typename TExtInfoType<TKey>::TResult keyExt;
            TMakeExtKey::Make(&key, &keyExt, val, &valExt);
            KeyFile.Push(&key, &keyExt);
        }
    }

    static void DispatchCallback(void* This, const TDatPage* page) {
        ((TMyType*)This)->NextPage(page);
    }
};
