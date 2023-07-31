#pragma once

#include <util/stream/zlib.h>

#include "microbdb.h"
#include "safeopen.h"

class TCompressedInputFileManip: public TInputFileManip {
public:
    inline i64 GetLength() const {
        return -1; // Some microbdb logic rely on unknown size of compressed files
    }

    inline i64 Seek(i64 offset, int whence) {
        i64 oldPos = DoGetPosition();
        i64 newPos = offset;
        switch (whence) {
            case SEEK_CUR:
                newPos += oldPos;
                [[fallthrough]]; // Complier happy. Please fix it!
            case SEEK_SET:
                break;
            default:
                return -1L;
        }
        if (oldPos > newPos) {
            VerifyRandomAccess();
            DoSeek(0, SEEK_SET, IsStreamOpen());
            oldPos = 0;
        }
        const size_t bufsize = 1 << 12;
        char buf[bufsize];
        for (i64 i = oldPos; i < newPos; i += bufsize)
            InputStream->Read(buf, (i + (i64)bufsize < newPos) ? bufsize : (size_t)(newPos - i));
        return newPos;
    }

    i64 RealSeek(i64 offset, int whence) {
        InputStream.Destroy();
        i64 ret = DoSeek(offset, whence, !!CompressedInput);
        if (ret != -1)
            DoStreamOpen(DoCreateStream(), true);
        return ret;
    }

protected:
    IInputStream* CreateStream(const TFile& file) override {
        CompressedInput.Reset(new TUnbufferedFileInput(file));
        return DoCreateStream();
    }
    inline IInputStream* DoCreateStream() {
        return new TZLibDecompress(CompressedInput.Get(), ZLib::GZip);
        //return new TLzqDecompress(CompressedInput.Get());
    }
    THolder<IInputStream> CompressedInput;
};

class TCompressedBufferedInputFileManip: public TCompressedInputFileManip {
protected:
    IInputStream* CreateStream(const TFile& file) override {
        CompressedInput.Reset(new TFileInput(file, 0x100000));
        return DoCreateStream();
    }
};

using TCompressedInputPageFile = TInputPageFileImpl<TCompressedInputFileManip>;
using TCompressedBufferedInputPageFile = TInputPageFileImpl<TCompressedBufferedInputFileManip>;

template <class TVal>
struct TGzKey {
    ui64 Offset;
    TVal Key;

    static const ui32 RecordSig = TVal::RecordSig + 0x50495a47;

    TGzKey() {
    }

    TGzKey(ui64 offset, const TVal& key)
        : Offset(offset)
        , Key(key)
    {
    }

    size_t SizeOf() const {
        if (this)
            return sizeof(Offset) + ::SizeOf(&Key);
        else {
            size_t sizeOfKey = ::SizeOf((TVal*)NULL);
            return sizeOfKey ? (sizeof(Offset) + sizeOfKey) : 0;
        }
    }
};

template <class TVal>
class TInZIndexFile: protected  TInDatFileImpl<TGzKey<TVal>> {
    typedef TInDatFileImpl<TGzKey<TVal>> TDatFile;
    typedef TGzKey<TVal> TGzVal;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;

public:
    TInZIndexFile()
        : Index0(nullptr)
    {
    }

    int Open(const char* fname, size_t pages = 1, int pagesOrBytes = 1, ui32* gotRecordSig = nullptr) {
        int ret = TDatFile::Open(fname, pages, pagesOrBytes, gotRecordSig);
        if (ret)
            return ret;
        if (!(Index0 = (TDatPage*)malloc(TPageIter::GetPageSize()))) {
            TDatFile::Close();
            return MBDB_NO_MEMORY;
        }
        if (SizeOf((TGzVal*)NULL))
            RecsOnPage = (TPageIter::GetPageSize() - sizeof(TDatPage)) / DatCeil(SizeOf((TGzVal*)NULL));
        TDatFile::Next();
        memcpy(Index0, TPageIter::Current(), TPageIter::GetPageSize());
        return 0;
    }

    int Close() {
        free(Index0);
        Index0 = NULL;
        return TDatFile::Close();
    }

    inline int GetError() const {
        return TDatFile::GetError();
    }

    int FindKey(const TVal* akey, const typename TExtInfoType<TVal>::TResult* = NULL) {
        assert(IsOpen());
        if (!SizeOf((TVal*)NULL))
            return FindVszKey(akey);
        int pageno;
        i64 offset;
        FindKeyOnPage(pageno, offset, Index0, akey);
        TDatPage* page = TPageIter::GotoPage(pageno + 1);
        int num_add = (int)offset;
        FindKeyOnPage(pageno, offset, page, akey);
        return pageno + num_add;
    }

    using TDatFile::IsOpen;

    int FindVszKey(const TVal* akey, const typename TExtInfoType<TVal>::TResult* = NULL) {
        int pageno;
        i64 offset;
        FindVszKeyOnPage(pageno, offset, Index0, akey);
        TDatPage* page = TPageIter::GotoPage(pageno + 1);
        int num_add = (int)offset;
        FindVszKeyOnPage(pageno, offset, page, akey);
        return pageno + num_add;
    }

    i64 FindPage(int pageno) {
        if (!SizeOf((TVal*)NULL))
            return FindVszPage(pageno);
        int recsize = DatCeil(SizeOf((TGzVal*)NULL));
        TDatPage* page = TPageIter::GotoPage(1 + pageno / RecsOnPage);
        if (!page) // can happen if pageno is beyond EOF
            return -1;
        unsigned int localpageno = pageno % RecsOnPage;
        if (localpageno >= page->RecNum) // can happen if pageno is beyond EOF
            return -1;
        TGzVal* v = (TGzVal*)((char*)page + sizeof(TDatPage) + localpageno * recsize);
        return v->Offset;
    }

    i64 FindVszPage(int pageno) {
        TGzVal* cur = (TGzVal*)((char*)Index0 + sizeof(TDatPage));
        TGzVal* prev = cur;
        unsigned int n = 0;
        while (n < Index0->RecNum && cur->Offset <= (unsigned int)pageno) {
            prev = cur;
            cur = (TGzVal*)((char*)cur + DatCeil(SizeOf(cur)));
            n++;
        }
        TDatPage* page = TPageIter::GotoPage(n);
        unsigned int num_add = (unsigned int)(prev->Offset);
        n = 0;
        cur = (TGzVal*)((char*)page + sizeof(TDatPage));
        while (n < page->RecNum && n + num_add < (unsigned int)pageno) {
            cur = (TGzVal*)((char*)cur + DatCeil(SizeOf(cur)));
            n++;
        }
        if (n == page->RecNum) // can happen if pageno is beyond EOF
            return -1;
        return cur->Offset;
    }

protected:
    void FindKeyOnPage(int& pageno, i64& offset, TDatPage* page, const TVal* Key) {
        int left = 0;
        int right = page->RecNum - 1;
        int recsize = DatCeil(SizeOf((TGzVal*)NULL));
        while (left < right) {
            int middle = (left + right) >> 1;
            if (((TGzVal*)((char*)page + sizeof(TDatPage) + middle * recsize))->Key < *Key)
                left = middle + 1;
            else
                right = middle;
        }
        //borders check (left and right)
        pageno = (left == 0 || ((TGzVal*)((char*)page + sizeof(TDatPage) + left * recsize))->Key < *Key) ? left : left - 1;
        offset = ((TGzVal*)((char*)page + sizeof(TDatPage) + pageno * recsize))->Offset;
    }

    void FindVszKeyOnPage(int& pageno, i64& offset, TDatPage* page, const TVal* key) {
        TGzVal* cur = (TGzVal*)((char*)page + sizeof(TDatPage));
        ui32 RecordSig = page->RecNum;
        i64 tmpoffset = cur->Offset;
        for (; RecordSig > 0 && cur->Key < *key; --RecordSig) {
            tmpoffset = cur->Offset;
            cur = (TGzVal*)((char*)cur + DatCeil(SizeOf(cur)));
        }
        int idx = page->RecNum - RecordSig - 1;
        pageno = (idx >= 0) ? idx : 0;
        offset = tmpoffset;
    }

    TDatPage* Index0;
    int RecsOnPage;
};

template <class TKey>
class TCompressedIndexedInputPageFile: public TCompressedInputPageFile {
public:
    int GotoPage(int pageno);

protected:
    TInZIndexFile<TKey> KeyFile;
};

template <class TVal, class TKey>
class TDirectCompressedInDatFile: public TDirectInDatFile<TVal, TKey,
                                                           TInDatFileImpl<TVal, TInputRecordIterator<TVal,
                                                                                                     TInputPageIterator<TCompressedIndexedInputPageFile<TKey>>>>> {
};

class TCompressedOutputFileManip: public TOutputFileManip {
public:
    inline i64 GetLength() const {
        return -1; // Some microbdb logic rely on unknown size of compressed files
    }

    inline i64 Seek(i64 offset, int whence) {
        i64 oldPos = DoGetPosition();
        i64 newPos = offset;
        switch (whence) {
            case SEEK_CUR:
                newPos += oldPos;
                [[fallthrough]]; // Compler happy. Please fix it!
            case SEEK_SET:
                break;
            default:
                return -1L;
        }
        if (oldPos > newPos)
            return -1L;

        const size_t bufsize = 1 << 12;
        char buf[bufsize] = {0};
        for (i64 i = oldPos; i < newPos; i += bufsize)
            OutputStream->Write(buf, (i + (i64)bufsize < newPos) ? bufsize : (size_t)(newPos - i));
        return newPos;
    }

    i64 RealSeek(i64 offset, int whence) {
        OutputStream.Destroy();
        i64 ret = DoSeek(offset, whence, !!CompressedOutput);
        if (ret != -1)
            DoStreamOpen(DoCreateStream(), true);
        return ret;
    }

protected:
    IOutputStream* CreateStream(const TFile& file) override {
        CompressedOutput.Reset(new TUnbufferedFileOutput(file));
        return DoCreateStream();
    }
    inline IOutputStream* DoCreateStream() {
        return new TZLibCompress(CompressedOutput.Get(), ZLib::GZip, 1);
    }
    THolder<IOutputStream> CompressedOutput;
};

class TCompressedBufferedOutputFileManip: public TCompressedOutputFileManip {
protected:
    IOutputStream* CreateStream(const TFile& file) override {
        CompressedOutput.Reset(new TUnbufferedFileOutput(file));
        return DoCreateStream();
    }
    inline IOutputStream* DoCreateStream() {
        return new TZLibCompress(CompressedOutput.Get(), ZLib::GZip, 1, 0x100000);
    }
};

using TCompressedOutputPageFile = TOutputPageFileImpl<TCompressedOutputFileManip>;
using TCompressedBufferedOutputPageFile = TOutputPageFileImpl<TCompressedBufferedOutputFileManip>;

template <class TVal>
class TOutZIndexFile: public TOutDatFileImpl<
                           TGzKey<TVal>,
                           TOutputRecordIterator<TGzKey<TVal>, TOutputPageIterator<TOutputPageFile>, TCallbackIndexer>> {
    typedef TOutDatFileImpl<
        TGzKey<TVal>,
        TOutputRecordIterator<TGzKey<TVal>, TOutputPageIterator<TOutputPageFile>, TCallbackIndexer>>
        TDatFile;
    typedef TOutZIndexFile<TVal> TMyType;
    typedef TGzKey<TVal> TGzVal;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TIndexer TIndexer;

public:
    TOutZIndexFile() {
        TotalRecNum = 0;
        TIndexer::SetCallback(this, DispatchCallback);
    }

    int Open(const char* fname, size_t pagesize, size_t pages, int pagesOrBytes = 1) {
        int ret = TDatFile::Open(fname, pagesize, pages, pagesOrBytes);
        if (ret)
            return ret;
        if ((ret = TRecIter::GotoPage(1)))
            TDatFile::Close();
        return ret;
    }

    int Close() {
        TPageIter::Unfreeze();
        if (TRecIter::RecNum)
            NextPage(TPageIter::Current());
        int ret = 0;
        if (Index0.size() && !(ret = TRecIter::GotoPage(0))) {
            typename std::vector<TGzVal>::iterator it, end = Index0.end();
            for (it = Index0.begin(); it != end; ++it)
                TRecIter::Push(&*it);
            ret = (TPageIter::GetPageNum() != 0) ? MBDB_PAGE_OVERFLOW : TPageIter::GetError();
        }
        Index0.clear();
        int ret1 = TDatFile::Close();
        return ret ? ret : ret1;
    }

protected:
    int TotalRecNum; // should be enough because we have GotoPage(int)
    std::vector<TGzVal> Index0;

    void NextPage(const TDatPage* page) {
        TGzVal* rec = (TGzVal*)((char*)page + sizeof(TDatPage));
        Index0.push_back(TGzVal(TotalRecNum, rec->Key));
        TotalRecNum += TRecIter::RecNum;
    }

    static void DispatchCallback(void* This, const TDatPage* page) {
        ((TMyType*)This)->NextPage(page);
    }
};

template <class TVal, class TKey, class TPageFile = TCompressedOutputPageFile>
class TOutDirectCompressedFileImpl: public TOutDatFileImpl<
                                         TVal,
                                         TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TCallbackIndexer>> {
    typedef TOutDatFileImpl<
        TVal,
        TOutputRecordIterator<TVal, TOutputPageIterator<TPageFile>, TCallbackIndexer>>
        TDatFile;
    typedef TOutDirectCompressedFileImpl<TVal, TKey, TPageFile> TMyType;
    typedef typename TDatFile::TRecIter TRecIter;
    typedef typename TRecIter::TPageIter TPageIter;
    typedef typename TRecIter::TIndexer TIndexer;
    typedef TGzKey<TKey> TMyKey;
    typedef TOutZIndexFile<TKey> TKeyFile;

protected:
    using TDatFile::Tell;

public:
    TOutDirectCompressedFileImpl() {
        TIndexer::SetCallback(this, DispatchCallback);
    }

    int Open(const char* fname, size_t pagesize, size_t ipagesize = 0) {
        char iname[FILENAME_MAX];
        int ret;
        if (ipagesize == 0)
            ipagesize = pagesize;

        ret = TDatFile::Open(fname, pagesize, 1, 1);
        ret = ret ? ret : DatNameToIdx(iname, fname);
        ret = ret ? ret : KeyFile.Open(iname, ipagesize, 1, 1);
        if (ret)
            TDatFile::Close();
        return ret;
    }

    int Close() {
        if (TRecIter::RecNum)
            NextPage(TPageIter::Current());
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
        size_t sz = SizeOf((TMyKey*)NULL);
        TMyKey* rec = KeyFile.Reserve(sz ? sz : MaxSizeOf<TMyKey>());
        if (rec) {
            rec->Offset = Tell();
            rec->Key = *(TVal*)((char*)page + sizeof(TDatPage));
            KeyFile.ResetDat();
        }
    }

    static void DispatchCallback(void* This, const TDatPage* page) {
        ((TMyType*)This)->NextPage(page);
    }
};

template <class TKey>
int TCompressedIndexedInputPageFile<TKey>::GotoPage(int pageno) {
    if (Error)
        return Error;

    Eof = 0;

    i64 offset = KeyFile.FindPage(pageno);
    if (!offset)
        return Error = MBDB_BAD_FILE_SIZE;

    if (offset != FileManip.RealSeek(offset, SEEK_SET))
        Error = MBDB_BAD_FILE_SIZE;

    return Error;
}

template <typename TVal>
class TCompressedInDatFile: public TInDatFile<TVal, TCompressedInputPageFile> {
public:
    TCompressedInDatFile(const char* name, size_t pages, int pagesOrBytes = 1)
        : TInDatFile<TVal, TCompressedInputPageFile>(name, pages, pagesOrBytes)
    {
    }
};

template <typename TVal>
class TCompressedOutDatFile: public TOutDatFile<TVal, TFakeCompression, TCompressedOutputPageFile> {
public:
    TCompressedOutDatFile(const char* name, size_t pagesize, size_t pages, int pagesOrBytes = 1)
        : TOutDatFile<TVal, TFakeCompression, TCompressedOutputPageFile>(name, pagesize, pages, pagesOrBytes)
    {
    }
};

template <typename TVal, typename TKey, typename TPageFile = TCompressedOutputPageFile>
class TOutDirectCompressedFile: protected  TOutDirectCompressedFileImpl<TVal, TKey, TPageFile> {
    typedef TOutDirectCompressedFileImpl<TVal, TKey, TPageFile> TBase;

public:
    TOutDirectCompressedFile(const char* name, size_t pagesize, size_t ipagesize = 0)
        : Name(strdup(name))
        , PageSize(pagesize)
        , IdxPageSize(ipagesize)
    {
    }

    ~TOutDirectCompressedFile() {
        Close();
        free(Name);
        Name = NULL;
    }

    void Open(const char* fname) {
        int ret = TBase::Open(fname, PageSize, IdxPageSize);
        if (ret)
            ythrow yexception() << ErrorMessage(ret, "Failed to open output file", fname);
        free(Name);
        Name = strdup(fname);
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
        return Name;
    }

    using TBase::Freeze;
    using TBase::Push;
    using TBase::Reserve;
    using TBase::Unfreeze;

protected:
    char* Name;
    size_t PageSize, IdxPageSize;
};

class TCompressedInterFileTypes {
public:
    typedef TCompressedBufferedOutputPageFile TOutPageFile;
    typedef TCompressedBufferedInputPageFile TInPageFile;
};
