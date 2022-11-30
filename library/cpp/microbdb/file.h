#pragma once

#include "header.h"

#include <library/cpp/deprecated/mapped_file/mapped_file.h>

#include <util/generic/noncopyable.h>
#include <util/stream/file.h>
#include <util/system/filemap.h>

#define FS_BLOCK_SIZE 512

class TFileManipBase {
protected:
    TFileManipBase();

    virtual ~TFileManipBase() {
    }

    i64 DoSeek(i64 offset, int whence, bool isStreamOpen);

    int DoFileOpen(const TFile& file);

    int DoFileClose();

    int IsFileBased() const;

    inline void SetFileBased(bool fileBased) {
        FileBased = fileBased;
    }

    inline i64 DoGetPosition() const {
        Y_ASSERT(FileBased);
        return File.GetPosition();
    }

    inline i64 DoGetLength() const {
        return (FileBased) ? File.GetLength() : -1;
    }

    inline void VerifyRandomAccess() const {
        Y_VERIFY(FileBased, "non-file stream can not be accessed randomly");
    }

    inline i64 GetPosition() const {
        return (i64)File.GetPosition();
    }

private:
    TFile File;
    bool FileBased;
};

class TInputFileManip: public TFileManipBase {
public:
    using TFileManipBase::GetPosition;

    TInputFileManip();

    int Open(const char* fname, bool direct = false);

    int Open(IInputStream& input);

    int Open(TAutoPtr<IInputStream> input);

    int Init(const TFile& file);

    int Close();

    ssize_t Read(void* buf, unsigned len);

    inline bool IsOpen() const {
        return IsStreamOpen();
    }

    inline i64 GetLength() const {
        return DoGetLength();
    }

    inline i64 Seek(i64 offset, int whence) {
        return DoSeek(offset, whence, IsStreamOpen());
    }

    inline i64 RealSeek(i64 offset, int whence) {
        return Seek(offset, whence);
    }

protected:
    inline bool IsStreamOpen() const {
        return !!InputStream;
    }

    inline int DoStreamOpen(IInputStream* input, bool fileBased = false) {
        InputStream.Reset(input);
        SetFileBased(fileBased);
        return 0;
    }

    inline int DoStreamOpen(const TFile& file) {
        int ret;
        return (ret = DoFileOpen(file)) ? ret : DoStreamOpen(CreateStream(file), IsFileBased());
    }

    virtual IInputStream* CreateStream(const TFile& file);

    inline bool DoClose() {
        if (IsStreamOpen()) {
            InputStream.Destroy();
            return DoFileClose();
        }
        return 0;
    }

    THolder<IInputStream> InputStream;
};

class TMappedInputPageFile: private TNonCopyable {
public:
    TMappedInputPageFile();

    ~TMappedInputPageFile();

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
        return Open;
    }

    inline char* GetData() const {
        return Open ? (char*)Mappedfile.getData() : nullptr;
    }

    inline size_t GetSize() const {
        return Open ? Mappedfile.getSize() : 0;
    }

protected:
    int Init(const char* fname, ui32 recsig, ui32* gotRecordSig = nullptr, bool direct = false);

    int Term();

    TMappedFile Mappedfile;
    size_t Pagesize;
    int Error;
    int Pagenum;
    ui32 Recordsig;
    bool Open;
};

class TOutputFileManip: public TFileManipBase {
public:
    TOutputFileManip();

    int Open(const char* fname, EOpenMode mode = WrOnly | CreateAlways | ARW | AWOther);

    int Open(IOutputStream& output);

    int Open(TAutoPtr<IOutputStream> output);

    int Init(const TFile& file);

    int Rotate(const char* newfname);

    int Write(const void* buf, unsigned len);

    int Close();

    inline bool IsOpen() const {
        return IsStreamOpen();
    }

    inline i64 GetLength() const {
        return DoGetLength();
    }

    inline i64 Seek(i64 offset, int whence) {
        return DoSeek(offset, whence, IsStreamOpen());
    }

    inline i64 RealSeek(i64 offset, int whence) {
        return Seek(offset, whence);
    }

protected:
    inline bool IsStreamOpen() const {
        return !!OutputStream;
    }

    inline int DoStreamOpen(IOutputStream* output, bool fileBased = false) {
        OutputStream.Reset(output);
        SetFileBased(fileBased);
        return 0;
    }

    inline int DoStreamOpen(const TFile& file) {
        int ret;
        return (ret = DoFileOpen(file)) ? ret : DoStreamOpen(CreateStream(file), true);
    }

    virtual IOutputStream* CreateStream(const TFile& file);

    inline bool DoClose() {
        if (IsStreamOpen()) {
            OutputStream.Destroy();
            return DoFileClose();
        }
        return 0;
    }

    THolder<IOutputStream> OutputStream;
};
