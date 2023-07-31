#include "file.h"

#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#ifdef _win32_
#define S_ISREG(x) !!(x & S_IFREG)
#endif

TFileManipBase::TFileManipBase()
    : FileBased(true)
{
}

i64 TFileManipBase::DoSeek(i64 offset, int whence, bool isStreamOpen) {
    if (!isStreamOpen)
        return -1;
    VerifyRandomAccess();
    return File.Seek(offset, (SeekDir)whence);
}

int TFileManipBase::DoFileOpen(const TFile& file) {
    File = file;
    SetFileBased(IsFileBased());
    return (File.IsOpen()) ? 0 : MBDB_OPEN_ERROR;
}

int TFileManipBase::DoFileClose() {
    if (File.IsOpen()) {
        File.Close();
        return MBDB_ALREADY_INITIALIZED;
    }
    return 0;
}

int TFileManipBase::IsFileBased() const {
    bool fileBased = true;
#if defined(_win_)
#elif defined(_unix_)
    FHANDLE h = File.GetHandle();
    struct stat sb;
    fileBased = false;
    if (h != INVALID_FHANDLE && !::fstat(h, &sb) && S_ISREG(sb.st_mode)) {
        fileBased = true;
    }
#else
#error
#endif
    return fileBased;
}

TInputFileManip::TInputFileManip()
    : InputStream(nullptr)
{
}

int TInputFileManip::Open(const char* fname, bool direct) {
    int ret;
    return (ret = DoClose()) ? ret : DoStreamOpen(TFile(fname, RdOnly | (direct ? DirectAligned : EOpenMode())));
}

int TInputFileManip::Open(IInputStream& input) {
    int ret;
    return (ret = DoClose()) ? ret : DoStreamOpen(&input);
}

int TInputFileManip::Open(TAutoPtr<IInputStream> input) {
    int ret;
    return (ret = DoClose()) ? ret : DoStreamOpen(input.Release());
}

int TInputFileManip::Init(const TFile& file) {
    int ret;
    if (ret = DoClose())
        return ret;
    DoStreamOpen(file);
    return 0;
}

int TInputFileManip::Close() {
    DoClose();
    return 0;
}

ssize_t TInputFileManip::Read(void* buf, unsigned len) {
    if (!IsStreamOpen())
        return -1;
    return InputStream->Load(buf, len);
}

IInputStream* TInputFileManip::CreateStream(const TFile& file) {
    return new TUnbufferedFileInput(file);
}

TMappedInputPageFile::TMappedInputPageFile()
    : Pagesize(0)
    , Error(0)
    , Pagenum(0)
    , Recordsig(0)
    , Open(false)
{
    Term();
}

TMappedInputPageFile::~TMappedInputPageFile() {
    Term();
}

int TMappedInputPageFile::Init(const char* fname, ui32 recsig, ui32* gotRecordSig, bool) {
    Mappedfile.init(fname);
    Open = true;

    TDatMetaPage* meta = (TDatMetaPage*)Mappedfile.getData();
    if (gotRecordSig)
        *gotRecordSig = meta->RecordSig;

    if (meta->MetaSig != METASIG)
        Error = MBDB_BAD_METAPAGE;
    else if (meta->RecordSig != recsig)
        Error = MBDB_BAD_RECORDSIG;

    if (Error) {
        Mappedfile.term();
        return Error;
    }

    size_t fsize = Mappedfile.getSize();
    if (fsize < METASIZE)
        return Error = MBDB_BAD_FILE_SIZE;
    fsize -= METASIZE;
    if (fsize % meta->PageSize)
        return Error = MBDB_BAD_FILE_SIZE;
    Pagenum = (int)(fsize / meta->PageSize);
    Pagesize = meta->PageSize;
    Recordsig = meta->RecordSig;
    Error = 0;
    return Error;
}

int TMappedInputPageFile::Term() {
    Mappedfile.term();
    Open = false;
    return 0;
}

TOutputFileManip::TOutputFileManip()
    : OutputStream(nullptr)
{
}

int TOutputFileManip::Open(const char* fname, EOpenMode mode) {
    if (IsStreamOpen()) {
        return MBDB_ALREADY_INITIALIZED; // should it be closed as TInputFileManip
    }

    try {
        if (unlink(fname) && errno != ENOENT) {
            if (strncmp(fname, "/dev/std", 8))
                return MBDB_OPEN_ERROR;
        }
        TFile file(fname, mode);
        DoStreamOpen(file);
    } catch (const TFileError&) {
        return MBDB_OPEN_ERROR;
    }
    return 0;
}

int TOutputFileManip::Open(IOutputStream& output) {
    if (IsStreamOpen())
        return MBDB_ALREADY_INITIALIZED;
    DoStreamOpen(&output);
    return 0;
}

int TOutputFileManip::Open(TAutoPtr<IOutputStream> output) {
    if (IsStreamOpen())
        return MBDB_ALREADY_INITIALIZED;
    DoStreamOpen(output.Release());
    return 0;
}

int TOutputFileManip::Init(const TFile& file) {
    if (IsStreamOpen())
        return MBDB_ALREADY_INITIALIZED; // should it be closed as TInputFileManip
    DoStreamOpen(file);
    return 0;
}

int TOutputFileManip::Rotate(const char* newfname) {
    if (!IsStreamOpen()) {
        return MBDB_NOT_INITIALIZED;
    }

    try {
        TFile file(newfname, WrOnly | OpenAlways | TruncExisting | ARW | AWOther);
        DoClose();
        DoStreamOpen(file);
    } catch (const TFileError&) {
        return MBDB_OPEN_ERROR;
    }
    return 0;
}

int TOutputFileManip::Close() {
    DoClose();
    return 0;
}

int TOutputFileManip::Write(const void* buf, unsigned len) {
    if (!IsStreamOpen())
        return -1;
    OutputStream->Write(buf, len);
    return len;
}

IOutputStream* TOutputFileManip::CreateStream(const TFile& file) {
    return new TUnbufferedFileOutput(file);
}
