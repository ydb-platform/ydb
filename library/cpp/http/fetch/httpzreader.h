#pragma once

#include "httpheader.h"
#include "httpparser.h"
#include "exthttpcodes.h"

#include <util/system/defaults.h>
#include <util/generic/yexception.h>

#include <contrib/libs/zlib/zlib.h>

#include <errno.h>

#ifndef ENOTSUP
#define ENOTSUP 45
#endif

template <class Reader>
class TCompressedHttpReader: public THttpReader<Reader> {
    typedef THttpReader<Reader> TBase;

public:
    using TBase::AssumeConnectionClosed;
    using TBase::Header;
    using TBase::ParseGeneric;
    using TBase::State;

    static constexpr size_t DefaultBufSize = 64 << 10;
    static constexpr unsigned int DefaultWinSize = 15;

    TCompressedHttpReader()
        : CompressedInput(false)
        , BufSize(0)
        , CurContSize(0)
        , MaxContSize(0)
        , Buf(nullptr)
        , ZErr(0)
        , ConnectionClosed(0)
        , IgnoreTrailingGarbage(true)
    {
        memset(&Stream, 0, sizeof(Stream));
    }

    ~TCompressedHttpReader() {
        ClearStream();

        if (Buf) {
            free(Buf);
            Buf = nullptr;
        }
    }

    void SetConnectionClosed(int cc) {
        ConnectionClosed = cc;
    }

    void SetIgnoreTrailingGarbage(bool ignore) {
        IgnoreTrailingGarbage = ignore;
    }

    int Init(
        THttpHeader* H,
        int parsHeader,
        const size_t maxContSize = Max<size_t>(),
        const size_t bufSize = DefaultBufSize,
        const unsigned int winSize = DefaultWinSize,
        bool headRequest = false)
    {
        ZErr = 0;
        CurContSize = 0;
        MaxContSize = maxContSize;

        int ret = TBase::Init(H, parsHeader, ConnectionClosed, headRequest);
        if (ret)
            return ret;

        ret = SetCompression(H->compression_method, bufSize, winSize);
        return ret;
    }

    long Read(void*& buf) {
        if (!CompressedInput) {
            long res = TBase::Read(buf);
            if (res > 0) {
                CurContSize += (size_t)res;
                if (CurContSize > MaxContSize) {
                    ZErr = E2BIG;
                    return -1;
                }
            }
            return res;
        }

        while (true) {
            if (Stream.avail_in == 0) {
                void* tmpin = Stream.next_in;
                long res = TBase::Read(tmpin);
                Stream.next_in = (Bytef*)tmpin;
                if (res <= 0)
                    return res;
                Stream.avail_in = (uInt)res;
            }

            Stream.next_out = Buf;
            Stream.avail_out = (uInt)BufSize;
            buf = Buf;

            int err = inflate(&Stream, Z_SYNC_FLUSH);

            //Y_ASSERT(Stream.avail_in == 0);

            switch (err) {
                case Z_OK:
                    // there is no data in next_out yet
                    if (BufSize == Stream.avail_out)
                        continue;
                    [[fallthrough]]; // don't break or return; continue with Z_STREAM_END case

                case Z_STREAM_END:
                    if (Stream.total_out > MaxContSize) {
                        ZErr = E2BIG;
                        return -1;
                    }
                    if (!IgnoreTrailingGarbage && BufSize == Stream.avail_out && Stream.avail_in > 0) {
                        Header->error = EXT_HTTP_GZIPERROR;
                        ZErr = EFAULT;
                        Stream.msg = (char*)"trailing garbage";
                        return -1;
                    }
                    return long(BufSize - Stream.avail_out);

                case Z_NEED_DICT:
                case Z_DATA_ERROR:
                    Header->error = EXT_HTTP_GZIPERROR;
                    ZErr = EFAULT;
                    return -1;

                case Z_MEM_ERROR:
                    ZErr = ENOMEM;
                    return -1;

                default:
                    ZErr = EINVAL;
                    return -1;
            }
        }

        return -1;
    }

    const char* ZMsg() const {
        return Stream.msg;
    }

    int ZError() const {
        return ZErr;
    }

    size_t GetCurContSize() const {
        return CompressedInput ? Stream.total_out : CurContSize;
    }

protected:
    int SetCompression(const int compression, const size_t bufSize,
                       const unsigned int winSize) {
        ClearStream();

        int winsize = winSize;
        switch ((enum HTTP_COMPRESSION)compression) {
            case HTTP_COMPRESSION_UNSET:
            case HTTP_COMPRESSION_IDENTITY:
                CompressedInput = false;
                return 0;
            case HTTP_COMPRESSION_GZIP:
                CompressedInput = true;
                winsize += 16; // 16 indicates gzip, see zlib.h
                break;
            case HTTP_COMPRESSION_DEFLATE:
                CompressedInput = true;
                winsize = -winsize; // negative indicates raw deflate stream, see zlib.h
                break;
            case HTTP_COMPRESSION_COMPRESS:
            case HTTP_COMPRESSION_ERROR:
            default:
                CompressedInput = false;
                ZErr = ENOTSUP;
                return -1;
        }

        if (bufSize != BufSize) {
            if (Buf)
                free(Buf);
            Buf = (ui8*)malloc(bufSize);
            if (!Buf) {
                ZErr = ENOMEM;
                return -1;
            }
            BufSize = bufSize;
        }

        int err = inflateInit2(&Stream, winsize);
        switch (err) {
            case Z_OK:
                Stream.total_in = 0;
                Stream.total_out = 0;
                Stream.avail_in = 0;
                return 0;

            case Z_DATA_ERROR: // never happens, see zlib.h
                CompressedInput = false;
                ZErr = EFAULT;
                return -1;

            case Z_MEM_ERROR:
                CompressedInput = false;
                ZErr = ENOMEM;
                return -1;

            default:
                CompressedInput = false;
                ZErr = EINVAL;
                return -1;
        }
    }

    void ClearStream() {
        if (CompressedInput) {
            inflateEnd(&Stream);
            CompressedInput = false;
        }
    }

    z_stream Stream;
    bool CompressedInput;
    size_t BufSize;
    size_t CurContSize, MaxContSize;
    ui8* Buf;
    int ZErr;
    int ConnectionClosed;
    bool IgnoreTrailingGarbage;
};

class zlib_exception: public yexception {
};

template <class Reader>
class SCompressedHttpReader: public TCompressedHttpReader<Reader> {
    typedef TCompressedHttpReader<Reader> TBase;

public:
    using TBase::ZError;
    using TBase::ZMsg;

    SCompressedHttpReader()
        : TBase()
    {
    }

    int Init(
        THttpHeader* H,
        int parsHeader,
        const size_t maxContSize = Max<size_t>(),
        const size_t bufSize = TBase::DefaultBufSize,
        const unsigned int winSize = TBase::DefaultWinSize,
        bool headRequest = false)
    {
        int ret = TBase::Init(H, parsHeader, maxContSize, bufSize, winSize, headRequest);
        return (int)HandleRetValue((long)ret);
    }

    long Read(void*& buf) {
        long ret = TBase::Read(buf);
        return HandleRetValue(ret);
    }

protected:
    long HandleRetValue(long ret) {
        switch (ZError()) {
            case 0:
                return ret;
            case ENOMEM:
                ythrow yexception() << "SCompressedHttpReader: not enough memory";
            case EINVAL:
                ythrow yexception() << "SCompressedHttpReader: zlib error: " << ZMsg();
            case ENOTSUP:
                ythrow yexception() << "SCompressedHttpReader: unsupported compression method";
            case EFAULT:
                ythrow zlib_exception() << "SCompressedHttpReader: " << ZMsg();
            case E2BIG:
                ythrow zlib_exception() << "SCompressedHttpReader: Content exceeds maximum length";
            default:
                ythrow yexception() << "SCompressedHttpReader: unknown error";
        }
    }
};
