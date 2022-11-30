#include "decompress.h"

#include <contrib/libs/lzma/liblzma/api/lzma.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/stream/zerocopy.h>

// Based on https://fossies.org/linux/xz/doc/examples/02_decompress.c

///////////////////////////////////////////////////////////////////////////////
//
/// \file       02_decompress.c
/// \brief      Decompress .xz files to stdout
///
/// Usage:      ./02_decompress INPUT_FILES... > OUTFILE
///
/// Example:    ./02_decompress foo.xz bar.xz > foobar
//
//  Author:     Lasse Collin
//
//  This file has been put into the public domain.
//  You can do whatever you want with this file.
//
///////////////////////////////////////////////////////////////////////////////

namespace {
    class IInput {
    public:
        virtual ~IInput() = default;
        virtual size_t Next(const ui8*& ptr) = 0;
    };

    class TCopyInput: public IInput {
    public:
        TCopyInput(IInputStream* slave)
            : Slave_(slave)
        {
        }

        size_t Next(const ui8*& ptr) override {
            ptr = Inbuf_;
            return Slave_->Read(Inbuf_, sizeof(Inbuf_));
        }

    private:
        IInputStream* Slave_;
        ui8 Inbuf_[4096];
    };

    class TZeroCopy: public IInput {
    public:
        TZeroCopy(IZeroCopyInput* slave)
            : Slave_(slave)
        {
        }

        size_t Next(const ui8*& ptr) override {
            return Slave_->Next(&ptr);
        }

    private:
        IZeroCopyInput* Slave_;
    };

    std::unique_ptr<IInput> createInput(IInputStream* slave) {
        return std::make_unique<TCopyInput>(slave);
    }

    std::unique_ptr<IInput> createInput(IZeroCopyInput* slave) {
        return std::make_unique<TZeroCopy>(slave);
    }
}

class TUnbufferedXzDecompress::TImpl {
public:
    template <class T>
    TImpl(T* slave)
        : Input_(createInput(slave))
        , Strm_(LZMA_STREAM_INIT)
    {
        TString err;
        Y_ENSURE(initDecoder(&Strm_, err),
                 "Error initializing the decoder: " << err);
        Strm_.next_in = NULL;
        Strm_.avail_in = 0;
    }

    ~TImpl() {
        // Free the memory allocated for the decoder
        lzma_end(&Strm_);
    }

    size_t DoRead(void* buf, size_t len) {
        if (IsOutFinished_) {
            return 0;
        }

        size_t res;
        TString err;

        Y_ENSURE(decompress(buf, len, res, err),
                 "lzma decoder error: " << err);

        return res;
    }

private:
    bool decompress(void* buf, size_t len, size_t& outLen, TString& err) {
        // When LZMA_CONCATENATED flag was used when initializing the decoder,
        // we need to tell lzma_code() when there will be no more input.
        // This is done by setting action to LZMA_FINISH instead of LZMA_RUN
        // in the same way as it is done when encoding.
        //
        // When LZMA_CONCATENATED isn't used, there is no need to use
        // LZMA_FINISH to tell when all the input has been read, but it
        // is still OK to use it if you want. When LZMA_CONCATENATED isn't
        // used, the decoder will stop after the first .xz stream. In that
        // case some unused data may be left in strm->next_in.
        lzma_action action = LZMA_RUN;

        Strm_.next_out = (ui8*)buf;
        Strm_.avail_out = len;

        while (true) {
            if (Strm_.avail_in == 0 && !IsInFinished_) {
                size_t size = Input_->Next(Strm_.next_in);

                if (size == 0) {
                    IsInFinished_ = true;
                } else {
                    Strm_.avail_in = size;
                }

                // Once the end of the input file has been reached,
                // we need to tell lzma_code() that no more input
                // will be coming. As said before, this isn't required
                // if the LZMA_CONCATENATED flag isn't used when
                // initializing the decoder.
                if (IsInFinished_)
                    action = LZMA_FINISH;
            }

            lzma_ret ret = lzma_code(&Strm_, action);

            if (ret == LZMA_STREAM_END) {
                // Once everything has been decoded successfully, the
                // return value of lzma_code() will be LZMA_STREAM_END.
                //
                // It is important to check for LZMA_STREAM_END. Do not
                // assume that getting ret != LZMA_OK would mean that
                // everything has gone well or that when you aren't
                // getting more output it must have successfully
                // decoded everything.
                IsOutFinished_ = true;
            }

            if (Strm_.avail_out == 0 || ret == LZMA_STREAM_END) {
                outLen = len - Strm_.avail_out;
                return true;
            }

            if (ret != LZMA_OK) {
                // It's not LZMA_OK nor LZMA_STREAM_END,
                // so it must be an error code. See lzma/base.h
                // (src/liblzma/api/lzma/base.h in the source package
                // or e.g. /usr/include/lzma/base.h depending on the
                // install prefix) for the list and documentation of
                // possible values. Many values listen in lzma_ret
                // enumeration aren't possible in this example, but
                // can be made possible by enabling memory usage limit
                // or adding flags to the decoder initialization.
                switch (ret) {
                    case LZMA_MEM_ERROR:
                        err = "Memory allocation failed";
                        break;

                    case LZMA_FORMAT_ERROR:
                        // .xz magic bytes weren't found.
                        err = "The input is not in the .xz format";
                        break;

                    case LZMA_OPTIONS_ERROR:
                        // For example, the headers specify a filter
                        // that isn't supported by this liblzma
                        // version (or it hasn't been enabled when
                        // building liblzma, but no-one sane does
                        // that unless building liblzma for an
                        // embedded system). Upgrading to a newer
                        // liblzma might help.
                        //
                        // Note that it is unlikely that the file has
                        // accidentally became corrupt if you get this
                        // error. The integrity of the .xz headers is
                        // always verified with a CRC32, so
                        // unintentionally corrupt files can be
                        // distinguished from unsupported files.
                        err = "Unsupported compression options";
                        break;

                    case LZMA_DATA_ERROR:
                        err = "Compressed file is corrupt";
                        break;

                    case LZMA_BUF_ERROR:
                        // Typically this error means that a valid
                        // file has got truncated, but it might also
                        // be a damaged part in the file that makes
                        // the decoder think the file is truncated.
                        // If you prefer, you can use the same error
                        // message for this as for LZMA_DATA_ERROR.
                        err = "Compressed file is truncated or "
                              "otherwise corrupt";
                        break;

                    default:
                        // This is most likely LZMA_PROG_ERROR.
                        err = "Unknown error, possibly a bug";
                        break;
                }

                TStringOutput out(err);
                out << "[" << (int)ret << "]";
                return false;
            }
        }
    }

    static bool initDecoder(lzma_stream* strm, TString& err) {
        // Initialize a .xz decoder. The decoder supports a memory usage limit
        // and a set of flags.
        //
        // The memory usage of the decompressor depends on the settings used
        // to compress a .xz file. It can vary from less than a megabyte to
        // a few gigabytes, but in practice (at least for now) it rarely
        // exceeds 65 MiB because that's how much memory is required to
        // decompress files created with "xz -9". Settings requiring more
        // memory take extra effort to use and don't (at least for now)
        // provide significantly better compression in most cases.
        //
        // Memory usage limit is useful if it is important that the
        // decompressor won't consume gigabytes of memory. The need
        // for limiting depends on the application. In this example,
        // no memory usage limiting is used. This is done by setting
        // the limit to UINT64_MAX.
        //
        // The .xz format allows concatenating compressed files as is:
        //
        //     echo foo | xz > foobar.xz
        //     echo bar | xz >> foobar.xz
        //
        // When decompressing normal standalone .xz files, LZMA_CONCATENATED
        // should always be used to support decompression of concatenated
        // .xz files. If LZMA_CONCATENATED isn't used, the decoder will stop
        // after the first .xz stream. This can be useful when .xz data has
        // been embedded inside another file format.
        //
        // Flags other than LZMA_CONCATENATED are supported too, and can
        // be combined with bitwise-or. See lzma/container.h
        // (src/liblzma/api/lzma/container.h in the source package or e.g.
        // /usr/include/lzma/container.h depending on the install prefix)
        // for details.
        lzma_ret ret = lzma_auto_decoder(
            strm, UINT64_MAX, LZMA_CONCATENATED);

        // Return successfully if the initialization went fine.
        if (ret == LZMA_OK)
            return true;

        // Something went wrong. The possible errors are documented in
        // lzma/container.h (src/liblzma/api/lzma/container.h in the source
        // package or e.g. /usr/include/lzma/container.h depending on the
        // install prefix).
        //
        // Note that LZMA_MEMLIMIT_ERROR is never possible here. If you
        // specify a very tiny limit, the error will be delayed until
        // the first headers have been parsed by a call to lzma_code().
        switch (ret) {
            case LZMA_MEM_ERROR:
                err = "Memory allocation failed";
                break;

            case LZMA_OPTIONS_ERROR:
                err = "Unsupported decompressor flags";
                break;

            default:
                // This is most likely LZMA_PROG_ERROR indicating a bug in
                // this program or in liblzma. It is inconvenient to have a
                // separate error message for errors that should be impossible
                // to occur, but knowing the error code is important for
                // debugging. That's why it is good to print the error code
                // at least when there is no good error message to show.
                err = "Unknown error, possibly a bug";
                break;
        }

        TStringOutput out(err);
        out << "[" << (int)ret << "]";
        return false;
    }

private:
    std::unique_ptr<IInput> Input_;
    lzma_stream Strm_;

    bool IsInFinished_ = false;
    bool IsOutFinished_ = false;
};

TUnbufferedXzDecompress::TUnbufferedXzDecompress(IInputStream* slave)
    : Impl_(std::make_unique<TImpl>(slave))
{
}

TUnbufferedXzDecompress::TUnbufferedXzDecompress(IZeroCopyInput* slave)
    : Impl_(std::make_unique<TImpl>(slave))
{
}

TUnbufferedXzDecompress::~TUnbufferedXzDecompress() = default;

size_t TUnbufferedXzDecompress::DoRead(void* buf, size_t len) {
    return Impl_->DoRead(buf, len);
}
