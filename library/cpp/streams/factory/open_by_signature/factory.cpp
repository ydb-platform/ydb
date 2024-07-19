#include "factory.h"

#include <library/cpp/streams/bzip2/bzip2.h>
#include <library/cpp/streams/factory/open_common/factory.h>
#include <util/stream/holder.h>
#include <util/stream/file.h>
#include <library/cpp/streams/lz/lz.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/stream/multi.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace {
    template <class T>
    struct TInputHolderX: public T {
        inline decltype(T().Get()) Set(T t) noexcept {
            t.Swap(*this);

            return this->Get();
        }
    };

    template <class T>
    struct TInputHolderX<T*> {
        static inline T* Set(T* t) noexcept {
            return t;
        }
    };

    template <class TInput>
    struct TStringMultiInput: private TInputHolderX<TInput>, private TString, private THolder<IInputStream>, public TMultiInput {
        TStringMultiInput(const TString& head, TInput tail)
            : TString(head)
            , THolder<IInputStream>(new TStringInput(*this))
            , TMultiInput(THolder<IInputStream>::Get(), this->Set(tail))
        {
        }

        ~TStringMultiInput() override {
        }
    };
}

template <class TInput>
THolder<IInputStream> OpenMaybeCompressedInputX(TInput input) {
    const size_t MAX_SIGNATURE_SIZE = 4;
    char buffer[MAX_SIGNATURE_SIZE];
    TString header(buffer, input->Load(buffer, MAX_SIGNATURE_SIZE));

    if (header.size() == MAX_SIGNATURE_SIZE) {
        // any lz
        THolder<IInputStream> lz = TryOpenOwnedLzDecompressor(new TStringMultiInput<TInput>(header, input));

        if (lz.Get()) {
            return lz;
        }
    }

    THolder<IInputStream> multi(new TStringMultiInput<TInput>(header, input));

    // gzip
    const TStringBuf GZIP = "\x1F\x8B";
    const TStringBuf ZLIB = "\x78\x9C";

    if (header.StartsWith(GZIP) || header.StartsWith(ZLIB)) {
        return MakeHolder<THoldingStream<TBufferedZLibDecompress>>(std::move(multi));
    }

    // bzip2
    constexpr TStringBuf BZIP2 = "BZ";
    if (header.StartsWith(BZIP2)) {
        return MakeHolder<THoldingStream<TBZipDecompress>>(std::move(multi));
    }

    return multi;
}

THolder<IInputStream> OpenMaybeCompressedInput(IInputStream* input) {
    return OpenMaybeCompressedInputX(input);
}

THolder<IInputStream> OpenOwnedMaybeCompressedInput(THolder<IInputStream> input) {
    return OpenMaybeCompressedInputX(TAtomicSharedPtr<IInputStream>(input));
}

THolder<IInputStream> OpenMaybeCompressedInput(const TString& path) {
    if (!path || path == TStringBuf("-")) {
        return OpenOwnedMaybeCompressedInput(OpenStdin());
    }
    return OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path));
}

THolder<IInputStream> OpenMaybeCompressedInput(const TString& path, ui32 bufSize) {
    if (!path || path == TStringBuf("-")) {
        return OpenOwnedMaybeCompressedInput(OpenStdin(bufSize));
    }
    return OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path, bufSize));
}
