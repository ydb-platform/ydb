#pragma once

#include "align.h"
#include "header.h"
#include "extinfo.h"

#include <contrib/libs/zlib/zlib.h>
#include <contrib/libs/fastlz/fastlz.h>
#include <contrib/libs/snappy/snappy.h>

#include <util/generic/vector.h>
#include <util/memory/tempbuf.h>

namespace NMicroBDB {
    static const size_t DEFAULT_BUFFER_SIZE = (64 << 10);

    //!
    template <class TVal>
    class IBasePageReader {
    public:
        virtual size_t GetRecSize() const = 0;
        virtual size_t GetExtSize() const = 0;
        virtual bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const = 0;
        virtual const ui8* GetExtInfoRaw(size_t* len) const = 0;
        virtual const TVal* Next() = 0;
        virtual void Reset() = 0;
        //! set clearing flag, so temporary buffers will be cleared
        //! in next call of Next()
        virtual void SetClearFlag() {
        }

        virtual ~IBasePageReader() {
        }
    };

    template <class TVal, typename TPageIter>
    class TRawPageReader: public IBasePageReader<TVal> {
    public:
        TRawPageReader(TPageIter* const iter)
            : PageIter(iter)
        {
            Reset();
        }

        bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const override {
            Y_VERIFY(TExtInfoType<TVal>::Exists, "GetExtInfo should only be used with extended records");
            if (!Rec)
                return false;
            ui8* raw = (ui8*)Rec + RecSize + ExtLenSize;
            return extInfo->ParseFromArray(raw, ExtSize);
        }

        size_t GetRecSize() const override {
            return RecSize + ExtLenSize;
        }

        size_t GetExtSize() const override {
            return ExtSize;
        }

        const ui8* GetExtInfoRaw(size_t* len) const override {
            Y_VERIFY(TExtInfoType<TVal>::Exists, "GetExtInfo should only be used with extended records");
            if (!Rec) {
                *len = 0;
                return nullptr;
            }
            *len = ExtLenSize + ExtSize;
            return (ui8*)Rec + RecSize;
        }

        const TVal* Next() override {
            if (!Rec)
                Rec = (TVal*)((char*)PageIter->Current() + sizeof(TDatPage));
            else
                Rec = (TVal*)((char*)Rec + DatCeil(RecSize + ExtLenSize + ExtSize));
            if (!TExtInfoType<TVal>::Exists)
                RecSize = SizeOf(Rec);
            else
                RecSize = SizeOfExt(Rec, &ExtLenSize, &ExtSize);
            return Rec;
        }

        void Reset() override {
            Rec = nullptr;
            RecSize = 0;
            ExtLenSize = 0;
            ExtSize = 0;
        }

    private:
        const TVal* Rec;
        size_t RecSize;
        size_t ExtLenSize;
        size_t ExtSize;
        TPageIter* const PageIter;
    };

    template <class TVal, typename TPageIter>
    class TCompressedReader: public IBasePageReader<TVal> {
        inline size_t GetFirstRecordSize(const TVal* const in) const {
            if (!TExtInfoType<TVal>::Exists) {
                return DatCeil(SizeOf(in));
            } else {
                size_t ll;
                size_t l;
                size_t ret = SizeOfExt(in, &ll, &l);

                return DatCeil(ret + ll + l);
            }
        }

        void DecompressBlock() {
            if (PageIter->IsFrozen() && Buffer.Get())
                Blocks.push_back(Buffer.Release());

            const TCompressedHeader* hdr = (const TCompressedHeader*)(Page);

            Page += sizeof(TCompressedHeader);

            const size_t first = GetFirstRecordSize((const TVal*)Page);

            if (!Buffer.Get() || Buffer->Size() < hdr->Original)
                Buffer.Reset(new TTempBuf(Max<size_t>(hdr->Original, DEFAULT_BUFFER_SIZE)));

            memcpy(Buffer->Data(), Page, first);
            Page += first;

            if (hdr->Count > 1) {
                switch (Algo) {
                    case MBDB_COMPRESSION_ZLIB: {
                        uLongf dst = hdr->Original - first;

                        int ret = uncompress((Bytef*)Buffer->Data() + first, &dst, Page, hdr->Compressed);

                        if (ret != Z_OK)
                            ythrow yexception() << "error then uncompress " << ret;
                    } break;
                    case MBDB_COMPRESSION_FASTLZ: {
                        int dst = hdr->Original - first;
                        int ret = yfastlz_decompress(Page, hdr->Compressed, Buffer->Data() + first, dst);

                        if (!ret)
                            ythrow yexception() << "error then uncompress";
                    } break;
                    case MBDB_COMPRESSION_SNAPPY: {
                        if (!snappy::RawUncompress((const char*)Page, hdr->Compressed, Buffer->Data() + first))
                            ythrow yexception() << "error then uncompress";
                    } break;
                }
            }

            Rec = nullptr;
            RecNum = hdr->Count;
            Page += hdr->Compressed;
        }

        void ClearBuffer() {
            for (size_t i = 0; i < Blocks.size(); ++i)
                delete Blocks[i];
            Blocks.clear();
            ClearFlag = false;
        }

    public:
        TCompressedReader(TPageIter* const iter)
            : Rec(nullptr)
            , RecSize(0)
            , ExtLenSize(0)
            , ExtSize(0)
            , Page(nullptr)
            , PageIter(iter)
            , RecNum(0)
            , BlockNum(0)
            , ClearFlag(false)
        {
        }

        ~TCompressedReader() override {
            ClearBuffer();
        }

        size_t GetRecSize() const override {
            return RecSize + ExtLenSize;
        }

        size_t GetExtSize() const override {
            return ExtSize;
        }

        bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const override {
            Y_VERIFY(TExtInfoType<TVal>::Exists, "GetExtInfo should only be used with extended records");
            if (!Rec)
                return false;
            ui8* raw = (ui8*)Rec + RecSize + ExtLenSize;
            return extInfo->ParseFromArray(raw, ExtSize);
        }

        const ui8* GetExtInfoRaw(size_t* len) const override {
            Y_VERIFY(TExtInfoType<TVal>::Exists, "GetExtInfo should only be used with extended records");
            if (!Rec) {
                *len = 0;
                return nullptr;
            }
            *len = ExtLenSize + ExtSize;
            return (ui8*)Rec + RecSize;
        }

        const TVal* Next() override {
            Y_ASSERT(RecNum >= 0);

            if (ClearFlag)
                ClearBuffer();

            if (!Page) {
                if (!PageIter->Current())
                    return nullptr;

                Page = (ui8*)PageIter->Current() + sizeof(TDatPage);

                BlockNum = ((TCompressedPage*)Page)->BlockCount - 1;
                Algo = (ECompressionAlgorithm)((TCompressedPage*)Page)->Algorithm;
                Page += sizeof(TCompressedPage);

                DecompressBlock();
            }

            if (!RecNum) {
                if (BlockNum <= 0)
                    return nullptr;
                else {
                    --BlockNum;
                    DecompressBlock();
                }
            }

            --RecNum;
            if (!Rec)
                Rec = (const TVal*)Buffer->Data();
            else
                Rec = (const TVal*)((char*)Rec + DatCeil(RecSize + ExtLenSize + ExtSize));

            if (!TExtInfoType<TVal>::Exists)
                RecSize = SizeOf(Rec);
            else
                RecSize = SizeOfExt(Rec, &ExtLenSize, &ExtSize);

            return Rec;
        }

        void Reset() override {
            Page = nullptr;
            BlockNum = 0;
            Rec = nullptr;
            RecSize = 0;
            ExtLenSize = 0;
            ExtSize = 0;
            RecNum = 0;
        }

        void SetClearFlag() override {
            ClearFlag = true;
        }

    public:
        THolder<TTempBuf> Buffer;
        TVector<TTempBuf*> Blocks;
        const TVal* Rec;
        size_t RecSize;
        size_t ExtLenSize;
        size_t ExtSize;
        const ui8* Page;
        TPageIter* const PageIter;
        int RecNum; //!< count of recs in current block
        int BlockNum;
        ECompressionAlgorithm Algo;
        bool ClearFlag;
    };

    class TZLibCompressionImpl {
    public:
        static const ECompressionAlgorithm Code = MBDB_COMPRESSION_ZLIB;

        inline void Init() {
            // -
        }

        inline void Term() {
            // -
        }

        inline size_t CompressBound(size_t size) const noexcept {
            return ::compressBound(size);
        }

        inline void Compress(void* out, size_t& outSize, const void* in, size_t inSize) {
            uLongf size = outSize;

            if (compress((Bytef*)out, &size, (const Bytef*)in, inSize) != Z_OK)
                ythrow yexception() << "not compressed";
            outSize = size;
        }
    };

    class TFastlzCompressionImpl {
    public:
        static const ECompressionAlgorithm Code = MBDB_COMPRESSION_FASTLZ;

        inline void Init() {
            // -
        }

        inline void Term() {
            // -
        }

        inline size_t CompressBound(size_t size) const noexcept {
            size_t rval = size_t(size * 1.07);
            return rval < 66 ? 66 : rval;
        }

        inline void Compress(void* out, size_t& outSize, const void* in, size_t inSize) {
            outSize = yfastlz_compress_level(2, in, inSize, out);
            if (!outSize)
                ythrow yexception() << "not compressed";
        }
    };

    class TSnappyCompressionImpl {
    public:
        static const ECompressionAlgorithm Code = MBDB_COMPRESSION_SNAPPY;

        inline void Init() {
            // -
        }

        inline void Term() {
            // -
        }

        inline size_t CompressBound(size_t size) const noexcept {
            return snappy::MaxCompressedLength(size);
        }

        inline void Compress(void* out, size_t& outSize, const void* in, size_t inSize) {
            snappy::RawCompress((const char*)in, inSize, (char*)out, &outSize);
        }
    };

}

using TFakeCompression = void;
using TZLibCompression = NMicroBDB::TZLibCompressionImpl;
using TFastlzCompression = NMicroBDB::TFastlzCompressionImpl;
using TSnappyCompression = NMicroBDB::TSnappyCompressionImpl;
