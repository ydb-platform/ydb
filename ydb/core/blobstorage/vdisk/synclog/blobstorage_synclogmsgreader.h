#pragma once

#include "defs.h"
#include "blobstorage_synclogformat.h"
#include "blobstorage_synclogmsgimpl.h"

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // READERS
        ////////////////////////////////////////////////////////////////////////////
        using TReadLogoBlobRec = std::function<void(const TLogoBlobRec *)>;
        using TReadBlockRec = std::function<void(const TBlockRec *)>;
        using TReadBarrierRec = std::function<void(const TBarrierRec *)>;
        using TReadBlockRecV2 = std::function<void(const TBlockRecV2 *)>;


        ////////////////////////////////////////////////////////////////////////////
        // IFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class IFragmentReader {
        public:
            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) = 0;
            virtual bool Check(TString &errorString) = 0;
            virtual ~IFragmentReader() {}
        };


        ////////////////////////////////////////////////////////////////////////////
        // TNaiveFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TNaiveFragmentReader : public IFragmentReader {
        public:
            TNaiveFragmentReader(const TString &data)
                : Data(data)
            {}

            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) override;
            virtual bool Check(TString &errorString) override;

        protected:
            const TString &Data;
            void ForEach(const TString &d, TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2);
        };


        ////////////////////////////////////////////////////////////////////////////
        // TLz4FragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TLz4FragmentReader : public TNaiveFragmentReader {
        public:
            TLz4FragmentReader(const TString &data)
                : TNaiveFragmentReader(data)
                , Uncompressed()
            {}

            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) override;
            virtual bool Check(TString &errorString) override;

        private:
            mutable TString Uncompressed;
            void Decompress();
        };


        ////////////////////////////////////////////////////////////////////////////
        // TBaseOrderedFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TBaseOrderedFragmentReader : public IFragmentReader {
        public:
            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) override;

        protected:
            TRecordsWithSerial Records;

            virtual bool Decompress() = 0;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TOrderedLz4FragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TOrderedLz4FragmentReader : public TBaseOrderedFragmentReader {
        public:
            TOrderedLz4FragmentReader(const TString &data)
                : Data(data)
                , Decompressed(false)
            {}

            virtual bool Check(TString &errorString) override;

        private:
            const TString &Data;
            bool Decompressed;

            bool Decompress() override;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TCustomCodecFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TCustomCodecFragmentReader : public TBaseOrderedFragmentReader {
        public:
            TCustomCodecFragmentReader(const TString &data)
                : Data(data)
                , Decompressed(false)
            {}

            virtual bool Check(TString &errorString) override;

        private:
            const TString &Data;
            bool Decompressed;

            bool Decompress() override;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TFragmentReader {
        public:
            TFragmentReader(const TString &data);

            void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
                Impl->ForEach(fblob, fblock, fbar, fblock2);
            }

            bool Check(TString &errorString) {
                return Impl->Check(errorString);
            }

        private:
            std::unique_ptr<IFragmentReader> Impl;
        };

    } // NSyncLog
} // NKikimr
