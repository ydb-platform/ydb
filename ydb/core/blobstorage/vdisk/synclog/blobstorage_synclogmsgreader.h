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

        struct TWriteRecordToList {
            void operator()(const void* ptr) {
                Records.push_back((const TRecordHdr*)((char*)ptr - sizeof(TRecordHdr)));
            };

            std::vector<const TRecordHdr*>& Records;
        };


        template <class TLogoBlobCallback, class TBlockCallback, class TReadBarrierCallback, class TBlockV2Callback>
        void HandleRecordHdr(const TRecordHdr* rec, TLogoBlobCallback fblob, TBlockCallback fblock, TReadBarrierCallback fbar,
            TBlockV2Callback fblock2) {
            switch (rec->RecType) {
                case TRecordHdr::RecLogoBlob:
                    fblob(rec->GetLogoBlob());
                    break;
                case TRecordHdr::RecBlock:
                    fblock(rec->GetBlock());
                    break;
                case TRecordHdr::RecBarrier:
                    fbar(rec->GetBarrier());
                    break;
                case TRecordHdr::RecBlockV2:
                    fblock2(rec->GetBlockV2());
                    break;
                default:
                    Y_ABORT("Unknown RecType: %s", rec->ToString().data());
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // IFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class IFragmentReader {
        public:
            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) = 0;
            virtual bool Check(TString &errorString) = 0;
            virtual std::vector<const TRecordHdr*> ListRecords() = 0;
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
            virtual std::vector<const TRecordHdr*> ListRecords() override;

        protected:
            const TString &Data;

            template <class TLogoBlobCallback, class TBlockCallback, class TReadBarrierCallback, class TBlockV2Callback>
            void ForEach(const TString &d, TLogoBlobCallback fblob, TBlockCallback fblock, TReadBarrierCallback fbar,
                    TBlockV2Callback fblock2) {
                const TRecordHdr* begin = (const TRecordHdr*)(d.data());
                const TRecordHdr* end = (const TRecordHdr*)(d.data() + d.size());

                for (const TRecordHdr* it = begin; it < end; it = it->Next()) {
                    HandleRecordHdr(it, fblob, fblock, fbar, fblock2);
                }
            }
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
            virtual std::vector<const TRecordHdr*> ListRecords() override;

        private:
            mutable TString Uncompressed;
            void Decompress();
        };


        ////////////////////////////////////////////////////////////////////////////
        // TBaseOrderedFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        class TBaseOrderedFragmentReader : public IFragmentReader {
        public:
            virtual std::vector<const TRecordHdr*> ListRecords() override;
            virtual void ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) override;

        protected:
            template <class TLogoBlobCallback, class TBlockCallback, class TReadBarrierCallback, class TBlockV2Callback>
            void ForEachImpl(TLogoBlobCallback fblob, TBlockCallback fblock, TReadBarrierCallback fbar,
                    TBlockV2Callback
                     fblock2) {
                Decompress();

                using THeapItem = std::tuple<TRecordHdr::ESyncLogRecType, const void*, ui32>;
                auto comp = [](const THeapItem& x, const THeapItem& y) { return std::get<2>(y) < std::get<2>(x); };

                TStackVec<THeapItem, 4> heap;

    #define ADD_HEAP(NAME, TYPE) \
                if (!Records.NAME.empty()) { \
                    heap.emplace_back(TRecordHdr::TYPE, &Records.NAME.front(), Records.NAME.front().Counter); \
                }
                ADD_HEAP(LogoBlobs, RecLogoBlob)
                ADD_HEAP(Blocks, RecBlock)
                ADD_HEAP(Barriers, RecBarrier)
                ADD_HEAP(BlocksV2, RecBlockV2)

                std::make_heap(heap.begin(), heap.end(), comp);

                while (!heap.empty()) {
                    std::pop_heap(heap.begin(), heap.end(), comp);
                    auto& item = heap.back(); // say thanks to Microsoft compiler for not supporting tuple binding correctly
                    auto& type = std::get<0>(item);
                    auto& ptr = std::get<1>(item);
                    auto& counter = std::get<2>(item);
                    switch (type) {
    #define PROCESS(NAME, TYPE, FUNC) \
                        case TRecordHdr::TYPE: { \
                            using T = std::decay_t<decltype(Records.NAME)>::value_type; \
                            const T *item = static_cast<const T*>(ptr); \
                            FUNC(item); \
                            if (++item != Records.NAME.data() + Records.NAME.size()) { \
                                ptr = item; \
                                counter = item->Counter; \
                                std::push_heap(heap.begin(), heap.end(), comp); \
                            } else { \
                                heap.pop_back(); \
                            } \
                            break; \
                        }
                        PROCESS(LogoBlobs, RecLogoBlob, fblob)
                        PROCESS(Blocks, RecBlock, fblock)
                        PROCESS(Barriers, RecBarrier, fbar)
                        PROCESS(BlocksV2, RecBlockV2, fblock2)
                    }
                }
            }
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

            std::vector<const TRecordHdr*> ListRecords() {
                return Impl->ListRecords();
            }

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
