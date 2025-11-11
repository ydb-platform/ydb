#pragma once

#include "defs.h"
#include "blobstorage_synclogformat.h"
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/list.h>
#include <util/thread/singleton.h>
#include <library/cpp/blockcodecs/codecs.h>


namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // ECodec -- how data is encoded
        ////////////////////////////////////////////////////////////////////////////
        enum class ECodec {
            Naive,
            Lz4,
            OrderedLz4,
            CustomCodec
        };

        // signatures (Naive doesn't have a signature)
        static constexpr ui64 Lz4Signature =        0x1fa43ecddb4f6ea9;
        static constexpr ui64 OrderedLz4Signature = 0x57412cb243a900c1;
        static constexpr ui64 CustomCodecSignature = 0x3cbac4bccc51fd6b;

        ////////////////////////////////////////////////////////////////////////////
        // GetLz4Codec
        ////////////////////////////////////////////////////////////////////////////
        static inline const NBlockCodecs::ICodec *GetLz4Codec() {
            const NBlockCodecs::ICodec **lz4 = FastTlsSingleton<const NBlockCodecs::ICodec*>();
            if (*lz4 == nullptr) {
                // create codec if we don't have one
                *lz4 = NBlockCodecs::Codec("lz4fast");
            }
            return *lz4;
        }

        ////////////////////////////////////////////////////////////////////////////
        // Codec detector
        ////////////////////////////////////////////////////////////////////////////
        ECodec FragmentCodecDetector(const TString &data);

        ////////////////////////////////////////////////////////////////////////////
        // Codec headers
        ////////////////////////////////////////////////////////////////////////////
        static inline size_t GetLz4HeaderSize() {
            return 48;
        }

        static inline std::pair<const char *, size_t> GetLz4Header() {
            static const ui64 hdr[] = {0, 0, 0, 0, 0, Lz4Signature};
            static_assert(sizeof(hdr) == 48, "incorrect header size");
            return {reinterpret_cast<const char *>(hdr), sizeof(hdr)};
        }

        static inline size_t GetOrderedLz4HeaderSize() {
            return 48;
        }

        static inline std::pair<const char *, size_t> GetOrderedLz4Header() {
            static const ui64 hdr[] = {0, 0, 0, 0, 0, OrderedLz4Signature};
            static_assert(sizeof(hdr) == 48, "incorrect header size");
            return {reinterpret_cast<const char *>(hdr), sizeof(hdr)};
        }

        static inline size_t GetCustomCodecHeaderSize() {
            return 48;
        }

        static inline std::pair<const char *, size_t> GetCustomCodecHeader() {
            static const ui64 hdr[] = {0, 0, 0, 0, 0, CustomCodecSignature};
            static_assert(sizeof(hdr) == 48, "incorrect header size");
            return {reinterpret_cast<const char *>(hdr), sizeof(hdr)};
        }



        ////////////////////////////////////////////////////////////////////////////
        // SyncLog message data structures we pass to other nodes
        ////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
        struct TLogoBlobRecWithSerial : public TLogoBlobRec {
            explicit TLogoBlobRecWithSerial(const TLogoBlobRec &rec, ui32 counter)
                : TLogoBlobRec(rec)
                , Counter(counter)
            {}

            explicit TLogoBlobRecWithSerial(const TLogoBlobID &id, ui64 ingressRaw, ui32 counter)
                : TLogoBlobRec(id, ingressRaw)
                , Counter(counter)
            {}

            ui32 Counter;
        };

        struct TBlockRecWithSerial : public TBlockRec {
            explicit TBlockRecWithSerial() = default;

            explicit TBlockRecWithSerial(const TBlockRec &rec, ui32 counter)
                : TBlockRec(rec)
                , Counter(counter)
            {}

            explicit TBlockRecWithSerial(ui64 tabletId, ui32 gen, ui32 counter)
                : TBlockRec(tabletId, gen)
                , Counter(counter)
            {}

            ui32 Counter = 0;
        };

        struct TBlockRecWithSerialV2 : public TBlockRecV2 {
            explicit TBlockRecWithSerialV2() = default;

            explicit TBlockRecWithSerialV2(const TBlockRecV2& rec, ui32 counter)
                : TBlockRecV2(rec)
                , Counter(counter)
            {}

            ui32 Counter = 0;
        };

        struct TBarrierRecWithSerial : public TBarrierRec {
            explicit TBarrierRecWithSerial()
                : TBarrierRec()
                , Counter(0)
            {}

            explicit TBarrierRecWithSerial(const TBarrierRec &rec, ui32 counter)
                : TBarrierRec(rec)
                , Counter(counter)
            {}

            explicit TBarrierRecWithSerial(ui64 tabletId, ui32 channel, ui32 gen, ui32 genCounter, ui32 collGen,
                                  ui32 collStep, bool hard, ui64 ingressRaw, ui32 counter)
                : TBarrierRec(tabletId, channel, gen, genCounter, collGen, collStep, hard, ingressRaw)
                , Counter(counter)
            {}

            ui32 Counter;
        };
#pragma pack(pop)

        struct TRecordsWithSerial {
            TVector<TLogoBlobRecWithSerial> LogoBlobs;
            TVector<TBlockRecWithSerial> Blocks;
            TVector<TBarrierRecWithSerial> Barriers;
            TVector<TBlockRecWithSerialV2> BlocksV2;
        };

        ////////////////////////////////////////////////////////////////////////////
        // SyncLog message specific codec interface
        // It works with vectors of TLogoBlobRec, TBlockRec and TBarrierRec
        ////////////////////////////////////////////////////////////////////////////
        class ISpecificCodec {
        public:
            virtual ~ISpecificCodec() {}

            // NOTE: Encode method writes into arguments (sort them, for instance)
            virtual TString Encode(TRecordsWithSerial& records) = 0;
            virtual bool Decode(const char *pos, const char *end, TRecordsWithSerial& records) = 0;

            bool DecodeString(const TString &s, TRecordsWithSerial& records) {
                return Decode(s.data(), s.data() + s.size(), records);
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TReorderCodec
        // Sort logoblobs, store them by columns
        ////////////////////////////////////////////////////////////////////////////
        class TReorderCodec : public ISpecificCodec {
        public:
            enum class EEncoding {
                Trivial,
                Custom
            };

            TReorderCodec(EEncoding enc);
            ~TReorderCodec();

            virtual TString Encode(TRecordsWithSerial& records) override;
            virtual bool Decode(const char *pos, const char *end, TRecordsWithSerial& records) override;

        private:
            class TImpl;
            std::unique_ptr<TImpl> Impl;
        };

    } // NSyncLog
} // NKikimr
