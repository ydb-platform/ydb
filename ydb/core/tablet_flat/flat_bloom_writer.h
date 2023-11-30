#pragma once

#include "flat_page_bloom.h"
#include "flat_bloom_hash.h"
#include "flat_util_binary.h"
#include "util_deref.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/ymath.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {
namespace NBloom {

    class IWriter {
    public:
        virtual ~IWriter() = default;

        virtual void Reset() = 0;
        virtual ui64 EstimateBytesUsed(size_t extraItems) const = 0;
        virtual void Add(TArrayRef<const TCell> row) = 0;
        virtual TSharedData Make() = 0;
    };

    class TEstimator {
    public:
        TEstimator(float error)
        {
            Y_ABORT_UNLESS(error > 0. && error < 1.,
                "Invalid error estimation, should be in (0, 1)");

            double log2err = Log2(error);

            Amp = -1.44 * log2err;
            Y_ABORT_UNLESS(Amp < 256., "Too high rows amplification factor");

            HashCount = Min(ui64(Max<ui16>()), ui64(ceil(-log2err)));
        }

        ui64 Bits(ui64 rows) const noexcept
        {
            Y_ABORT_UNLESS(!(rows >> 54),
                "Too many rows, probably an invalid value passed");

            return ((Max(ui64(ceil(Amp * rows)), ui64(1)) + 63) >> 6) << 6;
        }

        ui16 Hashes() const noexcept
        {
            return HashCount;
        }

    private:
        double Amp;
        ui16 HashCount;
    };

    class TWriter final : public IWriter {
    public:
        TWriter(ui64 rows, float error)
        {
            TEstimator estimator(error);
            Hashes = estimator.Hashes();
            Items = estimator.Bits(rows);
            Y_ABORT_UNLESS(Hashes && Items);

            Reset();
        }

        void Reset() override
        {
            using THeader = NPage::TBloom::THeader;

            ui64 array = (Items >> 6) * sizeof(ui64);

            auto size = sizeof(NPage::TLabel) + sizeof(THeader) + array;

            Raw = TSharedData::Uninitialized(size);

            NUtil::NBin::TPut out(Raw.mutable_begin());

            WriteUnaligned<NPage::TLabel>(out.Skip<NPage::TLabel>(),
                NPage::TLabel::Encode(NPage::EPage::Bloom, 0, size));

            if (auto *post = out.Skip<THeader>()) {
                Zero(*post);

                post->Type = 0;
                post->Hashes = Hashes;
                post->Items = Items;
            }

            Array = { TDeref<ui64>::At(*out, 0), size_t(Items >> 6) };

            Y_ABORT_UNLESS(size_t(*out) % sizeof(ui64) == 0, "Invalid aligment");
            Y_ABORT_UNLESS(TDeref<char>::At(Array.end(), 0) == Raw.mutable_end());

            std::fill(Array.begin(), Array.end(), 0);
        }

        ui64 EstimateBytesUsed(size_t) const override
        {
            return Raw.size();
        }

        void Add(THashRoot root)
        {
            THash hash(root);

            for (ui32 seq = 0; seq++ < Hashes; ) {
                const ui64 num = hash.Next() % Items;

                Array[num >> 6] |= ui64(1) << (num & 0x3f);
            }
        }

        void Add(TArrayRef<const TCell> row) override
        {
            const TPrefix prefix(row);
            Add(THash::Root(prefix.Get(row.size())));
        }

        TSharedData Make() override
        {
            NSan::CheckMemIsInitialized(Raw.data(), Raw.size());

            return std::move(Raw);
        }

    private:
        ui32 Hashes = 0;
        ui64 Items = 0;
        TSharedData Raw;
        TArrayRef<ui64> Array;
    };

    class TQueue final : public IWriter {
    public:
        TQueue(float error)
            : Estimator(error)
            , Error(error)
        {
        }

        void Reset() override
        {
            Roots.clear();
        }

        ui64 EstimateBytesUsed(size_t extraItems) const override
        {
            using THeader = NPage::TBloom::THeader;

            ui64 array = (Estimator.Bits(Roots.size() + extraItems) >> 6) * sizeof(ui64);

            return sizeof(NPage::TLabel) + sizeof(THeader) + array;
        }

        void Add(TArrayRef<const TCell> row) override
        {
            const TPrefix prefix(row);
            Roots.push_back(THash::Root(prefix.Get(row.size())));
        }

        TSharedData Make() override
        {
            if (!Roots) {
                return { };
            }

            TWriter writer(Roots.size(), Error);
            for (const auto& root : Roots) {
                writer.Add(root);
            }
            return writer.Make();
        }

    private:
        TVector<THashRoot> Roots;
        TEstimator Estimator;
        float Error;
    };

}
}
}
