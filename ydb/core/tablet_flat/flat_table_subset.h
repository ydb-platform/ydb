#pragma once

#include "flat_mem_warm.h"
#include "flat_mem_snapshot.h"
#include "flat_table_part.h"
#include "flat_part_laid.h"
#include "flat_table_committed.h"

#include <util/generic/vector.h>

namespace NKikimr {
namespace NTable {

    struct TSubset {
        explicit operator bool() const
        {
            return bool(Frozen) || bool(Flatten) || bool(ColdParts);
        }

        TEpoch Epoch() const noexcept
        {
            TEpoch epoch = TEpoch::Min();

            for (auto &mem: Frozen) {
                epoch = Max(epoch, mem->Epoch);
            }

            for (auto &hunk: Flatten) {
                epoch = Max(epoch, hunk.Part->Epoch);
            }

            for (auto &part: ColdParts) {
                epoch = Max(epoch, part->Epoch);
            }

            for (auto &part: TxStatus) {
                epoch = Max(epoch, part->Epoch);
            }

            return epoch;
        }

        bool IsStickedToHead() const
        {
            return Head == TEpoch::Zero() || Head == Epoch() + 1;
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "TSubset{" << "head " << Head
                << ", " << Frozen.size() << "m"
                << " " << Flatten.size() << "p"
                << " " << ColdParts.size() << "c"
                << "}";
        }

        ui64 MaxRows() const noexcept
        {
            ui64 rows = 0;

            for (const auto &memTable : Frozen)
                rows += memTable->GetRowCount();

            for (const auto &part : Flatten)
                rows += part->Stat.Rows;

            if (ColdParts) {
                // We don't know, signal it to bloom filter
                rows = 0;
            }

            return rows;
        }

        TRowVersion MinRowVersion() const noexcept
        {
            TRowVersion minVersion = TRowVersion::Max();

            for (const auto &memTable : Frozen)
                minVersion = Min(minVersion, memTable->GetMinRowVersion());

            for (const auto &part : Flatten)
                minVersion = Min(minVersion, part->MinRowVersion);

            if (ColdParts) {
                // We don't know, assume the worst
                minVersion = TRowVersion::Min();
            }

            return minVersion;
        }

        TSubset(TEpoch head, TIntrusiveConstPtr<TRowScheme> scheme)
            : Head(head)
            , Scheme(std::move(scheme))
        { }

        // This constructor is mainly for tests
        TSubset(TEpoch head, TIntrusiveConstPtr<TRowScheme> scheme, TVector<TMemTableSnapshot> frozen)
            : Head(head)
            , Scheme(std::move(scheme))
            , Frozen(std::move(frozen))
        { }

        // This constructor is mainly for tests
        TSubset(TEpoch head, TIntrusiveConstPtr<TRowScheme> scheme, TVector<TPartView> flatten)
            : Head(head)
            , Scheme(std::move(scheme))
            , Flatten(std::move(flatten))
        { }

        const TEpoch Head;

        TIntrusiveConstPtr<TRowScheme> Scheme;
        TVector<TMemTableSnapshot> Frozen;
        TVector<TPartView> Flatten;
        TVector<TIntrusiveConstPtr<TColdPart>> ColdParts;
        TTransactionMap CommittedTransactions;
        TTransactionSet RemovedTransactions;
        TVector<TIntrusiveConstPtr<TTxStatusPart>> TxStatus;
    };

    using TGarbage = TVector<TAutoPtr<TSubset>>; /* data of deleted tables */
}
}
