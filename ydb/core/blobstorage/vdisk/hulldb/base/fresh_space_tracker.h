#pragma once

#include <util/system/spinlock.h>
#include <util/system/yassert.h>
#include <util/string/builder.h>

#include <array>
#include <limits>
#include <optional>

namespace NKikimr {

    enum class EFreshDb : ui8 {
        LogoBlobs,
        Blocks,
        Barriers,
        Count,
    };

    struct TFreshSpaceAdmission {
        static constexpr size_t DatabaseCount = static_cast<size_t>(EFreshDb::Count);

        std::array<ui64, DatabaseCount> Bytes = {};

        void Add(EFreshDb db, ui64 bytes) {
            ui64& value = Bytes[static_cast<size_t>(db)];
            value = value > Max<ui64>() - bytes ? Max<ui64>() : value + bytes;
        }

        bool Empty() const {
            for (ui64 bytes : Bytes) {
                if (bytes) {
                    return false;
                }
            }
            return true;
        }
    };

    // Credits are deliberately not persisted. This object accounts the
    // current Fresh contents together with recovery-log updates which have
    // passed admission but have not reached Fresh yet. PDisk owns the actual
    // quota charge; this class mirrors only this VDisk incarnation's balance.
    class TFreshSpaceTracker {
        static constexpr size_t DatabaseCount = TFreshSpaceAdmission::DatabaseCount;

        const bool Enabled;
        const ui64 ChunkSize;
        const ui32 ChunksPerSst;
        const ui32 RefillChunks;
        const ui32 TotalPartCount;
        static constexpr ui64 MetadataRecordBytes = 512;

        mutable TSpinLock Lock;
        std::array<ui64, DatabaseCount> RecordBytes = {};
        std::array<ui64, DatabaseCount> InFlightBytes = {};
        ui64 GrantedChunks = 0;
        ui64 ConsumedBeforeGrantObserved = 0;
        bool CreditRequestPending = false;
        bool CreditRequestSuppressed = false;
        ui32 CreditReleasePending = 0;

        static ui64 SaturatingAdd(ui64 lhs, ui64 rhs) {
            return lhs > Max<ui64>() - rhs ? Max<ui64>() : lhs + rhs;
        }

        static ui64 SaturatingMultiply(ui64 lhs, ui64 rhs) {
            return lhs && rhs > Max<ui64>() / lhs ? Max<ui64>() : lhs * rhs;
        }

        ui64 BytesToChunks(ui64 bytes) const {
            if (!bytes) {
                return 0;
            }
            const ui64 chunks = 1 + (bytes - 1) / ChunkSize;
            const ui64 batches = 1 + (chunks - 1) / ChunksPerSst;
            return SaturatingMultiply(batches, ChunksPerSst);
        }

        ui64 CalculateInFlightChunks(const std::array<ui64, DatabaseCount>& bytes) const {
            ui64 chunks = 0;
            for (ui64 value : bytes) {
                chunks = SaturatingAdd(chunks, BytesToChunks(value));
            }
            return chunks;
        }

        ui64 CalculateLogoBlobRecordBytes(ui64 maxInPlacePartSize) const {
            // A metadata-only Fresh record may merge with an older DiskBlob.
            // DiskBlob packs all locally available erasure parts into one
            // inline value, while the huge-blob threshold limits each part
            // separately.
            const ui64 inlineData = SaturatingMultiply(maxInPlacePartSize, TotalPartCount);
            const ui64 payload = SaturatingAdd(inlineData,
                SaturatingAdd(1024, SaturatingMultiply(TotalPartCount, 32)));
            // Next-fit data packing can waste almost one item worth of space
            // at a chunk boundary, hence the factor of two.
            return SaturatingMultiply(2, payload);
        }

    public:
        TFreshSpaceTracker(bool enabled, ui64 chunkSize, ui32 chunksPerSst,
                ui64 maxInPlaceLogoBlobSize, ui32 totalPartCount)
            : Enabled(enabled)
            , ChunkSize(Max<ui64>(chunkSize, 1))
            , ChunksPerSst(Max<ui32>(chunksPerSst, 1))
            , RefillChunks(static_cast<ui32>(Min<ui64>(ui64(ChunksPerSst) * 3, Max<ui32>())))
            , TotalPartCount(totalPartCount)
        {
            RecordBytes[static_cast<size_t>(EFreshDb::LogoBlobs)] =
                CalculateLogoBlobRecordBytes(maxInPlaceLogoBlobSize);
            RecordBytes[static_cast<size_t>(EFreshDb::Blocks)] = MetadataRecordBytes;
            RecordBytes[static_cast<size_t>(EFreshDb::Barriers)] = MetadataRecordBytes;
        }

        bool IsEnabled() const {
            return Enabled;
        }

        TFreshSpaceAdmission MakeAdmission(EFreshDb db, ui64 records = 1) const {
            TFreshSpaceAdmission admission;
            admission.Add(db, EstimateBytes(db, records));
            return admission;
        }

        // Keep/do-not-keep flags, huge-blob pointers, phantoms and similar
        // index-only LogoBlob records do not carry in-place payload.
        TFreshSpaceAdmission MakeMetadataAdmission(EFreshDb db, ui64 records = 1) const {
            TFreshSpaceAdmission admission;
            admission.Add(db, EstimateMetadataBytes(records));
            return admission;
        }

        ui64 EstimateBytes(EFreshDb db, ui64 records) const {
            TGuard<TSpinLock> guard(Lock);
            return SaturatingMultiply(RecordBytes[static_cast<size_t>(db)], records);
        }

        ui64 EstimateMetadataBytes(ui64 records) const {
            return SaturatingMultiply(MetadataRecordBytes, records);
        }

        ui64 CalculateSegmentChunks(EFreshDb db, ui64 records) const {
            return BytesToChunks(EstimateBytes(db, records));
        }

        void UpdateMaxInPlaceLogoBlobSize(ui64 bytes) {
            if (!Enabled) {
                return;
            }
            const ui64 recordBytes = CalculateLogoBlobRecordBytes(bytes);
            TGuard<TSpinLock> guard(Lock);
            RecordBytes[static_cast<size_t>(EFreshDb::LogoBlobs)] =
                Max(RecordBytes[static_cast<size_t>(EFreshDb::LogoBlobs)], recordBytes);
        }

        bool TryAdmit(const TFreshSpaceAdmission& admission, ui64 freshChunks) {
            if (!Enabled || admission.Empty()) {
                return true;
            }

            TGuard<TSpinLock> guard(Lock);
            auto candidate = InFlightBytes;
            for (size_t i = 0; i != DatabaseCount; ++i) {
                candidate[i] = SaturatingAdd(candidate[i], admission.Bytes[i]);
            }
            const ui64 required = SaturatingAdd(freshChunks, CalculateInFlightChunks(candidate));
            if (required > GrantedChunks) {
                return false;
            }
            InFlightBytes = candidate;
            return true;
        }

        void CommitAdmission(const TFreshSpaceAdmission& admission) {
            if (!Enabled || admission.Empty()) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            for (size_t i = 0; i != DatabaseCount; ++i) {
                Y_ABORT_UNLESS(admission.Bytes[i] <= InFlightBytes[i], "Fresh admission underflow at db# %zu"
                    " committed# %" PRIu64 " inFlight# %" PRIu64, i,
                    admission.Bytes[i], InFlightBytes[i]);
                InFlightBytes[i] -= admission.Bytes[i];
            }
        }

        void CancelAdmission(const TFreshSpaceAdmission& admission) {
            CommitAdmission(admission);
        }

        ui64 GetRequiredChunks(ui64 freshChunks) const {
            if (!Enabled) {
                return 0;
            }
            TGuard<TSpinLock> guard(Lock);
            return SaturatingAdd(freshChunks, CalculateInFlightChunks(InFlightBytes));
        }

        std::optional<ui32> BeginCreditRequest(ui64 freshChunks,
                const TFreshSpaceAdmission* desiredAdmission = nullptr) {
            if (!Enabled) {
                return std::nullopt;
            }
            TGuard<TSpinLock> guard(Lock);
            if (CreditRequestPending || CreditReleasePending || CreditRequestSuppressed) {
                return std::nullopt;
            }
            auto desiredBytes = InFlightBytes;
            if (desiredAdmission) {
                for (size_t i = 0; i != DatabaseCount; ++i) {
                    desiredBytes[i] = SaturatingAdd(desiredBytes[i], desiredAdmission->Bytes[i]);
                }
            }
            const ui64 required = SaturatingAdd(freshChunks, CalculateInFlightChunks(desiredBytes));
            const ui64 target = SaturatingAdd(required, RefillChunks);
            if (GrantedChunks >= target) {
                return std::nullopt;
            }
            CreditRequestPending = true;
            return static_cast<ui32>(Min<ui64>(target - GrantedChunks, Max<ui32>()));
        }

        void CompleteCreditRequest(ui32 grantedChunks) {
            if (!Enabled) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            Y_ABORT_UNLESS(CreditRequestPending);
            const ui64 consumed = Min<ui64>(ConsumedBeforeGrantObserved, grantedChunks);
            ConsumedBeforeGrantObserved -= consumed;
            GrantedChunks = SaturatingAdd(GrantedChunks, grantedChunks - consumed);
            CreditRequestPending = false;
            // A zero grant means PDisk is already at BLACK (or has no free
            // chunks). Immediate re-requests would keep user writes queued
            // forever; Skeleton retries on a timer via AllowCreditRequest.
            CreditRequestSuppressed = grantedChunks == 0;
        }

        // Return credit left over when the conservative Fresh estimate was
        // larger than the SST that was actually produced. The refill cushion
        // remains charged so that a small write burst does not need a PDisk
        // round trip. Local accounting is reduced optimistically; while this
        // request is in flight no other credit operation may be started.
        std::optional<ui32> BeginCreditRelease(ui64 freshChunks) {
            if (!Enabled) {
                return std::nullopt;
            }
            TGuard<TSpinLock> guard(Lock);
            if (CreditRequestPending || CreditReleasePending || ConsumedBeforeGrantObserved) {
                return std::nullopt;
            }
            const ui64 required = SaturatingAdd(freshChunks, CalculateInFlightChunks(InFlightBytes));
            const ui64 target = SaturatingAdd(required, RefillChunks);
            if (GrantedChunks <= target) {
                return std::nullopt;
            }
            CreditReleasePending = static_cast<ui32>(Min<ui64>(GrantedChunks - target, Max<ui32>()));
            GrantedChunks -= CreditReleasePending;
            return CreditReleasePending;
        }

        void CompleteCreditRelease(ui32 releasedChunks) {
            if (!Enabled) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            Y_ABORT_UNLESS(CreditReleasePending == releasedChunks,
                "Fresh credit release mismatch: pending# %u released# %u",
                CreditReleasePending, releasedChunks);
            CreditReleasePending = 0;
        }

        void FailCreditRequest() {
            if (!Enabled) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            CreditRequestPending = false;
            CreditRequestSuppressed = true;
        }

        void AllowCreditRequest() {
            if (!Enabled) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            CreditRequestSuppressed = false;
        }

        void ConsumeCredits(ui32 chunks) {
            if (!Enabled || !chunks) {
                return;
            }
            TGuard<TSpinLock> guard(Lock);
            const ui64 fromObserved = Min<ui64>(GrantedChunks, chunks);
            GrantedChunks -= fromObserved;
            ConsumedBeforeGrantObserved = SaturatingAdd(ConsumedBeforeGrantObserved, chunks - fromObserved);
        }

        ui64 GetGrantedChunks() const {
            TGuard<TSpinLock> guard(Lock);
            return GrantedChunks;
        }

        bool HasPendingCreditOperation() const {
            TGuard<TSpinLock> guard(Lock);
            return CreditRequestPending || CreditReleasePending;
        }
    };

} // NKikimr
