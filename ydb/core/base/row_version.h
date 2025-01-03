#pragma once

#include <util/system/types.h>
#include <util/generic/ylimits.h>

namespace NKikimrProto {
    class TRowVersion;
}

namespace NKikimr {

    /**
     * Row version is a (step,txid) pair
     */
    struct TRowVersion {
        ui64 Step;
        ui64 TxId;

        // Note: this leaves Step and TxId uninitialized by default
        TRowVersion() noexcept = default;

        constexpr TRowVersion(ui64 step, ui64 txId) noexcept
            : Step(step)
            , TxId(txId)
        { }

        static constexpr TRowVersion Min() noexcept { return TRowVersion(0, 0); }
        static constexpr TRowVersion Max() noexcept { return TRowVersion(::Max<ui64>(), ::Max<ui64>()); }

        constexpr bool IsMin() const noexcept {
            return Step == 0 && TxId == 0;
        }

        constexpr bool IsMax() const noexcept {
            return Step == ::Max<ui64>() && TxId == ::Max<ui64>();
        }

        constexpr explicit operator bool() const noexcept {
            return Step || TxId;
        }

        constexpr TRowVersion& operator--() noexcept {
            if (/* underflow */ 0 == TxId--) {
                --Step;
            }
            return *this;
        }

        constexpr TRowVersion& operator++() noexcept {
            if (/* overflow */ 0 == ++TxId) {
                ++Step; // overflow
            }
            return *this;
        }

        constexpr TRowVersion operator--(int) noexcept {
            TRowVersion copy = *this;
            --(*this);
            return copy;
        }

        constexpr TRowVersion operator++(int) noexcept {
            TRowVersion copy = *this;
            ++(*this);
            return copy;
        }

        constexpr TRowVersion Prev() const noexcept {
            TRowVersion copy = *this;
            return --copy;
        }

        constexpr TRowVersion Next() const noexcept {
            TRowVersion copy = *this;
            return ++copy;
        }

        static TRowVersion FromProto(const NKikimrProto::TRowVersion& proto);
        void ToProto(NKikimrProto::TRowVersion& proto) const;
        void ToProto(NKikimrProto::TRowVersion* proto) const;
        NKikimrProto::TRowVersion ToProto() const;

        friend constexpr bool operator==(const TRowVersion& a, const TRowVersion& b) {
            return a.Step == b.Step && a.TxId == b.TxId;
        }

        friend constexpr bool operator!=(const TRowVersion& a, const TRowVersion& b) {
            return a.Step != b.Step || a.TxId != b.TxId;
        }

        friend constexpr bool operator<(const TRowVersion& a, const TRowVersion& b) {
            return a.Step < b.Step || (a.Step == b.Step && a.TxId < b.TxId);
        }

        friend constexpr bool operator>(const TRowVersion& a, const TRowVersion& b) {
            return a.Step > b.Step || (a.Step == b.Step && a.TxId > b.TxId);
        }

        friend constexpr bool operator<=(const TRowVersion& a, const TRowVersion& b) {
            return a.Step < b.Step || (a.Step == b.Step && a.TxId <= b.TxId);
        }

        friend constexpr bool operator>=(const TRowVersion& a, const TRowVersion& b) {
            return a.Step > b.Step || (a.Step == b.Step && a.TxId >= b.TxId);
        }
    };

}
