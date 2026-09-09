#pragma once

#include <util/generic/algorithm.h>
#include <util/system/types.h>

namespace NActors {

    // Per-socket cap on serialized-but-not-CQE'd bytes. The max is taken from the configured
    // send-buffer bound; a short write trims that socket only, and a full write of the current
    // target grows it back. Underfilled writes do not shrink. Main and XDC are independent so
    // payload congestion cannot collapse command batching (and the reverse).
    class TSerializeWindow {
    public:
        static constexpr size_t MaxMainWindowWithXdc = 64 * 1024;

    private:
        struct TCap {
            size_t Target = 0;
            size_t MinSize = 0;
            size_t MaxSize = 0;

            TCap() = default;

            TCap(size_t minSize, size_t maxSize)
                : Target(Max(minSize, maxSize))
                , MinSize(minSize)
                , MaxSize(Max(minSize, maxSize))
            {}

            size_t Remaining(size_t unsent) const {
                return unsent < Target ? Target - unsent : 0;
            }

            void OnWriteComplete(size_t num, size_t requested) {
                if (num != requested) {
                    Target = Max(Target - MinSize, MinSize);
                } else if (requested >= Target) {
                    Target = Min(Target + MinSize, MaxSize);
                }
            }
        };

        TCap Main;
        TCap Xdc;
        bool HasXdc = false;

    public:
        TSerializeWindow() = default;

        TSerializeWindow(size_t minSize, size_t maxSize, bool hasXdc = false)
            : Main(minSize, hasXdc ? Min(maxSize, MaxMainWindowWithXdc) : maxSize)
            , Xdc(minSize, maxSize)
            , HasXdc(hasXdc)
        {}

        size_t RemainingMain(size_t unsent) const {
            return Main.Remaining(unsent);
        }

        size_t RemainingXdc(size_t unsent) const {
            return HasXdc ? Xdc.Remaining(unsent) : 0;
        }

        size_t GetMainSize() const {
            return Main.Target;
        }

        size_t GetXdcSize() const {
            return HasXdc ? Xdc.Target : 0;
        }

        // Combined in-flight bound shown in session HTML.
        size_t GetSize() const {
            return GetMainSize() + GetXdcSize();
        }

        void CompleteWrite(size_t num, size_t requested, bool xdc) {
            (xdc && HasXdc ? Xdc : Main).OnWriteComplete(num, requested);
        }
    };

} // namespace NActors
