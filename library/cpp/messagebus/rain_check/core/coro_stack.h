#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/system/valgrind.h>

namespace NRainCheck {
    namespace NPrivate {
        struct TCoroStack {
            THolder<void, TFree> DataHolder;
            size_t SizeValue;

#if defined(WITH_VALGRIND)
            size_t ValgrindStackId;
#endif

            TCoroStack(size_t size);
            ~TCoroStack();

            void* Data() {
                return DataHolder.Get();
            }

            size_t Size() {
                return SizeValue;
            }

            TArrayRef<char> MemRegion() {
                return TArrayRef((char*)Data(), Size());
            }

            ui32* MagicNumberLocation() {
#if STACK_GROW_DOWN == 1
                return (ui32*)Data();
#elif STACK_GROW_DOWN == 0
                return ((ui32*)(((char*)Data()) + Size())) - 1;
#else
#error "unknown"
#endif
            }

            static void FailStackOverflow();

            inline void VerifyNoStackOverflow() noexcept {
                if (Y_UNLIKELY(*MagicNumberLocation() != MAGIC_NUMBER)) {
                    FailStackOverflow();
                }
            }

            static const ui32 MAGIC_NUMBER = 0xAB4D15FE;
        };

    }
}
