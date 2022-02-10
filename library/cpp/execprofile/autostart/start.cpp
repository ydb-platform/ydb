#include <library/cpp/execprofile/profile.h> 

namespace {
    struct TInit {
        inline TInit() {
            BeginProfiling();
        }

        inline ~TInit() {
            EndProfiling();
        }
    };

    const TInit initer;
}
