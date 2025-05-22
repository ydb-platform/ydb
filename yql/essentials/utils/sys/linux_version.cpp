#include "linux_version.h"

#include <util/generic/yexception.h>
#include <util/system/platform.h>

#ifdef _linux_
#   include <sys/utsname.h>
#endif

namespace NYql {
    std::tuple<int, int, int> DetectLinuxKernelVersion3() {
#ifdef _linux_
        // see https://github.com/torvalds/linux/blob/master/Makefile
        // version is composed as follows:
        // VERSION = 4
        // PATCHLEVEL = 18
        // SUBLEVEL = 0
        // EXTRAVERSION = -rc4
        // KERNELVERSION = $(VERSION)$(if $(PATCHLEVEL),.$(PATCHLEVEL)$(if $(SUBLEVEL),.$(SUBLEVEL)))$(EXTRAVERSION)

        utsname buf = {};
        if (uname(&buf)) {
            ythrow TSystemError() << "uname call failed";
        }

        int v = 0;
        int p = 0;
        int s = 0;
        if (sscanf(buf.release, "%d.%d.%d", &v, &p, &s) != 3) {
            ythrow yexception() << "Failed to parse linux kernel version " << buf.release;
        }
        return std::make_tuple(v, p, s);
#else
        return {};
#endif
    }

    std::pair<int, int> DetectLinuxKernelVersion2() {
        auto v = DetectLinuxKernelVersion3();
        return std::make_pair(std::get<0>(v), std::get<1>(v));
    }

    bool IsLinuxKernelBelow4_3() {
        return DetectLinuxKernelVersion2() < std::make_pair(4, 3);
    }
}
