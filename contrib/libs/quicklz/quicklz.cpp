#include "quicklz.h"

extern "C" struct TQuickLZMethods* GetLzq151Table(unsigned level, unsigned buf);

const TQuickLZMethods* LzqTable(unsigned ver, unsigned level, unsigned buf) {
    if (ver > 2 || level > 3 || buf > 2) {
        return 0;
    }

    if (ver == 2) {
        if (!level) {
            return 0;
        }

        return GetLzq151Table(level - 1, buf);
    }

    return GetLzqTable(ver, level, buf);
}
