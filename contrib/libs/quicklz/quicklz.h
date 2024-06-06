#ifndef quicklz_h_sad8fs78d5f
#define quicklz_h_sad8fs78d5f

#include "table.h"
#include "quicklz.inc"
#define FROM_QUICKLZ_BUILD
#include "1_31_0_0.h"
#undef FROM_QUICKLZ_BUILD
#include "table.h"

#if defined(__cplusplus)
const TQuickLZMethods* LzqTable(unsigned ver, unsigned level, unsigned buf);
#endif

#endif
