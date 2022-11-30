#include <util/system/defaults.h>
//typedef unsigned int ui32;
//typedef unsigned short int ui16;

#include "table.h"
#include "tables.inc"

struct TQuickLZMethods* GetLzq151Table(unsigned level, unsigned buf) {
    return methods_qlz151[level][buf];
}
