#include "table.h"
#include "tables2.inc"

static struct TQuickLZMethods* qlz_tables[2][4][3] =
    #include "tables1.inc"
;

struct TQuickLZMethods* GetLzqTable(unsigned ver, unsigned level, unsigned buf) {
    return qlz_tables[ver][level][buf];
}
