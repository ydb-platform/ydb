#include <stdint.h>

#include "readstat_xport.h"
#include "../readstat_bits.h"

char _xport_months[12][4] = {
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };

void xport_namestr_bswap(xport_namestr_t *namestr) {
    if (!machine_is_little_endian())
        return;

    namestr->ntype = byteswap2(namestr->ntype);
    namestr->nhfun = byteswap2(namestr->nhfun);
    namestr->nlng = byteswap2(namestr->nlng);
    namestr->nvar0 = byteswap2(namestr->nvar0);

    namestr->nfl = byteswap2(namestr->nfl);
    namestr->nfd = byteswap2(namestr->nfd);
    namestr->nfj = byteswap2(namestr->nfj);

    namestr->nifl = byteswap2(namestr->nifl);
    namestr->nifd = byteswap2(namestr->nifd);
    namestr->npos = byteswap4(namestr->npos);

    namestr->labeln  = byteswap2(namestr->labeln);
}
