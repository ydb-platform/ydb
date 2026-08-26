#include "x86simdsort-static-incl.h"

int main()
{
    const int size = 1000;
    double arrd[size];
    float arrf[size];
    x86simdsortStatic::qsort(arrf, size);
    x86simdsortStatic::qsort(arrd, size);
    x86simdsortStatic::qselect(arrf, 10, size);
    x86simdsortStatic::qselect(arrd, 10, size);
    x86simdsortStatic::partial_qsort(arrf, 10, size);
    x86simdsortStatic::partial_qsort(arrd, 10, size);
    auto arg1 = x86simdsortStatic::argsort(arrf, size);
    auto arg2 = x86simdsortStatic::argselect(arrf, 10, size);
    auto arg3 = x86simdsortStatic::argsort(arrd, size);
    auto arg4 = x86simdsortStatic::argselect(arrd, 10, size);
    return 0;
}
