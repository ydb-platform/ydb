#include "x86simdsort-static-incl.h"

int main()
{
    const int size = 1000;
    _Float16 arr[size];
    x86simdsortStatic::qsort(arr, size);
    x86simdsortStatic::qselect(arr, 10, size);
    x86simdsortStatic::partial_qsort(arr, 10, size);
    return 0;
}
