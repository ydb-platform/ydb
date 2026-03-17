#include "pycallfault.h"

#include <stdlib.h>


static int pycall      = -1;
static int pycall_fail = -1;
static int pycall_trap = 0;

void initialize_pycallfault(void) {
    const char* fail   = getenv("PYCALL_FAIL");
    const char* trap   = getenv("PYCALL_TRAP");

    if (fail != NULL) {
        pycall_fail = atoi(fail);
    }

    if (trap != NULL) {
        pycall_trap = 1;
    }
}


int check(void) {
    pycall += 1;
    printf("Fail ID: %d\n", pycall);

    if (pycall == pycall_fail) {
        if (pycall_trap) {
            __builtin_trap();
        }

        printf("Failed pycall #%d\n", pycall);
        return 1;
    }

    return 0;
}


int check_and_set_error(void) {
    if (check()) {
        PyErr_NoMemory();
        return 1;
    }

    return 0;
}

