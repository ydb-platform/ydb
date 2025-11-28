#include "key_value.h"


int main(int argc, char** argv) {
    return DoMain(argc, argv, DoCreate, DoRun, DoCleanup);
}
