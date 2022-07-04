#include <library/cpp/balloc/optional/operators.h>

#undef NDEBUG

#include <cstdlib>
#include <cstring>
#include <cassert>

// internal hook for the testing
extern "C" bool IsOwnedByBalloc(void* ptr);

int main() {
    char* buf = (char*)malloc(100);
    assert(true == IsOwnedByBalloc(buf));

    ThreadDisableBalloc();
    char* buf2 = (char*)malloc(100);
    assert(false == IsOwnedByBalloc(buf2));

    free(buf);
    free(buf2);

    return 0;
}
