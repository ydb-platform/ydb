#include <cstdlib>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <iostream>
#include <string>
#include <string_view>
#include <string_view>
#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

int test(int argc, char** argv) {
    for(size_t i = 0; i < 100000; ++i) {
        if (argc > 1 && TStringBuf(argv[1]) ==  "use-after-free") {
            size_t s = 31;
            volatile int* a = new int[s];
            delete[] a;
            for (size_t i = 0; i != s; ++i){
                volatile int val = a[i];
            }
        } else if (argc > 1 && TStringBuf(argv[1]) ==  "bounds") {
            volatile int* b = new int[128];
            for (size_t i = 0; i != 129; ++i)
                volatile int val = b[i];
            delete[] b;
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    std::cout << "W/o GWP-ASan" << std::endl;
    test(argc, argv);
    std::cout << "OK\n\n" << std::endl;

    tcmalloc::MallocExtension::SetProfileSamplingInterval(1);
    tcmalloc::MallocExtension::SetGuardedSamplingInterval(1);
    tcmalloc::MallocExtension::ActivateGuardedSampling();

    std::cout << "With GWP-ASan" << std::endl; 
    test(argc, argv);
    std::cout << "OK\n\n" << std::endl;
}

