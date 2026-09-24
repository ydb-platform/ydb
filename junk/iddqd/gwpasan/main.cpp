#include <cstdlib>
//#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <string>
#include <string_view>

int main(int argc, char** argv) {
    for(size_t i = 0; i < 100000; ++i) {
        if (argc > 1 && TStringBuf(argv[1]) ==  "use-after-free") {
            int* a = new int;
            delete a;
            *a = 1;
        } else if (argc > 1 && TStringBuf(argv[1]) ==  "bounds") {
            auto b = new int[1000];
            b[1000] = 2;
            delete b;
        } else {
            std::string s = "Hellooooooooooooooo ";
            std::string_view sv = s + "World\n";
            Cout << sv << Endl;
        }
    }
    return 0;
}
