#include "str_hash.h"

#include <library/cpp/charset/ci_string.h>
#include <util/stream/output.h>
#include <util/stream/input.h>

HashSet::HashSet(const char** array, size_type size) {
    Resize(size);
    while (*array && **array)
        AddPermanent(*array++);
}

void HashSet::Read(IInputStream* input) {
    TString s;

    while (input->ReadLine(s)) {
        AddUniq(TCiString(s).c_str());
    }
}

void HashSet::Write(IOutputStream* output) const {
    for (const auto& it : *this) {
        *output << it.first << "\n";
    }
}

#ifdef TEST_STRHASH
#include <ctime>
#include <fstream>
#include <cstdio>
#include <cstdlib>

using namespace std;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("usage: stoplist <stop-words file ...\n");
        exit(EXIT_FAILURE); // FreeBSD: EX_USAGE
    }
    Hash hash;
    hash.Read(cin);
    for (--argc, ++argv; argc > 0; --argc, ++argv) {
        ifstream input(argv[0]);
        if (!input.good()) {
            perror(argv[0]);
            continue;
        }
        TCiString s;
        while (input >> s) {
            if (!hash.Has(s))
                cout << s << "\n";
            else
                cout << "[[" << s << "]]"
                     << "\n";
        }
    }
    return EXIT_SUCCESS; // EX_OK
}

#endif
