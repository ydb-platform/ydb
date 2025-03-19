#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/compiler.h>

#include <sys/times.h>

#include <util/random/random.h>
#include <util/stream/format.h>

#include "keys.h"

int main(int, const char* []) {

#if 0
    // gen key set(s)
    SetRandomSeed(0x39673D75ul);
    for (auto i = 0; i < 8; i++) {
        Cerr << "    {" << Endl;
        for (auto j = 0; j < 10; j++) {
            Cerr << "        ";
            for (auto k = 0; k < 10; k++) {
                Cerr << Hex(RandomNumber<ui64>()) << "ull, ";
            }
            Cerr << Endl;
        }
        Cerr << Endl << "    }," << Endl;
    }
    return 0;
#endif

#if 1
    {
        TFileOutput data10("akeys1Tx8x10.dat");
        ui64 line[16][8];
        SetRandomSeed(0x282B79F5ul);

        for (ui64 i = 0; i < (1ull << 30); i++) {
            for(auto b = 0; b < 16; b++) {
                for(auto k = 0; k < 8; k++) {
                    line[b][k] = agen_keys[k][RandomNumber<ui64>(10)];
                }
            }
            data10.Write(line, 16 * 8 * 8);
        }

        data10.Flush();
    }
#endif

#if 1
    {
        TFileOutput data10("akeys1Tx8x100.dat");
        ui64 line[16][8];
        SetRandomSeed(0x282B79F5ul);

        for (ui64 i = 0; i < (1ull << 30); i++) {
            for(auto b = 0; b < 16; b++) {
                for(auto k = 0; k < 8; k++) {
                    line[b][k] = agen_keys[k][RandomNumber<ui64>(100)];
                }
            }
            data10.Write(line, 16 * 8 * 8);
        }

        data10.Flush();
    }
#endif

    return 0;
}