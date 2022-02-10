#include <util/stream/output.h>

#define POLY_16 0x1021
#define POLY_32 0xEDB88320UL
#define POLY_64 ULL(0xE543279765927881)

static void crc16init() {
    ui32 CRCTAB16[256];
    ui32 crc;
    int i, j;

    for (i = 0; i < 256; CRCTAB16[i++] = 0xFFFF & ((crc << 8) ^ (crc >> 8)))
        for (crc = i, j = 8; j > 0; j--)
            if (crc & 1)
                crc = (crc >> 1) ^ POLY_16;
            else
                crc >>= 1;

    for (size_t k = 0; k < 256; ++k) {
        Cout << "    ULL(" << CRCTAB16[k] << ")";

        if (k != 255) {
            Cout << ",\n";
        }
    }
}

static void crc32init() {
    ui32 CRCTAB32[256];
    ui32 crc;
    int i, j;

    for (i = 0; i < 256; CRCTAB32[i++] = crc)
        for (crc = i, j = 8; j > 0; j--)
            if (crc & 1)
                crc = (crc >> 1) ^ POLY_32;
            else
                crc >>= 1;

    for (size_t k = 0; k < 256; ++k) {
        Cout << "    ULL(" << CRCTAB32[k] << ")";

        if (k != 255) {
            Cout << ",\n";
        }
    }
}

int main() {
    Cout << "static const ui32 CRCTAB16[] = {\n\n";
    crc16init();
    Cout << "\n};\n\n";
    Cout << "static const ui32 CRCTAB32[] = {\n\n";
    crc32init();
    Cout << "\n};\n\n";
}
