//
//  readstat_bits.c - Bit-twiddling utility functions
//

#include <sys/types.h>
#include <stdint.h>
#include <string.h>

#include "readstat_bits.h"

int machine_is_little_endian(void) {
    int test_byte_order = 1;
    return ((char *)&test_byte_order)[0];
}

char ones_to_twos_complement1(char num) {
    return num < 0 ? num+1 : num;
}

int16_t ones_to_twos_complement2(int16_t num) {
    return num < 0 ? num+1 : num;
}

int32_t ones_to_twos_complement4(int32_t num) {
    return num < 0 ? num+1 : num;
}

char twos_to_ones_complement1(char num) {
    return num < 0 ? num-1 : num;
}

int16_t twos_to_ones_complement2(int16_t num) {
    return num < 0 ? num-1 : num;
}

int32_t twos_to_ones_complement4(int32_t num) {
    return num < 0 ? num-1 : num;
}

uint16_t byteswap2(uint16_t num) {
    return ((num & 0xFF00) >> 8) | ((num & 0x00FF) << 8);
}

uint32_t byteswap4(uint32_t num) {
    num = ((num & 0xFFFF0000) >> 16) | ((num & 0x0000FFFF) << 16);
    return ((num & 0xFF00FF00) >> 8) | ((num & 0x00FF00FF) << 8);
}

uint64_t byteswap8(uint64_t num) {
    num = ((num & 0xFFFFFFFF00000000) >> 32) | ((num & 0x00000000FFFFFFFF) << 32);
    num = ((num & 0xFFFF0000FFFF0000) >> 16) | ((num & 0x0000FFFF0000FFFF) << 16);
    return ((num & 0xFF00FF00FF00FF00) >> 8) | ((num & 0x00FF00FF00FF00FF) << 8);
}

float byteswap_float(float num) {
    uint32_t answer = 0;
    memcpy(&answer, &num, 4);
    answer = byteswap4(answer);
    memcpy(&num, &answer, 4);
    return num;
}

double byteswap_double(double num) {
    uint64_t answer = 0;
    memcpy(&answer, &num, 8);
    answer = byteswap8(answer);
    memcpy(&num, &answer, 8);
    return num;
}
