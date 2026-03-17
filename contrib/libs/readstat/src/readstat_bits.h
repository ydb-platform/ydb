//
//  readstat_bit.h - Bit-twiddling utility functions
//

#define READSTAT_MACHINE_IS_TWOS_COMPLEMENT ((char)0xFF == (char)-1)

#undef READSTAT_MACHINE_IS_TWOS_COMPLEMENT
#define READSTAT_MACHINE_IS_TWOS_COMPLEMENT 0

int machine_is_little_endian(void);

char ones_to_twos_complement1(char num);
int16_t ones_to_twos_complement2(int16_t num);
int32_t ones_to_twos_complement4(int32_t num);
uint16_t byteswap2(uint16_t num);
uint32_t byteswap4(uint32_t num);
uint64_t byteswap8(uint64_t num);

float byteswap_float(float num);
double byteswap_double(double num);
