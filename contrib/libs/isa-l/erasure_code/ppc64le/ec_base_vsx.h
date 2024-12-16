#ifndef _ERASURE_CODE_PPC64LE_H_
#define _ERASURE_CODE_PPC64LE_H_

#include "erasure_code.h"
#include <altivec.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__ibmxl__)
#define EC_vec_xl(a, b) vec_xl_be(a, b)
#define EC_vec_permxor(va, vb, vc) __vpermxor(va, vb, vc)
#elif defined __GNUC__ && __GNUC__ >= 8
#define EC_vec_xl(a, b) vec_xl_be(a, b)
#define EC_vec_permxor(va, vb, vc) __builtin_crypto_vpermxor(va, vb, vc)
#elif defined __GNUC__ && __GNUC__ >= 7
#if defined _ARCH_PWR9
#define EC_vec_xl(a, b) vec_vsx_ld(a, b)
#define EC_vec_permxor(va, vb, vc) __builtin_crypto_vpermxor(va, vb, vec_nor(vc, vc))
#else
inline vector unsigned char EC_vec_xl(int off, unsigned char *ptr) {
	vector unsigned char vc;
	__asm__ __volatile__("lxvd2x %x0, %1, %2; xxswapd %x0, %x0" : "=wa" (vc) : "r" (off), "r" (ptr));
	return vc;
}
#define EC_vec_permxor(va, vb, vc) __builtin_crypto_vpermxor(va, vb, vec_nor(vc, vc))
#endif
#else
#if defined _ARCH_PWR8
inline vector unsigned char EC_vec_xl(int off, unsigned char *ptr) {
	vector unsigned char vc;
	__asm__ __volatile__("lxvd2x %x0, %1, %2; xxswapd %x0, %x0" : "=wa" (vc) : "r" (off), "r" (ptr));
	return vc;
}
#define EC_vec_permxor(va, vb, vc) __builtin_crypto_vpermxor(va, vb, vec_nor(vc, vc))
#else
#error "This code is only supported on ppc64le."
#endif
#endif

/**
 * @brief GF(2^8) vector multiply. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constant and save to destination array. Can be used for erasure coding encode
 * and decode update when only one source is available at a time. Function
 * requires pre-calculation of a 32 byte constant array based on the input
 * coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_mul_vsx(int len, unsigned char *gftbls, unsigned char *src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product. VSX version.
 *
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			  unsigned char **src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product with two outputs. VSX version.
 *
 * Vector dot product optimized to calculate two outputs at a time. Does two
 * GF(2^8) dot products across each byte of the input array and two constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 2*32*vlen byte constant array based on the two sets of input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 2*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_2vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			   unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with three outputs. VSX version.
 *
 * Vector dot product optimized to calculate three outputs at a time. Does three
 * GF(2^8) dot products across each byte of the input array and three constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 3*32*vlen byte constant array based on the three sets of input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 3*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_3vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			   unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with four outputs. VSX version.
 *
 * Vector dot product optimized to calculate four outputs at a time. Does four
 * GF(2^8) dot products across each byte of the input array and four constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 4*32*vlen byte constant array based on the four sets of input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 4*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_4vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			   unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with five outputs. VSX version.
 *
 * Vector dot product optimized to calculate five outputs at a time. Does five
 * GF(2^8) dot products across each byte of the input array and five constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 5*32*vlen byte constant array based on the five sets of input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes. Must >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 5*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_5vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			   unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with six outputs. VSX version.
 *
 * Vector dot product optimized to calculate six outputs at a time. Does six
 * GF(2^8) dot products across each byte of the input array and six constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 6*32*vlen byte constant array based on the six sets of input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 6*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_6vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			   unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constant and add to destination array. Can be used for erasure coding encode
 * and decode update when only one source is available at a time. Function
 * requires pre-calculation of a 32*vec byte constant array based on the input
 * coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		     unsigned char *dest);
/**
 * @brief GF(2^8) vector multiply with 2 accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_2vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 3 accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_3vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 4 accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_4vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 5 accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */
void gf_5vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 6 accumulate. VSX version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires VSX
 *
 * @param len    Length of each vector in bytes.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */
void gf_6vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

#ifdef __cplusplus
}
#endif

#endif //_ERASURE_CODE_PPC64LE_H_
