#include "erasure_code.h"
#include "ec_base_vsx.h"

void gf_vect_dot_prod(int len, int vlen, unsigned char *v,
		      unsigned char **src, unsigned char *dest)
{
	gf_vect_dot_prod_vsx(len, vlen, v, src, dest);
}

void gf_vect_mad(int len, int vec, int vec_i, unsigned char *v,
		 unsigned char *src, unsigned char *dest)
{
	gf_vect_mad_vsx(len, vec, vec_i, v, src, dest);

}

void ec_encode_data(int len, int srcs, int dests, unsigned char *v,
		    unsigned char **src, unsigned char **dest)
{
	if (len < 64) {
		ec_encode_data_base(len, srcs, dests, v, src, dest);
		return;
	}

	while (dests >= 6) {
		gf_6vect_dot_prod_vsx(len, srcs, v, src, dest);
		v += 6 * srcs * 32;
		dest += 6;
		dests -= 6;
	}
	switch (dests) {
	case 6:
		gf_6vect_dot_prod_vsx(len, srcs, v, src, dest);
		break;
	case 5:
		gf_5vect_dot_prod_vsx(len, srcs, v, src, dest);
		break;
	case 4:
		gf_4vect_dot_prod_vsx(len, srcs, v, src, dest);
		break;
	case 3:
		gf_3vect_dot_prod_vsx(len, srcs, v, src, dest);
		break;
	case 2:
		gf_2vect_dot_prod_vsx(len, srcs, v, src, dest);
		break;
	case 1:
		gf_vect_dot_prod_vsx(len, srcs, v, src, *dest);
		break;
	case 0:
		break;
	}
}

void ec_encode_data_update(int len, int k, int rows, int vec_i, unsigned char *v,
			   unsigned char *data, unsigned char **dest)
{
	if (len < 64) {
		ec_encode_data_update_base(len, k, rows, vec_i, v, data, dest);
		return;
	}

	while (rows >= 6) {
		gf_6vect_mad_vsx(len, k, vec_i, v, data, dest);
		v += 6 * k * 32;
		dest += 6;
		rows -= 6;
	}
	switch (rows) {
	case 6:
		gf_6vect_mad_vsx(len, k, vec_i, v, data, dest);
		break;
	case 5:
		gf_5vect_mad_vsx(len, k, vec_i, v, data, dest);
		break;
	case 4:
		gf_4vect_mad_vsx(len, k, vec_i, v, data, dest);
		break;
	case 3:
		gf_3vect_mad_vsx(len, k, vec_i, v, data, dest);
		break;
	case 2:
		gf_2vect_mad_vsx(len, k, vec_i, v, data, dest);
		break;
	case 1:
		gf_vect_mad_vsx(len, k, vec_i, v, data, *dest);
		break;
	case 0:
		break;
	}
}

int gf_vect_mul(int len, unsigned char *a, void *src, void *dest)
{
	/* Size must be aligned to 32 bytes */
	if ((len % 32) != 0)
		return -1;

	gf_vect_mul_vsx(len, a, (unsigned char *)src, (unsigned char *)dest);
	return 0;
}

void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
	return ec_init_tables_base(k, rows, a, g_tbls);
}
