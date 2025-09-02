#include "ec_base_vsx.h"

void gf_vect_dot_prod_vsx(int len, int vlen, unsigned char *gftbls,
			  unsigned char **src, unsigned char *dest)
{
	unsigned char *s, *t0;
	vector unsigned char vX1, vY1;
	vector unsigned char vX2, vY2;
	vector unsigned char vX3, vY3;
	vector unsigned char vX4, vY4;
	vector unsigned char vX5, vY5;
	vector unsigned char vX6, vY6;
	vector unsigned char vX7, vY7;
	vector unsigned char vX8, vY8;
	vector unsigned char vhi0, vlo0;
	int i, j, head;

	if (vlen < 128) {
		gf_vect_mul_vsx(len, &gftbls[0 * 32 * vlen], src[0], (unsigned char *)dest);

		for (j = 1; j < vlen; j++) {
			gf_vect_mad_vsx(len, vlen, j, gftbls, src[j], dest);
		}
		return;
	}

	t0 = (unsigned char *)dest;

	head = len % 128;
	if (head != 0) {
		gf_vect_dot_prod_base(head, vlen, &gftbls[0 * 32 * vlen], src, t0);
	}

	for (i = head; i < len - 127; i += 128) {
		vY1 = vY1 ^ vY1;
		vY2 = vY2 ^ vY2;
		vY3 = vY3 ^ vY3;
		vY4 = vY4 ^ vY4;

		vY5 = vY5 ^ vY5;
		vY6 = vY6 ^ vY6;
		vY7 = vY7 ^ vY7;
		vY8 = vY8 ^ vY8;

		unsigned char *g0 = &gftbls[0 * 32 * vlen];

		for (j = 0; j < vlen; j++) {
			s = (unsigned char *)src[j];
			vX1 = vec_xl(0, s + i);
			vX2 = vec_xl(16, s + i);
			vX3 = vec_xl(32, s + i);
			vX4 = vec_xl(48, s + i);

			vlo0 = EC_vec_xl(0, g0);
			vhi0 = EC_vec_xl(16, g0);

			vX5 = vec_xl(64, s + i);
			vX6 = vec_xl(80, s + i);
			vX7 = vec_xl(96, s + i);
			vX8 = vec_xl(112, s + i);

			vY1 = vY1 ^ EC_vec_permxor(vhi0, vlo0, vX1);
			vY2 = vY2 ^ EC_vec_permxor(vhi0, vlo0, vX2);
			vY3 = vY3 ^ EC_vec_permxor(vhi0, vlo0, vX3);
			vY4 = vY4 ^ EC_vec_permxor(vhi0, vlo0, vX4);

			vY5 = vY5 ^ EC_vec_permxor(vhi0, vlo0, vX5);
			vY6 = vY6 ^ EC_vec_permxor(vhi0, vlo0, vX6);
			vY7 = vY7 ^ EC_vec_permxor(vhi0, vlo0, vX7);
			vY8 = vY8 ^ EC_vec_permxor(vhi0, vlo0, vX8);

			g0 += 32;
		}
		vec_xst(vY1, 0, t0 + i);
		vec_xst(vY2, 16, t0 + i);
		vec_xst(vY3, 32, t0 + i);
		vec_xst(vY4, 48, t0 + i);

		vec_xst(vY5, 64, t0 + i);
		vec_xst(vY6, 80, t0 + i);
		vec_xst(vY7, 96, t0 + i);
		vec_xst(vY8, 112, t0 + i);
	}
	return;
}
