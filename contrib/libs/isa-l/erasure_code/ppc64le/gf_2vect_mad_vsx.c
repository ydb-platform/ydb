#include "ec_base_vsx.h"

void gf_2vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls,
		      unsigned char *src, unsigned char **dest)
{
	unsigned char *s, *t0, *t1;
	vector unsigned char vX1, vX2, vX3, vX4;
	vector unsigned char vY1, vY2, vY3, vY4;
	vector unsigned char vYD, vYE, vYF, vYG;
	vector unsigned char vhi0, vlo0, vhi1, vlo1;
	int i, head;

	s = (unsigned char *)src;
	t0 = (unsigned char *)dest[0];
	t1 = (unsigned char *)dest[1];

	head = len % 64;
	if (head != 0) {
		gf_vect_mad_base(head, vec, vec_i, &gftbls[0 * 32 * vec], src, t0);
		gf_vect_mad_base(head, vec, vec_i, &gftbls[1 * 32 * vec], src, t1);
	}

	vlo0 = EC_vec_xl(0, gftbls + (((0 * vec) << 5) + (vec_i << 5)));
	vhi0 = EC_vec_xl(16, gftbls + (((0 * vec) << 5) + (vec_i << 5)));
	vlo1 = EC_vec_xl(0, gftbls + (((1 * vec) << 5) + (vec_i << 5)));
	vhi1 = EC_vec_xl(16, gftbls + (((1 * vec) << 5) + (vec_i << 5)));

	for (i = head; i < len - 63; i += 64) {
		vX1 = vec_xl(0, s + i);
		vX2 = vec_xl(16, s + i);
		vX3 = vec_xl(32, s + i);
		vX4 = vec_xl(48, s + i);

		vY1 = vec_xl(0, t0 + i);
		vY2 = vec_xl(16, t0 + i);
		vYD = vec_xl(32, t0 + i);
		vYE = vec_xl(48, t0 + i);

		vY1 = vY1 ^ EC_vec_permxor(vhi0, vlo0, vX1);
		vY2 = vY2 ^ EC_vec_permxor(vhi0, vlo0, vX2);
		vYD = vYD ^ EC_vec_permxor(vhi0, vlo0, vX3);
		vYE = vYE ^ EC_vec_permxor(vhi0, vlo0, vX4);

		vY3 = vec_xl(0, t1 + i);
		vY4 = vec_xl(16, t1 + i);
		vYF = vec_xl(32, t1 + i);
		vYG = vec_xl(48, t1 + i);

		vec_xst(vY1, 0, t0 + i);
		vec_xst(vY2, 16, t0 + i);
		vec_xst(vYD, 32, t0 + i);
		vec_xst(vYE, 48, t0 + i);

		vY3 = vY3 ^ EC_vec_permxor(vhi1, vlo1, vX1);
		vY4 = vY4 ^ EC_vec_permxor(vhi1, vlo1, vX2);
		vYF = vYF ^ EC_vec_permxor(vhi1, vlo1, vX3);
		vYG = vYG ^ EC_vec_permxor(vhi1, vlo1, vX4);

		vec_xst(vY3, 0, t1 + i);
		vec_xst(vY4, 16, t1 + i);
		vec_xst(vYF, 32, t1 + i);
		vec_xst(vYG, 48, t1 + i);
	}
	return;
}
