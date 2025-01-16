#include "ec_base_vsx.h"

void gf_3vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls,
		      unsigned char *src, unsigned char **dest)
{
	unsigned char *s, *t0, *t1, *t2;
	vector unsigned char vX1, vX2, vX3, vX4;
	vector unsigned char vY1, vY2, vY3, vY4, vY5, vY6;
	vector unsigned char vYD, vYE, vYF, vYG, vYH, vYI;
	vector unsigned char vhi0, vlo0, vhi1, vlo1, vhi2, vlo2;
	int i, head;

	s = (unsigned char *)src;
	t0 = (unsigned char *)dest[0];
	t1 = (unsigned char *)dest[1];
	t2 = (unsigned char *)dest[2];

	head = len % 64;
	if (head != 0) {
		gf_vect_mad_base(head, vec, vec_i, &gftbls[0 * 32 * vec], src, t0);
		gf_vect_mad_base(head, vec, vec_i, &gftbls[1 * 32 * vec], src, t1);
		gf_vect_mad_base(head, vec, vec_i, &gftbls[2 * 32 * vec], src, t2);
	}

	vlo0 = EC_vec_xl(0, gftbls + (((0 * vec) << 5) + (vec_i << 5)));
	vhi0 = EC_vec_xl(16, gftbls + (((0 * vec) << 5) + (vec_i << 5)));
	vlo1 = EC_vec_xl(0, gftbls + (((1 * vec) << 5) + (vec_i << 5)));
	vhi1 = EC_vec_xl(16, gftbls + (((1 * vec) << 5) + (vec_i << 5)));
	vlo2 = EC_vec_xl(0, gftbls + (((2 * vec) << 5) + (vec_i << 5)));
	vhi2 = EC_vec_xl(16, gftbls + (((2 * vec) << 5) + (vec_i << 5)));

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

		vY5 = vec_xl(0, t2 + i);
		vY6 = vec_xl(16, t2 + i);
		vYH = vec_xl(32, t2 + i);
		vYI = vec_xl(48, t2 + i);

		vec_xst(vY3, 0, t1 + i);
		vec_xst(vY4, 16, t1 + i);
		vec_xst(vYF, 32, t1 + i);
		vec_xst(vYG, 48, t1 + i);

		vY5 = vY5 ^ EC_vec_permxor(vhi2, vlo2, vX1);
		vY6 = vY6 ^ EC_vec_permxor(vhi2, vlo2, vX2);
		vYH = vYH ^ EC_vec_permxor(vhi2, vlo2, vX3);
		vYI = vYI ^ EC_vec_permxor(vhi2, vlo2, vX4);

		vec_xst(vY5, 0, t2 + i);
		vec_xst(vY6, 16, t2 + i);
		vec_xst(vYH, 32, t2 + i);
		vec_xst(vYI, 48, t2 + i);
	}
	return;
}
