#include "ec_base_vsx.h"

void gf_vect_mad_vsx(int len, int vec, int vec_i, unsigned char *gftbls,
		     unsigned char *src, unsigned char *dest)
{
	unsigned char *s, *t0;
	vector unsigned char vX1, vY1;
	vector unsigned char vX2, vY2;
	vector unsigned char vX3, vY3;
	vector unsigned char vX4, vY4;
	vector unsigned char vhi0, vlo0;
	int i, head;

	s = (unsigned char *)src;
	t0 = (unsigned char *)dest;

	head = len % 64;
	if (head != 0) {
		gf_vect_mad_base(head, vec, vec_i, &gftbls[0 * 32 * vec], src, dest);
	}

	vlo0 = EC_vec_xl(0, gftbls + (((0 * vec) << 5) + (vec_i << 5)));
	vhi0 = EC_vec_xl(16, gftbls + (((0 * vec) << 5) + (vec_i << 5)));

	for (i = head; i < len - 63; i += 64) {
		vX1 = vec_xl(0, s + i);
		vX2 = vec_xl(16, s + i);
		vX3 = vec_xl(32, s + i);
		vX4 = vec_xl(48, s + i);

		vY1 = vec_xl(0, t0 + i);
		vY2 = vec_xl(16, t0 + i);
		vY3 = vec_xl(32, t0 + i);
		vY4 = vec_xl(48, t0 + i);

		vY1 = vY1 ^ EC_vec_permxor(vhi0, vlo0, vX1);
		vY2 = vY2 ^ EC_vec_permxor(vhi0, vlo0, vX2);
		vY3 = vY3 ^ EC_vec_permxor(vhi0, vlo0, vX3);
		vY4 = vY4 ^ EC_vec_permxor(vhi0, vlo0, vX4);

		vec_xst(vY1, 0, t0 + i);
		vec_xst(vY2, 16, t0 + i);
		vec_xst(vY3, 32, t0 + i);
		vec_xst(vY4, 48, t0 + i);
	}

	return;
}
