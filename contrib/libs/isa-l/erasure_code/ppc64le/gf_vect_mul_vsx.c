#include "ec_base_vsx.h"

/*
 * Same as gf_vect_mul_base in "ec_base.h" but without the size restriction.
 */
static void _gf_vect_mul_base(int len, unsigned char *a, unsigned char *src,
			      unsigned char *dest)
{
	//2nd element of table array is ref value used to fill it in
	unsigned char c = a[1];

	while (len-- > 0)
		*dest++ = gf_mul_erasure(c, *src++);
	return 0;
}

void gf_vect_mul_vsx(int len, unsigned char *gftbl, unsigned char *src, unsigned char *dest)
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
	int i, head;

	s = (unsigned char *)src;
	t0 = (unsigned char *)dest;

	head = len % 128;
	if (head != 0) {
		_gf_vect_mul_base(head, gftbl, src, dest);
	}

	vlo0 = EC_vec_xl(0, gftbl);
	vhi0 = EC_vec_xl(16, gftbl);

	for (i = head; i < len - 127; i += 128) {
		vX1 = vec_xl(0, s + i);
		vX2 = vec_xl(16, s + i);
		vX3 = vec_xl(32, s + i);
		vX4 = vec_xl(48, s + i);

		vX5 = vec_xl(64, s + i);
		vX6 = vec_xl(80, s + i);
		vX7 = vec_xl(96, s + i);
		vX8 = vec_xl(112, s + i);

		vY1 = EC_vec_permxor(vhi0, vlo0, vX1);
		vY2 = EC_vec_permxor(vhi0, vlo0, vX2);
		vY3 = EC_vec_permxor(vhi0, vlo0, vX3);
		vY4 = EC_vec_permxor(vhi0, vlo0, vX4);

		vY5 = EC_vec_permxor(vhi0, vlo0, vX5);
		vY6 = EC_vec_permxor(vhi0, vlo0, vX6);
		vY7 = EC_vec_permxor(vhi0, vlo0, vX7);
		vY8 = EC_vec_permxor(vhi0, vlo0, vX8);

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
