#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include "erasure_code.h"

#define MAX_CHECK 63		/* Size is limited by using uint64_t to represent subsets */
#define M_MAX 0x20
#define K_MAX 0x10
#define ROWS M_MAX
#define COLS K_MAX

static inline int min(int a, int b)
{
	if (a <= b)
		return a;
	else
		return b;
}

void gen_sub_matrix(unsigned char *out_matrix, int dim, unsigned char *in_matrix, int rows,
		    int cols, uint64_t row_indicator, uint64_t col_indicator)
{
	int i, j, r, s;

	for (i = 0, r = 0; i < rows; i++) {
		if (!(row_indicator & ((uint64_t) 1 << i)))
			continue;

		for (j = 0, s = 0; j < cols; j++) {
			if (!(col_indicator & ((uint64_t) 1 << j)))
				continue;
			out_matrix[dim * r + s] = in_matrix[cols * i + j];
			s++;
		}
		r++;
	}
}

/* Gosper's Hack */
uint64_t next_subset(uint64_t * subset, uint64_t element_count, uint64_t subsize)
{
	uint64_t tmp1 = *subset & -*subset;
	uint64_t tmp2 = *subset + tmp1;
	*subset = (((*subset ^ tmp2) >> 2) / tmp1) | tmp2;
	if (*subset & (((uint64_t) 1 << element_count))) {
		/* Overflow on last subset */
		*subset = ((uint64_t) 1 << subsize) - 1;
		return 1;
	}

	return 0;
}

int are_submatrices_singular(unsigned char *vmatrix, int rows, int cols)
{
	unsigned char matrix[COLS * COLS];
	unsigned char invert_matrix[COLS * COLS];
	uint64_t row_indicator, col_indicator, subset_init, subsize;

	/* Check all square subsize x subsize submatrices of the rows x cols
	 * vmatrix for singularity*/
	for (subsize = 1; subsize <= min(rows, cols); subsize++) {
		subset_init = (1 << subsize) - 1;
		col_indicator = subset_init;
		do {
			row_indicator = subset_init;
			do {
				gen_sub_matrix(matrix, subsize, vmatrix, rows,
					       cols, row_indicator, col_indicator);
				if (gf_invert_matrix(matrix, invert_matrix, subsize))
					return 1;

			} while (next_subset(&row_indicator, rows, subsize) == 0);
		} while (next_subset(&col_indicator, cols, subsize) == 0);
	}

	return 0;
}

int main(int argc, char **argv)
{
	unsigned char vmatrix[(ROWS + COLS) * COLS];
	int rows, cols;

	if (K_MAX > MAX_CHECK) {
		printf("K_MAX too large for this test\n");
		return 0;
	}
	if (M_MAX > MAX_CHECK) {
		printf("M_MAX too large for this test\n");
		return 0;
	}
	if (M_MAX < K_MAX) {
		printf("M_MAX must be smaller than K_MAX");
		return 0;
	}

	printf("Checking gen_rs_matrix for k <= %d and m <= %d.\n", K_MAX, M_MAX);
	printf("gen_rs_matrix creates erasure codes for:\n");

	for (cols = 1; cols <= K_MAX; cols++) {
		for (rows = 1; rows <= M_MAX - cols; rows++) {
			gf_gen_rs_matrix(vmatrix, rows + cols, cols);

			/* Verify the Vandermonde portion of vmatrix contains no
			 * singular submatrix */
			if (are_submatrices_singular(&vmatrix[cols * cols], rows, cols))
				break;

		}
		printf("   k = %2d, m <= %2d \n", cols, rows + cols - 1);

	}
	return 0;
}
