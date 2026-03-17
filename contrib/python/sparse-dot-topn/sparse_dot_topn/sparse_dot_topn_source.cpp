/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Author: Zhe Sun, Ahmet Erdem
// April 20, 2017
// Modified by: Particular Miner
// April 14, 2021

#include <vector>
#include <limits>
#include <algorithm>

#include "./sparse_dot_topn_source.h"


/*
	C++ implementation of sparse_dot_topn

	This function will return a matrix C in CSR format, where
	C = [sorted top n results > lower_bound for each row of A * B]

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		Ap, Aj, Ax: CSR expression of A matrix
		Bp, Bj, Bx: CSR expression of B matrix

		ntop: n top results
		lower_bound: a threshold that the element of A*B must greater than

	Output by reference:
		Cp, Cj, Cx: CSR expression of C matrix

	N.B. A and B must be CSR format!!!
*/
template<typename T>
void sparse_dot_topn_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[], //data of A
		int Bp[],
		int Bj[],
		T Bx[], //data of B
		int ntop,
		T lower_bound,
		int Cp[],
		int Cj[],
		T Cx[]
)
{
	std::vector<int> next(n_col,-1);
	std::vector<T> sums(n_col, 0);

	std::vector<Candidate<T>> candidates;

	int nnz = 0;

	Cp[0] = 0;

	for(int i = 0; i < n_row; i++){
		int head   = -2;
		int length =  0;

		int jj_start = Ap[i];
		int jj_end   = Ap[i+1];
		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj[jj];
			T v = Ax[jj]; //value of A in (i,j)

			int kk_start = Bp[j];
			int kk_end   = Bp[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj[kk]; //kth column of B in row j

				sums[k] += v*Bx[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound){ //append the nonzero elements
				Candidate<T> c;
				c.index = head;
				c.value = sums[head];
				candidates.push_back(c);
			}

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		int len = (int)candidates.size();
		if (len > ntop){
			std::partial_sort(candidates.begin(), candidates.begin()+ntop, candidates.end());
			len = ntop;
		} else {
			std::sort(candidates.begin(), candidates.end());
		}

		for(int a=0; a < len; a++){
			Cj[nnz] = candidates[a].index;
			Cx[nnz] = candidates[a].value;
			nnz++;
		}
		candidates.clear();

		Cp[i+1] = nnz;
	}
}

template void sparse_dot_topn_source<float>(int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound, int Cp[], int Cj[], float Cx[]);
template void sparse_dot_topn_source<double>(int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound, int Cp[], int Cj[], double Cx[]);

/*
	C++ implementation of sparse_dot_topn_extd_source

	This function will return a matrix C in CSR format, where
	C = [sorted top n results > lower_bound for each row of A * B].
	The maximum number n_minmax of elements per row of C (assuming ntop = n_col)
	is also returned.

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		Ap, Aj, Ax: CSR expression of A matrix
		Bp, Bj, Bx: CSR expression of B matrix

		ntop: n top results
		lower_bound: a threshold that the element of A*B must greater than
		nnz_max: the size of the memory allocated for the results Cj and Cx.  If
				nnz_max is found to be too small during the computation, then the
				results will be placed in vectors alt_Cj and alt_Cx instead

	Output by reference:
		Cp, Cj, Cx: CSR expression of C matrix
		n_minmax: The maximum number of elements per row of C (assuming ntop = n_col)
				alt_Cj, alt_Cx: CSR expression of C matrix as vectors.  These will
				contain the output only if nnz_max is found to be too small

	Returned output:
		nnz_max_is_too_small: int 1 or 0 depending on whether nnz_max was found to be
							too small or not respectively
	N.B. A and B must be CSR format!!!
*/
template<typename T>
int sparse_dot_topn_extd_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[],	//data of A
		int Bp[],
		int Bj[],
		T Bx[],	//data of B
		int ntop,
		T lower_bound,
		int Cp[],
		int Cj[],
		T Cx[], 	//data of C
		std::vector<int>* alt_Cj,
		std::vector<T>* alt_Cx,
		int nnz_max,
		int* n_minmax
)
{
	std::vector<int> next(n_col,-1);
	std::vector<T> sums(n_col, 0);

	std::vector<Candidate<T>> candidates;
	candidates.reserve(n_col);

	int nnz = 0;
	int nnz_max_is_too_small = 0;

	Cp[0] = 0;
	*n_minmax = 0;

	for(int i = 0; i < n_row; i++){
		int head   = -2;
		int length =  0;

		int jj_start = Ap[i];
		int jj_end   = Ap[i+1];
		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj[jj];
			T v = Ax[jj]; //value of A in (i,j)

			int kk_start = Bp[j];
			int kk_end   = Bp[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj[kk]; //kth column of B in row j

				sums[k] += v*Bx[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound){ //append the nonzero elements
				Candidate<T> c;
				c.index = head;
				c.value = sums[head];
				candidates.push_back(c);
			}

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		int len = (int)candidates.size();
		*n_minmax = (len > *n_minmax)? len : *n_minmax;
		if (len > ntop){
			std::partial_sort(candidates.begin(), candidates.begin()+ntop, candidates.end());
			len = ntop;
		} else {
			std::sort(candidates.begin(), candidates.end());
		}
		if (len + nnz > nnz_max){
			if (!nnz_max_is_too_small){
				nnz_max_is_too_small = true;
				alt_Cj->resize(nnz);
				alt_Cx->resize(nnz);
				std::copy(Cj, Cj + nnz, alt_Cj->data());
				std::copy(Cx, Cx + nnz, alt_Cx->data());
			}
			for(int a = 0; a < len; a++){
				alt_Cj->push_back(candidates[a].index);
				alt_Cx->push_back(candidates[a].value);
				nnz++;
			}
		}
		else {
			for(int a = 0; a < len; a++){
				Cj[nnz] = candidates[a].index;
				Cx[nnz] = candidates[a].value;
				nnz++;
			}
		}
		candidates.clear();

		Cp[i+1] = nnz;
	}
	return nnz_max_is_too_small;
}
template int sparse_dot_topn_extd_source<float>(int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound, int Cp[], int Cj[], float Cx[], std::vector<int>* alt_Cj, std::vector<float>* alt_Cx, int nnz_max, int* n_minmax);
template int sparse_dot_topn_extd_source<double>(int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound, int Cp[], int Cj[], double Cx[], std::vector<int>* alt_Cj, std::vector<double>* alt_Cx, int nnz_max, int* n_minmax);

/*
	C++ implementation of sparse_dot_nnz_source

	This function will return the number nnz of nonzero elements
	of the matrix C in CSR format, where
	C = [all results > lower_bound sorted for each row of A * B]
	and ntop the maximum number of elements per row of C.
	This function is designed primarily to help with memory management for
	very large sparse matrices.

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		Ap, Aj, Ax: CSR expression of A matrix
		Bp, Bj, Bx: CSR expression of B matrix

		lower_bound: a threshold that the element of A*B must greater than

	Output:
		nnz: number of nonzero elements of matrix C
		ntop: maximum number of elements per row of C

	N.B. A and B must be CSR format!!!
*/
template<typename T>
void sparse_dot_nnz_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[], //data of A
		int Bp[],
		int Bj[],
		T Bx[], //data of B
		T lower_bound,
		int* nnz,
		int* ntop
)
{
	std::vector<int> next(n_col,-1);
	std::vector<T> sums(n_col, 0);

	*nnz = 0;
	*ntop = 0;

	for(int i = 0; i < n_row; i++){
		int head   = -2;
		int length =  0;

		int jj_start = Ap[i];
		int jj_end   = Ap[i+1];
		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj[jj];
			T v = Ax[jj]; //value of A in (i,j)

			int kk_start = Bp[j];
			int kk_end   = Bp[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj[kk]; //kth column of B in row j

				sums[k] += v*Bx[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		int nnz_k = 0;
		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound) nnz_k++; //count this nonzero element in

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}
		*ntop = (nnz_k > *ntop)? nnz_k : *ntop;
		*nnz += nnz_k;
	}
}
template void sparse_dot_nnz_source<float>(int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], float lower_bound, int* nnz, int* ntop);
template void sparse_dot_nnz_source<double>(int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], double lower_bound, int* nnz, int* ntop);


/*
	C++ implementation of sparse_dot_only_max_nnz_col_source

	This function will return nnz, the total number of nonzero
	matrix-components of
	C = [top n results > lower_bound for each row of A * B].

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		Ap, Aj, Ax: CSR expression of A matrix
		Bp, Bj, Bx: CSR expression of B matrix

		ntop: top n results
		lower_bound: a threshold that the element of A*B must greater than

	Returned output:
		nnz: the total number of nonzero matrix-components of C

	N.B. A and B must be CSR format!!!
*/
template<typename T>
int sparse_dot_only_nnz_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[], //data of A
		int Bp[],
		int Bj[],
		T Bx[], //data of B
		int ntop,
		T lower_bound
)
{
	std::vector<int> next(n_col,-1);
	std::vector<T> sums(n_col, 0);

	int nnz = 0;

	for(int i = 0; i < n_row; i++){
		int head   = -2;
		int length =  0;
		int candidates_sz = 0;

		int jj_start = Ap[i];
		int jj_end   = Ap[i+1];
		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj[jj];
			T v = Ax[jj]; //value of A in (i,j)

			int kk_start = Bp[j];
			int kk_end   = Bp[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj[kk]; //kth column of B in row j

				sums[k] += v*Bx[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound) candidates_sz++;

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		if (candidates_sz > ntop) candidates_sz = ntop;

		nnz += candidates_sz;
	}
	return nnz;
}
template int sparse_dot_only_nnz_source<float>(int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound);
template int sparse_dot_only_nnz_source<double>(int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound);
