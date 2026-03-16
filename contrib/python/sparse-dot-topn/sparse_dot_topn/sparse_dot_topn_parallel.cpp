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

#include <cmath>
#include <vector>
#include <algorithm>
#include <numeric>
#include <thread>

#include "./sparse_dot_topn_source.h"
#include "./sparse_dot_topn_parallel.h"


struct job_range_type {int begin; int end;};

void distribute_load(
		int load_sz,
		int n_jobs,
		std::vector<job_range_type> &ranges
)
{
	// share the load among jobs:
	int equal_job_load_sz = load_sz/n_jobs;
	int rem = load_sz % n_jobs;
	ranges.resize(n_jobs);

	int start = 0;
	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		ranges[job_nr].begin = start;
		ranges[job_nr].end = start + equal_job_load_sz + ((job_nr < rem)? 1 : 0);
		start = ranges[job_nr].end;
	}
}

template<typename T>
void inner_gather_v2(
		job_range_type job_range,
		int Cp[],
		int Cp_start,
		int Cj[],
		T Cx[],
		std::vector<Candidate<T>>* real_candidates,
		std::vector<int>* row_nnz
)
{
	if (job_range.begin >= job_range.end) return;

	int* nnz_begin = row_nnz->data();
	int* nnz_end = nnz_begin + row_nnz->size();

	int* Cp_begin = &Cp[job_range.begin + 1];

	(*nnz_begin) += Cp_start;
	std::partial_sum(nnz_begin, nnz_end, Cp_begin);

	Candidate<T>* c_begin = real_candidates->data();
	Candidate<T>* c_end = c_begin + real_candidates->size();

	int* Cj_begin = &Cj[Cp_start];
	T* Cx_begin = &Cx[Cp_start];

	std::transform(c_begin, c_end, Cj_begin, [](Candidate<T> c) -> int { return c.index; });
	std::transform(c_begin, c_end, Cx_begin, [](Candidate<T> c) -> T { return c.value; });
}
template void inner_gather_v2<float>(job_range_type job_range, int Cp[], int Cp_start, int Cj[], float Cx[], std::vector<Candidate<float>>* real_candidates, std::vector<int>* row_nnz);
template void inner_gather_v2<double>(job_range_type job_range, int Cp[], int Cp_start, int Cj[], double Cx[], std::vector<Candidate<double>>* real_candidates, std::vector<int>* row_nnz);


template<typename T>
void inner_gather_v1(
		job_range_type job_range,
		int Cp[],
		int Cp_start,
		int vCj_start[],
		T vCx_start[],
		std::vector<Candidate<T>>* real_candidates,
		std::vector<int>* row_nnz
)
{
	Candidate<T>* c = real_candidates->data();
	int* vCj_cursor = &vCj_start[Cp_start];
	T* vCx_cursor = &vCx_start[Cp_start];

	int Cp_i = Cp_start;
	int* row_nnz_ptr = row_nnz->data();

	for (int i = job_range.begin; i < job_range.end; i++){
		for (int j = 0; j < (*row_nnz_ptr); j++){
			*(vCj_cursor++) = c->index;
			*(vCx_cursor++) = (c++)->value;
		}
		Cp_i += *(row_nnz_ptr++);
		Cp[i + 1] = Cp_i;
	}
}
template void inner_gather_v1<float>(job_range_type job_range, int Cp[], int Cp_start, int vCj_start[], float vCx_start[], std::vector<Candidate<float>>* real_candidates, std::vector<int>* row_nnz);
template void inner_gather_v1<double>(job_range_type job_range, int Cp[], int Cp_start, int vCj_start[], double vCx_start[], std::vector<Candidate<double>>* real_candidates, std::vector<int>* row_nnz);

template<typename T>
void inner_sparse_dot_topn(
		job_range_type job_range,
		int n_col_inner,
		int ntop_inner,
		T lower_bound_inner,
		int Ap_copy[],
		int Aj_copy[],
		T Ax_copy[],
		int Bp_copy[],
		int Bj_copy[],
		T Bx_copy[],
		std::vector<Candidate<T>>* real_candidates,
		std::vector<int>* row_nnz,
		int* total
)
{
	std::vector<int> next(n_col_inner,-1);
	std::vector<T> sums(n_col_inner, 0);

	real_candidates->reserve(job_range.end - job_range.begin);

	row_nnz->resize(job_range.end - job_range.begin);
	int* row_nnz_ptr = row_nnz->data();

	for (int i = job_range.begin; i < job_range.end; i++){

		int head   = -2;
		int length =  0;
		size_t sz = real_candidates->size();

		int jj_start = Ap_copy[i];
		int jj_end   = Ap_copy[i+1];

		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj_copy[jj];
			T v = Ax_copy[jj]; //value of A in (i,j)

			int kk_start = Bp_copy[j];
			int kk_end   = Bp_copy[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj_copy[kk]; //kth column of B in row j

				sums[k] += v*Bx_copy[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound_inner){ //append the nonzero elements
				Candidate<T> c;
				c.index = head;
				c.value = sums[head];
				real_candidates->push_back(c);
			}

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		int len = (int) (real_candidates->size() - sz);

		Candidate<T>* candidate_arr_begin = real_candidates->data() + sz;
		if (len > ntop_inner){
			std::partial_sort(
					candidate_arr_begin,
					candidate_arr_begin + ntop_inner,
					candidate_arr_begin + len
			);
			len = ntop_inner;
		}
		else {
			std::sort(
					candidate_arr_begin,
					candidate_arr_begin + len
			);
		}

		real_candidates->resize(sz + (size_t) len);
		*(row_nnz_ptr++) = len;
		(*total) += len;
	}
}
template void inner_sparse_dot_topn<float>(job_range_type job_range, int n_col_inner, int ntop_inner, float lower_bound_inner, int Ap_copy[], int Aj_copy[], float Ax_copy[], int Bp_copy[], int Bj_copy[], float Bx_copy[], std::vector<Candidate<float>>* real_candidates, std::vector<int>* row_nnz, int* total);
template void inner_sparse_dot_topn<double>(job_range_type job_range, int n_col_inner, int ntop_inner, double lower_bound_inner, int Ap_copy[], int Aj_copy[], double Ax_copy[], int Bp_copy[], int Bj_copy[], double Bx_copy[], std::vector<Candidate<double>>* real_candidates, std::vector<int>* row_nnz, int* total);

template<typename T>
void sparse_dot_topn_parallel(
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
		T Cx[],
		int n_jobs
)
{
	std::vector<job_range_type> job_ranges(n_jobs);
	distribute_load(n_row, n_jobs, job_ranges);

	std::vector<std::vector<Candidate<T>>> real_candidates(n_jobs);
	std::vector<std::vector<int>> row_nnz(n_jobs);

	// initialize aggregate:
	std::vector<int> sub_total(n_jobs, 0);

	std::vector<std::thread> thread_list(n_jobs);
	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		thread_list[job_nr] = std::thread(
				inner_sparse_dot_topn<T>,
				job_ranges[job_nr],
				n_col, ntop,
				lower_bound,
				Ap, Aj, Ax, Bp, Bj, Bx,
				&real_candidates[job_nr],
				&row_nnz[job_nr],
				&sub_total[job_nr]
		);
	}

	for (int job_nr = 0; job_nr < n_jobs; job_nr++)
		thread_list[job_nr].join();

	// gather the results:
	std::vector<int> nnz_job_starts(n_jobs + 1);
	nnz_job_starts[0] = 0;
	std::partial_sum(sub_total.begin(), sub_total.end(), nnz_job_starts.begin() + 1);

	Cp[0] = 0;
	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		thread_list[job_nr] = std::thread(
				inner_gather_v1<T>,
				job_ranges[job_nr],
				Cp,
				nnz_job_starts[job_nr],
				Cj,
				Cx,
				&real_candidates[job_nr],
				&row_nnz[job_nr]
		);
	}

	for (int job_nr = 0; job_nr < n_jobs; job_nr++)
		thread_list[job_nr].join();
}
template void sparse_dot_topn_parallel<float>(int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound, int Cp[], int Cj[], float Cx[], int n_jobs);
template void sparse_dot_topn_parallel<double>(int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound, int Cp[], int Cj[], double Cx[], int n_jobs);

template<typename T>
void inner_sparse_dot_topn_extd(
		job_range_type job_range,
		int n_col_inner,
		int ntop_inner,
		T lower_bound_inner,
		int Ap_copy[],
		int Aj_copy[],
		T Ax_copy[],
		int Bp_copy[],
		int Bj_copy[],
		T Bx_copy[],
		std::vector<Candidate<T>>* real_candidates,
		std::vector<int>* row_nnz,
		int* total,
		int* n_minmax,
		int mem_sz_per_row
)
{
	std::vector<int> next(n_col_inner,-1);
	std::vector<T> sums(n_col_inner, 0);

	real_candidates->reserve(mem_sz_per_row*(job_range.end - job_range.begin));

	row_nnz->resize(job_range.end - job_range.begin);
	int* row_nnz_ptr = row_nnz->data();

	for(int i = job_range.begin; i < job_range.end; i++){

		int head   = -2;
		int length =  0;
		size_t sz = real_candidates->size();

		int jj_start = Ap_copy[i];
		int jj_end   = Ap_copy[i+1];

		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj_copy[jj];
			T v = Ax_copy[jj]; //value of A in (i,j)

			int kk_start = Bp_copy[j];
			int kk_end   = Bp_copy[j+1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj_copy[kk]; //kth column of B in row j

				sums[k] += v*Bx_copy[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound_inner){ //append the nonzero elements
				Candidate<T> c;
				c.index = head;
				c.value = sums[head];
				real_candidates->push_back(c);
			}

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		int len = (int) (real_candidates->size() - sz);
		*n_minmax = (len > *n_minmax)? len : *n_minmax;

		Candidate<T>* candidate_arr_begin = real_candidates->data() + sz;
		if (len > ntop_inner){
			std::partial_sort(
					candidate_arr_begin,
					candidate_arr_begin + ntop_inner,
					candidate_arr_begin + len
			);
			len = ntop_inner;
		}
		else {
			std::sort(
					candidate_arr_begin,
					candidate_arr_begin + len
			);
		}

		real_candidates->resize(sz + (size_t) len);
		*(row_nnz_ptr++) = len;
		(*total) += len;
	}
}
template void inner_sparse_dot_topn_extd<float>( job_range_type job_range, int n_col_inner, int ntop_inner, float lower_bound_inner, int Ap_copy[], int Aj_copy[], float Ax_copy[], int Bp_copy[], int Bj_copy[], float Bx_copy[], std::vector<Candidate<float>>* real_candidates, std::vector<int>* row_nnz, int* total, int* n_minmax, int mem_sz_per_row);
template void inner_sparse_dot_topn_extd<double>( job_range_type job_range, int n_col_inner, int ntop_inner, double lower_bound_inner, int Ap_copy[], int Aj_copy[], double Ax_copy[], int Bp_copy[], int Bj_copy[], double Bx_copy[], std::vector<Candidate<double>>* real_candidates, std::vector<int>* row_nnz, int* total, int* n_minmax, int mem_sz_per_row);


template<typename T>
int sparse_dot_topn_extd_parallel(
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
		T Cx[],
		std::vector<int>* alt_Cj,
		std::vector<T>* alt_Cx,
		int nnz_max,
		int *n_minmax,
		int n_jobs
)
{
	std::vector<job_range_type> job_ranges(n_jobs);
	distribute_load(n_row, n_jobs, job_ranges);

	std::vector<std::vector<Candidate<T>>> real_candidates(n_jobs);
	std::vector<std::vector<int>> row_nnz(n_jobs);

	// initialize aggregates:
	std::vector<int> sub_total(n_jobs, 0);
	std::vector<int> split_n_minmax(n_jobs, 0);

	int mem_sz_per_row = std::max(1, (int) std::ceil(((T) nnz_max)/((T) n_row)));

	std::vector<std::thread> thread_list(n_jobs);

	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		thread_list[job_nr] = std::thread(
				inner_sparse_dot_topn_extd<T>,
				job_ranges[job_nr],
				n_col, ntop,
				lower_bound,
				Ap, Aj, Ax, Bp, Bj, Bx,
				&real_candidates[job_nr],
				&row_nnz[job_nr],
				&sub_total[job_nr],
				&split_n_minmax[job_nr],
				mem_sz_per_row
		);
	}

	for (int job_nr = 0; job_nr < n_jobs; job_nr++)
		thread_list[job_nr].join();

	// gather the results:
	*n_minmax = *std::max_element(split_n_minmax.begin(), split_n_minmax.end());

	std::vector<int> nnz_job_starts(n_jobs + 1);
	nnz_job_starts[0] = 0;
	std::partial_sum(sub_total.begin(), sub_total.end(), nnz_job_starts.begin() + 1);

	int* Cj_container;
	T* Cx_container;

	int total = nnz_job_starts.back();
	int nnz_max_is_too_small = (nnz_max < total);

	if (nnz_max_is_too_small) {
		alt_Cj->resize(total);
		alt_Cx->resize(total);
		Cj_container = &((*alt_Cj)[0]);
		Cx_container = &((*alt_Cx)[0]);
	}
	else {
		Cj_container = Cj;
		Cx_container = Cx;
	}

	Cp[0] = 0;
	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		thread_list[job_nr] = std::thread(
				inner_gather_v1<T>,
				job_ranges[job_nr],
				Cp,
				nnz_job_starts[job_nr],
				Cj_container,
				Cx_container,
				&real_candidates[job_nr],
				&row_nnz[job_nr]
		);
	}

	for (int job_nr = 0; job_nr < n_jobs; job_nr++)
		thread_list[job_nr].join();

	return nnz_max_is_too_small;
}
template int sparse_dot_topn_extd_parallel<float>( int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound, int Cp[], int Cj[], float Cx[], std::vector<int>* alt_Cj, std::vector<float>* alt_Cx, int nnz_max, int *n_minmax, int n_jobs);
template int sparse_dot_topn_extd_parallel<double>( int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound, int Cp[], int Cj[], double Cx[], std::vector<int>* alt_Cj, std::vector<double>* alt_Cx, int nnz_max, int *n_minmax, int n_jobs);

template<typename T>
void inner_sparse_nnz_only(
		job_range_type job_range,
		int n_col_inner,
		int ntop_inner,
		T lower_bound_inner,
		int Ap_copy[],
		int Aj_copy[],
		T Ax_copy[],
		int Bp_copy[],
		int Bj_copy[],
		T Bx_copy[],
		int* nnz
)
{

	std::vector<int> next(n_col_inner,-1);
	std::vector<T> sums(n_col_inner, 0);

	for(int i = job_range.begin; i < job_range.end; i++){

		int head   = -2;
		int length =  0;
		int candidates_sz = 0;

		int jj_start = Ap_copy[i];
		int jj_end   = Ap_copy[i + 1];

		for(int jj = jj_start; jj < jj_end; jj++){
			int j = Aj_copy[jj];
			T v = Ax_copy[jj]; //value of A in (i,j)

			int kk_start = Bp_copy[j];
			int kk_end   = Bp_copy[j + 1];
			for(int kk = kk_start; kk < kk_end; kk++){
				int k = Bj_copy[kk]; //kth column of B in row j

				sums[k] += v*Bx_copy[kk]; //multiply with value of B in (j,k) and accumulate to the result for kth column of row i

				if(next[k] == -1){
					next[k] = head; //keep a linked list, every element points to the next column index
					head  = k;
					length++;
				}
			}
		}

		for(int jj = 0; jj < length; jj++){ //length = number of columns set (may include 0s)

			if(sums[head] > lower_bound_inner) candidates_sz++;

			int temp = head;
			head = next[head]; //iterate over columns

			next[temp] = -1; //clear arrays
			sums[temp] =  0; //clear arrays
		}

		if (candidates_sz > ntop_inner) candidates_sz = ntop_inner;

		(*nnz) += candidates_sz;
	}
}
template void inner_sparse_nnz_only<float>( job_range_type job_range, int n_col_inner, int ntop_inner, float lower_bound_inner, int Ap_copy[], int Aj_copy[], float Ax_copy[], int Bp_copy[], int Bj_copy[], float Bx_copy[], int* nnz);
template void inner_sparse_nnz_only<double>( job_range_type job_range, int n_col_inner, int ntop_inner, double lower_bound_inner, int Ap_copy[], int Aj_copy[], double Ax_copy[], int Bp_copy[], int Bj_copy[], double Bx_copy[], int* nnz);

template<typename T>
int sparse_dot_only_nnz_parallel(
	int n_row,
	int n_col,
	int Ap[],
	int Aj[],
	T Ax[],
	int Bp[],
	int Bj[],
	T Bx[],
	int ntop,
	T lower_bound,
	int n_jobs
)
{
	std::vector<job_range_type> job_row_ranges(n_jobs);
	distribute_load(n_row, n_jobs, job_row_ranges);

	std::vector<int> split_nnz(n_jobs, 0);
	std::vector<std::thread> thread_list(n_jobs);

	for (int job_nr = 0; job_nr < n_jobs; job_nr++) {

		thread_list[job_nr] = std::thread (
				inner_sparse_nnz_only<T>,
				job_row_ranges[job_nr],
				n_col,
				ntop, lower_bound,
				Ap, Aj, Ax, Bp, Bj, Bx,
				&split_nnz[job_nr]
		);

	}

	for (int job_nr = 0; job_nr < n_jobs; job_nr++)
		thread_list[job_nr].join();

	return std::accumulate(split_nnz.begin(), split_nnz.end(), (int) 0);
}
template int sparse_dot_only_nnz_parallel<float>( int n_row, int n_col, int Ap[], int Aj[], float Ax[], int Bp[], int Bj[], float Bx[], int ntop, float lower_bound, int n_jobs);
template int sparse_dot_only_nnz_parallel<double>( int n_row, int n_col, int Ap[], int Aj[], double Ax[], int Bp[], int Bj[], double Bx[], int ntop, double lower_bound, int n_jobs);
