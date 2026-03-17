/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018 - 2019, Advanced Micro Devices, Inc.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name(s) of the copyright holder(s) nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

//
// thrinfo_t macros specific to packm.
//

/*
#define bli_packm_thread_my_iter( index, thread ) \
\
	( index % thread->n_way == thread->work_id % thread->n_way )
*/

#define bli_packm_my_iter_rr( i, start, end, work_id, n_way ) \
\
	( i % n_way == work_id % n_way )

#define bli_packm_my_iter_sl( i, start, end, work_id, n_way ) \
\
	( start <= i && i < end )

// Define a general-purpose version of bli_packm_my_iter() whose definition
// depends on whether slab or round-robin partitioning was requested at
// configure-time.
#ifdef BLIS_ENABLE_JRIR_SLAB

  #define bli_packm_my_iter bli_packm_my_iter_sl

#else // BLIS_ENABLE_JRIR_RR

  #define bli_packm_my_iter bli_packm_my_iter_rr

#endif


//
// thrinfo_t APIs specific to packm.
//

#if 0
thrinfo_t* bli_packm_thrinfo_create
     (
       thrcomm_t* ocomm,
       dim_t      ocomm_id,
       dim_t      n_way,
       dim_t      work_id,
       thrinfo_t* sub_node
     );
#endif

void bli_packm_thrinfo_init
     (
       thrinfo_t* thread,
       thrcomm_t* ocomm,
       dim_t      ocomm_id,
       dim_t      n_way,
       dim_t      work_id,
       bszid_t    bszid,
       thrinfo_t* sub_node
     );

void bli_packm_thrinfo_init_single
     (
       thrinfo_t* thread
     );

#if 0
void bli_packm_thrinfo_free
     (
       thrinfo_t* thread
     );
#endif

