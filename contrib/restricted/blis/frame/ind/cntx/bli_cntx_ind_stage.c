/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin

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

#include "blis.h"

typedef void (*cntx_stage_ft)( dim_t stage, cntx_t* cntx );

static void_fp bli_cntx_ind_stage_fp[BLIS_NUM_IND_METHODS] =
{
/* 3mh  */ bli_cntx_3mh_stage,
/* 3m1  */ bli_cntx_3m1_stage,
/* 4mh  */ bli_cntx_4mh_stage,
/* 4mb  */ bli_cntx_4mb_stage,
/* 4m1  */ bli_cntx_4m1_stage,
/* 1m   */ bli_cntx_1m_stage,
/* nat  */ bli_cntx_nat_stage
};


// -----------------------------------------------------------------------------

// Execute the context initialization/finalization function associated
// with a given induced method.

void bli_cntx_ind_stage( ind_t method, dim_t stage, cntx_t* cntx )
{
	cntx_stage_ft func = bli_cntx_ind_stage_fp[ method ];

	func( stage, cntx );
}

// -----------------------------------------------------------------------------

// These functions modify a context, if needed, for the particular "stage" of
// the induced method execution. Some induced methods do not make use of this
// feature. NOTE: ANY INDUCED METHOD THAT HAS A NON-EMPTY _stage() FUNCTION
// IS NOT THREAT-SAFE FOR APPLICATION-LEVEL THREADING.

// -----------------------------------------------------------------------------

void bli_cntx_3mh_stage( dim_t stage, cntx_t* cntx )
{
	// Set the pack_t schemas as a function of the stage of execution.
	if ( stage == 0 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_RO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_RO, cntx );
	}
	else if ( stage == 1 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_IO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_IO, cntx );
	}
	else // if ( stage == 2 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_RPI, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_RPI, cntx );
	}
}

// -----------------------------------------------------------------------------

void bli_cntx_3m1_stage( dim_t stage, cntx_t* cntx )
{
}

// -----------------------------------------------------------------------------

void bli_cntx_4mh_stage( dim_t stage, cntx_t* cntx )
{
	// Set the pack_t schemas as a function of the stage of execution.
	if ( stage == 0 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_RO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_RO, cntx );
	}
	else if ( stage == 1 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_IO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_IO, cntx );
	}
	else if ( stage == 2 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_RO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_IO, cntx );
	}
	else // if ( stage == 3 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_IO, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_RO, cntx );
	}
}

// -----------------------------------------------------------------------------

void bli_cntx_4mb_stage( dim_t stage, cntx_t* cntx )
{
}

// -----------------------------------------------------------------------------

void bli_cntx_4m1_stage( dim_t stage, cntx_t* cntx )
{
}

// -----------------------------------------------------------------------------

void bli_cntx_1m_stage( dim_t stage, cntx_t* cntx )
{
}

// -----------------------------------------------------------------------------

void bli_cntx_nat_stage( dim_t stage, cntx_t* cntx )
{
}

