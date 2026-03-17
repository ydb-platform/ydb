/*
 * Copyright (c) 2008 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2008-2009 Sun Microystems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

/*
 * Include all header files for the datatypes that we care about / use
 * in the DLL code
 */
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"
#include "opal/datatype/opal_datatype.h"
#include "ompi/datatype/ompi_datatype.h"

/*
 * Define ompi_field_offset() to be a debugging macro only -- just
 * instantiate a variable and then use the field member that we're
 * trying to use in the DLL.  If it compiles, good.  If it doesn't,
 * then it means that the DLL no longer matches the main OMPI code
 * base.
 */
#define ompi_field_offset(out_name, qh_type, struct_name, field_name)  \
    { struct_name foo; char *bogus = (char*) &foo.field_name; *bogus = 'a'; }

/*
 * Now include the common dll .c file that will use the above macro.
 */
#include "ompi_common_dll.c"
