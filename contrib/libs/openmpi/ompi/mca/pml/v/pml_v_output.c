/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/util/output.h"


#include "pml_v_output.h"

#if defined(HAVE_UNISTD_H)
#include <unistd.h>
#endif
#include <string.h>

int ompi_pml_v_output_open(char *output, int verbosity) {
    opal_output_stream_t lds;
    char hostname[OPAL_MAXHOSTNAMELEN] = "NA";

    OBJ_CONSTRUCT(&lds, opal_output_stream_t);
    if(!output) {
      mca_pml_v.output = 0;
    }
    else {
        if(!strcmp(output, "stdout")) {
            lds.lds_want_stdout = true;
        }
        else if(!strcmp(output, "stderr")) {
            lds.lds_want_stderr = true;
        }
        else
        {
            lds.lds_want_file = true;
            lds.lds_file_suffix = output;
        }
        lds.lds_is_debugging = true;
        gethostname(hostname, sizeof(hostname));
        asprintf(&lds.lds_prefix, "[%s:%05d] pml_v: ", hostname, getpid());
        lds.lds_verbose_level = verbosity;
        mca_pml_v.output = opal_output_open(&lds);
        free(lds.lds_prefix);
    }
    return mca_pml_v.output;
}

void ompi_pml_v_output_close(void) {
    opal_output_close(mca_pml_v.output);
    mca_pml_v.output = -1;
}
