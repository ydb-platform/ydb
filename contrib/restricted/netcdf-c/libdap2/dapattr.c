/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"
#include <stddef.h>

#define OCCHECK(exp) if((ocstat = (exp))) {THROWCHK(ocstat); goto done;}

/* Forward */
static NCerror buildattribute(char*,nc_type,size_t,char**,NCattribute**);

/*
Invoke oc_merge_das and then extract special
attributes such as "strlen" and "dimname"
and stuff from DODS_EXTRA.
*/
int
dapmerge(NCDAPCOMMON* nccomm, CDFnode* ddsroot, OCddsnode dasroot)
{
    size_t i,j;
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    NClist* allnodes;
    OClink conn;
    char* ocname = NULL;
    char** values = NULL;
    conn = nccomm->oc.conn;

    if(ddsroot == NULL || dasroot == NULL)
	return NC_NOERR;
    /* Merge the das tree onto the dds tree */ 
    ocstat = oc_merge_das(nccomm->oc.conn,dasroot,ddsroot->ocnode);
    if(ocstat != OC_NOERR) goto done;

    /* Create attributes on CDFnodes */
    allnodes = ddsroot->tree->nodes;
    for(i=0;i<nclistlength(allnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(allnodes,i);
	OCddsnode ocnode = node->ocnode;
	size_t attrcount;
	OCtype ocetype;
			
	OCCHECK(oc_dds_attr_count(conn,ocnode,&attrcount));
	for(j=0;j<attrcount;j++) {
	    size_t nvalues;
	    
	    NCattribute* att = NULL;

	    if(ocname != NULL) {
	      free(ocname); ocname = NULL;
	    } /* from last loop */
	    
	    OCCHECK(oc_dds_attr(conn,ocnode,j,&ocname,&ocetype,&nvalues,NULL));
	    if(nvalues > 0) {
	        values = (char**)malloc(sizeof(char*)*nvalues);
		if(values == NULL) {ncstat = NC_ENOMEM; goto done;}
	        OCCHECK(oc_dds_attr(conn,ocnode,j,NULL,NULL,NULL,values));
	    }
	    ncstat = buildattribute(ocname,octypetonc(ocetype),nvalues,values,&att);
	    if(ncstat != NC_NOERR) goto done;
	    if(node->attributes == NULL)
		node->attributes = nclistnew();
	    nclistpush(node->attributes,(void*)att);
	    if(strncmp(ocname,"DODS",strlen("DODS"))==0) {
		att->invisible = 1;
	        /* Define extra semantics associated with
                   DODS and DODS_EXTRA attributes */
		if(strcmp(ocname,"DODS.strlen")==0
		   || strcmp(ocname,"DODS_EXTRA.strlen")==0) {
		    unsigned int maxstrlen = 0;
		    if(values != NULL) {
			if(0==sscanf(values[0],"%u",&maxstrlen))
			    maxstrlen = 0;
		    }
		    node->dodsspecial.maxstrlen = maxstrlen;
#ifdef DEBUG
fprintf(stderr,"%s.maxstrlen=%d\n",node->ocname,(int)node->dodsspecial.maxstrlen);
#endif
		} else if(strcmp(ocname,"DODS.dimName")==0
		   || strcmp(ocname,"DODS_EXTRA.dimName")==0) {
	            nullfree(node->dodsspecial.dimname); /* in case repeated */
		    node->dodsspecial.dimname = NULL;
		    if(values != NULL) {
			nullfree(node->dodsspecial.dimname);
		        node->dodsspecial.dimname = nulldup(values[0]);
#ifdef DEBUG
fprintf(stderr,"%s.dimname=%s\n",node->ocname,node->dodsspecial.dimname);
#endif
		    } else {
		        nullfree(node->dodsspecial.dimname);
			node->dodsspecial.dimname = NULL;
		    }
		} else if(strcmp(ocname,"DODS.Unlimited_Dimension")==0
		   || strcmp(ocname,"DODS_EXTRA.Unlimited_Dimension")==0) {
		    char* val0 = NULL;
		    if(values != NULL)
			val0 = values[0];
		    if(val0 != NULL) {
		        if(nccomm->cdf.recorddimname != NULL) {
                            if(strcmp(nccomm->cdf.recorddimname,val0)!=0)
		            nclog(NCLOGWARN,"Duplicate DODS_EXTRA:Unlimited_Dimension specifications");
			} else {
		            nccomm->cdf.recorddimname = nulldup(values[0]);
#ifdef DEBUG
fprintf(stderr,"%s.Unlimited_Dimension=%s\n",node->ocname,nccomm->cdf.recorddimname);
#endif
			}
		    }
		}
	    }
	    /* clean up */
	    if(values) {
		oc_reclaim_strings(nvalues,values);
		free(values);
		values = NULL;
	    }
	}
    }

done:
    if(values != NULL) free(values);
    if(ocname != NULL) free(ocname);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

/*
Build an NCattribute
from a DAP attribute.
As of Jun 27, 2017, we modify
to suppress nul characters and terminate
the name at the first nul.
*/
static NCerror
buildattribute(char* name, nc_type ptype,
               size_t nvalues, char** values, NCattribute** attp)
{
    int i;
    NCerror ncstat = NC_NOERR;
    NCattribute* att = NULL;

    att = (NCattribute*)calloc(1,sizeof(NCattribute));
    MEMCHECK(att,NC_ENOMEM);
    att->name = nulldup(name);
    att->etype = ptype;

    att->values = nclistnew();
    for(i=0;i<nvalues;i++) {
	char* copy = nulldup(values[i]);
	nclistpush(att->values,(void*)copy);
    }
    if(attp) *attp = att;
    else
      free(att);

    return THROW(ncstat);
}

