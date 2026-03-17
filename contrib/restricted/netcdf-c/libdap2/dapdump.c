/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include "netcdf.h"
#include <stddef.h>
#ifdef USE_PARALLEL
#include "netcdf_par.h"
#endif
#include "dapincludes.h"
#include "dapdump.h"
#include "dceconstraints.h"

#define CHECK(n) if((n) != NC_NOERR) {return (n);} else {}

static char* indentstr = "  "; /* Used by dumpindent */

static void dumptreer(CDFnode* root, NCbytes* buf, int indent, int visible);

int
dumpmetadata(int ncid, NChdr** hdrp)
{
    int stat,i,j;
    NChdr* hdr = (NChdr*)calloc(1,sizeof(NChdr));
    MEMCHECK(hdr,NC_ENOMEM);
    hdr->ncid = ncid;
    hdr->content = ncbytesnew();
    if(hdrp) *hdrp = hdr;

    stat = nc_inq(hdr->ncid,
		  &hdr->ndims,
		  &hdr->nvars,
		  &hdr->ngatts,
		  &hdr->unlimid);
    CHECK(stat);
#ifdef DEBUG2
        fprintf(stdout,"ncid=%d ngatts=%d ndims=%d nvars=%d unlimid=%d\n",
		hdr->ncid,hdr->ngatts,hdr->ndims,hdr->nvars,hdr->unlimid);
#endif
    hdr->gatts = (NCattribute*)calloc(1, (size_t)hdr->ngatts*sizeof(NCattribute));
    MEMCHECK(hdr->gatts,NC_ENOMEM);
    if(hdr->ngatts > 0)
	fprintf(stdout,"global attributes:\n");
    for(i=0;i<hdr->ngatts;i++) {
	NCattribute* att = &hdr->gatts[i];
        char attname[NC_MAX_NAME];
	nc_type nctype;
	size_t typesize;
        size_t nvalues;

        stat = nc_inq_attname(hdr->ncid,NC_GLOBAL,i,attname);
        CHECK(stat);
	att->name = nulldup(attname);
	stat = nc_inq_att(hdr->ncid,NC_GLOBAL,att->name,&nctype,&nvalues);
        CHECK(stat);
	att->etype = nctypetodap(nctype);
 	typesize = nctypesizeof(att->etype);
	fprintf(stdout,"\t[%d]: name=%s type=%s values(%lu)=",
			i,att->name,nctypetostring(octypetonc(att->etype)),
                        (unsigned long)nvalues);
	if(nctype == NC_CHAR) {
	    size_t len = typesize*nvalues;
	    char* values = (char*)malloc(len+1);/* for null terminate*/
	    MEMCHECK(values,NC_ENOMEM);
	    stat = nc_get_att(hdr->ncid,NC_GLOBAL,att->name,values);
            CHECK(stat);
	    values[len] = '\0';
	    fprintf(stdout," '%s'",values);
	} else {
	    size_t len = typesize*nvalues;
	    char* values = (char*)malloc(len);
	    MEMCHECK(values,NC_ENOMEM);
	    stat = nc_get_att(hdr->ncid,NC_GLOBAL,att->name,values);
            CHECK(stat);
	    for(size_t k=0;k<nvalues;k++) {
		fprintf(stdout," ");
		dumpdata1(octypetonc(att->etype),k,values);
	    }
	}
	fprintf(stdout,"\n");
    }

    hdr->dims = (Dim*)malloc((size_t)hdr->ndims*sizeof(Dim));
    MEMCHECK(hdr->dims,NC_ENOMEM);
    for(i=0;i<hdr->ndims;i++) {
	hdr->dims[i].dimid = i;
        stat = nc_inq_dim(hdr->ncid,
	                  hdr->dims[i].dimid,
	                  hdr->dims[i].name,
	                  &hdr->dims[i].size);
        CHECK(stat);
	fprintf(stdout,"dim[%d]: name=%s size=%lu\n",
		i,hdr->dims[i].name,(unsigned long)hdr->dims[i].size);
    }
    hdr->vars = (Var*)malloc((size_t)hdr->nvars*sizeof(Var));
    MEMCHECK(hdr->vars,NC_ENOMEM);
    for(i=0;i<hdr->nvars;i++) {
	Var* var = &hdr->vars[i];
	nc_type nctype;
	var->varid = i;
        stat = nc_inq_var(hdr->ncid,
	                  var->varid,
	                  var->name,
			  &nctype,
			  &var->ndims,
			  var->dimids,
	                  &var->natts);
        CHECK(stat);
	var->nctype = (nctype);
	fprintf(stdout,"var[%d]: name=%s type=%s |dims|=%d",
		i,
		var->name,
		nctypetostring(var->nctype),
		var->ndims);
	fprintf(stdout," dims={");
	for(j=0;j<var->ndims;j++) {
	    fprintf(stdout," %d",var->dimids[j]);
	}
	fprintf(stdout,"}\n");
	var->atts = (NCattribute*)malloc((size_t)var->natts*sizeof(NCattribute));
        MEMCHECK(var->atts,NC_ENOMEM);
        for(j=0;j<var->natts;j++) {
	    NCattribute* att = &var->atts[j];
	    char attname[NC_MAX_NAME];
	    size_t typesize;
	    char* values;
	    nc_type nctype;
	    size_t nvalues;
            stat = nc_inq_attname(hdr->ncid,var->varid,j,attname);
	    CHECK(stat);
	    att->name = nulldup(attname);
	    stat = nc_inq_att(hdr->ncid,var->varid,att->name,&nctype,&nvalues);
	    CHECK(stat);
	    att->etype = nctypetodap(nctype);
	    typesize = nctypesizeof(att->etype);
	    values = (char*)malloc(typesize*nvalues);
	    MEMCHECK(values,NC_ENOMEM);
	    stat = nc_get_att(hdr->ncid,var->varid,att->name,values);
            CHECK(stat);
	    fprintf(stdout,"\tattr[%d]: name=%s type=%s values(%lu)=",
			j,att->name,nctypetostring(octypetonc(att->etype)),(unsigned long)nvalues);
	    for(size_t k=0;k<nvalues;k++) {
		fprintf(stdout," ");
		dumpdata1(octypetonc(att->etype),k,values);
	    }
	    fprintf(stdout,"\n");
	}
    }
    fflush(stdout);
    return NC_NOERR;
}

void
dumpdata1(nc_type nctype, size_t index, char* data)
{
    switch (nctype) {
    case NC_CHAR:
	fprintf(stdout,"'%c' %hhd",data[index],data[index]);
	break;
    case NC_BYTE:
	fprintf(stdout,"%hhdB",((signed char*)data)[index]);
	break;
    case NC_UBYTE:
	fprintf(stdout,"%hhuB",((unsigned char*)data)[index]);
	break;
    case NC_SHORT:
	fprintf(stdout,"%hdS",((short*)data)[index]);
	break;
    case NC_USHORT:
	fprintf(stdout,"%hdUS",((unsigned short*)data)[index]);
	break;
    case NC_INT:
	fprintf(stdout,"%d",((int*)data)[index]);
	break;
    case NC_UINT:
	fprintf(stdout,"%uU",((unsigned int*)data)[index]);
	break;
    case NC_FLOAT:
	fprintf(stdout,"%#gF",((float*)data)[index]);
	break;
    case NC_DOUBLE:
	fprintf(stdout,"%#gD",((double*)data)[index]);
	break;
    case NC_STRING:
	fprintf(stdout,"\"%s\"",((char**)data)[index]);
	break;
    default:
	fprintf(stdout,"Unknown type: %i",nctype);
	break;
    }
    fflush(stdout);
}

/* Following should be kept consistent with
   the makeXXXstring3 routines in constraints3.c
*/

/* Convert an NCprojection instance into a string
   that can be used with the url
*/
char*
dumpprojections(NClist* projections)
{
    char* tmp;
    tmp = dcelisttostring(projections,",");
    return tmp;
}

char*
dumpprojection(DCEprojection* proj)
{
    char* tmp;
    tmp = dcetostring((DCEnode*)proj);
    return tmp;
}

char*
dumpselections(NClist* selections)
{
    return dcelisttostring(selections,"&");
}

char*
dumpselection(DCEselection* sel)
{
    return dcetostring((DCEnode*)sel);
}

char*
dumpconstraint(DCEconstraint* con)
{
    char* tmp;
    tmp = dcetostring((DCEnode*)con);
    return tmp;
}

char*
dumpsegments(NClist* segments)
{
    return dcelisttostring(segments,".");
}

char*
dumppath(CDFnode* leaf)
{
    NClist* path = nclistnew();
    NCbytes* buf = ncbytesnew();
    char* result;
    size_t i;

    if(leaf == NULL) return nulldup("");
    collectnodepath(leaf,path,!WITHDATASET);
    for(i=0;i<nclistlength(path);i++) {
	CDFnode* node = (CDFnode*)nclistget(path,i);
	if(i > 0) ncbytescat(buf,".");
	ncbytescat(buf,node->ncbasename);
    }
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    nclistfree(path);
    return result;
}

static void
dumpindent(int indent, NCbytes* buf)
{
    int i;
    for(i=0;i<indent;i++) ncbytescat(buf,indentstr);
}

static void
dumptreer1(CDFnode* root, NCbytes* buf, int indent, char* tag, int visible)
{
    size_t i;
    dumpindent(indent,buf);
    ncbytescat(buf,tag);
    ncbytescat(buf," {\n");
    for(i=0;i<nclistlength(root->subnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(root->subnodes,i);
	if(visible && root->invisible) continue;
	if(root->nctype == NC_Grid) {
	    if(i==0) {
		dumpindent(indent+1,buf);
	        ncbytescat(buf,"Array:\n");
	    } else if(i==1) {
		dumpindent(indent+1,buf);
	        ncbytescat(buf,"Maps:\n");
	    }
	    dumptreer(node,buf,indent+2,visible);
	} else {
	    dumptreer(node,buf,indent+1,visible);
	}
    }
    dumpindent(indent,buf);
    ncbytescat(buf,"} ");
    ncbytescat(buf,(root->ncbasename?root->ncbasename:"<?>"));
}

static void
dumptreer(CDFnode* root, NCbytes* buf, int indent, int visible)
{
    size_t i;
    char* primtype = NULL;
    NClist* dimset = NULL;

    if(visible && root->invisible) return;
    switch (root->nctype) {
    case NC_Dataset:
	dumptreer1(root,buf,indent,"Dataset",visible);
	break;
    case NC_Sequence:
	dumptreer1(root,buf,indent,"Sequence",visible);
	break;
    case NC_Structure:
	dumptreer1(root,buf,indent,"Structure",visible);
	break;
    case NC_Grid:
	dumptreer1(root,buf,indent,"Grid",visible);
	break;
    case NC_Atomic:
	switch (root->etype) {
	case NC_BYTE: primtype = "byte"; break;
	case NC_CHAR: primtype = "char"; break;
	case NC_SHORT: primtype = "short"; break;
	case NC_INT: primtype = "int"; break;
	case NC_FLOAT: primtype = "float"; break;
	case NC_DOUBLE: primtype = "double"; break;
	case NC_UBYTE: primtype = "ubyte"; break;
	case NC_USHORT: primtype = "ushort"; break;
	case NC_UINT: primtype = "uint"; break;
	case NC_INT64: primtype = "int64"; break;
	case NC_UINT64: primtype = "uint64"; break;
	case NC_STRING: primtype = "string"; break;
	default: break;
	}
	dumpindent(indent,buf);
	ncbytescat(buf,primtype);
	ncbytescat(buf," ");
        ncbytescat(buf,(root->ncbasename?root->ncbasename:"<?>"));
	break;
    default: break;
    }

    if(nclistlength(root->array.dimsetplus) > 0) dimset = root->array.dimsetplus;
    else if(nclistlength(root->array.dimset0) > 0) dimset = root->array.dimset0;
    if(dimset != NULL) {
	for(i=0;i<nclistlength(dimset);i++) {
	    CDFnode* dim = (CDFnode*)nclistget(dimset,i);
	    char tmp[64];
	    ncbytescat(buf,"[");
	    if(dim->ncbasename != NULL) {
		ncbytescat(buf,dim->ncbasename);
	        ncbytescat(buf,"=");
	    }
	    snprintf(tmp,sizeof(tmp),"%lu",(unsigned long)dim->dim.declsize);
	    ncbytescat(buf,tmp);
	    ncbytescat(buf,"]");
	}
    }
    ncbytescat(buf,";\n");
}

char*
dumptree(CDFnode* root)
{
    NCbytes* buf = ncbytesnew();
    char* result;
    dumptreer(root,buf,0,0);
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

char*
dumpvisible(CDFnode* root)
{
    NCbytes* buf = ncbytesnew();
    char* result;
    dumptreer(root,buf,0,1);
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

/* Provide detailed data on a CDFnode */
char*
dumpnode(CDFnode* node)
{
    NCbytes* buf = ncbytesnew();
    char* result;
    size_t i;
    char* nctype = NULL;
    char* primtype = NULL;
    char tmp[1024];

    switch (node->nctype) {
    case NC_Dataset: nctype = "Dataset"; break;
    case NC_Sequence: nctype = "Sequence"; break;
    case NC_Structure: nctype = "Structure"; break;
    case NC_Grid: nctype = "Grid"; break;
    case NC_Atomic:
	switch (node->etype) {
	case NC_BYTE: primtype = "byte"; break;
	case NC_CHAR: primtype = "char"; break;
	case NC_SHORT: primtype = "short"; break;
	case NC_INT: primtype = "int"; break;
	case NC_FLOAT: primtype = "float"; break;
	case NC_DOUBLE: primtype = "double"; break;
	case NC_UBYTE: primtype = "ubyte"; break;
	case NC_USHORT: primtype = "ushort"; break;
	case NC_UINT: primtype = "uint"; break;
	case NC_INT64: primtype = "int64"; break;
	case NC_UINT64: primtype = "uint64"; break;
	case NC_STRING: primtype = "string"; break;
	default: break;
	}
	break;
    default: break;
    }
    snprintf(tmp,sizeof(tmp),"%s %s {\n",
		(nctype?nctype:primtype),node->ocname);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"ocnode=%p\n",node->ocnode);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"container=%s\n",
		(node->container?node->container->ocname:"null"));
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"root=%s\n",
		(node->root?node->root->ocname:"null"));
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"ncbasename=%s\n",node->ncbasename);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"ncfullname=%s\n",node->ncfullname);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"|subnodes|=%u\n",(unsigned)nclistlength(node->subnodes));
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"externaltype=%d\n",node->externaltype);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"ncid=%d\n",node->ncid);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"maxstringlength=%ld\n",node->maxstringlength);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"sequencelimit=%ld\n",node->sequencelimit);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"usesequence=%d\n",node->usesequence);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"elided=%d\n",node->elided);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"invisible=%d\n",node->invisible);
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"attachment=%s\n",
		(node->attachment?node->attachment->ocname:"null"));
    ncbytescat(buf,tmp);
    snprintf(tmp,sizeof(tmp),"rank=%u\n",(unsigned)nclistlength(node->array.dimset0));
    ncbytescat(buf,tmp);
    for(i=0;i<nclistlength(node->array.dimset0);i++) {
	CDFnode* dim = (CDFnode*)nclistget(node->array.dimset0,i);
        snprintf(tmp,sizeof(tmp),"dims[%zu]={\n",i);
        ncbytescat(buf,tmp);
	snprintf(tmp,sizeof(tmp),"    ocname=%s\n",dim->ocname);
        ncbytescat(buf,tmp);
	snprintf(tmp,sizeof(tmp),"    ncbasename=%s\n",dim->ncbasename);
        ncbytescat(buf,tmp);
	snprintf(tmp,sizeof(tmp),"    dimflags=%u\n",
			(unsigned int)dim->dim.dimflags);
        ncbytescat(buf,tmp);
	snprintf(tmp,sizeof(tmp),"    declsize=%lu\n",
		    (unsigned long)dim->dim.declsize);
        ncbytescat(buf,tmp);
        snprintf(tmp,sizeof(tmp),"    }\n");
        ncbytescat(buf,tmp);
    }

    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

char*
dumpalign(NCD2alignment* ncalign)
{
    char* result;
    char tmp[1024];
    if(ncalign == NULL)
	result = nulldup("NCD2alignment{size=-- alignment=-- offset=--}");
    else {
        snprintf(tmp,sizeof(tmp),"NCD2alignment{size=%lu alignment=%lu offset=%lu}",
		 ncalign->size,ncalign->alignment,ncalign->offset);
        result = nulldup(tmp);
    }
    return result;
}

char*
dumpcachenode(NCcachenode* node)
{
    char* result = NULL;
    char tmp[8192];
    size_t i;
    NCbytes* buf;

    if(node == NULL) return strdup("cachenode{null}");
    buf = ncbytesnew();
    result = dcebuildconstraintstring(node->constraint);
    snprintf(tmp,sizeof(tmp),"cachenode%s(%p){size=%lu; constraint=%s; vars=",
		node->isprefetch?"*":"",
		node,
		(unsigned long)node->xdrsize,
	        result);
    ncbytescat(buf,tmp);
    if(nclistlength(node->vars)==0)
	ncbytescat(buf,"null");
    else for(i=0;i<nclistlength(node->vars);i++) {
	CDFnode* var = (CDFnode*)nclistget(node->vars,i);
	if(i > 0) ncbytescat(buf,",");
	ncbytescat(buf,makecdfpathstring(var,"."));
    }
    ncbytescat(buf,"}");
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

char*
dumpcache(NCcache* cache)
{
    char* result = NULL;
    char tmp[8192];
    size_t i;
    NCbytes* buf;

    if(cache == NULL) return strdup("cache{null}");
    buf = ncbytesnew();
    snprintf(tmp,sizeof(tmp),"cache{limit=%lu; size=%lu;\n",
		(unsigned long)cache->cachelimit,
		(unsigned long)cache->cachesize);
    ncbytescat(buf,tmp);
    if(cache->prefetch) {
	ncbytescat(buf,"\tprefetch=");
	ncbytescat(buf,dumpcachenode(cache->prefetch));
	ncbytescat(buf,"\n");
    }
    if(nclistlength(cache->nodes) > 0) {
        for(i=0;i<nclistlength(cache->nodes);i++) {
   	    NCcachenode* node = (NCcachenode*)nclistget(cache->nodes,i);
	    ncbytescat(buf,"\t");
	    ncbytescat(buf,dumpcachenode(node));
	    ncbytescat(buf,"\n");
	}
    }
    ncbytescat(buf,"}");
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

/* This should be consistent with makeslicestring3 in constraints3.c */
char*
dumpslice(DCEslice* slice)
{
    char buf[8192];
    char tmp[8192];
    buf[0] = '\0';
    if(slice->last > slice->declsize && slice->declsize > 0)
        slice->last = slice->declsize - 1;
    if(slice->count == 1) {
        snprintf(tmp,sizeof(tmp),"[%lu]",
            (unsigned long)slice->first);
    } else if(slice->stride == 1) {
        snprintf(tmp,sizeof(tmp),"[%lu:%lu]",
                (unsigned long)slice->first,
                (unsigned long)slice->last);
    } else {
       snprintf(tmp,sizeof(tmp),"[%lu:%lu:%lu]",
                (unsigned long)slice->first,
                (unsigned long)slice->stride,
                (unsigned long)slice->last);
    }
    strlcat(buf,tmp,sizeof(buf));
    return strdup(tmp);
}

char*
dumpslices(DCEslice* slice, unsigned int rank)
{
    int i;
    NCbytes* buf;
    char* result = NULL;

    buf = ncbytesnew();
    for(i=0;i<rank;i++,slice++) {
	char* sslice = dumpslice(slice);
	if(sslice != NULL) {
	    ncbytescat(buf,sslice);
	    free(sslice);
	}
    }
    result = ncbytesdup(buf);
    ncbytesfree(buf);
    return result;
}

void
dumpraw(void* o)
{
    fprintf(stderr,"%s\n",dcerawtostring(o));
    fflush(stderr);
}

void
dumplistraw(NClist* l)
{
    fprintf(stderr,"%s\n",dcerawlisttostring(l));
    fflush(stderr);
}

/* For debugging */
void
dumpstringlist(NClist* l)
{
    size_t i;
    for(i=0;i<nclistlength(l);i++) {
	const char* s = (const char*)nclistget(l,i);
	fprintf(stderr,"[%zu]: |%s|\n",i,s);
    }
    fflush(stderr);
}
