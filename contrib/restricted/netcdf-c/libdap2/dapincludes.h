/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/
#ifndef DAPINCLUDES_H
#define DAPINCLUDES_H 1

#include "config.h"
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <string.h>
#include <stdio.h>

#include "netcdf.h"

#include "ncbytes.h"
#include "nclist.h"
#include "nclog.h"
#include "ncuri.h"

#include "fbits.h"
#include "dceconstraints.h"

#include "ncdispatch.h"
#include "nc.h"
#include "nc3internal.h"
 /* netcdf overrides*/
#include "dapnc.h"

#include "oc.h"

#include "ncdap.h"
#include "nccommon.h"
#include "dapdebug.h"
#include "daputil.h"

/**************************************************/
/* sigh, do the forwards */
struct NCprojection;
struct NCselection;
struct Getvara;
struct NCcachenode;
struct NCcache;
struct NCslice;
struct NCsegment;

/**************************************************/

#include "getvara.h"
#include "constraints.h"

/**************************************************/

extern struct NCTMODEL nctmodels[];

/**************************************************/
/* Import some internal procedures from libsrc*/

/* Internal, but non-static procedures */
extern NCerror computecdfvarnames(NCDAPCOMMON*,CDFnode*,NClist*);
extern NCerror computecdfnodesets(NCDAPCOMMON* nccomm, CDFtree* tree);
extern NCerror computevarnodes(NCDAPCOMMON*, NClist*, NClist*);
extern NCerror collectvardefdims(NCDAPCOMMON* drno, CDFnode* var, NClist* dimset);
extern NCerror fixgrids(NCDAPCOMMON* drno);
extern NCerror dapmerge(NCDAPCOMMON* drno, CDFnode* node, OCobject dasroot);
extern NCerror sequencecheck(NCDAPCOMMON* drno);
extern NCerror computecdfdimnames(NCDAPCOMMON*);
extern NCerror attachdatadds(struct NCDAPCOMMON*);
extern NCerror detachdatadds(struct NCDAPCOMMON*);
extern void dapdispatch3init(void);

/*
extern void dereference(NCconstraint* constraint);
extern NCerror rereference(NCconstraint*, NClist*);
*/

extern NCerror dapbuildvaraprojection(CDFnode*,
		     const size_t* startp, const size_t* countp, const ptrdiff_t* stridep,
		     struct DCEprojection** projectionlist);

extern NCerror nc3d_getvarx(int ncid, int varid,
	    const size_t *startp,
	    const size_t *countp,
	    const ptrdiff_t *stridep,
	    void *data,
	    nc_type dsttype0);

/**************************************************/

extern NCerror nc3d_open(const char* path, int mode, int* ncidp);
extern int nc3d_close(int ncid);
extern NCerror restruct(NCDAPCOMMON*, CDFnode* ddsroot, CDFnode* pattern, NClist*);
extern void setvisible(CDFnode* root, int visible);
extern NCerror mapnodes(CDFnode* dstroot, CDFnode* srcroot);
extern void unmap(CDFnode* root);

#if 0
extern NCerror fetchpatternmetadata(NCDAPCOMMON* nccomm);
extern NCerror fetchconstrainedmetadata(NCDAPCOMMON* nccomm);
extern void applyclientparamcontrols(NCDAPCOMMON*);
extern NCerror suppressunusablevars(NCDAPCOMMON*);
extern void estimatevarsizes(NCDAPCOMMON*);
extern NCerror showprojection(NCDAPCOMMON*, CDFnode* var);
#endif

/* From: dapcvt.c*/
extern NCerror dapconvert(nc_type, nc_type, char*, char*, size_t);
extern int dapcvtattrval(nc_type, void*, NClist*, NCattribute*);

#endif /*DAPINCLUDES_H*/
