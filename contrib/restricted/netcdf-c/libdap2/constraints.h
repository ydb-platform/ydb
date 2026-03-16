/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef CONSTRAINTS_H
#define CONSTRAINTS_H 1

extern NCerror dapparsedapconstraints(NCDAPCOMMON*, char*, DCEconstraint*);
extern NCerror dapmapconstraints(DCEconstraint*,CDFnode*);
extern NCerror dapqualifyconstraints(DCEconstraint* constraint);
extern NCerror dapcomputeprojectedvars(NCDAPCOMMON*,DCEconstraint*);

extern char* dapsimplepathstring(NClist* segments, char* separator);
extern void dapmakesegmentstring(NClist* segments, NCbytes* buf, char* separator);

extern int dapiswholeslice(DCEslice*, struct CDFnode* dim);
extern int dapiswholesegment(DCEsegment*);

extern int dapiswholeconstraint(DCEconstraint* con);

extern void dapmakewholesegment(DCEsegment*,struct CDFnode*);
extern void dapmakewholeslice(DCEslice* slice, struct CDFnode* dim);

extern NCerror dapfixprojections(NClist* list);

extern int dapvar2projection(CDFnode* var, DCEprojection** projectionp);
extern int daprestrictprojection(NClist* projections, DCEprojection* var, DCEprojection** resultp);
extern int dapshiftprojection(DCEprojection*);

#endif /*CONSTRAINTS_H*/
