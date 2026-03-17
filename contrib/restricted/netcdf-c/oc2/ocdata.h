/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCDATA_H
#define OCDATA_H

/*
This structure is used to set up
pointers into the DataDDS data packet
to speed up access.
This has some similarities to OCNODE.
*/

struct OCdata {
    OCheader  header;
    OCDT      datamode;
    OCnode*   pattern;
    OCdata*   container; /* link back to container instance */
    size_t    index;     /* WRT to the container */
    off_t  xdroffset;	/* Of this instance wrt xxdr_getpos() */
    off_t  xdrsize;   /* for leafs, and as defined by xdr; if known else 0*/
    size_t    ninstances;/* |instances| */
    OCdata**  instances;	/* vector of instances; if rank > 0, then
                                   it captures the array elements, else
                                   it captures the field instances. */
    size_t     nstrings;
    off_t*    strings;
};


extern void ocdata_free(OCstate*, OCdata*);

extern OCerror ocdata_ithfield(OCstate*, OCdata* container, size_t index, OCdata** fieldp);
extern OCerror ocdata_container(OCstate*, OCdata* data, OCdata** containerp);
extern OCerror ocdata_root(OCstate*, OCdata* data, OCdata** rootp);

extern OCerror ocdata_ithelement(OCstate*, OCdata* data, size_t* indices, OCdata** elementp);
extern OCerror ocdata_ithrecord(OCstate*, OCdata* data, size_t index, OCdata** recordp);

extern OCerror ocdata_position(OCstate*, OCdata* data, size_t* indices);
extern OCerror ocdata_recordcount(OCstate*, OCdata*, size_t*);

extern OCerror ocdata_getroot(OCstate*, OCnode*, OCdata**);

/* Atomic leaf reading */
extern OCerror ocdata_read(OCstate*,OCdata*,size_t,size_t,void*,size_t);

#endif /*OCDATA_H*/
