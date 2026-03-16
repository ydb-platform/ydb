"""Pyrex code to provide python interfaces to functions
in the NCEP grib2c library. Make changes to this file, not the
c-wrappers that Pyrex generates."""

import math

# Some helper routines from the Python API
cdef extern from "Python.h":
    # To access integers
    object PyLong_FromLong(long)
    long PyLong_AsLong(object)
    # To access doubles
    object PyFloat_FromDouble(double)
    # To access strings
    char * PyBytes_AsString(object string)
    object PyBytes_FromString(char *s)
    object PyBytes_FromStringAndSize(char *s, size_t size)
    int PyObject_AsWriteBuffer(object, void **rbuf, Py_ssize_t *len)
    int PyObject_AsReadBuffer(object, void **rbuf, Py_ssize_t *len)
    int PyObject_CheckReadBuffer(object)

cdef extern from "stdlib.h":
    void free(void *ptr)

# get 32 bit integer type
cdef extern from "inttypes.h":
    ctypedef long int32_t

# Functions from grib2c lib.
cdef extern from "grib2.h":
    ctypedef int32_t g2int # for 64 bit machines, this should be int
    ctypedef float g2float
    g2int g2_unpack1(unsigned char *,g2int *,g2int **,g2int *)
    g2int g2_unpack3(unsigned char *,g2int,g2int *,g2int **,g2int **, 
                         g2int *,g2int **,g2int *)
    g2int g2_unpack4(unsigned char *,g2int,g2int *,g2int *,g2int **,
                         g2int *,g2float **,g2int *)
    g2int g2_unpack5(unsigned char *,g2int,g2int *,g2int *,g2int *, g2int **,g2int *)
    g2int g2_unpack6(unsigned char *,g2int,g2int *,g2int ,g2int *, g2int **)
    g2int g2_unpack7(unsigned char *,g2int,g2int *,g2int ,g2int *,
                         g2int ,g2int *,g2int ,g2float **)
    g2int g2_create(unsigned char *,g2int *,g2int *)
    g2int g2_addgrid(unsigned char *,g2int *,g2int *,g2int *,g2int ) 
    g2int g2_addfield(unsigned char *,g2int ,g2int *,
                     g2float *,g2int ,g2int ,g2int *,
                     g2float *,g2int ,g2int ,g2int *)
    g2int g2_gribend(unsigned char *)
    void mkieee(g2float *,g2int *,g2int)
    void rdieee(g2int *,g2float *,g2int)

# Python wrappers for grib2c functions.

# routines for convert to/from IEEE integers.

def rtoi_ieee(object rarr, object iarr):
    """
 Converts a float32 array into an int32 array of IEEE formatted values
    """
    cdef void *rdat
    cdef void *idat
    cdef g2float *rdata
    cdef g2float r1
    cdef g2int *idata
    cdef g2int i1
    cdef Py_ssize_t bufleni, buflenr
    if PyObject_AsReadBuffer(rarr, &rdat, &buflenr) <> 0:
        raise RuntimeError, 'error getting buffer for input real array'
    if PyObject_AsWriteBuffer(iarr, &idat, &bufleni)  <> 0 :
        raise RuntimeError, 'error getting buffer for output integer array'
    if bufleni < buflenr:
        raise RuntimeError, 'integer output array must be as least as long a real input array'
    rdata = <g2float *>rdat
    idata = <g2int *>idat
    mkieee(rdata, idata, buflenr//4)

def itor_ieee(object iarr, object rarr):
    """
 Converts an int32 array of IEEE values into a float32 array.
    """
    cdef void *rdat
    cdef void *idat
    cdef g2float *rdata
    cdef g2int *idata
    cdef Py_ssize_t bufleni, buflenr
    if PyObject_AsReadBuffer(rarr, &rdat, &buflenr) <> 0:
        raise RuntimeError, 'error getting buffer for output real array'
    if PyObject_AsWriteBuffer(iarr, &idat, &bufleni)  <> 0 :
        raise RuntimeError, 'error getting buffer for input integer array'
    if buflenr < bufleni:
        raise RuntimeError, 'real output array must be as least as long a integerinput array'
    rdata = <g2float *>rdat
    idata = <g2int *>idat
    rdieee(idata, rdata, bufleni//4)

cdef _toarray(void *items, object a):
    """
 Fill a numpy array from the grib2 file.  Note that this free()s the items argument!
    """
    cdef void *abuf
    cdef Py_ssize_t buflen
    cdef g2int *idata
    cdef g2float *fdata

    # get pointer to data buffer.
    PyObject_AsWriteBuffer(a, &abuf, &buflen)

    if str(a.dtype) == 'int32':
      idata = <g2int *>abuf
      # fill buffer.
      for i from 0 <= i < len(a):
        idata[i] = (<g2int *>items)[i]
    elif str(a.dtype) == 'float32':
      fdata = <g2float *>abuf
      # fill buffer.
      for i from 0 <= i < len(a):
        fdata[i] = (<g2float *>items)[i]
    else:
      free(items)
      raise RuntimeError('unknown array type %s' % a.dtype)

    free(items)
    return a


# routines for reading grib2 files.

def unpack1(gribmsg, ipos, object zeros):
    """              .      .    .                                       .
 Unpacks Section 1 (Identification Section) as defined in GRIB Edition 2.
 
 idsect,ipos = unpack1(gribmsg,ipos)

 INPUTS:
  gribmsg - chararcter string containing Section 1 of the GRIB2 message
  ipos    - Byte offset for the beginning of Section 1 in gribmsg.
  zeros   - Numeric/numpy array/scip.base zeros function (used to allocate
            output python arrays).  Python is used to allocate the arrays
            so no implementation-specific (Numeric, numpy array, scipy_core)
            C API functions need to be used.  This means that any of these
            array modules can be used simply by changing the import statement
            in your python code.
 RETURNS:
  ipos    - Byte offset at the end of Section 1.
  idsect  - numpy array containing information 
            read from Section 1, the Identification section.
            idsect[0]   = Identification of originating Centre
                                 ( see Common Code Table C-1 )
            idsect[1]   = Identification of originating Sub-centre
            idsect[2]   = GRIB Master Tables Version Number
                                 ( see Code Table 1.0 )
            idsect[3]   = GRIB Local Tables Version Number
                                 ( see Code Table 1.1 )
            idsect[4]   = Significance of Reference Time (Code Table 1.2)
            idsect[5]   = Year ( 4 digits )
            idsect[6]   = Month
            idsect[7]   = Day
            idsect[8]   = Hour
            idsect[9]   = Minute
            idsect[10]  = Second
            idsect[11]  = Production status of processed data
                                 ( see Code Table 1.3 )
            idsect[12]  = Type of processed data ( see Code Table 1.4 )
 ERROR CODES:   2 = Array passed is not section 1
                6 = memory allocation error
    """
    cdef unsigned char *cgrib
    cdef g2int i, iofst, ierr, idslen
    cdef g2int *ids
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ierr = g2_unpack1(cgrib, &iofst, &ids, &idslen)
    if ierr != 0:
       msg = "Error unpacking section 1 - error code = %i" % ierr
       raise RuntimeError(msg)

    idsect = _toarray(ids, zeros(idslen, 'i4'))
    return idsect,iofst//8



def unpack3(gribmsg, ipos, object zeros):
    """
 Unpacks Section 3 (Grid Definition Section) as defined in GRIB Edition 2.

 gds,gdtmpl,deflist,ipos = unpack3(gribmsg,ipos)
 
 INPUTS:
 cgrib    - Char array ontaining Section 3 of the GRIB2 message
 iofst    - Byte offset for the beginning of Section 3 in cgrib.
 zeros    - Numeric/numpy array/scip.base zeros function (used to allocate
            output python arrays).  Python is used to allocate the arrays
            so no implementation-specific (Numeric, numpy array, scipy_core)
            C API functions need to be used.  This means that any of these
            array modules can be used simply by changing the import statement
            in your python code.

 RETURNS:      
 ipos     - Byte offset at the end of Section 3, returned.
  gds     - numpy array containg information read from the appropriate GRIB Grid 
            Definition Section 3 for the field being returned.
            gds[0]=Source of grid definition (see Code Table 3.0)
            gds[1]=Number of grid points in the defined grid.
            gds[2]=Number of octets needed for each 
                        additional grid points definition.  
                        Used to define number of
                        points in each row ( or column ) for
                        non-regular grids.  
                        = 0, if using regular grid.
            gds[3]=Interpretation of list for optional points 
                        definition.  (Code Table 3.11)
            gds[4]=Grid Definition Template Number (Code Table 3.1)
 gdstmpl -  numpy array containing the data values for 
            the specified Grid Definition
            Template ( NN=gds[4] ).  Each element of this integer 
            array contains an entry (in the order specified) of Grid
            Defintion Template 3.NN
 deflist -  (Used if gds[2] != 0) numpy array containing
            the number of grid points contained in each row ( or column ).
            (part of Section 3)
 ERROR CODES:   2 = Not Section 3
                5 = "GRIB" message contains an undefined Grid Definition
                    Template.
                6 = memory allocation error
    """
    cdef unsigned char *cgrib
    cdef g2int *igds
    cdef g2int *igdstmpl
    cdef g2int *ideflist
    cdef g2int mapgridlen, iofst, idefnum, ierr
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ierr=g2_unpack3(cgrib,len(gribmsg),&iofst,&igds,&igdstmpl,&mapgridlen,&ideflist,&idefnum)
    if ierr != 0:
       msg = "Error unpacking section 3 - error code = %i" % ierr
       raise RuntimeError(msg)

    gdtmpl = _toarray(igdstmpl, zeros(mapgridlen, 'i4'))
    gds = _toarray(igds, zeros(5, 'i4'))
    deflist = _toarray(ideflist, zeros(idefnum, 'i4'))

    return gds,gdtmpl,deflist,iofst//8

def unpack4(gribmsg,ipos,object zeros):
    """
 Unpacks Section 4 (Product Definition Section) as defined in GRIB Edition 2.

 pdtmpl,pdtnum,coordlist,ipos = unpack4(gribmsg,ipos)
 
 INPUTS:
   gribmsg  - Character string containing Section 4 of the GRIB2 message
   ipos     - Byte offset of the beginning of Section 4 in cgrib.
   zeros    - Numeric/numpy array/scip.base zeros function (used to allocate
              output python arrays).  Python is used to allocate the arrays
              so no implementation-specific (Numeric, numpy array, scipy_core)
              C API functions need to be used.  This means that any of these
              array modules can be used simply by changing the import statement
              in your python code.

 RETURNS:       
   pdtmpl   -  numpy array containing the data values for 
               the specified Product Definition
               Template ( N=ipdsnum ).  Each element of this integer
               array contains an entry (in the order specified) 
               of Product Defintion Template 4.N               
   pdtnum    - Product Definition Template Number ( see Code Table 4.0)   
   coordlist - numpy array containing floating point values 
               intended to document
               the vertical discretisation associated to model data
               on hybrid coordinate vertical levels.  (part of Section 4)
   ipos      - Byte offset of the end of Section 4, returned.

 ERROR CODES: 2 = Not section 4
              5 = "GRIB" message contains an undefined Product Definition
                  Template.
              6 = memory allocation error
    """
    cdef unsigned char *cgrib
    cdef g2int *ipdstmpl
    cdef g2float *icoordlist
    cdef g2int mappdslen, iofst, ipdsnum, ierr, numcoord
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ierr=g2_unpack4(cgrib,len(gribmsg),&iofst,&ipdsnum,&ipdstmpl,&mappdslen,&icoordlist,&numcoord)
    if ierr != 0:
       msg = "Error unpacking section 4 - error code = %i" % ierr
       raise RuntimeError(msg)

    pdtmpl = _toarray(ipdstmpl, zeros(mappdslen, 'i4'))
    coordlist = _toarray(icoordlist, zeros(numcoord, 'f4'))

    return pdtmpl,ipdsnum,coordlist,iofst//8
    
def unpack5(gribmsg,ipos,object zeros):
    """
 Unpacks Section 5 (Data Representation Section) as defined in GRIB Edition 2.

 drtmpl,drtnum,ndpts,ipos = unpack5(gribmsg,ipos)

 INPUTS:
   gribmsg  - Character string containing Section 5 of the GRIB2 message
   ipos     - Byte offset for the beginning of Section 5 in cgrib.
   zeros   - Numeric/numpy array/scip.base zeros function (used to allocate
             output python arrays).  Python is used to allocate the arrays
             so no implementation-specific (Numeric, numpy array, scipy_core)
             C API functions need to be used.  This means that any of these
             array modules can be used simply by changing the import statement
             in your python code.

 RETURNS:      
   drtmpl  -  numpy array containing the data values for 
              the specified Data Representation
              Template ( N=idrsnum ).  Each element of this integer
              array contains an entry (in the order specified) of Data
              Representation Template 5.N
   drtnum   - Data Representation Template Number ( see Code Table 5.0)
   ndpts    - Number of data points to be unpacked (in sxns 6 and 7).
   ipos     - Byte offset at the end of Section 5.

 ERROR CODES: 2 = Not Section 5
              6 = memory allocation error
              7 = "GRIB" message contains an undefined Data
                  Representation Template
    """
    cdef unsigned char *cgrib
    cdef g2int *idrstmpl
    cdef g2int iofst, ierr, ndpts, idrsnum, mapdrslen
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ierr=g2_unpack5(cgrib,len(gribmsg),&iofst,&ndpts,&idrsnum,&idrstmpl,&mapdrslen)
    if ierr != 0:
       msg = "Error unpacking section 5 - error code = %i" % ierr
       raise RuntimeError(msg)

    drtmpl = _toarray(idrstmpl, zeros(mapdrslen, 'i4'))
    return drtmpl,idrsnum,ndpts,iofst//8
    
def unpack6(gribmsg,ndpts,ipos,object zeros):
    """
 Unpacks Section 6 (Bit-Map Section) as defined in GRIB Edition 2.

 bitmap = unpack6(gribmsg,ndpts,ipos)         

 INPUTS:
   gribmsg  - String containing Section 6 of the GRIB2 message
   ndpts    - Number of grid points specified in the bit-map (from
              gds[1] returned by unpack3).
   ipos     - Byte offset of the beginning of Section 6 in cgrib.
   zeros   - Numeric/numpy array/scip.base zeros function (used to allocate
            output python arrays).  Python is used to allocate the arrays
            so no implementation-specific (Numeric, numpy array, scipy_core)
            C API functions need to be used.  This means that any of these
            array modules can be used simply by changing the import statement
            in your python code.
 RETURNS:      
   bmap     - numpy array array containing decoded bitmap. 
              ( if ibmap=0 ).  Otherwise None.
   ibmap    - Bitmap indicator ( see Code Table 6.0 )
              0 = bitmap applies and is included in Section 6.
              1-253 = Predefined bitmap applies
              254 = Previously defined bitmap applies to this field
 ERROR CODES: 2 = Not Section 6
              4 = Unrecognized pre-defined bit-map.
              6 = memory allocation error
    """
    cdef object bitmap
    cdef unsigned char *cgrib
    cdef g2int iofst, ierr, ngpts, ibmap
    cdef g2int *bmap
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ngpts = <g2int>PyLong_AsLong(ndpts)
    ierr=g2_unpack6(cgrib,len(gribmsg),&iofst,ngpts,&ibmap,&bmap)
    if ierr != 0:
        msg = "Error unpacking section 6 - error code = %i" % ierr
        raise RuntimeError(msg)
    if ibmap == 0:
        bitmap = _toarray(bmap, zeros(ngpts, 'i4'))
    else:
        bitmap = None
        free(bmap)
    return bitmap,ibmap
    
def unpack7(gribmsg,gdtnum,object gdtmpl,drtnum,object drtmpl,ndpts,ipos,object zeros,printminmax=False,storageorder='C'):
    """
 Unpacks Section 7 (Data Section) as defined in GRIB Edition 2.

 fld,fldmin,fldmax = unpack7(gribmsg,gdtnum,gdtmpl,drtnum,drtmpl,ndpts,ipos)
 INPUTS:
   gribmsg  - String containing Section 7 of the GRIB2 message
   gdtnum   - Grid Definition Template Number ( see Code Table 3.0)
              ( Only used for DRS Template 5.51 )
   gdtmpl   - numpy array containing the data values for
              the specified Grid Definition
              Template ( N=igdsnum ).  Each element of this integer
              array contains an entry (in the order specified) of Grid
              Definition Template 3.N
              ( Only used for DRS Template 5.51 )
   drtnum   - Data Representation Template Number ( see Code Table 5.0)
   drtmpl   - numpy array containing the data values for
              the specified Data Representation
              Template ( N=idrsnum ).  Each element of this integer
              array contains an entry (in the order specified) of Data
              Representation Template 5.N
   ndpts    - Number of data points unpacked and returned.
   ipos     - Byte offset of the beginning of Section 7 in cgrib.
   zeros   - Numeric/numpy array/scip.base zeros function (used to allocate
            output python arrays).  Python is used to allocate the arrays
            so no implementation-specific (Numeric, numpy array, scipy_core)
            C API functions need to be used.  This means that any of these
            array modules can be used simply by changing the import statement
            in your python code.
   storageorder - 'C' for row-major, or 'F' for column-major (Default 'C')
   printminmax - if True, min/max of fld is printed.

 RETURNS:      
   fld      - numpy array (float32) containing the unpacked data field.

 ERROR CODES: 2 = Not section 7
              4 = Unrecognized Data Representation Template
              5 = need one of GDT 3.50 through 3.53 to decode DRT 5.51
              6 = memory allocation error
    """
    cdef unsigned char *cgrib
    cdef g2int iofst, ierr, ngpts, idrsnum, igdsnum
    cdef g2int *igdstmpl
    cdef g2int *idrstmpl
    cdef g2float *fld
    cdef void *drtmpldat
    cdef void *gdtmpldat
    cdef float rmin, rmax
    cdef int n
    cdef Py_ssize_t buflen
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    iofst = <g2int>PyLong_AsLong(ipos*8)
    ngpts = <g2int>PyLong_AsLong(ndpts)
    idrsnum = <g2int>PyLong_AsLong(drtnum)
    igdsnum = <g2int>PyLong_AsLong(gdtnum)
    PyObject_AsReadBuffer(drtmpl, &drtmpldat, &buflen) 
    PyObject_AsReadBuffer(gdtmpl, &gdtmpldat, &buflen) 
    idrstmpl = <g2int *>drtmpldat
    igdstmpl = <g2int *>gdtmpldat
    ierr=g2_unpack7(cgrib,len(gribmsg),&iofst,igdsnum,igdstmpl,idrsnum,idrstmpl,ngpts,&fld)
    if ierr != 0:
       msg = "Error unpacking section 7 - error code = %i" % ierr
       raise RuntimeError(msg)


    if printminmax:
        rmax=-<float>9.9e31
        rmin=<float>9.9e31
        for n from 0 <= n < ndpts:
            if fld[n] > rmax: rmax=fld[n]
            if fld[n] < rmin: rmin=fld[n]
        fldmax = PyFloat_FromDouble(rmax)
        fldmin = PyFloat_FromDouble(rmin)
        bitsofprecision = drtmpl[3]
        digitsofprecision = int(math.ceil(math.log10(math.pow(2,bitsofprecision))))
        format = "%."+repr(digitsofprecision+1)+"g"
        minmaxstring = 'min/max='+format+'/'+format
        minmaxstring = minmaxstring % (fldmin,fldmax)
        print(minmaxstring)

    data = _toarray(fld, zeros(ngpts, 'f4', order=storageorder))
    return data

# routines for writing grib2 files.

def grib2_create(object listsec0, object listsec1):
    """
 Initializes a new GRIB2 message and packs
 GRIB2 sections 0 (Indicator Section) and 1 (Identification Section).
 This routine is used with routines "g2_addgrid", 
 "grib2_addfield", and "grib2_gribend" to create a complete GRIB2 message.  
 grib2_create must be called first to initialize a new GRIB2 message.
 Also, a call to grib2_gribend is required to complete GRIB2 message
 after all fields have been added.

 gribmsg, ierr = grib2_create(listsec0, listsec1)

   INPUT ARGUMENTS:
     
     listsec0 - numpy array containing info needed for GRIB Indicator Section 0.
                Must have length >= 2.
                listsec0[0]=Discipline-GRIB Master Table Number
                            (see Code Table 0.0)
                listsec0[1]=GRIB Edition Number (currently 2)
     listsec1 - numpy array containing info needed for GRIB Identification Section 1.
                Must have len >= 13.
                listsec1[0]=Id of orginating centre (Common Code Table C-1)
                listsec1[1]=Id of orginating sub-centre (local table)
                listsec1[2]=GRIB Master Tables Version Number (Code Table 1.0)
                listsec1[3]=GRIB Local Tables Version Number (Code Table 1.1)
                listsec1[4]=Significance of Reference Time (Code Table 1.2)
                listsec1[5]=Reference Time - Year (4 digits)
                listsec1[6]=Reference Time - Month
                listsec1[7]=Reference Time - Day
                listsec1[8]=Reference Time - Hour
                listsec1[9]=Reference Time - Minute
                listsec1[10]=Reference Time - Second
                listsec1[11]=Production status of data (Code Table 1.3)
                listsec1[12]=Type of processed data (Code Table 1.4)

   OUTPUT ARGUMENTS:      
     gribmsg  - string containing the new GRIB2 message.
     ierr     - return code.
              > 0 = Current size of new GRIB2 message
               -1 = Tried to use for version other than GRIB Edition 2
    """
    cdef g2int *isec0
    cdef g2int *isec1
    cdef g2int ierr
    cdef void *listsec0dat
    cdef void *listsec1dat
    cdef Py_ssize_t buflen
    cdef unsigned char *cgrib
    # cgrib needs to be big enough to hold sec0 and sec1.
    lgrib = 4*(len(listsec0)+len(listsec1))
    gribmsg = lgrib*b" "
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    PyObject_AsReadBuffer(listsec0, &listsec0dat, &buflen) 
    PyObject_AsReadBuffer(listsec1, &listsec1dat, &buflen) 
    isec0 = <g2int *>listsec0dat
    isec1 = <g2int *>listsec1dat
    ierr = g2_create(cgrib,isec0,isec1)
    if ierr < 0:
       msg = "Error in grib2_create, error code = %i" % ierr
       raise RuntimeError(msg)
    gribmsg = PyBytes_FromStringAndSize(<char *>cgrib, ierr)
    return gribmsg, ierr

def grib2_end(gribmsg):
    """

    Finalizes a GRIB2 message after all grids
    and fields have been added.  It adds the End Section ( "7777" )
    to the end of the GRIB message and calculates the length and stores
    it in the appropriate place in Section 0.
    This routine is used with routines "g2_create",  
    "g2_addgrid", and "g2_addfield" to create a complete GRIB2 message.
    g2_create must be called first to initialize a new GRIB2 message.

  gribmsg, ierr = grib2_end(gribmsg)
  
    INPUT ARGUMENT:
      gribmsg  - String containing all the data sections added
                 be previous calls to g2_create, g2_addgrid,
                 and g2_addfield.

    OUTPUT ARGUMENTS:      
      gribmsg  - String containing the finalized GRIB2 message

    RETURN VALUES:
      ierr     - Return code.
               > 0 = Length of the final GRIB2 message in bytes.
                -1 = GRIB message was not initialized.  Need to call
                     routine g2_create first.
                -2 = GRIB message already complete.  
                -3 = Sum of Section byte counts doesn't add to total byte count
                -4 = Previous Section was not 7.
    """
    cdef g2int ierr
    cdef unsigned char *cgrib
    # add some extra space to grib message (enough to hold section 8).
    gribmsg = gribmsg + b'        '
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    ierr = g2_gribend(cgrib)
    if ierr < 0:
       msg = "error in grib2_end, error code = %i" % ierr
       raise RuntimeError(msg)
    gribmsg = PyBytes_FromStringAndSize(<char *>cgrib, ierr)
    return gribmsg, ierr

def grib2_addgrid(gribmsg,object gds,object gdstmpl,object deflist=None, defnum = 0):
    """
   Packs up a Grid Definition Section (Section 3) 
   and adds it to a GRIB2 message.  It is used with routines "g2_create",
   "g2_addfield" and "g2_gribend" to create a complete GRIB2 message.
   g2_create must be called first to initialize a new GRIB2 message.

   gribmsg, ierr = grib2_addgrid(gribmsg,gds,gdstmpl,ideflist)
 
   INPUT ARGUMENTS:
     cgrib    - Char array that contains the GRIB2 message to which
                section should be added.
     gds      - Contains information needed for GRIB Grid Definition Section 3
                Must be dimensioned >= 5.
                gds[0]=Source of grid definition (see Code Table 3.0)
                gds[1]=Number of grid points in the defined grid.
                gds[2]=Number of octets needed for each 
                            additional grid points definition.  
                            Used to define number of
                            points in each row ( or column ) for
                            non-regular grids.  
                            = 0, if using regular grid.
                gds[3]=Interpretation of list for optional points 
                            definition.  (Code Table 3.11)
                gds[4]=Grid Definition Template Number (Code Table 3.1)
     gdstmpl  - Contains the data values for the specified Grid Definition
                Template ( NN=gds[4] ).  Each element of this integer 
                array contains an entry (in the order specified) of Grid
                Defintion Template 3.NN
     deflist  - (Used if gds[2] != 0)  This array contains the
                number of grid points contained in each row ( or column )

   OUTPUT ARGUMENTS:      
     cgrib    - Char array to contain the updated GRIB2 message.
                Must be allocated large enough to store the entire
                GRIB2 message.

   RETURN VALUES:
     ierr     - Return code.
              > 0 = Current size of updated GRIB2 message
               -1 = GRIB message was not initialized.  Need to call
                    routine gribcreate first.
               -2 = GRIB message already complete.  Cannot add new section.
               -3 = Sum of Section byte counts doesn't add to total byte count
               -4 = Previous Section was not 1, 2 or 7.
               -5 = Could not find requested Grid Definition Template.
    """
    cdef g2int ierr, idefnum
    cdef g2int *igds
    cdef g2int *igdstmpl
    cdef g2int *ideflist
    cdef unsigned char  *cgrib
    cdef void *gdsdat
    cdef void *deflistdat
    cdef void *gdstmpldat
    cdef Py_ssize_t buflen
    PyObject_AsReadBuffer(gds, &gdsdat, &buflen) 
    PyObject_AsReadBuffer(gdstmpl, &gdstmpldat, &buflen) 
    igds = <g2int *>gdsdat
    igdstmpl = <g2int *>gdstmpldat
    if igds[2] != 0:
       PyObject_AsReadBuffer(deflist, &deflistdat, &buflen) 
       ideflist = <g2int *>deflistdat
       idefnum = <g2int>PyLong_AsLong(len(deflist))
    else:
       ideflist = NULL
       idefnum = 0
    gribmsg = gribmsg + 4*(256+4+gds[2]+1)*b" "
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    ierr = g2_addgrid(cgrib, igds, igdstmpl, ideflist, idefnum)
    if ierr < 0:
       msg = "error in grib2_addgrid, error code = %i" % ierr
       raise RuntimeError(msg)
    gribmsg = PyBytes_FromStringAndSize(<char *>cgrib, ierr)
    return gribmsg, ierr


def grib2_addfield(gribmsg,pdsnum,object pdstmpl,object coordlist,
                   drsnum,object drstmpl,object field,
                   ibitmap,object bitmap):
    """
   Packs up Sections 4 through 7 for a given field
   and adds them to a GRIB2 message.  They are Product Definition Section,
   Data Representation Section, Bit-Map Section and Data Section, respectively.
   This routine is used with routines "g2_create",  
   "g2_addgrid", and "g2_gribend" to create a complete GRIB2 message.  
   g2_create must be called first to initialize a new GRIB2 message.
   g2_addgrid must be called after g2_create and
   before this routine to add the appropriate grid description to
   the GRIB2 message. A call to g2_gribend is required to complete 
   GRIB2 message after all fields have been added.

   gribmsg, ierr = g2_addfield(gribmsg,pdsnum,pdstmpl,coordlist,drsnum,
                               drstmpl,fld,ngrdpts,ibmap,bmap)
   INPUT ARGUMENT LIST:
     cgrib    - Char array that contains the GRIB2 message to which sections
                4 through 7 should be added.
     pdsnum   - Product Definition Template Number ( see Code Table 4.0)
     pdstmpl  - numpy array with data values for the specified Product Definition
                Template ( N=pdsnum ).  Each element of this integer 
                array contains an entry (in the order specified) of Product
                Defintion Template 4.N
     coordlist- numpy array (float32) containg floating point values intended to
                document the vertical discretisation associated to model data
                on hybrid coordinate vertical levels.
     numcoord - number of values in array coordlist.
     drsnum   - Data Representation Template Number ( see Code Table 5.0 )
     drstmpl  - numpy array with data values for the specified Data Representation
                Template ( N=drsnum ).  Each element of this integer 
                array contains an entry (in the order specified) of Data
                Representation Template 5.N
                Note that some values in this template (eg. reference
                values, number of bits, etc...) may be changed by the
                data packing algorithms.
                Use this to specify scaling factors and order of
                spatial differencing, if desired.
     field    - numpy array (float32) of data points to pack.
     ngrdpts  - Number of data points in grid.
                i.e.  size of fld and bmap.
     ibitmap  - Bitmap indicator ( see Code Table 6.0 )
                0 = bitmap applies and is included in Section 6.
                1-253 = Predefined bitmap applies
                254 = Previously defined bitmap applies to this field
                255 = Bit map does not apply to this product.
     bitmap   - numpy array (int32) containing bitmap to be added. 
                (if ibitmap=0 or 254)

   OUTPUT ARGUMENT LIST:      
     gribmsg  - String with the updated GRIB2 message.
                Must be allocated large enough to store the entire
                GRIB2 message.

   RETURN VALUES:
     ierr     - Return code.
              > 0 = Current size of updated GRIB2 message
               -1 = GRIB message was not initialized.  Need to call
                    routine gribcreate first.
               -2 = GRIB message already complete.  Cannot add new section.
               -3 = Sum of Section byte counts doesn't add to total byte count
               -4 = Previous Section was not 3 or 7.
               -5 = Could not find requested Product Definition Template.
               -6 = Section 3 (GDS) not previously defined in message
               -7 = Tried to use unsupported Data Representationi Template
               -8 = Specified use of a previously defined bitmap, but one
                    does not exist in the GRIB message.
               -9 = GDT of one of 5.50 through 5.53 required to pack field 
                    using DRT 5.51.
              -10 = Error packing data field. 
    """
    cdef g2int ierr,ipdsnum,numcoord,idrsnum
    cdef g2int *ipdstmpl
    cdef g2int *idrstmpl
    cdef g2float *fld
    cdef g2float *fcoordlist
    cdef g2int *bmap
    cdef g2int ngrdpts, ibmap
    cdef void *pdtmpldat
    cdef void *drtmpldat
    cdef void *coordlistdat
    cdef void *fielddat
    cdef void *bitmapdat
    cdef Py_ssize_t buflen
    cdef unsigned char *cgrib
    ipdsnum = <g2int>PyLong_AsLong(pdsnum)
    PyObject_AsReadBuffer(pdstmpl, &pdtmpldat, &buflen) 
    ipdstmpl = <g2int *>pdtmpldat
    idrsnum = <g2int>PyLong_AsLong(drsnum)
    PyObject_AsReadBuffer(drstmpl, &drtmpldat, &buflen) 
    idrstmpl = <g2int *>drtmpldat
    if coordlist is not None:
        PyObject_AsReadBuffer(coordlist, &coordlistdat, &buflen) 
        fcoordlist = <g2float *>coordlistdat
        numcoord = len(coordlist)
    else:
        fcoordlist = NULL
        numcoord = 0
    PyObject_AsReadBuffer(field, &fielddat, &buflen) 
    fld = <g2float *>fielddat
    ibmap = <g2int>PyLong_AsLong(ibitmap)
    ngrdpts = len(field)
    if ibitmap == 0 or ibitmap == 254:
        PyObject_AsReadBuffer(bitmap, &bitmapdat, &buflen) 
        bmap  = <g2int *>bitmapdat
    else:
        bmap = NULL
    gribmsg = gribmsg + 4*(len(drstmpl)+ngrdpts+4)*b" "
    cgrib = <unsigned char *>PyBytes_AsString(gribmsg)
    ierr = g2_addfield(cgrib,ipdsnum,ipdstmpl,fcoordlist,numcoord,idrsnum,idrstmpl,fld,ngrdpts,ibmap,bmap)
    if ierr < 0:
       msg = "error in grib2_addfield, error code = %i" % ierr
       raise RuntimeError(msg)
    gribmsg = PyBytes_FromStringAndSize(<char *>cgrib, ierr)
    return gribmsg, ierr
