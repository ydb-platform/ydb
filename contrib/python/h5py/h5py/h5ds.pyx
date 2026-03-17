# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Low-level HDF5 "H5DS" Dimension Scale interface.
"""

# Compile-time imports
from .h5d cimport DatasetID
from .utils cimport emalloc, efree

from ._objects import phil, with_phil

@with_phil
def set_scale(DatasetID dset not None, char* dimname=''):
    """(DatasetID dset, STRING dimname)

    Convert dataset dset to a dimension scale, with optional name dimname.
    """
    H5DSset_scale(dset.id, dimname)

@with_phil
def is_scale(DatasetID dset not None):
    """(DatasetID dset) => BOOL

    Determines whether dset is a dimension scale.
    """
    return <bint>(H5DSis_scale(dset.id))

@with_phil
def attach_scale(DatasetID dset not None, DatasetID dscale not None, unsigned
                 int idx):
    """(DatasetID dset, DatasetID dscale, UINT idx)

    Attach Dimension Scale dscale to Dimension idx of Dataset dset.
    """
    H5DSattach_scale(dset.id, dscale.id, idx)

@with_phil
def is_attached(DatasetID dset not None, DatasetID dscale not None,
                unsigned int idx):
    """(DatasetID dset, DatasetID dscale, UINT idx) => BOOL

    Report if Dimension Scale dscale is currently attached to Dimension
    idx of Dataset dset.
    """
    return <bint>(H5DSis_attached(dset.id, dscale.id, idx))

@with_phil
def detach_scale(DatasetID dset not None, DatasetID dscale not None,
                 unsigned int idx):
    """(DatasetID dset, DatasetID dscale, UINT idx)

    Detach Dimension Scale dscale from the Dimension idx of Dataset dset.
    """
    H5DSdetach_scale(dset.id, dscale.id, idx)

@with_phil
def get_num_scales(DatasetID dset not None, unsigned int dim):
    """(DatasetID dset, UINT dim) => INT number_of_scales

    Determines how many Dimension Scales are attached to Dimension dim
    of Dataset dset.
    """
    return H5DSget_num_scales(dset.id, dim)

@with_phil
def set_label(DatasetID dset not None, unsigned int idx, char* label):
    """(DatasetID dset, UINT idx, STRING label)

    Set label for the Dimension idx of Dataset dset to the value label.
    """
    H5DSset_label(dset.id, idx, label)

@with_phil
def get_label(DatasetID dset not None, unsigned int idx):
    """(DatasetID dset, UINT idx) => STRING name_of_label

    Read the label for Dimension idx of Dataset dset into buffer label.
    """
    cdef ssize_t size
    cdef char* label
    label = NULL

    size = H5DSget_label(dset.id, idx, NULL, 0)
    if size <= 0:
        return b''
    label = <char*>emalloc(sizeof(char)*(size+1))
    try:
        H5DSget_label(dset.id, idx, label, size+1)
        plabel = label
        return plabel
    finally:
        efree(label)

@with_phil
def get_scale_name(DatasetID dscale not None):
    """(DatasetID dscale) => STRING name_of_scale

    Retrieves name of Dimension Scale dscale.
    """
    cdef ssize_t namelen
    cdef char* name = NULL

    namelen = H5DSget_scale_name(dscale.id, NULL, 0)
    if namelen <= 0:
        return b''
    name = <char*>emalloc(sizeof(char)*(namelen+1))
    try:
        H5DSget_scale_name(dscale.id, name, namelen+1)
        pname = name
        return pname
    finally:
        efree(name)


cdef class _DimensionScaleVisitor:

    cdef object func
    cdef object retval

    def __init__(self, func):
        self.func = func
        self.retval = None


cdef herr_t cb_ds_iter(hid_t dset, unsigned int dim, hid_t scale, void* vis_in) except 2 with gil:

    cdef _DimensionScaleVisitor vis = <_DimensionScaleVisitor>vis_in

    # we did not retrieve the scale identifier using the normal machinery,
    # so we need to inc_ref it before using it to create a DatasetID.
    H5Iinc_ref(scale)
    vis.retval = vis.func(DatasetID(scale))

    if vis.retval is not None:
        return 1
    return 0

@with_phil
def iterate(DatasetID dset not None, unsigned int dim, object func,
            int startidx=0):
    """ (DatasetID loc, UINT dim, CALLABLE func, UINT startidx=0)
    => Return value from func

    Iterate a callable (function, method or callable object) over the
    members of a group.  Your callable should have the signature::

        func(STRING name) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value. Keywords:
    """
    if startidx < 0:
        raise ValueError("Starting index must be non-negative")

    cdef int i = startidx
    cdef _DimensionScaleVisitor vis = _DimensionScaleVisitor(func)

    H5DSiterate_scales(dset.id, dim, &i, <H5DS_iterate_t>cb_ds_iter, <void*>vis)

    return vis.retval
