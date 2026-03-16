from array import array


def _copytobuffer_return_scalar(x):
    try:
        # inx,isfloat,islist,istuple
        return array("d", (float(x),)), True, False, False
    except Exception:
        raise TypeError("input must be an array, list, tuple or scalar")


def _copytobuffer(x):
    """
    return a copy of x as an object that supports the python Buffer
    API (python array if input is float, list or tuple, numpy array
    if input is a numpy array). returns copyofx, isfloat, islist,
    istuple (islist is True if input is a list, istuple is true if
    input is a tuple, isfloat is true if input is a float).
    """
    # make sure x supports Buffer API and contains doubles.
    isfloat = False
    islist = False
    istuple = False
    # first, if it's a numpy array scalar convert to float
    # (array scalars don't support buffer API)
    if hasattr(x, "shape"):
        if x.shape == ():
            return _copytobuffer_return_scalar(x)
        else:
            try:
                # typecast numpy arrays to double.
                # (this makes a copy - which is crucial
                #  since buffer is modified in place)
                x.dtype.char
                # Basemap issue
                # https://github.com/matplotlib/basemap/pull/223/files
                # (deal with input array in fortran order)
                inx = x.copy(order="C").astype("d")
                # inx,isfloat,islist,istuple
                return inx, False, False, False
            except Exception:
                try:  # perhaps they are Numeric/numarrays?
                    # sorry, not tested yet.
                    # i don't know Numeric/numarrays has `shape'.
                    x.typecode()
                    inx = x.astype("d")
                    # inx,isfloat,islist,istuple
                    return inx, False, False, False
                except Exception:
                    raise TypeError("input must be an array, list, tuple or scalar")
    else:
        # perhaps they are regular python arrays?
        if hasattr(x, "typecode"):
            # x.typecode
            inx = array("d", x)
        # try to convert to python array
        # a list.
        elif type(x) == list:
            inx = array("d", x)
            islist = True
        # a tuple.
        elif type(x) == tuple:
            inx = array("d", x)
            istuple = True
        # a scalar?
        else:
            return _copytobuffer_return_scalar(x)
    return inx, isfloat, islist, istuple


def _convertback(isfloat, islist, istuple, inx):
    # if inputs were lists, tuples or floats, convert back to original type.
    if isfloat:
        return inx[0]
    elif islist:
        return inx.tolist()
    elif istuple:
        return tuple(inx)
    else:
        return inx
