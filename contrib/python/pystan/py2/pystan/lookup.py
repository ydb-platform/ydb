#-----------------------------------------------------------------------------
# Copyright (c) 2017, PyStan developers
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

import numpy as np
import re
import pkg_resources
import io

lookuptable = None
stanftable = None

def lookup(name, min_similarity_ratio=.75):
    """
    Look up for a Stan function with similar functionality to a Python
    function (or even an R function, see examples). If the function is
    not present on the lookup table, then attempts to find similar one
    and prints the results. This function requires package `pandas`.

    Parameters
    -----------
    name : str
        Name of the function one wants to look for.
    min_similarity_ratio : float
        In case no exact match is found on the lookup table, the
        function will attempt to find similar names using
        `difflib.SequenceMatcher.ratio()`, and then results with
        calculated ratio below `min_similarity_ratio` will be discarded.

    Examples
    ---------
    #Look up for a Stan function similar to scipy.stats.skewnorm
    lookup("scipy.stats.skewnorm")
    #Look up for a Stan function similar to R dnorm
    lookup("R.dnorm")
    #Look up for a Stan function similar to numpy.hstack
    lookup("numpy.hstack")
    #List Stan log probability mass functions
    lookup("lpmfs")
    #List Stan log cumulative density functions
    lookup("lcdfs")

    Returns
    ---------
    A pandas.core.frame.DataFrame if exact or at least one similar
    result is found, None otherwise.
    """
    if lookuptable is None:
        build()
    if name not in lookuptable.keys():
        from difflib import SequenceMatcher
        from operator import itemgetter
        print("No match for " + name + " in the lookup table.")

        lkt_keys = list(lookuptable.keys())
        mapfunction = lambda x: SequenceMatcher(a=name, b=x).ratio()
        similars = list(map(mapfunction, lkt_keys))
        similars = zip(range(len(similars)), similars)
        similars = list(filter(lambda x: x[1] >= min_similarity_ratio,
                               similars))
        similars = sorted(similars, key=itemgetter(1))

        if (len(similars)):
            print("But the following similar entries were found: ")
            for i in range(len(similars)):
                print(lkt_keys[similars[i][0]] + " ===> with similary "
                      "ratio of " + str(round(similars[i][1], 3)) + "")
            print("Will return results for entry"
                  " " + lkt_keys[similars[i][0]] + " "
                  "(which is the most similar entry found).")
            return lookup(lkt_keys[similars[i][0]])
        else:
            print("And no similar entry found. You may try to decrease"
                  "the min_similarity_ratio parameter.")
        return
    entries = stanftable[lookuptable[name]]
    if not len(entries):
        return "Found no equivalent Stan function available for " + name

    try:
        import pandas as pd
    except ImportError:
        raise ImportError('Package pandas is require to use this '
                          'function.')

    return pd.DataFrame(entries)



def build():
    def load_table_file(fname):
        fname = "lookuptable/" + fname
        fbytes = pkg_resources.resource_string(__name__, fname)
        return io.BytesIO(fbytes)
    stanfunctions_file = load_table_file("stan-functions.txt")
    rfunctions_file = load_table_file("R.txt")
    pythontb_file = load_table_file("python.txt")

    stanftb = np.genfromtxt(stanfunctions_file, delimiter=';',
                            names=True, skip_header=True,
                            dtype=['<U200','<U200','<U200' ,"int"])
    rpl_textbar = np.vectorize(lambda x: x.replace("\\textbar \\", "|"))
    stanftb['Arguments'] = rpl_textbar(stanftb['Arguments'])

    StanFunction = stanftb["StanFunction"]

    #Auto-extract R functions
    rmatches = [re.findall(r'('
                           '(?<=RFunction\[StanFunction == \").+?(?=\")'
                           '|(?<=grepl\(").+?(?=", StanFunction\))'
                           '|(?<= \<\- ").+?(?="\)))'
                           '|NA\_character\_', l.decode("utf-8"))
                for l in rfunctions_file]
    tomatch = list(filter(lambda x: len(x) == 2, rmatches))
    tomatch = np.array(tomatch, dtype=str)
    tomatch[:, 1] = np.vectorize(lambda x: "R." + x)(tomatch[:,1])

    #Get packages lookup table for Python packages
    pymatches = np.genfromtxt(pythontb_file, delimiter='; ', dtype=str)
    tomatch = np.vstack((tomatch, pymatches))

    lookuptb = dict()
    for i in range(tomatch.shape[0]):
        matchedlines = np.vectorize(lambda x: re.match(tomatch[i, 0],
                                    x))(StanFunction)
        lookuptb[tomatch[i, 1]] = np.where(matchedlines)[0]

    #debug: list of rmatches that got wrong
    #print(list(filter(lambda x: len(x) != 2 and len(x) != 0,
    #                  rmatches)))

    #debug: list of nodes without matches on lookup table
    #for k in lookuptb:
    #    if len(lookuptb[k]) == 0:
    #        print(k)
    global lookuptable
    global stanftable

    stanftable = stanftb
    lookuptable = lookuptb
