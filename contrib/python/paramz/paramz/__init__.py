#===============================================================================
# Copyright (c) 2012 - 2014, GPy authors (see AUTHORS.txt).
# Copyright (c) 2015, Max Zwiessele
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of paramax nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

from . import util
from .model import Model
from .parameterized import Parameterized
from .param import Param
from .core.observable_array import ObsAr
from paramz import transformations as constraints
from . import caching, optimization
from . import examples

from .__version__ import __version__

def _unpickle(file_or_path, pickle, strcl, p3kw):
    if isinstance(file_or_path, strcl):
        with open(file_or_path, 'rb') as f:
            m = pickle.load(f, **p3kw)
    else:
        m = pickle.load(file_or_path, **p3kw)
    return m

def load(file_or_path):
    """
    Load a previously pickled model, using `m.pickle('path/to/file.pickle)'`

    :param file_name: path/to/file.pickle
    """
    from pickle import UnpicklingError
    _python3 = True
    try: 
        import cPickle as pickle
        _python3 = False
    except ImportError: #python3
        import pickle
   
    try:
        if _python3:
            strcl = str
            p3kw = dict(encoding='latin1')
            return _unpickle(file_or_path, pickle, strcl, p3kw)
        else:
            strcl = basestring
            p3kw = {}
            return _unpickle(file_or_path, pickle, strcl, p3kw)
    
    except UnpicklingError: # pragma: no coverage
        import pickle
        return _unpickle(file_or_path, pickle, strcl, p3kw)