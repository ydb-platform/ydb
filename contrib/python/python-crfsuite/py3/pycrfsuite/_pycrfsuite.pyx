# cython: embedsignature=True
# cython: c_string_type=str
# cython: c_string_encoding=utf-8
# cython: profile=False
# distutils: language=c++
from . cimport crfsuite_api
from libcpp.string cimport string

import sys
import os
import contextlib
import tempfile

from pycrfsuite import _dumpparser
from pycrfsuite import _logparser

CRFSUITE_VERSION = crfsuite_api.version()


class CRFSuiteError(Exception):

    _messages = {
        crfsuite_api.CRFSUITEERR_UNKNOWN: "Unknown error occurred",
        crfsuite_api.CRFSUITEERR_OUTOFMEMORY: "Insufficient memory",
        crfsuite_api.CRFSUITEERR_NOTSUPPORTED: "Unsupported operation",
        crfsuite_api.CRFSUITEERR_INCOMPATIBLE: "Incompatible data",
        crfsuite_api.CRFSUITEERR_INTERNAL_LOGIC: "Internal error",
        crfsuite_api.CRFSUITEERR_OVERFLOW: "Overflow",
        crfsuite_api.CRFSUITEERR_NOTIMPLEMENTED: "Not implemented",
    }

    def __init__(self, code):
        self.code = code
        Exception.__init__(self._messages.get(self.code, "Unexpected error"))


cdef string _SEP = b':'

cdef extern crfsuite_api.Item to_item(x) except+:
    """ Convert a Python object to an Item. """
    cdef crfsuite_api.Item c_item
    cdef double c_value
    cdef string c_key
    cdef bint is_dict, is_nested_value

    is_dict = isinstance(x, dict)
    c_item = crfsuite_api.Item()
    c_item.reserve(len(x))  # at least this amount is required
    for key in x:
        if isinstance(key, unicode):
            c_key = (<unicode>key).encode('utf8')
        else:
            c_key = key

        if not is_dict:
            # "string_key"
            c_value = 1.0
            c_item.push_back(crfsuite_api.Attribute(c_key, c_value))
        else:
            value = (<dict>x)[key]

            if isinstance(value, (dict, list, set)):
                # {"string_prefix": {...}}
                for attr in to_item(value):
                    c_item.push_back(
                        crfsuite_api.Attribute(c_key + _SEP + attr.attr, attr.value)
                    )
            else:
                if isinstance(value, unicode):
                    # {"string_key": "string_value"}
                    c_key += _SEP
                    c_key += <string>(<unicode>value).encode('utf8')
                    c_value = 1.0
                elif isinstance(value, bytes):
                    # {"string_key": "string_value"}
                    c_key += _SEP
                    c_key += <string>value
                    c_value = 1.0
                else:
                    # {"string_key": float_value}
                    # {"string_key": bool}
                    c_value = value

                c_item.push_back(crfsuite_api.Attribute(c_key, c_value))

    return c_item


cdef extern crfsuite_api.ItemSequence to_seq(pyseq) except+:
    """
    Convert an iterable to an ItemSequence.
    Elements of an iterable could be:

    * {"string_key": float_value} dicts;
    * {"string_key": bool} dicts: True is converted to 1.0, False - to 0.0;
    * {"string_key": "string_value"} dicts: result is {"string_key=string_value": 1.0}
    * "string_key": result is {"string_key": 1.0}
    * {"string_prefix": {...}} nested dicts: nested dict is processed and
      "string_prefix" s prepended to each key.
    * {"string_prefix": [...]} dicts: nested list is processed and
      "string_prefix" s prepended to each key.
    """
    cdef crfsuite_api.ItemSequence c_seq

    if isinstance(pyseq, ItemSequence):
        c_seq = (<ItemSequence>pyseq).c_seq
    else:
        for x in pyseq:
            c_seq.push_back(to_item(x))
    return c_seq


cdef class ItemSequence(object):
    """
    A wrapper for crfsuite ItemSequence - a class for storing
    features for all items in a single sequence.

    Using this class is an alternative to passing data to :class:`~Trainer`
    and :class:`Tagger` directly. By using this class it is possible to
    save some time if the same input sequence is passed to trainers/taggers
    more than once - features won't be processed multiple times.
    It also allows to get "processed" features/attributes that are sent
    to CRFsuite - they could be helpful e.g. to check which attributes
    (returned by :meth:`~Tagger.info`) are active for a given observation.

    Initialize ItemSequence with a list of item features:

    >>> ItemSequence([{'foo': 1, 'bar': 0}, {'foo': 1.5, 'baz': 2}])
    <ItemSequence of size 2>

    Item features could be in one of the following formats:

    * {"string_key": float_weight, ...} dict where keys are
      observed features and values are their weights;
    * {"string_key": bool, ...} dict; True is converted to 1.0 weight,
      False - to 0.0;
    * {"string_key": "string_value", ...} dict; that's the same as
      {"string_key=string_value": 1.0, ...}
    * ["string_key1", "string_key2", ...] list; that's the same as
      {"string_key1": 1.0, "string_key2": 1.0, ...}
    * {"string_prefix": {...}} dicts: nested dict is processed and
      "string_prefix" s prepended to each key.
    * {"string_prefix": [...]} dicts: nested list is processed and
      "string_prefix" s prepended to each key.
    * {"string_prefix": set([...])} dicts: nested list is processed and
      "string_prefix" s prepended to each key.

    Dict-based features can be mixed, i.e. this is allowed::

        {"key1": float_weight,
         "key2": "string_value",
         "key3": bool_value,
         "key4: {"key5": ["x", "y"], "key6": float_value},
         }

    """
    cdef crfsuite_api.ItemSequence c_seq

    def __init__(self, pyseq):
        self.c_seq = to_seq(pyseq)

    def items(self):
        """
        Return a list of prepared item features:
        a list of ``{unicode_key: float_value}`` dicts.

        >>> ItemSequence([["foo"], {"bar": {"baz": 1}}]).items()
        [{'foo': 1.0}, {'bar:baz': 1.0}]

        """
        cdef crfsuite_api.Item c_item
        cdef crfsuite_api.Attribute c_attr
        cdef bytes key
        seq = []

        for c_item in self.c_seq:
            x = {}
            for c_attr in c_item:
                # Always decode keys from utf-8. It means binary keys are
                # not supported. I think it is OK because Tagger.info()
                # also only supports utf-8.

                # XXX: (<bytes>c_attr.attr).decode('utf8') doesn't
                # work properly in Cython 0.21
                key = <bytes>c_attr.attr.c_str()
                x[key.decode('utf8')] = c_attr.value
            seq.append(x)
        return seq

    def __len__(self):
        return self.c_seq.size()

    def __repr__(self):
        return "<ItemSequence of size %d>" % len(self)


def _intbool(txt):
    return bool(int(txt))


cdef class BaseTrainer(object):
    """
    The trainer class.

    This class maintains a data set for training, and provides an interface
    to various training algorithms.

    Parameters
    ----------
    algorithm : {'lbfgs', 'l2sgd', 'ap', 'pa', 'arow'}
        The name of the training algorithm. See :meth:`Trainer.select`.

    params : dict, optional
        Training parameters. See :meth:`Trainer.set_params`
        and :meth:`Trainer.set`.

    verbose : boolean
        Whether to print debug messages during training. Default is True.

    """
    cdef crfsuite_api.Trainer c_trainer

    _PARAMETER_TYPES = {
        'feature.minfreq': float,
        'feature.possible_states': _intbool,
        'feature.possible_transitions': _intbool,
        'c1': float,
        'c2': float,
        'max_iterations': int,
        'num_memories': int,
        'epsilon': float,
        'period': int,  # XXX: is it called 'stop' in docs?
        'delta': float,
        'linesearch': str,
        'max_linesearch': int,
        'calibration.eta': float,
        'calibration.rate': float,
        'calibration.samples': float,
        'calibration.candidates': int,
        'calibration.max_trials': int,
        'type': int,
        'c': float,
        'error_sensitive': _intbool,
        'averaging': _intbool,
        'variance': float,
        'gamma': float,
    }

    _ALGORITHM_ALIASES = {
        'ap': 'averaged-perceptron',
        'pa': 'passive-aggressive',
    }

    cdef public verbose

    def __init__(self, algorithm=None, params=None, verbose=True):
        if algorithm is not None:
            self.select(algorithm)
        if params is not None:
            self.set_params(params)
        self.verbose = verbose

    def __cinit__(self):
        # setup message handler
        self.c_trainer.set_handler(self, <crfsuite_api.messagefunc>self._on_message)

        # fix segfaults, see https://github.com/chokkan/crfsuite/pull/21
        self.c_trainer.select("lbfgs".encode('ascii'), "crf1d".encode('ascii'))
        self.c_trainer._init_hack()

    cdef _on_message(self, string message):
        self.message(message)

    def message(self, message):
        """
        Receive messages from the training algorithm.
        Override this method to receive messages of the training
        process.

        By default, this method prints messages
        if ``Trainer.verbose`` is True.

        Parameters
        ----------
        message : string
            The message
        """
        if self.verbose:
            print(message, end='')

    def append(self, xseq, yseq, int group=0):
        """
        Append an instance (item/label sequence) to the data set.

        Parameters
        ----------
        xseq : a sequence of item features
            The item sequence of the instance. ``xseq`` should be a list
            of item features or an :class:`~ItemSequence` instance.
            Allowed item features formats are the same as described
            in :class:`~ItemSequence` docs.

        yseq : a sequence of strings
            The label sequence of the instance. The number
            of elements in yseq must be identical to that
            in xseq.

        group : int, optional
            The group number of the instance. Group numbers are used to
            select subset of data for heldout evaluation.
        """
        self.c_trainer.append(to_seq(xseq), yseq, group)

    def select(self, algorithm, type='crf1d'):
        """
        Initialize the training algorithm.

        Parameters
        ----------
        algorithm : {'lbfgs', 'l2sgd', 'ap', 'pa', 'arow'}
            The name of the training algorithm.

            * 'lbfgs' for Gradient descent using the L-BFGS method,
            * 'l2sgd' for Stochastic Gradient Descent with L2 regularization term
            * 'ap' for Averaged Perceptron
            * 'pa' for Passive Aggressive
            * 'arow' for Adaptive Regularization Of Weight Vector

        type : string, optional
            The name of the graphical model.
        """
        algorithm = algorithm.lower()
        algorithm = self._ALGORITHM_ALIASES.get(algorithm, algorithm)
        if not self.c_trainer.select(algorithm.encode('ascii'), type.encode('ascii')):
            raise ValueError(
                "Bad arguments: algorithm=%r, type=%r" % (algorithm, type)
            )

    def train(self, model, int holdout=-1):
        """
        Run the training algorithm.
        This function starts the training algorithm with the data set given
        by :meth:`Trainer.append` method.

        Parameters
        ----------
        model : string
            The filename to which the trained model is stored.
            If this value is empty, this function does not
            write out a model file.

        holdout : int, optional
            The group number of holdout evaluation. The
            instances with this group number will not be used
            for training, but for holdout evaluation.
            Default value is -1, meaning "use all instances for training".
        """
        self._before_train()
        status_code = self.c_trainer.train(model, holdout)
        if status_code != crfsuite_api.CRFSUITE_SUCCESS:
            raise CRFSuiteError(status_code)

    def params(self):
        """
        Obtain the list of parameters.

        This function returns the list of parameter names available for the
        graphical model and training algorithm specified in Trainer constructor
        or by :meth:`Trainer.select` method.

        Returns
        -------
        list of strings
            The list of parameters available for the current
            graphical model and training algorithm.

        """
        return self.c_trainer.params()

    def set_params(self, params):
        """
        Set training parameters.

        Parameters
        ----------
        params : dict
            A dict with parameters ``{name: value}``
        """
        for key, value in params.items():
            self.set(key, value)

    def get_params(self):
        """
        Get training parameters.

        Returns
        -------
        dict
            A dictionary with ``{parameter_name: parameter_value}``
            with all trainer parameters.
        """
        # params = self.params()
        return dict((name, self.get(name)) for name in self.params())

    def set(self, name, value):
        """
        Set a training parameter.
        This function sets a parameter value for the graphical model and
        training algorithm specified by :meth:`Trainer.select` method.

        Parameters
        ----------
        name : string
            The parameter name.
        value : string
            The value of the parameter.

        """
        if isinstance(value, bool):
            value = int(value)
        self.c_trainer.set(name, str(value))

    def get(self, name):
        """
        Get the value of a training parameter.
        This function gets a parameter value for the graphical model and
        training algorithm specified by :meth:`Trainer.select` method.

        Parameters
        ----------
        name : string
            The parameter name.
        """
        return self._cast_parameter(name, self.c_trainer.get(name))

    def help(self, name):
        """
        Get the description of a training parameter.
        This function obtains the help message for the parameter specified
        by the name. The graphical model and training algorithm must be
        selected by :meth:`Trainer.select` method before calling this method.

        Parameters
        ----------
        name : string
            The parameter name.

        Returns
        -------
        string
            The description (help message) of the parameter.

        """
        if name not in self.params():
            # c_trainer.help(name) segfaults without this workaround;
            # see https://github.com/chokkan/crfsuite/pull/21
            raise ValueError("Parameter not found: %s" % name)
        return self.c_trainer.help(name)

    def clear(self):
        """ Remove all instances in the data set. """
        self.c_trainer.clear()

    def _cast_parameter(self, name, value):
        if name in self._PARAMETER_TYPES:
            return self._PARAMETER_TYPES[name](value)
        return value

    def _before_train(self):
        pass


class Trainer(BaseTrainer):
    """
    The trainer class.

    This class maintains a data set for training, and provides an interface
    to various training algorithms.

    Parameters
    ----------
    algorithm : {'lbfgs', 'l2sgd', 'ap', 'pa', 'arow'}
        The name of the training algorithm. See :meth:`Trainer.select`.

    params : dict, optional
        Training parameters. See :meth:`Trainer.set_params`
        and :meth:`Trainer.set`.

    verbose : boolean
        Whether to print debug messages during training. Default is True.

    """
    logparser = None

    def _before_train(self):
        self.logparser = _logparser.TrainLogParser()

    def message(self, message):
        event = self.logparser.feed(message)

        if not self.verbose or event is None:
            return

        log = self.logparser.last_log
        if event == 'start':
            self.on_start(log)
        elif event == 'featgen_progress':
            self.on_featgen_progress(log, self.logparser.featgen_percent)
        elif event == 'featgen_end':
            self.on_featgen_end(log)
        elif event == 'prepared':
            self.on_prepared(log)
        elif event == 'prepare_error':
            self.on_prepare_error(log)
        elif event == 'iteration':
            self.on_iteration(log, self.logparser.last_iteration)
        elif event == 'optimization_end':
            self.on_optimization_end(log)
        elif event == 'end':
            self.on_end(log)
        else:
            raise Exception("Unknown event %r" % event)

    def on_start(self, log):
        print(log, end='')

    def on_featgen_progress(self, log, percent):
        print(log, end='')

    def on_featgen_end(self, log):
        print(log, end='')

    def on_prepared(self, log):
        print(log, end='')

    def on_prepare_error(self, log):
        print(log, end='')

    def on_iteration(self, log, info):
        print(log, end='')

    def on_optimization_end(self, log):
        print(log, end='')

    def on_end(self, log):
        print(log, end='')


cdef class Tagger(object):
    """
    The tagger class.

    This class provides the functionality for predicting label sequences for
    input sequences using a model.
    """
    cdef crfsuite_api.Tagger c_tagger

    def open(self, name):
        """
        Open a model file.

        Parameters
        ----------
        name : string
            The file name of the model file.

        """
        # We need to do some basic checks ourselves because crfsuite
        # may segfault if the file is invalid.
        # See https://github.com/chokkan/crfsuite/pull/24
        self._check_model(name)
        if not self.c_tagger.open(name):
            raise ValueError("Error opening model file %r" % name)
        return contextlib.closing(self)

    def open_inmemory(self, bytes value):
        """
        Open a model from memory.

        Parameters
        ----------
        value : bytes
            Binary model data (content of a file saved by Trainer.train).

        """
        self._check_inmemory_model(value)
        cdef const char *v = value
        if not self.c_tagger.open(v, len(value)):
            raise ValueError("Error opening model")
        return contextlib.closing(self)

    def close(self):
        """
        Close the model.
        """
        self.c_tagger.close()

    def labels(self):
        """
        Obtain the list of labels.

        Returns
        -------
        list of strings
            The list of labels in the model.
        """
        return self.c_tagger.labels()

    def tag(self, xseq=None):
        """
        Predict the label sequence for the item sequence.

        Parameters
        ----------
        xseq : item sequence, optional
            The item sequence. If omitted, the current sequence is used
            (a sequence set using :meth:`Tagger.set` method or
            a sequence used in a previous :meth:`Tagger.tag` call).

            ``xseq`` should be a list of item features or
            an :class:`~ItemSequence` instance. Allowed item features formats
            are the same as described in :class:`~ItemSequence` docs.

        Returns
        -------
        list of strings
            The label sequence predicted.
        """
        if xseq is not None:
            self.set(xseq)

        return self.c_tagger.viterbi()

    def probability(self, yseq):
        """
        Compute the probability of the label sequence for the current input
        sequence (a sequence set using :meth:`Tagger.set` method or
        a sequence used in a previous :meth:`Tagger.tag` call).

        Parameters
        ----------
        yseq : list of strings
            The label sequence.

        Returns
        -------
        float
            The probability ``P(yseq|xseq)``.
        """
        return self.c_tagger.probability(yseq)

    def marginal(self, y, pos):
        """
        Compute the marginal probability of the label ``y`` at position ``pos``
        for the current input sequence (i.e. a sequence set using
        :meth:`Tagger.set` method or a sequence used in a previous
        :meth:`Tagger.tag` call).

        Parameters
        ----------
        y : string
            The label.
        t : int
            The position of the label.

        Returns
        -------
        float
            The marginal probability of the label ``y`` at position ``t``.
        """
        return self.c_tagger.marginal(y, pos)

    cpdef extern set(self, xseq) except +:
        """
        Set an instance (item sequence) for future calls of
        :meth:`Tagger.tag`, :meth:`Tagger.probability`
        and :meth:`Tagger.marginal` methods.

        Parameters
        ----------
        xseq : item sequence
            The item sequence of the instance. ``xseq`` should be a list of
            item features or an :class:`~ItemSequence` instance.
            Allowed item features formats are the same as described
            in :class:`~ItemSequence` docs.

        """
        self.c_tagger.set(to_seq(xseq))

    def dump(self, filename=None):
        """
        Dump a CRF model in plain-text format.

        Parameters
        ----------
        filename : string, optional
            File name to dump the model to.
            If None, the model is dumped to stdout.
        """
        if filename is None:
            self.c_tagger.dump(os.dup(sys.stdout.fileno()))
        else:
            fd = os.open(filename, os.O_CREAT | os.O_WRONLY)
            try:
                self.c_tagger.dump(fd)
            finally:
                try:
                    os.close(fd)
                except OSError:
                    pass  # already closed by Tagger::dump

    def info(self):
        """
        Return a :class:`~.ParsedDump` structure with model internal information.
        """
        parser = _dumpparser.CRFsuiteDumpParser()
        fd, name = tempfile.mkstemp()
        try:
            self.c_tagger.dump(fd)
            with open(name, 'rb') as f:
                for line in f:
                    parser.feed(line.decode('utf8'))
        finally:
            try:
                os.unlink(name)
            except OSError:
                pass
        return parser.result

    def _check_model(self, name):
        # See https://github.com/chokkan/crfsuite/pull/24
        # 1. Check that the file can be opened.
        with open(name, 'rb') as f:

            # 2. Check that file magic is correct.
            magic = f.read(4)
            if magic != b'lCRF':
                raise ValueError("Invalid model file %r" % name)

            # 3. Make sure crfsuite won't read past allocated memory
            # in case of incomplete header.
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 48:  # header size
                raise ValueError("Model file %r doesn't have a complete header" % name)

    def _check_inmemory_model(self, bytes value):
        magic = value[:4]
        if magic != b'lCRF':
            raise ValueError("Invalid model")

        if len(value) < 48:
            raise ValueError("Invalid model: incomplete header")
