"""
Miscellaneous utilities.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/projects/Pyro
"""

import sys, zlib, logging
import traceback, linecache
import Pyro4

log=logging.getLogger("Pyro.util")


def getPyroTraceback(ex_type=None, ex_value=None, ex_tb=None):
    """Returns a list of strings that form the traceback information of a
    Pyro exception. Any remote Pyro exception information is included.
    Traceback information is automatically obtained via ``sys.exc_info()`` if
    you do not supply the objects yourself."""
    def formatRemoteTraceback(remote_tb_lines):
        result=[" +--- This exception occured remotely (Pyro) - Remote traceback:"]
        for line in remote_tb_lines:
            if line.endswith("\n"):
                line=line[:-1]
            lines = line.split("\n")
            for line in lines:
                result.append("\n | ")
                result.append(line)
        result.append("\n +--- End of remote traceback\n")
        return result
    try:
        if ex_type is not None and ex_value is None and ex_tb is None:
            # possible old (3.x) call syntax where caller is only providing exception object
            if type(ex_type) is not type:
                raise TypeError("invalid argument: ex_type should be an exception type, or just supply no arguments at all")
        if ex_type is None and ex_tb is None:
            ex_type, ex_value, ex_tb=sys.exc_info()

        remote_tb=getattr(ex_value, "_pyroTraceback", None)
        local_tb=formatTraceback(ex_type, ex_value, ex_tb, Pyro4.config.DETAILED_TRACEBACK)
        if remote_tb:
            remote_tb=formatRemoteTraceback(remote_tb)
            return local_tb + remote_tb
        else:
            # hmm. no remote tb info, return just the local tb.
            return local_tb
    finally:
        # clean up cycle to traceback, to allow proper GC
        del ex_type, ex_value, ex_tb


def formatTraceback(ex_type=None, ex_value=None, ex_tb=None, detailed=False):
    """Formats an exception traceback. If you ask for detailed formatting,
    the result will contain info on the variables in each stack frame.
    You don't have to provide the exception info objects, if you omit them,
    this function will obtain them itself using ``sys.exc_info()``."""
    if ex_type is not None and ex_value is None and ex_tb is None:
        # possible old (3.x) call syntax where caller is only providing exception object
        if type(ex_type) is not type:
            raise TypeError("invalid argument: ex_type should be an exception type, or just supply no arguments at all")
    if ex_type is None and ex_tb is None:
        ex_type, ex_value, ex_tb=sys.exc_info()
    if detailed and sys.platform!="cli":    # detailed tracebacks don't work in ironpython (most of the local vars are omitted)
        def makeStrValue(value):
            try:
                return repr(value)
            except:
                try:
                    return str(value)
                except:
                    return "<ERROR>"
        try:
            result=["-"*52+"\n"]
            result.append(" EXCEPTION %s: %s\n" % (ex_type,ex_value))
            result.append(" Extended stacktrace follows (most recent call last)\n")
            skipLocals=True  # don't print the locals of the very first stackframe
            while ex_tb:
                frame=ex_tb.tb_frame
                sourceFileName=frame.f_code.co_filename
                if "self" in frame.f_locals:
                    location="%s.%s" % (frame.f_locals["self"].__class__.__name__, frame.f_code.co_name)
                else:
                    location=frame.f_code.co_name
                result.append("-"*52+"\n")
                result.append("File \"%s\", line %d, in %s\n" % (sourceFileName, ex_tb.tb_lineno, location))
                result.append("Source code:\n")
                result.append("    "+linecache.getline(sourceFileName, ex_tb.tb_lineno).strip()+"\n")
                if not skipLocals:
                    names=set()
                    names.update(getattr(frame.f_code,"co_varnames",()))
                    names.update(getattr(frame.f_code,"co_names",()))
                    names.update(getattr(frame.f_code,"co_cellvars",()))
                    names.update(getattr(frame.f_code,"co_freevars",()))
                    result.append("Local values:\n")
                    for name in sorted(names):
                        if name in frame.f_locals:
                            value=frame.f_locals[name]
                            result.append("    %s = %s\n" % (name,makeStrValue(value)))
                            if name=="self":
                                # print the local variables of the class instance
                                for name,value in vars(value).items():
                                    result.append("        self.%s = %s\n" % (name,makeStrValue(value)))
                skipLocals=False
                ex_tb=ex_tb.tb_next
            result.append("-"*52+"\n")
            result.append(" EXCEPTION %s: %s\n" % (ex_type, ex_value))
            result.append("-"*52+"\n")
            return result
        except Exception:
            return ["-"*52+"\nError building extended traceback!!! :\n",
                  "".join(traceback.format_exception(*sys.exc_info())) + '-'*52 + '\n',
                  "Original Exception follows:\n",
                  "".join(traceback.format_exception(ex_type, ex_value, ex_tb))]
    else:
        # default traceback format.
        return traceback.format_exception(ex_type, ex_value, ex_tb)


class Serializer(object):
    """
    A (de)serializer that wraps a certain serialization protocol.
    Currently it only supports the standard pickle protocol.
    It can optionally compress the serialized data, and is thread safe.
    """
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    if pickle.HIGHEST_PROTOCOL<2:
        raise RuntimeError("pickle serializer needs to support protocol 2 or higher")

    def serialize(self, data, compress=False):
        """Serialize the given data object, try to compress if told so.
        Returns a tuple of the serialized data and a bool indicating if it is compressed or not."""
        data=self.pickle.dumps(data, 2)
        if not compress or len(data)<200:
            return data, False  # don't waste time compressing small messages
        compressed=zlib.compress(data)
        if len(compressed)<len(data):
            return compressed, True
        return data, False

    def deserialize(self, data, compressed=False):
        """Deserializes the given data. Set compressed to True to decompress the data first."""
        if compressed:
            data=zlib.decompress(data)
        return self.pickle.loads(data)

    def __eq__(self, other):
        """this equality method is only to support the unit tests of this class"""
        return type(other) is Serializer and vars(self)==vars(other)
    __hash__=object.__hash__


def resolveDottedAttribute(obj, attr, allowDotted):
    """
    Resolves a dotted attribute name to an object.  Raises
    an AttributeError if any attribute in the chain starts with a '``_``'.
    If the optional allowDotted argument is false, dots are not
    supported and this function operates similar to ``getattr(obj, attr)``.
    """
    if allowDotted:
        attrs = attr.split('.')
        for i in attrs:
            if i.startswith('_'):
                raise AttributeError('attempt to access private attribute "%s"' % i)
            else:
                obj = getattr(obj, i)
        return obj
    else:
        return getattr(obj, attr)


def excepthook(ex_type, ex_value, ex_tb):
    """An exception hook you can use for ``sys.excepthook``, to automatically print remote Pyro tracebacks"""
    traceback="".join(getPyroTraceback(ex_type, ex_value, ex_tb))
    sys.stderr.write(traceback)
