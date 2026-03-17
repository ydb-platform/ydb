"""Routines that pretty-print a hierarchy of logging `Node` objects."""

import logging.handlers
import sys

if sys.version_info < (2, 6):
    def next(generator):
        return generator.next()

def printout(node=None):
    """Print a tree of loggers, given a `Node` from `logging_tree.nodes`.

    If no `node` argument is provided, then the entire tree of currently
    active `logging` loggers is printed out.

    """
    print(build_description(node)[:-1])


def build_description(node=None):
    """Return a multi-line string describing a `logging_tree.nodes.Node`.

    If no `node` argument is provided, then the entire tree of currently
    active `logging` loggers is printed out.

    """
    if node is None:
        from logging_tree.nodes import tree
        node = tree()
    return '\n'.join([ line.rstrip() for line in describe(node) ]) + '\n'


def describe(node):
    """Generate lines describing the given `node` tuple.

    The `node` should be a tuple returned by `logging_tree.nodes.tree()`.

    """
    return _describe(node, None)

def _describe(node, parent):
    """Generate lines describing the given `node` tuple.

    This is the recursive back-end that powers ``describe()``.  With its
    extra ``parent`` parameter, this routine remembers the nearest
    non-placeholder ancestor so that it can compare it against the
    actual value of the ``.parent`` attribute of each node.

    """
    name, logger, children = node
    is_placeholder = isinstance(logger, logging.PlaceHolder)
    if is_placeholder:
        yield '<--[%s]' % name
    else:
        parent_is_correct = (parent is None) or (logger.parent is parent)
        if not logger.propagate:
            arrow = '   '
        elif parent_is_correct:
            arrow = '<--'
        else:
            arrow = ' !-'
        yield '%s"%s"' % (arrow, name)
        if not parent_is_correct:
            if logger.parent is None:
                yield ('   Broken .parent is None, so messages stop here')
            else:
                yield ('   Broken .parent redirects messages to %r instead'
                       % (logger.parent.name,))
        if logger.level == logging.NOTSET:
            yield '   Level NOTSET so inherits level ' + logging.getLevelName(
                logger.getEffectiveLevel())
        else:
            yield '   Level ' + logging.getLevelName(logger.level)
        if not logger.propagate:
            yield '   Propagate OFF'
        if logger.disabled:
            yield '   Disabled'

        # In case someone has defined a custom logger that lacks a
        # `filters` or `handlers` attribute, we call getattr() and
        # provide an empty sequence as a fallback.

        for f in getattr(logger, 'filters', ()):
            yield '   Filter %s' % describe_filter(f)
        for h in getattr(logger, 'handlers', ()):
            g = describe_handler(h)
            yield '   Handler %s' % next(g)
            for line in g:
                yield '   ' + line

    if children:
        if not is_placeholder:
            parent = logger
        last_child = children[-1]
        for child in children:
            g = _describe(child, parent)
            yield '   |'
            yield '   o' + next(g)
            if child is last_child:
                prefix = '    '
            else:
                prefix = '   |'
            for line in g:
                yield prefix + line


# The functions below must avoid `isinstance()`, since a Filter or
# Handler subclass might implement behavior that renders our tidy
# description quite useless.


def describe_filter(f):
    """Return text describing the logging filter `f`."""
    if f.__class__ is logging.Filter:  # using type() breaks in Python <= 2.6
        return 'name=%r' % f.name
    return repr(f)


handler_formats = {  # Someday we will switch to .format() when Py2.6 is gone.
    logging.StreamHandler: 'Stream %(stream)r',
    logging.FileHandler: 'File %(baseFilename)r',
    logging.handlers.RotatingFileHandler: 'RotatingFile %(baseFilename)r'
        ' maxBytes=%(maxBytes)r backupCount=%(backupCount)r',
    logging.handlers.SocketHandler: 'Socket %(host)s %(port)r',
    logging.handlers.DatagramHandler: 'Datagram %(host)s %(port)r',
    logging.handlers.SysLogHandler: 'SysLog %(address)r facility=%(facility)r',
    logging.handlers.SMTPHandler: 'SMTP via %(mailhost)s to %(toaddrs)s',
    logging.handlers.HTTPHandler: 'HTTP %(method)s to http://%(host)s/%(url)s',
    logging.handlers.BufferingHandler: 'Buffering capacity=%(capacity)r',
    logging.handlers.MemoryHandler: 'Memory capacity=%(capacity)r',
    }

if sys.version_info >= (2, 5): handler_formats.update({
    logging.handlers.TimedRotatingFileHandler:
        'TimedRotatingFile %(baseFilename)r when=%(when)r'
        ' interval=%(interval)r backupCount=%(backupCount)r',
    })

if sys.version_info >= (2, 6): handler_formats.update({
    logging.handlers.WatchedFileHandler: 'WatchedFile %(baseFilename)r',
    })


def describe_handler(h):
    """Yield one or more lines describing the logging handler `h`."""
    t = h.__class__  # using type() breaks in Python <= 2.6
    format = handler_formats.get(t)
    if format is not None:
        yield format % h.__dict__
    else:
        yield repr(h)
    level = getattr(h, 'level', logging.NOTSET)
    if level != logging.NOTSET:
        yield '  Level ' + logging.getLevelName(level)
    for f in getattr(h, 'filters', ()):
        yield '  Filter %s' % describe_filter(f)
    formatter = getattr(h, 'formatter', None)
    if formatter is not None:
        if class_of(formatter) is logging.Formatter:
            yield '  Formatter fmt=%r datefmt=%r' % (
                getattr(formatter, '_fmt', None),
                getattr(formatter, 'datefmt', None))
        else:
            yield '  Formatter %r' % (formatter,)
    if t is logging.handlers.MemoryHandler and h.target is not None:
        yield '  Flushes output to:'
        g = describe_handler(h.target)
        yield '    Handler ' + next(g)
        for line in g:
            yield '    ' + line


def class_of(obj):
    """Try to learn the class of `obj`.

    We perform the operation gingerly, as `obj` could be any kind of
    user-supplied object: an old-style class, a new-style class, or a
    built-in type that doesn't follow normal rules.

    """
    cls = getattr(obj, '__class__', None)
    if cls is None:
        cls = type(obj)
    return cls
