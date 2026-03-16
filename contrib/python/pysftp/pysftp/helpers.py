"""functions and classes that support the Connection object"""

from contextlib import contextmanager
import ntpath
import posixpath
import os
from stat import S_IMODE, S_ISDIR, S_ISREG


def known_hosts():
    '''return a proper path to ssh's known_host file for the user'''
    return os.path.expanduser(os.path.join('~', '.ssh', 'known_hosts'))


def st_mode_to_int(val):
    '''SFTAttributes st_mode returns an stat type that shows more than what
    can be set.  Trim off those bits and convert to an int representation.
    if you want an object that was `chmod 711` to return a value of 711, use
    this function

    :param int val: the value of an st_mode attr returned by SFTPAttributes

    :returns int: integer representation of octal mode

    '''
    return int(str(oct(S_IMODE(val)))[-3:])


class WTCallbacks(object):
    '''an object to house the callbacks, used internally'''
    def __init__(self):
        '''set instance vars'''
        self._flist = []
        self._dlist = []
        self._ulist = []

    def file_cb(self, pathname):
        '''called for regular files, appends pathname to .flist

        :param str pathname: file path
        '''
        self._flist.append(pathname)

    def dir_cb(self, pathname):
        '''called for directories, appends pathname to .dlist

        :param str pathname: directory path
        '''
        self._dlist.append(pathname)

    def unk_cb(self, pathname):
        '''called for unknown file types, appends pathname to .ulist

        :param str pathname: unknown entity path
        '''
        self._ulist.append(pathname)

    @property
    def flist(self):
        '''return a sorted list of files currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._flist)

    @flist.setter
    def flist(self, val):
        '''setter for _flist '''
        self._flist = val

    @property
    def dlist(self):
        '''return a sorted list of directories currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._dlist)

    @dlist.setter
    def dlist(self, val):
        '''setter for _dlist '''
        self._dlist = val

    @property
    def ulist(self):
        '''return a sorted list of unknown entities currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._ulist)

    @ulist.setter
    def ulist(self, val):
        '''setter for _ulist '''
        self._ulist = val


def path_advance(thepath, sep=os.sep):
    '''generator to iterate over a file path forwards

    :param str thepath: the path to navigate forwards
    :param str sep: *Default: os.sep* - the path separator to use

    :returns: (iter)able of strings

    '''
    # handle a direct path
    pre = ''
    if thepath[0] == sep:
        pre = sep
    curpath = ''
    parts = thepath.split(sep)
    if pre:
        if parts[0]:
            parts[0] = pre + parts[0]
        else:
            parts[1] = pre + parts[1]
    for part in parts:
        curpath = os.path.join(curpath, part)
        if curpath:
            yield curpath


def path_retreat(thepath, sep=os.sep):
    '''generator to iterate over a file path in reverse

    :param str thepath: the path to retreat over
    :param str sep: *Default: os.sep* - the path separator to use

    :returns: (iter)able of strings

    '''
    pre = ''
    if thepath[0] == sep:
        pre = sep
    parts = thepath.split(sep)
    while parts:
        if os.path.join(*parts):
            yield '%s%s' % (pre, os.path.join(*parts))
        parts = parts[:-1]


def reparent(newparent, oldpath):
    '''when copying or moving a directory structure, you need to re-parent the
    oldpath.  When using os.path.join to calculate this new path, the
    appearance of a / root path at the beginning of oldpath, supplants the
    newparent and we don't want this to happen, so we need to make the oldpath
    root appear as a child of the newparent.

    :param: str newparent: the new parent location for oldpath (target)
    :param str oldpath: the path being adopted by newparent (source)

    :returns: (str) resulting adoptive path
    '''

    if oldpath[0] in (posixpath.sep, ntpath.sep):
        oldpath = '.' + oldpath
    return os.path.join(newparent, oldpath)


def walktree(localpath, fcallback, dcallback, ucallback, recurse=True):
    '''on the local file system, recursively descend, depth first, the
    directory tree rooted at localpath, calling discreet callback functions
    for each regular file, directory and unknown file type.

    :param str localpath:
        root of remote directory to descend, use '.' to start at
        :attr:`.pwd`
    :param callable fcallback:
        callback function to invoke for a regular file.
        (form: ``func(str)``)
    :param callable dcallback:
        callback function to invoke for a directory. (form: ``func(str)``)
    :param callable ucallback:
        callback function to invoke for an unknown file type.
        (form: ``func(str)``)
    :param bool recurse: *Default: True* -  should it recurse

    :returns: None

    :raises: OSError, if localpath doesn't exist

    '''

    for entry in os.listdir(localpath):
        pathname = os.path.join(localpath, entry)
        mode = os.stat(pathname).st_mode
        if S_ISDIR(mode):
            # It's a directory, call the dcallback function
            dcallback(pathname)
            if recurse:
                # now, recurse into it
                walktree(pathname, fcallback, dcallback, ucallback)
        elif S_ISREG(mode):
            # It's a file, call the fcallback function
            fcallback(pathname)
        else:
            # Unknown file type
            ucallback(pathname)


@contextmanager
def cd(localpath=None):     # pylint:disable=c0103
    """context manager that can change to a optionally specified local
    directory and restores the old pwd on exit.

    :param str|None localpath: *Default: None* -
        local path to temporarily make the current directory
    :returns: None
    :raises: OSError, if local path doesn't exist
    """
    try:
        original_path = os.getcwd()
        if localpath is not None:
            os.chdir(localpath)
        yield
    finally:
        os.chdir(original_path)
