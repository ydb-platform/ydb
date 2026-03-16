# Kinds

K_DIR = 'dir'
K_FILE = 'file'

# Repository location types

LT_URL = 'url'
LT_PATH = 'path'

# Status types
#
# http://svn.apache.org/viewvc/subversion/trunk/subversion/svn/schema/status.rnc?view=markup

ST_ADDED       = 1
ST_CONFLICTED  = 2
ST_DELETED     = 3
ST_EXTERNAL    = 4
ST_IGNORED     = 5
ST_INCOMPLETE  = 6
ST_MERGED      = 7
ST_MISSING     = 8
ST_MODIFIED    = 9
ST_NONE        = 10
ST_NORMAL      = 11
ST_OBSTRUCTED  = 12
ST_REPLACED    = 13
ST_UNVERSIONED = 14

STATUS_TYPE_LOOKUP = {
    'added': ST_ADDED,
    'conflicted': ST_CONFLICTED,
    'deleted': ST_DELETED,
    'external': ST_EXTERNAL,
    'ignored': ST_IGNORED,
    'incomplete': ST_INCOMPLETE,
    'merged': ST_MERGED,
    'missing': ST_MISSING,
    'modified': ST_MODIFIED,
    'none': ST_NONE,
    'normal': ST_NORMAL,
    'obstructed': ST_OBSTRUCTED,
    'replaced': ST_REPLACED,
    'unversioned': ST_UNVERSIONED,
}
