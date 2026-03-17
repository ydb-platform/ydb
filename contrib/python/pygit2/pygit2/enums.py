# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

from enum import IntEnum, IntFlag

from . import _pygit2, options
from .ffi import C


class ApplyLocation(IntEnum):
    """Possible application locations for patches"""

    WORKDIR = _pygit2.GIT_APPLY_LOCATION_WORKDIR
    """
    Apply the patch to the workdir, leaving the index untouched.
    This is the equivalent of `git apply` with no location argument.
    """

    INDEX = _pygit2.GIT_APPLY_LOCATION_INDEX
    """
    Apply the patch to the index, leaving the working directory
    untouched.  This is the equivalent of `git apply --cached`.
    """

    BOTH = _pygit2.GIT_APPLY_LOCATION_BOTH
    """
    Apply the patch to both the working directory and the index.
    This is the equivalent of `git apply --index`.
    """


class AttrCheck(IntFlag):
    FILE_THEN_INDEX = C.GIT_ATTR_CHECK_FILE_THEN_INDEX
    INDEX_THEN_FILE = C.GIT_ATTR_CHECK_INDEX_THEN_FILE
    INDEX_ONLY = C.GIT_ATTR_CHECK_INDEX_ONLY
    NO_SYSTEM = C.GIT_ATTR_CHECK_NO_SYSTEM
    INCLUDE_HEAD = C.GIT_ATTR_CHECK_INCLUDE_HEAD
    INCLUDE_COMMIT = C.GIT_ATTR_CHECK_INCLUDE_COMMIT


class BlameFlag(IntFlag):
    NORMAL = _pygit2.GIT_BLAME_NORMAL
    'Normal blame, the default'

    TRACK_COPIES_SAME_FILE = _pygit2.GIT_BLAME_TRACK_COPIES_SAME_FILE
    'Not yet implemented and reserved for future use (as of libgit2 1.9.0).'

    TRACK_COPIES_SAME_COMMIT_MOVES = _pygit2.GIT_BLAME_TRACK_COPIES_SAME_COMMIT_MOVES
    'Not yet implemented and reserved for future use (as of libgit2 1.9.0).'

    TRACK_COPIES_SAME_COMMIT_COPIES = _pygit2.GIT_BLAME_TRACK_COPIES_SAME_COMMIT_COPIES
    'Not yet implemented and reserved for future use (as of libgit2 1.9.0).'

    TRACK_COPIES_ANY_COMMIT_COPIES = _pygit2.GIT_BLAME_TRACK_COPIES_ANY_COMMIT_COPIES
    'Not yet implemented and reserved for future use (as of libgit2 1.9.0).'

    FIRST_PARENT = _pygit2.GIT_BLAME_FIRST_PARENT
    'Restrict the search of commits to those reachable following only the first parents.'

    USE_MAILMAP = _pygit2.GIT_BLAME_USE_MAILMAP
    """
    Use mailmap file to map author and committer names and email addresses
    to canonical real names and email addresses. The mailmap will be read
    from the working directory, or HEAD in a bare repository.
    """

    IGNORE_WHITESPACE = _pygit2.GIT_BLAME_IGNORE_WHITESPACE
    'Ignore whitespace differences'


class BlobFilter(IntFlag):
    CHECK_FOR_BINARY = _pygit2.GIT_BLOB_FILTER_CHECK_FOR_BINARY
    'Do not apply filters to binary files.'

    NO_SYSTEM_ATTRIBUTES = _pygit2.GIT_BLOB_FILTER_NO_SYSTEM_ATTRIBUTES
    'Filters will not load configuration from the system-wide `gitattributes` in `/etc` (or system equivalent).'

    ATTRIBUTES_FROM_HEAD = _pygit2.GIT_BLOB_FILTER_ATTRIBUTES_FROM_HEAD
    'Load filters from a `.gitattributes` file in the HEAD commit.'

    ATTRIBUTES_FROM_COMMIT = _pygit2.GIT_BLOB_FILTER_ATTRIBUTES_FROM_COMMIT
    'Load filters from a `.gitattributes` file in the specified commit.'


class BranchType(IntFlag):
    LOCAL = _pygit2.GIT_BRANCH_LOCAL
    REMOTE = _pygit2.GIT_BRANCH_REMOTE
    ALL = _pygit2.GIT_BRANCH_ALL


class CheckoutNotify(IntFlag):
    """
    Checkout notification flags

    Checkout will invoke an options notification callback
    (`CheckoutCallbacks.checkout_notify`) for certain cases - you pick which
    ones via `CheckoutCallbacks.checkout_notify_flags`.
    """

    NONE = C.GIT_CHECKOUT_NOTIFY_NONE

    CONFLICT = C.GIT_CHECKOUT_NOTIFY_CONFLICT
    'Invokes checkout on conflicting paths.'

    DIRTY = C.GIT_CHECKOUT_NOTIFY_DIRTY
    """
    Notifies about "dirty" files, i.e. those that do not need an update
    but no longer match the baseline.  Core git displays these files when
    checkout runs, but won't stop the checkout.
    """

    UPDATED = C.GIT_CHECKOUT_NOTIFY_UPDATED
    'Sends notification for any file changed.'

    UNTRACKED = C.GIT_CHECKOUT_NOTIFY_UNTRACKED
    'Notifies about untracked files.'

    IGNORED = C.GIT_CHECKOUT_NOTIFY_IGNORED
    'Notifies about ignored files.'

    ALL = C.GIT_CHECKOUT_NOTIFY_ALL


class CheckoutStrategy(IntFlag):
    NONE = _pygit2.GIT_CHECKOUT_NONE
    'Dry run, no actual updates'

    SAFE = _pygit2.GIT_CHECKOUT_SAFE
    """
    Allow safe updates that cannot overwrite uncommitted data.
    If the uncommitted changes don't conflict with the checked out files,
    the checkout will still proceed, leaving the changes intact.

    Mutually exclusive with FORCE.
    FORCE takes precedence over SAFE.
    """

    FORCE = _pygit2.GIT_CHECKOUT_FORCE
    """
    Allow all updates to force working directory to look like index.

    Mutually exclusive with SAFE.
    FORCE takes precedence over SAFE.
    """

    RECREATE_MISSING = _pygit2.GIT_CHECKOUT_RECREATE_MISSING
    """ Allow checkout to recreate missing files """

    ALLOW_CONFLICTS = _pygit2.GIT_CHECKOUT_ALLOW_CONFLICTS
    """ Allow checkout to make safe updates even if conflicts are found """

    REMOVE_UNTRACKED = _pygit2.GIT_CHECKOUT_REMOVE_UNTRACKED
    """ Remove untracked files not in index (that are not ignored) """

    REMOVE_IGNORED = _pygit2.GIT_CHECKOUT_REMOVE_IGNORED
    """ Remove ignored files not in index """

    UPDATE_ONLY = _pygit2.GIT_CHECKOUT_UPDATE_ONLY
    """ Only update existing files, don't create new ones """

    DONT_UPDATE_INDEX = _pygit2.GIT_CHECKOUT_DONT_UPDATE_INDEX
    """
    Normally checkout updates index entries as it goes; this stops that.
    Implies `DONT_WRITE_INDEX`.
    """

    NO_REFRESH = _pygit2.GIT_CHECKOUT_NO_REFRESH
    """ Don't refresh index/config/etc before doing checkout """

    SKIP_UNMERGED = _pygit2.GIT_CHECKOUT_SKIP_UNMERGED
    """ Allow checkout to skip unmerged files """

    USE_OURS = _pygit2.GIT_CHECKOUT_USE_OURS
    """ For unmerged files, checkout stage 2 from index """

    USE_THEIRS = _pygit2.GIT_CHECKOUT_USE_THEIRS
    """ For unmerged files, checkout stage 3 from index """

    DISABLE_PATHSPEC_MATCH = _pygit2.GIT_CHECKOUT_DISABLE_PATHSPEC_MATCH
    """ Treat pathspec as simple list of exact match file paths """

    SKIP_LOCKED_DIRECTORIES = _pygit2.GIT_CHECKOUT_SKIP_LOCKED_DIRECTORIES
    """ Ignore directories in use, they will be left empty """

    DONT_OVERWRITE_IGNORED = _pygit2.GIT_CHECKOUT_DONT_OVERWRITE_IGNORED
    """ Don't overwrite ignored files that exist in the checkout target """

    CONFLICT_STYLE_MERGE = _pygit2.GIT_CHECKOUT_CONFLICT_STYLE_MERGE
    """ Write normal merge files for conflicts """

    CONFLICT_STYLE_DIFF3 = _pygit2.GIT_CHECKOUT_CONFLICT_STYLE_DIFF3
    """ Include common ancestor data in diff3 format files for conflicts """

    DONT_REMOVE_EXISTING = _pygit2.GIT_CHECKOUT_DONT_REMOVE_EXISTING
    """ Don't overwrite existing files or folders """

    DONT_WRITE_INDEX = _pygit2.GIT_CHECKOUT_DONT_WRITE_INDEX
    """ Normally checkout writes the index upon completion; this prevents that. """

    DRY_RUN = _pygit2.GIT_CHECKOUT_DRY_RUN
    """
    Show what would be done by a checkout.  Stop after sending
    notifications; don't update the working directory or index.
    """

    CONFLICT_STYLE_ZDIFF3 = _pygit2.GIT_CHECKOUT_CONFLICT_STYLE_DIFF3
    """ Include common ancestor data in zdiff3 format for conflicts """


class ConfigLevel(IntEnum):
    """
    Priority level of a config file.
    These priority levels correspond to the natural escalation logic
    (from higher to lower) when searching for config entries in git.git.
    """

    PROGRAMDATA = _pygit2.GIT_CONFIG_LEVEL_PROGRAMDATA
    'System-wide on Windows, for compatibility with portable git'

    SYSTEM = _pygit2.GIT_CONFIG_LEVEL_SYSTEM
    'System-wide configuration file; /etc/gitconfig on Linux systems'

    XDG = _pygit2.GIT_CONFIG_LEVEL_XDG
    'XDG compatible configuration file; typically ~/.config/git/config'

    GLOBAL = _pygit2.GIT_CONFIG_LEVEL_GLOBAL
    'User-specific configuration file (also called Global configuration file); typically ~/.gitconfig'

    LOCAL = _pygit2.GIT_CONFIG_LEVEL_LOCAL
    'Repository specific configuration file; $WORK_DIR/.git/config on non-bare repos'

    WORKTREE = _pygit2.GIT_CONFIG_LEVEL_WORKTREE
    'Worktree specific configuration file; $GIT_DIR/config.worktree'

    APP = _pygit2.GIT_CONFIG_LEVEL_APP
    'Application specific configuration file; freely defined by applications'

    HIGHEST_LEVEL = _pygit2.GIT_CONFIG_HIGHEST_LEVEL
    """Represents the highest level available config file (i.e. the most
    specific config file available that actually is loaded)"""


class CredentialType(IntFlag):
    """
    Supported credential types. This represents the various types of
    authentication methods supported by the library.
    """

    USERPASS_PLAINTEXT = C.GIT_CREDENTIAL_USERPASS_PLAINTEXT
    'A vanilla user/password request'

    SSH_KEY = C.GIT_CREDENTIAL_SSH_KEY
    'An SSH key-based authentication request'

    SSH_CUSTOM = C.GIT_CREDENTIAL_SSH_CUSTOM
    'An SSH key-based authentication request, with a custom signature'

    DEFAULT = C.GIT_CREDENTIAL_DEFAULT
    'An NTLM/Negotiate-based authentication request.'

    SSH_INTERACTIVE = C.GIT_CREDENTIAL_SSH_INTERACTIVE
    'An SSH interactive authentication request.'

    USERNAME = C.GIT_CREDENTIAL_USERNAME
    """
    Username-only authentication request.
    Used as a pre-authentication step if the underlying transport (eg. SSH,
    with no username in its URL) does not know which username to use.
    """

    SSH_MEMORY = C.GIT_CREDENTIAL_SSH_MEMORY
    """
    An SSH key-based authentication request.
    Allows credentials to be read from memory instead of files.
    Note that because of differences in crypto backend support, it might
    not be functional.
    """


class DeltaStatus(IntEnum):
    """
    What type of change is described by a DiffDelta?

    `RENAMED` and `COPIED` will only show up if you run
    `find_similar()` on the Diff object.

    `TYPECHANGE` only shows up given `INCLUDE_TYPECHANGE`
    in the DiffOption option flags (otherwise type changes
    will be split into ADDED / DELETED pairs).
    """

    UNMODIFIED = _pygit2.GIT_DELTA_UNMODIFIED
    'no changes'

    ADDED = _pygit2.GIT_DELTA_ADDED
    'entry does not exist in old version'

    DELETED = _pygit2.GIT_DELTA_DELETED
    'entry does not exist in new version'

    MODIFIED = _pygit2.GIT_DELTA_MODIFIED
    'entry content changed between old and new'

    RENAMED = _pygit2.GIT_DELTA_RENAMED
    'entry was renamed between old and new'

    COPIED = _pygit2.GIT_DELTA_COPIED
    'entry was copied from another old entry'

    IGNORED = _pygit2.GIT_DELTA_IGNORED
    'entry is ignored item in workdir'

    UNTRACKED = _pygit2.GIT_DELTA_UNTRACKED
    'entry is untracked item in workdir'

    TYPECHANGE = _pygit2.GIT_DELTA_TYPECHANGE
    'type of entry changed between old and new'

    UNREADABLE = _pygit2.GIT_DELTA_UNREADABLE
    'entry is unreadable'

    CONFLICTED = _pygit2.GIT_DELTA_CONFLICTED
    'entry in the index is conflicted'


class DescribeStrategy(IntEnum):
    """
    Reference lookup strategy.

    These behave like the --tags and --all options to git-describe,
    namely they say to look for any reference in either refs/tags/ or
    refs/ respectively.
    """

    DEFAULT = _pygit2.GIT_DESCRIBE_DEFAULT
    TAGS = _pygit2.GIT_DESCRIBE_TAGS
    ALL = _pygit2.GIT_DESCRIBE_ALL


class DiffFind(IntFlag):
    """Flags to control the behavior of diff rename/copy detection."""

    FIND_BY_CONFIG = _pygit2.GIT_DIFF_FIND_BY_CONFIG
    """ Obey `diff.renames`. Overridden by any other FIND_... flag. """

    FIND_RENAMES = _pygit2.GIT_DIFF_FIND_RENAMES
    """ Look for renames? (`--find-renames`) """

    FIND_RENAMES_FROM_REWRITES = _pygit2.GIT_DIFF_FIND_RENAMES_FROM_REWRITES
    """ Consider old side of MODIFIED for renames? (`--break-rewrites=N`) """

    FIND_COPIES = _pygit2.GIT_DIFF_FIND_COPIES
    """ Look for copies? (a la `--find-copies`). """

    FIND_COPIES_FROM_UNMODIFIED = _pygit2.GIT_DIFF_FIND_COPIES_FROM_UNMODIFIED
    """
    Consider UNMODIFIED as copy sources? (`--find-copies-harder`).
    For this to work correctly, use INCLUDE_UNMODIFIED when the initial
    `Diff` is being generated.
    """

    FIND_REWRITES = _pygit2.GIT_DIFF_FIND_REWRITES
    """ Mark significant rewrites for split (`--break-rewrites=/M`) """

    BREAK_REWRITES = _pygit2.GIT_DIFF_BREAK_REWRITES
    """ Actually split large rewrites into delete/add pairs """

    FIND_AND_BREAK_REWRITES = _pygit2.GIT_DIFF_FIND_AND_BREAK_REWRITES
    """ Mark rewrites for split and break into delete/add pairs """

    FIND_FOR_UNTRACKED = _pygit2.GIT_DIFF_FIND_FOR_UNTRACKED
    """
    Find renames/copies for UNTRACKED items in working directory.
    For this to work correctly, use INCLUDE_UNTRACKED when the initial
    `Diff` is being generated (and obviously the diff must be against
    the working directory for this to make sense).
    """

    FIND_ALL = _pygit2.GIT_DIFF_FIND_ALL
    """ Turn on all finding features. """

    FIND_IGNORE_LEADING_WHITESPACE = _pygit2.GIT_DIFF_FIND_IGNORE_LEADING_WHITESPACE
    """ Measure similarity ignoring leading whitespace (default) """

    FIND_IGNORE_WHITESPACE = _pygit2.GIT_DIFF_FIND_IGNORE_WHITESPACE
    """ Measure similarity ignoring all whitespace """

    FIND_DONT_IGNORE_WHITESPACE = _pygit2.GIT_DIFF_FIND_DONT_IGNORE_WHITESPACE
    """ Measure similarity including all data """

    FIND_EXACT_MATCH_ONLY = _pygit2.GIT_DIFF_FIND_EXACT_MATCH_ONLY
    """ Measure similarity only by comparing SHAs (fast and cheap) """

    BREAK_REWRITES_FOR_RENAMES_ONLY = _pygit2.GIT_DIFF_BREAK_REWRITES_FOR_RENAMES_ONLY
    """
    Do not break rewrites unless they contribute to a rename.

    Normally, FIND_AND_BREAK_REWRITES will measure the self-
    similarity of modified files and split the ones that have changed a
    lot into a DELETE / ADD pair.  Then the sides of that pair will be
    considered candidates for rename and copy detection.

    If you add this flag in and the split pair is *not* used for an
    actual rename or copy, then the modified record will be restored to
    a regular MODIFIED record instead of being split.
    """

    FIND_REMOVE_UNMODIFIED = _pygit2.GIT_DIFF_FIND_REMOVE_UNMODIFIED
    """
    Remove any UNMODIFIED deltas after find_similar is done.

    Using FIND_COPIES_FROM_UNMODIFIED to emulate the
    --find-copies-harder behavior requires building a diff with the
    INCLUDE_UNMODIFIED flag.  If you do not want UNMODIFIED records
    in the final result, pass this flag to have them removed.
    """


class DiffFlag(IntFlag):
    """
    Flags for the delta object and the file objects on each side.

    These flags are used for both the `flags` value of the `DiffDelta`
    and the flags for the `DiffFile` objects representing the old and
    new sides of the delta.  Values outside of this public range should be
    considered reserved for internal or future use.
    """

    BINARY = _pygit2.GIT_DIFF_FLAG_BINARY
    'file(s) treated as binary data'

    NOT_BINARY = _pygit2.GIT_DIFF_FLAG_NOT_BINARY
    'file(s) treated as text data'

    VALID_ID = _pygit2.GIT_DIFF_FLAG_VALID_ID
    '`id` value is known correct'

    EXISTS = _pygit2.GIT_DIFF_FLAG_EXISTS
    'file exists at this side of the delta'

    VALID_SIZE = _pygit2.GIT_DIFF_FLAG_VALID_SIZE
    'file size value is known correct'


class DiffOption(IntFlag):
    """
    Flags for diff options.  A combination of these flags can be passed
    in via the `flags` value in `diff_*` functions.
    """

    NORMAL = _pygit2.GIT_DIFF_NORMAL
    'Normal diff, the default'

    REVERSE = _pygit2.GIT_DIFF_REVERSE
    'Reverse the sides of the diff'

    INCLUDE_IGNORED = _pygit2.GIT_DIFF_INCLUDE_IGNORED
    'Include ignored files in the diff'

    RECURSE_IGNORED_DIRS = _pygit2.GIT_DIFF_RECURSE_IGNORED_DIRS
    """
    Even with INCLUDE_IGNORED, an entire ignored directory
    will be marked with only a single entry in the diff; this flag
    adds all files under the directory as IGNORED entries, too.
    """

    INCLUDE_UNTRACKED = _pygit2.GIT_DIFF_INCLUDE_UNTRACKED
    'Include untracked files in the diff'

    RECURSE_UNTRACKED_DIRS = _pygit2.GIT_DIFF_RECURSE_UNTRACKED_DIRS
    """
    Even with INCLUDE_UNTRACKED, an entire untracked
    directory will be marked with only a single entry in the diff
    (a la what core Git does in `git status`); this flag adds *all*
    files under untracked directories as UNTRACKED entries, too.
    """

    INCLUDE_UNMODIFIED = _pygit2.GIT_DIFF_INCLUDE_UNMODIFIED
    'Include unmodified files in the diff'

    INCLUDE_TYPECHANGE = _pygit2.GIT_DIFF_INCLUDE_TYPECHANGE
    """
    Normally, a type change between files will be converted into a
    DELETED record for the old and an ADDED record for the new; this
    options enabled the generation of TYPECHANGE delta records.
    """

    INCLUDE_TYPECHANGE_TREES = _pygit2.GIT_DIFF_INCLUDE_TYPECHANGE_TREES
    """
    Even with INCLUDE_TYPECHANGE, blob->tree changes still generally
    show as a DELETED blob.  This flag tries to correctly label
    blob->tree transitions as TYPECHANGE records with new_file's
    mode set to tree.  Note: the tree SHA will not be available.
    """

    IGNORE_FILEMODE = _pygit2.GIT_DIFF_IGNORE_FILEMODE
    'Ignore file mode changes'

    IGNORE_SUBMODULES = _pygit2.GIT_DIFF_IGNORE_SUBMODULES
    'Treat all submodules as unmodified'

    IGNORE_CASE = _pygit2.GIT_DIFF_IGNORE_CASE
    'Use case insensitive filename comparisons'

    INCLUDE_CASECHANGE = _pygit2.GIT_DIFF_INCLUDE_CASECHANGE
    """
    May be combined with IGNORE_CASE to specify that a file
    that has changed case will be returned as an add/delete pair.
    """

    DISABLE_PATHSPEC_MATCH = _pygit2.GIT_DIFF_DISABLE_PATHSPEC_MATCH
    """
    If the pathspec is set in the diff options, this flags indicates
    that the paths will be treated as literal paths instead of
    fnmatch patterns.  Each path in the list must either be a full
    path to a file or a directory.  (A trailing slash indicates that
    the path will _only_ match a directory).  If a directory is
    specified, all children will be included.
    """

    SKIP_BINARY_CHECK = _pygit2.GIT_DIFF_SKIP_BINARY_CHECK
    """
    Disable updating of the `binary` flag in delta records.  This is
    useful when iterating over a diff if you don't need hunk and data
    callbacks and want to avoid having to load file completely.
    """

    ENABLE_FAST_UNTRACKED_DIRS = _pygit2.GIT_DIFF_ENABLE_FAST_UNTRACKED_DIRS
    """
    When diff finds an untracked directory, to match the behavior of
    core Git, it scans the contents for IGNORED and UNTRACKED files.
    If *all* contents are IGNORED, then the directory is IGNORED; if
    any contents are not IGNORED, then the directory is UNTRACKED.
    This is extra work that may not matter in many cases.  This flag
    turns off that scan and immediately labels an untracked directory
    as UNTRACKED (changing the behavior to not match core Git).
    """

    UPDATE_INDEX = _pygit2.GIT_DIFF_UPDATE_INDEX
    """
    When diff finds a file in the working directory with stat
    information different from the index, but the OID ends up being the
    same, write the correct stat information into the index.  Note:
    without this flag, diff will always leave the index untouched.
    """

    INCLUDE_UNREADABLE = _pygit2.GIT_DIFF_INCLUDE_UNREADABLE
    'Include unreadable files in the diff'

    INCLUDE_UNREADABLE_AS_UNTRACKED = _pygit2.GIT_DIFF_INCLUDE_UNREADABLE_AS_UNTRACKED
    'Include unreadable files in the diff'

    INDENT_HEURISTIC = _pygit2.GIT_DIFF_INDENT_HEURISTIC
    """
    Use a heuristic that takes indentation and whitespace into account
    which generally can produce better diffs when dealing with ambiguous
    diff hunks.
    """

    IGNORE_BLANK_LINES = _pygit2.GIT_DIFF_IGNORE_BLANK_LINES
    'Ignore blank lines'

    FORCE_TEXT = _pygit2.GIT_DIFF_FORCE_TEXT
    'Treat all files as text, disabling binary attributes & detection'

    FORCE_BINARY = _pygit2.GIT_DIFF_FORCE_BINARY
    'Treat all files as binary, disabling text diffs'

    IGNORE_WHITESPACE = _pygit2.GIT_DIFF_IGNORE_WHITESPACE
    'Ignore all whitespace'

    IGNORE_WHITESPACE_CHANGE = _pygit2.GIT_DIFF_IGNORE_WHITESPACE_CHANGE
    'Ignore changes in amount of whitespace'

    IGNORE_WHITESPACE_EOL = _pygit2.GIT_DIFF_IGNORE_WHITESPACE_EOL
    'Ignore whitespace at end of line'

    SHOW_UNTRACKED_CONTENT = _pygit2.GIT_DIFF_SHOW_UNTRACKED_CONTENT
    """
    When generating patch text, include the content of untracked files.
    This automatically turns on INCLUDE_UNTRACKED but it does not turn
    on RECURSE_UNTRACKED_DIRS.  Add that flag if you want the content
    of every single UNTRACKED file.
    """

    SHOW_UNMODIFIED = _pygit2.GIT_DIFF_SHOW_UNMODIFIED
    """
    When generating output, include the names of unmodified files if
    they are included in the git_diff.  Normally these are skipped in
    the formats that list files (e.g. name-only, name-status, raw).
    Even with this, these will not be included in patch format.
    """

    PATIENCE = _pygit2.GIT_DIFF_PATIENCE
    "Use the 'patience diff' algorithm"

    MINIMAL = _pygit2.GIT_DIFF_MINIMAL
    'Take extra time to find minimal diff'

    SHOW_BINARY = _pygit2.GIT_DIFF_SHOW_BINARY
    """
    Include the necessary deflate / delta information so that `git-apply`
    can apply given diff information to binary files.
    """


class DiffStatsFormat(IntFlag):
    """Formatting options for diff stats"""

    NONE = _pygit2.GIT_DIFF_STATS_NONE
    'No stats'

    FULL = _pygit2.GIT_DIFF_STATS_FULL
    'Full statistics, equivalent of `--stat`'

    SHORT = _pygit2.GIT_DIFF_STATS_SHORT
    'Short statistics, equivalent of `--shortstat`'

    NUMBER = _pygit2.GIT_DIFF_STATS_NUMBER
    'Number statistics, equivalent of `--numstat`'

    INCLUDE_SUMMARY = _pygit2.GIT_DIFF_STATS_INCLUDE_SUMMARY
    'Extended header information such as creations, renames and mode changes, equivalent of `--summary`'


class Feature(IntFlag):
    """
    Combinations of these values describe the features with which libgit2
    was compiled.
    """

    THREADS = C.GIT_FEATURE_THREADS
    HTTPS = C.GIT_FEATURE_HTTPS
    SSH = C.GIT_FEATURE_SSH
    NSEC = C.GIT_FEATURE_NSEC


class FetchPrune(IntEnum):
    """Acceptable prune settings when fetching."""

    UNSPECIFIED = C.GIT_FETCH_PRUNE_UNSPECIFIED
    'Use the setting from the configuration'

    PRUNE = C.GIT_FETCH_PRUNE
    """Force pruning on: remove any remote branch in the local repository
    that does not exist in the remote."""

    NO_PRUNE = C.GIT_FETCH_NO_PRUNE
    """Force pruning off: always keep the remote branches."""


class FileMode(IntFlag):
    UNREADABLE = _pygit2.GIT_FILEMODE_UNREADABLE
    TREE = _pygit2.GIT_FILEMODE_TREE
    BLOB = _pygit2.GIT_FILEMODE_BLOB
    BLOB_EXECUTABLE = _pygit2.GIT_FILEMODE_BLOB_EXECUTABLE
    LINK = _pygit2.GIT_FILEMODE_LINK
    COMMIT = _pygit2.GIT_FILEMODE_COMMIT


class FileStatus(IntFlag):
    """
    Status flags for a single file.

    A combination of these values will be returned to indicate the status of
    a file. Status compares the working directory, the index, and the current
    HEAD of the repository.  The `INDEX_...` set of flags represents the status
    of the file in the index relative to the HEAD, and the `WT_...` set of
    flags represents the status of the file in the working directory relative
    to the index.
    """

    CURRENT = _pygit2.GIT_STATUS_CURRENT

    INDEX_NEW = _pygit2.GIT_STATUS_INDEX_NEW
    INDEX_MODIFIED = _pygit2.GIT_STATUS_INDEX_MODIFIED
    INDEX_DELETED = _pygit2.GIT_STATUS_INDEX_DELETED
    INDEX_RENAMED = _pygit2.GIT_STATUS_INDEX_RENAMED
    INDEX_TYPECHANGE = _pygit2.GIT_STATUS_INDEX_TYPECHANGE

    WT_NEW = _pygit2.GIT_STATUS_WT_NEW
    WT_MODIFIED = _pygit2.GIT_STATUS_WT_MODIFIED
    WT_DELETED = _pygit2.GIT_STATUS_WT_DELETED
    WT_TYPECHANGE = _pygit2.GIT_STATUS_WT_TYPECHANGE
    WT_RENAMED = _pygit2.GIT_STATUS_WT_RENAMED
    WT_UNREADABLE = _pygit2.GIT_STATUS_WT_UNREADABLE

    IGNORED = _pygit2.GIT_STATUS_IGNORED
    CONFLICTED = _pygit2.GIT_STATUS_CONFLICTED


class FilterFlag(IntFlag):
    """Filter option flags."""

    DEFAULT = _pygit2.GIT_FILTER_DEFAULT

    ALLOW_UNSAFE = _pygit2.GIT_FILTER_ALLOW_UNSAFE
    "Don't error for `safecrlf` violations, allow them to continue."

    NO_SYSTEM_ATTRIBUTES = _pygit2.GIT_FILTER_NO_SYSTEM_ATTRIBUTES
    "Don't load `/etc/gitattributes` (or the system equivalent)"

    ATTRIBUTES_FROM_HEAD = _pygit2.GIT_FILTER_ATTRIBUTES_FROM_HEAD
    'Load attributes from `.gitattributes` in the root of HEAD'

    ATTRIBUTES_FROM_COMMIT = _pygit2.GIT_FILTER_ATTRIBUTES_FROM_COMMIT
    'Load attributes from `.gitattributes` in a given commit. This can only be specified in a `git_filter_options`.'


class FilterMode(IntEnum):
    """
    Filters are applied in one of two directions: smudging - which is
    exporting a file from the Git object database to the working directory,
    and cleaning - which is importing a file from the working directory to
    the Git object database.  These values control which direction of
    change is being applied.
    """

    TO_WORKTREE = _pygit2.GIT_FILTER_TO_WORKTREE
    SMUDGE = _pygit2.GIT_FILTER_SMUDGE
    TO_ODB = _pygit2.GIT_FILTER_TO_ODB
    CLEAN = _pygit2.GIT_FILTER_CLEAN


class MergeAnalysis(IntFlag):
    """The results of `Repository.merge_analysis` indicate the merge opportunities."""

    NONE = _pygit2.GIT_MERGE_ANALYSIS_NONE
    'No merge is possible.  (Unused.)'

    NORMAL = _pygit2.GIT_MERGE_ANALYSIS_NORMAL
    """
    A "normal" merge; both HEAD and the given merge input have diverged
    from their common ancestor.  The divergent commits must be merged.
    """

    UP_TO_DATE = _pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE
    """
    All given merge inputs are reachable from HEAD, meaning the
    repository is up-to-date and no merge needs to be performed.
    """

    FASTFORWARD = _pygit2.GIT_MERGE_ANALYSIS_FASTFORWARD
    """
    The given merge input is a fast-forward from HEAD and no merge
    needs to be performed.  Instead, the client can check out the
    given merge input.
    """

    UNBORN = _pygit2.GIT_MERGE_ANALYSIS_UNBORN
    """
    The HEAD of the current repository is "unborn" and does not point to
    a valid commit.  No merge can be performed, but the caller may wish
    to simply set HEAD to the target commit(s).
    """


class MergeFavor(IntEnum):
    """
    Merge file favor options for `Repository.merge` instruct the file-level
    merging functionality how to deal with conflicting regions of the files.
    """

    NORMAL = C.GIT_MERGE_FILE_FAVOR_NORMAL
    """
    When a region of a file is changed in both branches, a conflict will be
    recorded in the index so that `checkout` can produce a merge file with
    conflict markers in the working directory.

    This is the default.
    """

    OURS = C.GIT_MERGE_FILE_FAVOR_OURS
    """
    When a region of a file is changed in both branches, the file created in
    the index will contain the "ours" side of any conflicting region.

    The index will not record a conflict.
    """

    THEIRS = C.GIT_MERGE_FILE_FAVOR_THEIRS
    """
    When a region of a file is changed in both branches, the file created in
    the index will contain the "theirs" side of any conflicting region.

    The index will not record a conflict.
    """

    UNION = C.GIT_MERGE_FILE_FAVOR_UNION
    """
    When a region of a file is changed in both branches, the file
    created in the index will contain each unique line from each side,
    which has the result of combining both files.

    The index will not record a conflict.
    """


class MergeFileFlag(IntFlag):
    """File merging flags"""

    DEFAULT = C.GIT_MERGE_FILE_DEFAULT
    """ Defaults """

    STYLE_MERGE = C.GIT_MERGE_FILE_STYLE_MERGE
    """ Create standard conflicted merge files """

    STYLE_DIFF3 = C.GIT_MERGE_FILE_STYLE_DIFF3
    """ Create diff3-style files """

    SIMPLIFY_ALNUM = C.GIT_MERGE_FILE_SIMPLIFY_ALNUM
    """ Condense non-alphanumeric regions for simplified diff file """

    IGNORE_WHITESPACE = C.GIT_MERGE_FILE_IGNORE_WHITESPACE
    """ Ignore all whitespace """

    IGNORE_WHITESPACE_CHANGE = C.GIT_MERGE_FILE_IGNORE_WHITESPACE_CHANGE
    """ Ignore changes in amount of whitespace """

    IGNORE_WHITESPACE_EOL = C.GIT_MERGE_FILE_IGNORE_WHITESPACE_EOL
    """ Ignore whitespace at end of line """

    DIFF_PATIENCE = C.GIT_MERGE_FILE_DIFF_PATIENCE
    """ Use the "patience diff" algorithm """

    DIFF_MINIMAL = C.GIT_MERGE_FILE_DIFF_MINIMAL
    """ Take extra time to find minimal diff """

    STYLE_ZDIFF3 = C.GIT_MERGE_FILE_STYLE_ZDIFF3
    """ Create zdiff3 ("zealous diff3")-style files """

    ACCEPT_CONFLICTS = C.GIT_MERGE_FILE_ACCEPT_CONFLICTS
    """
    Do not produce file conflicts when common regions have changed;
    keep the conflict markers in the file and accept that as the merge result.
    """


class MergeFlag(IntFlag):
    """
    Flags for `Repository.merge` options.
    A combination of these flags can be passed in via the `flags` value.
    """

    FIND_RENAMES = C.GIT_MERGE_FIND_RENAMES
    """
    Detect renames that occur between the common ancestor and the "ours"
    side or the common ancestor and the "theirs" side.  This will enable
    the ability to merge between a modified and renamed file.
    """

    FAIL_ON_CONFLICT = C.GIT_MERGE_FAIL_ON_CONFLICT
    """
    If a conflict occurs, exit immediately instead of attempting to
    continue resolving conflicts.  The merge operation will raise GitError
    (GIT_EMERGECONFLICT) and no index will be returned.
    """

    SKIP_REUC = C.GIT_MERGE_SKIP_REUC
    """
    Do not write the REUC extension on the generated index.
    """

    NO_RECURSIVE = C.GIT_MERGE_NO_RECURSIVE
    """
    If the commits being merged have multiple merge bases, do not build
    a recursive merge base (by merging the multiple merge bases),
    instead simply use the first base.  This flag provides a similar
    merge base to `git-merge-resolve`.
    """

    VIRTUAL_BASE = C.GIT_MERGE_VIRTUAL_BASE
    """
    Treat this merge as if it is to produce the virtual base of a recursive
    merge.  This will ensure that there are no conflicts, any conflicting
    regions will keep conflict markers in the merge result.
    """


class MergePreference(IntFlag):
    """The user's stated preference for merges."""

    NONE = _pygit2.GIT_MERGE_PREFERENCE_NONE
    'No configuration was found that suggests a preferred behavior for merge.'

    NO_FASTFORWARD = _pygit2.GIT_MERGE_PREFERENCE_NO_FASTFORWARD
    """
    There is a `merge.ff=false` configuration setting, suggesting that
    the user does not want to allow a fast-forward merge.
    """

    FASTFORWARD_ONLY = _pygit2.GIT_MERGE_PREFERENCE_FASTFORWARD_ONLY
    """
    There is a `merge.ff=only` configuration setting, suggesting that
    the user only wants fast-forward merges.
    """


class ObjectType(IntEnum):
    ANY = _pygit2.GIT_OBJECT_ANY
    'Object can be any of the following'

    INVALID = _pygit2.GIT_OBJECT_INVALID
    'Object is invalid.'

    COMMIT = _pygit2.GIT_OBJECT_COMMIT
    'A commit object.'

    TREE = _pygit2.GIT_OBJECT_TREE
    'A tree (directory listing) object.'

    BLOB = _pygit2.GIT_OBJECT_BLOB
    'A file revision object.'

    TAG = _pygit2.GIT_OBJECT_TAG
    'An annotated tag object.'

    OFS_DELTA = _pygit2.GIT_OBJECT_OFS_DELTA
    'A delta, base is given by an offset.'

    REF_DELTA = _pygit2.GIT_OBJECT_REF_DELTA
    'A delta, base is given by object id.'


class Option(IntEnum):
    """Global libgit2 library options"""

    # Commented out values --> exists in libgit2 but not supported in pygit2's options.c yet
    GET_MWINDOW_SIZE = options.GIT_OPT_GET_MWINDOW_SIZE
    SET_MWINDOW_SIZE = options.GIT_OPT_SET_MWINDOW_SIZE
    GET_MWINDOW_MAPPED_LIMIT = options.GIT_OPT_GET_MWINDOW_MAPPED_LIMIT
    SET_MWINDOW_MAPPED_LIMIT = options.GIT_OPT_SET_MWINDOW_MAPPED_LIMIT
    GET_SEARCH_PATH = options.GIT_OPT_GET_SEARCH_PATH
    SET_SEARCH_PATH = options.GIT_OPT_SET_SEARCH_PATH
    SET_CACHE_OBJECT_LIMIT = options.GIT_OPT_SET_CACHE_OBJECT_LIMIT
    SET_CACHE_MAX_SIZE = options.GIT_OPT_SET_CACHE_MAX_SIZE
    ENABLE_CACHING = options.GIT_OPT_ENABLE_CACHING
    GET_CACHED_MEMORY = options.GIT_OPT_GET_CACHED_MEMORY
    GET_TEMPLATE_PATH = options.GIT_OPT_GET_TEMPLATE_PATH
    SET_TEMPLATE_PATH = options.GIT_OPT_SET_TEMPLATE_PATH
    SET_SSL_CERT_LOCATIONS = options.GIT_OPT_SET_SSL_CERT_LOCATIONS
    SET_USER_AGENT = options.GIT_OPT_SET_USER_AGENT
    ENABLE_STRICT_OBJECT_CREATION = options.GIT_OPT_ENABLE_STRICT_OBJECT_CREATION
    ENABLE_STRICT_SYMBOLIC_REF_CREATION = (
        options.GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION
    )
    SET_SSL_CIPHERS = options.GIT_OPT_SET_SSL_CIPHERS
    GET_USER_AGENT = options.GIT_OPT_GET_USER_AGENT
    ENABLE_OFS_DELTA = options.GIT_OPT_ENABLE_OFS_DELTA
    ENABLE_FSYNC_GITDIR = options.GIT_OPT_ENABLE_FSYNC_GITDIR
    GET_WINDOWS_SHAREMODE = options.GIT_OPT_GET_WINDOWS_SHAREMODE
    SET_WINDOWS_SHAREMODE = options.GIT_OPT_SET_WINDOWS_SHAREMODE
    ENABLE_STRICT_HASH_VERIFICATION = options.GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION
    SET_ALLOCATOR = options.GIT_OPT_SET_ALLOCATOR
    ENABLE_UNSAVED_INDEX_SAFETY = options.GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY
    GET_PACK_MAX_OBJECTS = options.GIT_OPT_GET_PACK_MAX_OBJECTS
    SET_PACK_MAX_OBJECTS = options.GIT_OPT_SET_PACK_MAX_OBJECTS
    DISABLE_PACK_KEEP_FILE_CHECKS = options.GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS
    ENABLE_HTTP_EXPECT_CONTINUE = options.GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE
    GET_MWINDOW_FILE_LIMIT = options.GIT_OPT_GET_MWINDOW_FILE_LIMIT
    SET_MWINDOW_FILE_LIMIT = options.GIT_OPT_SET_MWINDOW_FILE_LIMIT
    SET_ODB_PACKED_PRIORITY = options.GIT_OPT_SET_ODB_PACKED_PRIORITY
    SET_ODB_LOOSE_PRIORITY = options.GIT_OPT_SET_ODB_LOOSE_PRIORITY
    GET_EXTENSIONS = options.GIT_OPT_GET_EXTENSIONS
    SET_EXTENSIONS = options.GIT_OPT_SET_EXTENSIONS
    GET_OWNER_VALIDATION = options.GIT_OPT_GET_OWNER_VALIDATION
    SET_OWNER_VALIDATION = options.GIT_OPT_SET_OWNER_VALIDATION
    GET_HOMEDIR = options.GIT_OPT_GET_HOMEDIR
    SET_HOMEDIR = options.GIT_OPT_SET_HOMEDIR
    SET_SERVER_CONNECT_TIMEOUT = options.GIT_OPT_SET_SERVER_CONNECT_TIMEOUT
    GET_SERVER_CONNECT_TIMEOUT = options.GIT_OPT_GET_SERVER_CONNECT_TIMEOUT
    SET_SERVER_TIMEOUT = options.GIT_OPT_SET_SERVER_TIMEOUT
    GET_SERVER_TIMEOUT = options.GIT_OPT_GET_SERVER_TIMEOUT
    GET_USER_AGENT_PRODUCT = options.GIT_OPT_GET_USER_AGENT_PRODUCT
    SET_USER_AGENT_PRODUCT = options.GIT_OPT_SET_USER_AGENT_PRODUCT
    ADD_SSL_X509_CERT = options.GIT_OPT_ADD_SSL_X509_CERT


class ReferenceFilter(IntEnum):
    """Filters for References.iterator()."""

    ALL = _pygit2.GIT_REFERENCES_ALL
    BRANCHES = _pygit2.GIT_REFERENCES_BRANCHES
    TAGS = _pygit2.GIT_REFERENCES_TAGS


class ReferenceType(IntFlag):
    """Basic type of any Git reference."""

    INVALID = C.GIT_REFERENCE_INVALID
    'Invalid reference'

    DIRECT = C.GIT_REFERENCE_DIRECT
    'A reference that points at an object id'

    SYMBOLIC = C.GIT_REFERENCE_SYMBOLIC
    'A reference that points at another reference'

    ALL = C.GIT_REFERENCE_ALL
    'Bitwise OR of (DIRECT | SYMBOLIC)'


class RepositoryInitFlag(IntFlag):
    """
    Option flags for pygit2.init_repository().
    """

    BARE = C.GIT_REPOSITORY_INIT_BARE
    'Create a bare repository with no working directory.'

    NO_REINIT = C.GIT_REPOSITORY_INIT_NO_REINIT
    'Raise GitError if the path appears to already be a git repository.'

    NO_DOTGIT_DIR = C.GIT_REPOSITORY_INIT_NO_DOTGIT_DIR
    """Normally a "/.git/" will be appended to the repo path for
    non-bare repos (if it is not already there), but passing this flag
    prevents that behavior."""

    MKDIR = C.GIT_REPOSITORY_INIT_MKDIR
    """Make the repo_path (and workdir_path) as needed. Init is always willing
    to create the ".git" directory even without this flag. This flag tells
    init to create the trailing component of the repo and workdir paths
    as needed."""

    MKPATH = C.GIT_REPOSITORY_INIT_MKPATH
    'Recursively make all components of the repo and workdir paths as necessary.'

    EXTERNAL_TEMPLATE = C.GIT_REPOSITORY_INIT_EXTERNAL_TEMPLATE
    """libgit2 normally uses internal templates to initialize a new repo.
    This flags enables external templates, looking at the "template_path" from
    the options if set, or the `init.templatedir` global config if not,
    or falling back on "/usr/share/git-core/templates" if it exists."""

    RELATIVE_GITLINK = C.GIT_REPOSITORY_INIT_RELATIVE_GITLINK
    """If an alternate workdir is specified, use relative paths for the gitdir
    and core.worktree."""


class RepositoryInitMode(IntEnum):
    """
    Mode options for pygit2.init_repository().
    """

    SHARED_UMASK = C.GIT_REPOSITORY_INIT_SHARED_UMASK
    'Use permissions configured by umask - the default.'

    SHARED_GROUP = C.GIT_REPOSITORY_INIT_SHARED_GROUP
    """
    Use '--shared=group' behavior, chmod'ing the new repo to be group
    writable and "g+sx" for sticky group assignment.
    """

    SHARED_ALL = C.GIT_REPOSITORY_INIT_SHARED_ALL
    "Use '--shared=all' behavior, adding world readability."


class RepositoryOpenFlag(IntFlag):
    """
    Option flags for Repository.__init__().
    """

    DEFAULT = 0
    'Default flags.'

    NO_SEARCH = C.GIT_REPOSITORY_OPEN_NO_SEARCH
    """
    Only open the repository if it can be immediately found in the
    start_path. Do not walk up from the start_path looking at parent
    directories.
    """

    CROSS_FS = C.GIT_REPOSITORY_OPEN_CROSS_FS
    """
    Unless this flag is set, open will not continue searching across
    filesystem boundaries (i.e. when `st_dev` changes from the `stat`
    system call).  For example, searching in a user's home directory at
    "/home/user/source/" will not return "/.git/" as the found repo if
    "/" is a different filesystem than "/home".
    """

    BARE = C.GIT_REPOSITORY_OPEN_BARE
    """
    Open repository as a bare repo regardless of core.bare config, and
    defer loading config file for faster setup.
    Unlike `git_repository_open_bare`, this can follow gitlinks.
    """

    NO_DOTGIT = C.GIT_REPOSITORY_OPEN_NO_DOTGIT
    """
    Do not check for a repository by appending /.git to the start_path;
    only open the repository if start_path itself points to the git
    directory.
    """

    FROM_ENV = C.GIT_REPOSITORY_OPEN_FROM_ENV
    """
    Find and open a git repository, respecting the environment variables
    used by the git command-line tools.
    If set, `git_repository_open_ext` will ignore the other flags and
    the `ceiling_dirs` argument, and will allow a NULL `path` to use
    `GIT_DIR` or search from the current directory.
    The search for a repository will respect $GIT_CEILING_DIRECTORIES and
    $GIT_DISCOVERY_ACROSS_FILESYSTEM.  The opened repository will
    respect $GIT_INDEX_FILE, $GIT_NAMESPACE, $GIT_OBJECT_DIRECTORY, and
    $GIT_ALTERNATE_OBJECT_DIRECTORIES.
    In the future, this flag will also cause `git_repository_open_ext`
    to respect $GIT_WORK_TREE and $GIT_COMMON_DIR; currently,
    `git_repository_open_ext` with this flag will error out if either
    $GIT_WORK_TREE or $GIT_COMMON_DIR is set.
    """


class RepositoryState(IntEnum):
    """
    Repository state: These values represent possible states for the repository
    to be in, based on the current operation which is ongoing.
    """

    NONE = C.GIT_REPOSITORY_STATE_NONE
    MERGE = C.GIT_REPOSITORY_STATE_MERGE
    REVERT = C.GIT_REPOSITORY_STATE_REVERT
    REVERT_SEQUENCE = C.GIT_REPOSITORY_STATE_REVERT_SEQUENCE
    CHERRYPICK = C.GIT_REPOSITORY_STATE_CHERRYPICK
    CHERRYPICK_SEQUENCE = C.GIT_REPOSITORY_STATE_CHERRYPICK_SEQUENCE
    BISECT = C.GIT_REPOSITORY_STATE_BISECT
    REBASE = C.GIT_REPOSITORY_STATE_REBASE
    REBASE_INTERACTIVE = C.GIT_REPOSITORY_STATE_REBASE_INTERACTIVE
    REBASE_MERGE = C.GIT_REPOSITORY_STATE_REBASE_MERGE
    APPLY_MAILBOX = C.GIT_REPOSITORY_STATE_APPLY_MAILBOX
    APPLY_MAILBOX_OR_REBASE = C.GIT_REPOSITORY_STATE_APPLY_MAILBOX_OR_REBASE


class ResetMode(IntEnum):
    """Kinds of reset operation."""

    SOFT = _pygit2.GIT_RESET_SOFT
    'Move the head to the given commit'

    MIXED = _pygit2.GIT_RESET_MIXED
    'SOFT plus reset index to the commit'

    HARD = _pygit2.GIT_RESET_HARD
    'MIXED plus changes in working tree discarded'


class RevSpecFlag(IntFlag):
    """
    Revparse flags.
    These indicate the intended behavior of the spec passed to Repository.revparse()
    """

    SINGLE = _pygit2.GIT_REVSPEC_SINGLE
    'The spec targeted a single object.'

    RANGE = _pygit2.GIT_REVSPEC_RANGE
    'The spec targeted a range of commits.'

    MERGE_BASE = _pygit2.GIT_REVSPEC_MERGE_BASE
    "The spec used the '...' operator, which invokes special semantics."


class SortMode(IntFlag):
    """
    Flags to specify the sorting which a revwalk should perform.
    """

    NONE = _pygit2.GIT_SORT_NONE
    """
    Sort the output with the same default method from `git`: reverse
    chronological order. This is the default sorting for new walkers.
    """

    TOPOLOGICAL = _pygit2.GIT_SORT_TOPOLOGICAL
    """
    Sort the repository contents in topological order (no parents before
    all of its children are shown); this sorting mode can be combined
    with TIME sorting to produce `git`'s `--date-order``.
    """

    TIME = _pygit2.GIT_SORT_TIME
    """
    Sort the repository contents by commit time; this sorting mode can be
    combined with TOPOLOGICAL.
    """

    REVERSE = _pygit2.GIT_SORT_REVERSE
    """
    Iterate through the repository contents in reverse order;
    this sorting mode can be combined with any of the above.
    """


class StashApplyProgress(IntEnum):
    """
    Stash apply progression states
    """

    NONE = C.GIT_STASH_APPLY_PROGRESS_NONE

    LOADING_STASH = C.GIT_STASH_APPLY_PROGRESS_LOADING_STASH
    'Loading the stashed data from the object database.'

    ANALYZE_INDEX = C.GIT_STASH_APPLY_PROGRESS_ANALYZE_INDEX
    'The stored index is being analyzed.'

    ANALYZE_MODIFIED = C.GIT_STASH_APPLY_PROGRESS_ANALYZE_MODIFIED
    'The modified files are being analyzed.'

    ANALYZE_UNTRACKED = C.GIT_STASH_APPLY_PROGRESS_ANALYZE_UNTRACKED
    'The untracked and ignored files are being analyzed.'

    CHECKOUT_UNTRACKED = C.GIT_STASH_APPLY_PROGRESS_CHECKOUT_UNTRACKED
    'The untracked files are being written to disk.'

    CHECKOUT_MODIFIED = C.GIT_STASH_APPLY_PROGRESS_CHECKOUT_MODIFIED
    'The modified files are being written to disk.'

    DONE = C.GIT_STASH_APPLY_PROGRESS_DONE
    'The stash was applied successfully.'


class SubmoduleIgnore(IntEnum):
    UNSPECIFIED = _pygit2.GIT_SUBMODULE_IGNORE_UNSPECIFIED
    "use the submodule's configuration"

    NONE = _pygit2.GIT_SUBMODULE_IGNORE_NONE
    'any change or untracked == dirty'

    UNTRACKED = _pygit2.GIT_SUBMODULE_IGNORE_UNTRACKED
    'dirty if tracked files change'

    DIRTY = _pygit2.GIT_SUBMODULE_IGNORE_DIRTY
    'only dirty if HEAD moved'

    ALL = _pygit2.GIT_SUBMODULE_IGNORE_ALL
    'never dirty'


class SubmoduleStatus(IntFlag):
    IN_HEAD = _pygit2.GIT_SUBMODULE_STATUS_IN_HEAD
    'superproject head contains submodule'

    IN_INDEX = _pygit2.GIT_SUBMODULE_STATUS_IN_INDEX
    'superproject index contains submodule'

    IN_CONFIG = _pygit2.GIT_SUBMODULE_STATUS_IN_CONFIG
    'superproject gitmodules has submodule'

    IN_WD = _pygit2.GIT_SUBMODULE_STATUS_IN_WD
    'superproject workdir has submodule'

    INDEX_ADDED = _pygit2.GIT_SUBMODULE_STATUS_INDEX_ADDED
    'in index, not in head (flag available if ignore is not ALL)'

    INDEX_DELETED = _pygit2.GIT_SUBMODULE_STATUS_INDEX_DELETED
    'in head, not in index (flag available if ignore is not ALL)'

    INDEX_MODIFIED = _pygit2.GIT_SUBMODULE_STATUS_INDEX_MODIFIED
    "index and head don't match (flag available if ignore is not ALL)"

    WD_UNINITIALIZED = _pygit2.GIT_SUBMODULE_STATUS_WD_UNINITIALIZED
    'workdir contains empty repository (flag available if ignore is not ALL)'

    WD_ADDED = _pygit2.GIT_SUBMODULE_STATUS_WD_ADDED
    'in workdir, not index (flag available if ignore is not ALL)'

    WD_DELETED = _pygit2.GIT_SUBMODULE_STATUS_WD_DELETED
    'in index, not workdir (flag available if ignore is not ALL)'

    WD_MODIFIED = _pygit2.GIT_SUBMODULE_STATUS_WD_MODIFIED
    "index and workdir head don't match (flag available if ignore is not ALL)"

    WD_INDEX_MODIFIED = _pygit2.GIT_SUBMODULE_STATUS_WD_INDEX_MODIFIED
    'submodule workdir index is dirty (flag available if ignore is NONE or UNTRACKED)'

    WD_WD_MODIFIED = _pygit2.GIT_SUBMODULE_STATUS_WD_WD_MODIFIED
    'submodule workdir has modified files (flag available if ignore is NONE or UNTRACKED)'

    WD_UNTRACKED = _pygit2.GIT_SUBMODULE_STATUS_WD_UNTRACKED
    'submodule workdir contains untracked files (flag available if ignore is NONE)'
