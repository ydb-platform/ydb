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

"""
GIT_* enum values for compatibility with legacy code.

These values are deprecated starting with pygit2 1.14.
User programs should migrate to the enum classes defined in `pygit2.enums`.

Note that our C module _pygit2 already exports many libgit2 enums
(which are all imported by __init__.py). This file only exposes the enums
that are not available through _pygit2.
"""

from . import enums

GIT_FEATURE_THREADS = enums.Feature.THREADS
GIT_FEATURE_HTTPS = enums.Feature.HTTPS
GIT_FEATURE_SSH = enums.Feature.SSH
GIT_FEATURE_NSEC = enums.Feature.NSEC

GIT_REPOSITORY_INIT_BARE = enums.RepositoryInitFlag.BARE
GIT_REPOSITORY_INIT_NO_REINIT = enums.RepositoryInitFlag.NO_REINIT
GIT_REPOSITORY_INIT_NO_DOTGIT_DIR = enums.RepositoryInitFlag.NO_DOTGIT_DIR
GIT_REPOSITORY_INIT_MKDIR = enums.RepositoryInitFlag.MKDIR
GIT_REPOSITORY_INIT_MKPATH = enums.RepositoryInitFlag.MKPATH
GIT_REPOSITORY_INIT_EXTERNAL_TEMPLATE = enums.RepositoryInitFlag.EXTERNAL_TEMPLATE
GIT_REPOSITORY_INIT_RELATIVE_GITLINK = enums.RepositoryInitFlag.RELATIVE_GITLINK

GIT_REPOSITORY_INIT_SHARED_UMASK = enums.RepositoryInitMode.SHARED_UMASK
GIT_REPOSITORY_INIT_SHARED_GROUP = enums.RepositoryInitMode.SHARED_GROUP
GIT_REPOSITORY_INIT_SHARED_ALL = enums.RepositoryInitMode.SHARED_ALL

GIT_REPOSITORY_OPEN_NO_SEARCH = enums.RepositoryOpenFlag.NO_SEARCH
GIT_REPOSITORY_OPEN_CROSS_FS = enums.RepositoryOpenFlag.CROSS_FS
GIT_REPOSITORY_OPEN_BARE = enums.RepositoryOpenFlag.BARE
GIT_REPOSITORY_OPEN_NO_DOTGIT = enums.RepositoryOpenFlag.NO_DOTGIT
GIT_REPOSITORY_OPEN_FROM_ENV = enums.RepositoryOpenFlag.FROM_ENV

GIT_REPOSITORY_STATE_NONE = enums.RepositoryState.NONE
GIT_REPOSITORY_STATE_MERGE = enums.RepositoryState.MERGE
GIT_REPOSITORY_STATE_REVERT = enums.RepositoryState.REVERT
GIT_REPOSITORY_STATE_REVERT_SEQUENCE = enums.RepositoryState.REVERT_SEQUENCE
GIT_REPOSITORY_STATE_CHERRYPICK = enums.RepositoryState.CHERRYPICK
GIT_REPOSITORY_STATE_CHERRYPICK_SEQUENCE = enums.RepositoryState.CHERRYPICK_SEQUENCE
GIT_REPOSITORY_STATE_BISECT = enums.RepositoryState.BISECT
GIT_REPOSITORY_STATE_REBASE = enums.RepositoryState.REBASE
GIT_REPOSITORY_STATE_REBASE_INTERACTIVE = enums.RepositoryState.REBASE_INTERACTIVE
GIT_REPOSITORY_STATE_REBASE_MERGE = enums.RepositoryState.REBASE_MERGE
GIT_REPOSITORY_STATE_APPLY_MAILBOX = enums.RepositoryState.APPLY_MAILBOX
GIT_REPOSITORY_STATE_APPLY_MAILBOX_OR_REBASE = (
    enums.RepositoryState.APPLY_MAILBOX_OR_REBASE
)

GIT_ATTR_CHECK_FILE_THEN_INDEX = enums.AttrCheck.FILE_THEN_INDEX
GIT_ATTR_CHECK_INDEX_THEN_FILE = enums.AttrCheck.INDEX_THEN_FILE
GIT_ATTR_CHECK_INDEX_ONLY = enums.AttrCheck.INDEX_ONLY
GIT_ATTR_CHECK_NO_SYSTEM = enums.AttrCheck.NO_SYSTEM
GIT_ATTR_CHECK_INCLUDE_HEAD = enums.AttrCheck.INCLUDE_HEAD
GIT_ATTR_CHECK_INCLUDE_COMMIT = enums.AttrCheck.INCLUDE_COMMIT

GIT_FETCH_PRUNE_UNSPECIFIED = enums.FetchPrune.UNSPECIFIED
GIT_FETCH_PRUNE = enums.FetchPrune.PRUNE
GIT_FETCH_NO_PRUNE = enums.FetchPrune.NO_PRUNE

GIT_CHECKOUT_NOTIFY_NONE = enums.CheckoutNotify.NONE
GIT_CHECKOUT_NOTIFY_CONFLICT = enums.CheckoutNotify.CONFLICT
GIT_CHECKOUT_NOTIFY_DIRTY = enums.CheckoutNotify.DIRTY
GIT_CHECKOUT_NOTIFY_UPDATED = enums.CheckoutNotify.UPDATED
GIT_CHECKOUT_NOTIFY_UNTRACKED = enums.CheckoutNotify.UNTRACKED
GIT_CHECKOUT_NOTIFY_IGNORED = enums.CheckoutNotify.IGNORED
GIT_CHECKOUT_NOTIFY_ALL = enums.CheckoutNotify.ALL

GIT_STASH_APPLY_PROGRESS_NONE = enums.StashApplyProgress.NONE
GIT_STASH_APPLY_PROGRESS_LOADING_STASH = enums.StashApplyProgress.LOADING_STASH
GIT_STASH_APPLY_PROGRESS_ANALYZE_INDEX = enums.StashApplyProgress.ANALYZE_INDEX
GIT_STASH_APPLY_PROGRESS_ANALYZE_MODIFIED = enums.StashApplyProgress.ANALYZE_MODIFIED
GIT_STASH_APPLY_PROGRESS_ANALYZE_UNTRACKED = enums.StashApplyProgress.ANALYZE_UNTRACKED
GIT_STASH_APPLY_PROGRESS_CHECKOUT_UNTRACKED = (
    enums.StashApplyProgress.CHECKOUT_UNTRACKED
)
GIT_STASH_APPLY_PROGRESS_CHECKOUT_MODIFIED = enums.StashApplyProgress.CHECKOUT_MODIFIED
GIT_STASH_APPLY_PROGRESS_DONE = enums.StashApplyProgress.DONE

GIT_CREDENTIAL_USERPASS_PLAINTEXT = enums.CredentialType.USERPASS_PLAINTEXT
GIT_CREDENTIAL_SSH_KEY = enums.CredentialType.SSH_KEY
GIT_CREDENTIAL_SSH_CUSTOM = enums.CredentialType.SSH_CUSTOM
GIT_CREDENTIAL_DEFAULT = enums.CredentialType.DEFAULT
GIT_CREDENTIAL_SSH_INTERACTIVE = enums.CredentialType.SSH_INTERACTIVE
GIT_CREDENTIAL_USERNAME = enums.CredentialType.USERNAME
GIT_CREDENTIAL_SSH_MEMORY = enums.CredentialType.SSH_MEMORY
