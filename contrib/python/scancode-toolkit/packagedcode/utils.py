#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from license_expression import Licensing

PLAIN_URLS = (
    'https://',
    'http://',
)

VCS_URLS = (
    'git://',
    'git+git://',
    'git+https://',
    'git+http://',

    'hg://',
    'hg+http://',
    'hg+https://',

    'svn://',
    'svn+https://',
    'svn+http://',
)


# TODO this does not really normalize the URL
# TODO handle vcs_tool
def normalize_vcs_url(repo_url, vcs_tool=None):
    """
    Return a normalized vcs_url version control URL given some `repo_url` and an
    optional `vcs_tool` hint (such as 'git', 'hg', etc.

    Handles shortcuts for GitHub, GitHub gist, Bitbucket, or GitLab repositories
    and more using the same approach as npm install:

    See https://docs.npmjs.com/files/package.json#repository
    or https://getcomposer.org/doc/05-repositories.md

    This is done here in npm:
    https://github.com/npm/npm/blob/d3c858ce4cfb3aee515bb299eb034fe1b5e44344/node_modules/hosted-git-info/git-host-info.js

    These should be resolved:
        npm/npm
        gist:11081aaa281
        bitbucket:example/repo
        gitlab:another/repo
        expressjs/serve-static
        git://github.com/angular/di.js.git
        git://github.com/hapijs/boom
        git@github.com:balderdashy/waterline-criteria.git
        http://github.com/ariya/esprima.git
        http://github.com/isaacs/nopt
        https://github.com/chaijs/chai
        https://github.com/christkv/kerberos.git
        https://gitlab.com/foo/private.git
        git@gitlab.com:foo/private.git
    """
    if not repo_url or not isinstance(repo_url, str):
        return

    repo_url = repo_url.strip()
    if not repo_url:
        return

    # TODO: If we match http and https, we may should add more check in
    # case if the url is not a repo one. For example, check the domain
    # name in the url...
    if repo_url.startswith(VCS_URLS + PLAIN_URLS):
        return repo_url

    if repo_url.startswith('git@'):
        tool, _, right = repo_url.partition('@')
        if ':' in repo_url:
            host, _, repo = right.partition(':')
        else:
            # git@github.com/Filirom1/npm2aur.git
            host, _, repo = right.partition('/')

        if any(r in host for r in ('bitbucket', 'gitlab', 'github')):
            scheme = 'https'
        else:
            scheme = 'git'

        return '%(scheme)s://%(host)s/%(repo)s' % locals()

    # FIXME: where these URL schemes come from??
    if repo_url.startswith(('bitbucket:', 'gitlab:', 'github:', 'gist:')):
        hoster_urls = {
            'bitbucket': 'https://bitbucket.org/%(repo)s',
            'github': 'https://github.com/%(repo)s',
            'gitlab': 'https://gitlab.com/%(repo)s',
            'gist': 'https://gist.github.com/%(repo)s', }
        hoster, _, repo = repo_url.partition(':')
        return hoster_urls[hoster] % locals()

    if len(repo_url.split('/')) == 2:
        # implicit github, but that's only on NPM?
        return 'https://github.com/%(repo_url)s' % locals()

    return repo_url


# for legacy compat
parse_repo_url = normalize_vcs_url


def build_description(summary, description):
    """
    Return a description string from a summary and description
    """
    summary = (summary or '').strip()
    description = (description or '').strip()

    if not description:
        description = summary
    else:
        if summary and summary not in description:
            description = '\n'.join([summary , description])

    return description


def combine_expressions(expressions, relation='AND', unique=True, licensing=Licensing()):
    """
    Return a combined license expression string with relation, given a list of
    license expressions strings.

    For example::

        >>> a = 'mit'
        >>> b = 'gpl'
        >>> combine_expressions([a, b])
        'mit AND gpl'
        >>> assert 'mit' == combine_expressions([a])
        >>> combine_expressions([])
        >>> combine_expressions(None)
        >>> combine_expressions(('gpl', 'mit', 'apache',))
        'gpl AND mit AND apache'
        >>> combine_expressions(('gpl', 'mit', 'mit',))
        'gpl AND mit'
        >>> combine_expressions(('mit WITH foo', 'gpl', 'mit',))
        'mit WITH foo AND gpl AND mit'
        >>> combine_expressions(('gpl', 'mit', 'mit',), relation='OR', unique=False)
        'gpl OR mit OR mit'
        >>> combine_expressions(('mit', 'gpl', 'mit',))
        'mit AND gpl'
    """
    if not expressions:
        return

    if not isinstance(expressions, (list, tuple)):
        raise TypeError(
            'expressions should be a list or tuple and not: {}'.format(
                type(expressions)))

    if unique:
        # Remove duplicate element in the expressions list
        expressions = list(dict((x, True) for x in expressions).keys())

    if len(expressions) == 1:
        return expressions[0]

    expressions = [licensing.parse(le, simple=True) for le in expressions]

    # licensing.OR or licensing.AND
    assert relation and relation.upper() in ('AND', 'OR',)
    relationship = getattr(licensing, relation)
    return str(relationship(*expressions))
