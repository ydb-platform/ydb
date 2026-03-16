# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import json
import logging

import attr
from packageurl import PackageURL
from pygments import highlight
from pygments.formatter import Formatter
from pygments.lexers import RubyLexer
from pygments.token import Token

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models


"""
Handle Chef cookbooks
"""


TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


@attr.s()
class ChefPackage(models.Package):
    metafiles = ('metadata.json', 'metadata.rb')
    filetypes = ('.tgz',)
    mimetypes = ('application/x-tar',)
    default_type = 'chef'
    default_primary_language = 'Ruby'
    default_web_baseurl = 'https://supermarket.chef.io/cookbooks'
    default_download_baseurl = 'https://supermarket.chef.io/cookbooks'
    default_api_baseurl = 'https://supermarket.chef.io/api/v1'

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)

    def repository_download_url(self, baseurl=default_download_baseurl):
        return chef_download_url(self.name, self.version, registry=baseurl)

    def api_data_url(self, baseurl=default_api_baseurl):
        return chef_api_url(self.name, self.version, registry=baseurl)

    def compute_normalized_license(self):
        return models.compute_normalized_license(self.declared_license)


def chef_download_url(name, version, registry='https://supermarket.chef.io/cookbooks'):
    """
    Return an Chef cookbook download url given a name, version, and base registry URL.

    For example:
    >>> c = chef_download_url('seven_zip', '1.0.4')
    >>> assert c == u'https://supermarket.chef.io/cookbooks/seven_zip/versions/1.0.4/download'
    """
    registry = registry.rstrip('/')
    return '{registry}/{name}/versions/{version}/download'.format(**locals())


def chef_api_url(name, version, registry='https://supermarket.chef.io/api/v1'):
    """
    Return a package API data URL given a name, version and a base registry URL.

    For example:
    >>> c = chef_api_url('seven_zip', '1.0.4')
    >>> assert c == u'https://supermarket.chef.io/api/v1/cookbooks/seven_zip/versions/1.0.4'
    """
    registry = registry.rstrip('/')
    return '{registry}/cookbooks/{name}/versions/{version}'.format(**locals())


def is_metadata_json(location):
    """
    Return True if `location` path is for a Chef metadata.json file.
    The metadata.json is also used in Python installed packages in a 'dist-info'
    directory.
    """
    return (
        filetype.is_file(location)
        and fileutils.file_name(location).lower() == 'metadata.json'
        and not fileutils.file_name(fileutils.parent_directory(location))
            .lower().endswith('dist-info')
    )


def is_metadata_rb(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'metadata.rb')


class ChefMetadataFormatter(Formatter):

    def format(self, tokens, outfile):
        """
        Parse lines from a Chef `metadata.rb` file.

        For example, a field in `metadata.rb` can look like this:

        name               "python"

        `RubyLexer()` interprets the line as so:

        ['Token.Name.Builtin', "u'name'"],
        ['Token.Text', "u'              '"],
        ['Token.Literal.String.Double', 'u\'"\''],
        ['Token.Literal.String.Double', "u'python'"],
        ['Token.Literal.String.Double', 'u\'"\''],
        ['Token.Text', "u'\\n'"]

        With this pattern of tokens, we iterate through the token stream to
        create a dictionary whose keys are the variable names from `metadata.rb`
        and its values are those variable's values. This dictionary is then dumped
        to `outfile` as JSON.
        """
        metadata = dict(depends={})
        line = []
        identifiers_and_literals = (
            Token.Name,
            Token.Name.Builtin,  # NOQA
            Token.Punctuation,
            Token.Literal.String.Single,  # NOQA
            Token.Literal.String.Double  # NOQA
        )
        quotes = '"', "'"
        quoted = lambda x: (x.startswith('"') and x.endswith('"')) or (x.startswith("'") and value.endswith("'"))

        for ttype, value in tokens:
            # We don't allow tokens that are just '\"' or '\''
            if (ttype in identifiers_and_literals and value not in quotes):
                # Some tokens are strings with leading and trailing quotes, so
                # we remove them
                if quoted(value):
                    value = value[1:-1]
                line.append(value)

            if ttype in (Token.Text,) and value.endswith('\n') and line:
                # The field name should be the first element in the list
                key = line.pop(0)
                # Join all tokens as a single string
                joined_line = ''.join(line)

                # Store dependencies as dependency_name:dependency_requirement
                # in an Object instead of a single string
                if key == 'depends':
                    # Dependencies are listed in the form of dependency,requirement
                    dep_requirement = joined_line.rsplit(',')
                    if len(dep_requirement) == 2:
                        dep_name = dep_requirement[0]
                        requirement = dep_requirement[1]
                    else:
                        dep_name = joined_line
                        requirement = None
                    metadata[key][dep_name] = requirement
                else:
                    metadata[key] = joined_line

                line = []
        json.dump(metadata, outfile)


def parse(location):
    """
    Return a Package object from a metadata.json file or a metadata.rb file or None.
    """
    if is_metadata_json(location):
        with io.open(location, encoding='utf-8') as loc:
            package_data = json.load(loc)
        return build_package(package_data)

    if is_metadata_rb(location):
        with io.open(location, encoding='utf-8') as loc:
            file_contents = loc.read()
        formatted_file_contents = highlight(
            file_contents, RubyLexer(), ChefMetadataFormatter())
        package_data = json.loads(formatted_file_contents)
        return build_package(package_data)


def build_package(package_data):
    """
    Return a Package object from a package_data mapping (from a metadata.json or
    similar) or None.
    """
    name = package_data.get('name')
    version = package_data.get('version')
    if not name or not version:
        # a metadata.json without name and version is not a usable chef package
        # FIXME: raise error?
        return

    maintainer_name = package_data.get('maintainer', '')
    maintainer_email = package_data.get('maintainer_email', '')
    parties = []
    if maintainer_name or maintainer_email:
        parties.append(
            models.Party(
                name=maintainer_name or None,
                role='maintainer',
                email=maintainer_email or None,
            )
        )

    description = package_data.get('description', '') or package_data.get('long_description', '')
    lic = package_data.get('license', '')
    code_view_url = package_data.get('source_url', '')
    bug_tracking_url = package_data.get('issues_url', '')

    dependencies = package_data.get('dependencies', {}) or package_data.get('depends', {})
    package_dependencies = []
    for dependency_name, requirement in dependencies.items():
        package_dependencies.append(
            models.DependentPackage(
                purl=PackageURL(type='chef', name=dependency_name).to_string(),
                scope='dependencies',
                requirement=requirement,
                is_runtime=True,
                is_optional=False,
            )
        )

    return ChefPackage(
        name=name,
        version=version,
        parties=parties,
        description=description.strip() or None,
        declared_license=lic.strip() or None,
        code_view_url=code_view_url.strip() or None,
        bug_tracking_url=bug_tracking_url.strip() or None,
        download_url=chef_download_url(name, version).strip(),
        dependencies=package_dependencies,
    )
