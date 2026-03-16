#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import logging
import os.path
from os.path import dirname
from os.path import join
from pprint import pformat

import attr
import javaproperties
from lxml import etree
from packageurl import PackageURL
from pymaven import artifact
from pymaven import pom

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models
from packagedcode.models import Package
from packagedcode.utils import combine_expressions
from packagedcode.utils import normalize_vcs_url
from packagedcode.utils import VCS_URLS
from textcode import analysis
from typecode import contenttype
from pymaven.pom import strip_namespace


TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
Support Maven2 POMs.
Attempts to resolve Maven properties when possible.
"""

@attr.s()
class MavenPomPackage(models.Package):
    metafiles = ('*.pom', 'pom.xml',)
    extensions = ('.pom',)
    default_type = 'maven'
    default_primary_language = 'Java'

    default_web_baseurl = 'https://repo1.maven.org/maven2'
    default_download_baseurl = 'https://repo1.maven.org/maven2'
    default_api_baseurl = 'https://repo1.maven.org/maven2'

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        if manifest_resource.name.endswith(('pom.xml', '.pom',)):
            # the root is either the parent or further up for poms stored under
            # a META-INF dir
            package_data = manifest_resource.packages
            if not package_data:
                return manifest_resource
            package_data = package_data[0]
            package = Package.create(**package_data)
            ns = package.namespace
            name = package.name
            path = 'META-INF/maven/{ns}/{name}/pom.xml'.format(**locals())
            if manifest_resource.path.endswith(path):
                for ancestor in manifest_resource.ancestors(codebase):
                    if ancestor.name == 'META-INF':
                        jar_root_dir = ancestor.parent(codebase)
                        return jar_root_dir

            return manifest_resource.parent(codebase)

        return manifest_resource

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        return build_url(
            group_id=self.namespace,
            artifact_id=self.name,
            version=self.version,
            filename=None,
            baseurl=baseurl)

    def repository_download_url(self, baseurl=default_download_baseurl):
        qualifiers = self.qualifiers or {}
        filename = build_filename(
            artifact_id=self.name,
            version=self.version,
            extension=qualifiers.get('type') or 'jar',
            classifier=qualifiers.get('classifier'))

        return build_url(
            group_id=self.namespace,
            artifact_id=self.name,
            version=self.version,
            filename=filename,
            baseurl=baseurl)

    def api_data_url(self, baseurl=default_api_baseurl):
        # treat the POM as "API"
        filename = build_filename(
            artifact_id=self.name,
            version=self.version,
            extension='pom',
            classifier=None)

        return build_url(
            group_id=self.namespace,
            artifact_id=self.name,
            version=self.version,
            filename=filename,
            baseurl=baseurl)

    def compute_normalized_license(self):
        return compute_normalized_license(self.declared_license)

    @classmethod
    def extra_key_files(cls):
        return [
            'META-INF/MANIFEST.MF',
            'META-INF/LICENSE',
        ]

    @classmethod
    def extra_root_dirs(cls):
        return [
            'META-INF'
        ]


def compute_normalized_license(declared_license):
    """
    Return a detected license expression from a declared license mapping.
    """
    if not declared_license:
        return

    detected_licenses = []

    for license_declaration in declared_license:
        # 1. try detection on the value of name if not empty and keep this
        name = license_declaration.get('name')
        via_name = models.compute_normalized_license(name)

        # 2. try detection on the value of url  if not empty and keep this
        url = license_declaration.get('url')
        via_url = models.compute_normalized_license(url)

        # 3. try detection on the value of comment  if not empty and keep this
        comments = license_declaration.get('comments')
        via_comments = models.compute_normalized_license(comments)

        if via_name:
            # The name should have precedence and any unknowns
            # in url and comment should be ignored.
            if via_url == 'unknown':
                via_url = None
            if via_comments == 'unknown':
                via_comments = None

        # Check the three detections to decide which license to keep
        name_and_url = via_name == via_url
        name_and_comment = via_name == via_comments
        all_same = name_and_url and name_and_comment

        if via_name:
            if all_same:
                detected_licenses.append(via_name)

            # name and (url or comment) are same
            elif name_and_url and not via_comments:
                detected_licenses.append(via_name)
            elif name_and_comment and not via_url:
                detected_licenses.append(via_name)

            else:
                # we have some non-unknown license detected in url or comment
                detections = via_name, via_url, via_comments
                detections = [l for l in detections if l]
                if detections:
                    combined_expression = combine_expressions(detections)
                    if combined_expression:
                        detected_licenses.append(combined_expression)
        elif via_url:
            detected_licenses.append(via_url)
        elif via_comments:
            detected_licenses.append(via_comments)

    if detected_licenses:
        return combine_expressions(detected_licenses)


def build_url(group_id, artifact_id, version, filename, baseurl='http://repo1.maven.org/maven2'):
    """
    Return a download URL for a Maven artifact built from its coordinates.
    """
    filename = filename or ''
    group_id = group_id.replace('.', '/')
    path = '{group_id}/{artifact_id}/{version}'.format(**locals())
    return '{baseurl}/{path}/{filename}'.format(**locals())


def build_filename(artifact_id, version, extension, classifier):
    """
    Return a filename for a Maven artifact built from its coordinates.
    """
    extension = extension or ''
    classifier = classifier or ''
    if classifier:
        classifier = '-' + classifier
    return '{artifact_id}-{version}{classifier}.{extension}'.format(**locals())


class ParentPom(artifact.Artifact):
    """
    A minimal Artifact subclass used to store parent poms when no POM
    file is available for these.
    """

    def __init__(self, coordinate):
        super(ParentPom, self).__init__(coordinate)

        # add empty, pom.Pom-class-like empty attributes
        self.client = None
        self.dependencies = {}
        self.dependency_management = {}
        self.parent = None
        self.properties = {}
        # TODO: ????
        # self.pom_data/self.pom_data = None

    def resolve(self, **properties):
        """
        Resolve possible parent POM properties using the provided
        properties.
        """
        if not properties:
            return
        self.group_id = MavenPom._replace_props(self.group_id, properties)
        self.artifact_id = MavenPom._replace_props(self.artifact_id, properties)
        self.version = MavenPom._replace_props(self.version, properties)

    def to_dict(self):
        """
        Return a mapping representing this POM
        """
        return dict([
            ('group_id', self.group_id),
            ('artifact_id', self.artifact_id),
            ('version', str(self.version) if self.version else None),
            ('classifier', self.classifier),
            ('type', self.type),
        ])


class MavenPom(pom.Pom):

    def __init__(self, location=None, text=None):
        """
        Build a POM from a location or unicode text.
        """
        assert (location or text) and (not (location and text))

        if location:
            try:
                with io.open(location, encoding='utf-8') as fh:
                    xml_text = fh.read()
            except UnicodeDecodeError as _a:
                xml_text = analysis.unicode_text(location)
        else:
            xml_text = text
        xml_text = strip_namespace(xml_text)
        xml_text = xml_text.encode('utf-8')
        if TRACE:
            logger.debug('MavenPom.__init__: xml_text: {}'.format(xml_text))

        self._pom_data = etree.fromstring(xml_text, parser=pom.POM_PARSER)

        # collect and then remove XML comments from the XML elements tree
        self.comments = self._get_comments()
        etree.strip_tags(self._pom_data, etree.Comment)

        # FIXME: we do not use a client for now.
        # There are pending issues at pymaven to address this
        self._client = None

        self.model_version = self._get_attribute('modelVersion')
        if not self.model_version:
            # for older POM version 3
            self.model_version = self._get_attribute('pomVersion')
        self.group_id = self._get_attribute('groupId')
        self.artifact_id = self._get_attribute('artifactId')
        if TRACE:
            logger.debug('MavenPom.__init__: self.artifact_id: {}'.format(self.artifact_id))

        self.version = self._get_attribute('version')
        self.classifier = self._get_attribute('classifier')
        self.packaging = self._get_attribute('packaging') or 'jar'
        self.name = self._get_attribute('name')
        self.description = self._get_attribute('description')
        self.inception_year = self._get_attribute('inceptionYear')
        self.url = self._get_attribute('url')
        self.organization_name = self._get_attribute('organization/name')
        self.organization_url = self._get_attribute('organization/url')
        self.licenses = list(self._find_licenses())
        self.developers = list(self._find_parties('developers/developer'))
        self.contributors = list(self._find_parties('contributors/contributor'))
        self.mailing_lists = list(self._find_mailing_lists())
        self.scm = self._find_scm()
        self.issue_management = self._find_issue_management()
        self.ci_management = self._find_ci_management()
        self.distribution_management = self._find_distribution_management()
        self.repositories = list(self._find_repositories('repositories/repository'))
        self.plugin_repositories = list(self._find_repositories('pluginRepositories/pluginRepository'))
        self.modules = self._get_attributes_list('modules/module')

        # FIXME: this attribute should be collected with the parent but
        # is not retrieved yet by pymaven it points to the relative path
        # where to find the full parent POM
        self.parent_relative_path = self._get_attribute('relativePath')  # or '../pom.xml_text'

        # FIXME: Other types that are not collected for now (or
        # indirectly through dependencies management) include: build,
        # reporting, profiles, etc

        # dynamic attributes
        self._parent = None
        self._dep_mgmt = None
        self._dependencies = None
        self._properties = None

    def _extra_properties(self):
        """
        Return a mapping of extra properties
        """
        properties = {}
        for name in build_property_variants('classifier'):
            properties[name] = self.classifier

        for name in build_property_variants('packaging'):
            properties[name] = self.packaging

        for name in build_property_variants('organization.name'):
            properties[name] = self.organization_name

        for name in build_property_variants('organization.url'):
            properties[name] = self.organization_url

        # TODO: collect props defined in a properties file side-by-side the pom file
        # see https://maven.apache.org/shared/maven-archiver/#class_archive
        # this applies to POMs stored inside a JAR or in a plain directory

        return properties

    @classmethod
    def _replace_props(cls, text, properties):
        if not text:
            return text

        def subfunc(matchobj):
            """Return the replacement value for a matched property key."""
            key = matchobj.group(1)
            # does this key contain a substring?
            real_key, start_end = _get_substring_expression(key)
            if not start_end:
                value = properties.get(key)

                return value
            # apply the substring transform
            value = properties.get(real_key)
            if not value:
                return value
            start, end = start_end
            return substring(value, start, end)

        result = pom.PROPERTY_RE.sub(subfunc, text)
        # we loop a maximum of 5 times to avoid cases of infinite recursion
        cycles = 5
        while cycles > 0 and result and pom.PROPERTY_RE.match(result):
            result = pom.PROPERTY_RE.sub(subfunc, result)
            cycles -= 1

        if not result:
            result = text
        return result.strip()

    def _replace_properties(self, text, properties=None):
        # copied from pymavem.pom.Pom
        if not text:
            return text
        if properties is None:
            properties = self.properties
        return MavenPom._replace_props(text, properties)

    def resolve(self, **extra_properties):
        """
        Resolve POM Maven "properties" in attribute values and inherit
        from parent. Update the POM attributes in place. Use the extra
        keywords to override and supplement existing properties.
        """
        # inherit first to get essential parent properties
        # FIXME: the parent needs to be resolved first!? but we have a chicken and egg problem then
        if self.parent:
            pass

        self._inherit_from_parent()

        # then collect properties + extra
        properties = dict(self.properties)
        properties.update(self._extra_properties())

        # "extra" properties are things that would be loaded from a
        # properties files and provided as is they overwrite any
        # existing property
        properties.update(extra_properties)

        if TRACE:
            logger.debug('MavenPom.resolve: properties before self-resolution:\n{}'.format(pformat(properties)))

        # FIXME: we could remove any property that itself contains
        # ${property} as we do not know how to resolve these
        # recursively. Or we could do a single pass on properties to
        # resolve values against themselves
        for key, value in list(properties.items()):
            properties[key] = MavenPom._replace_props(value, properties)

        if TRACE:
            logger.debug('MavenPom.resolve: used properties:\n{}'.format(pformat(properties)))

        # these attributes are plain strings
        plain_attributes = [
            'group_id',
            'version',
            'classifier',
            'packaging',
            'name',
            'description',
            'inception_year',
            'url',
            'organization_name',
            'organization_url',
        ]
        for attr in plain_attributes:
            attr_val = getattr(self, attr, None)
            if not attr_val:
                continue
            resolved = self._replace_properties(attr_val, properties)
            setattr(self, attr, resolved)

        # these attributes are mappings
        mapping_attributes = [
            'scm',
            'issue_management',
            'ci_management',
        ]
        for map_attr in mapping_attributes:
            mapping = getattr(self, map_attr, {})
            if not mapping:
                continue
            for key, value in mapping.items():
                if not value:
                    continue
                mapping[key] = self._replace_properties(value, properties)

        # these attributes are lists of mappings
        mappings_list_attributes = [
            'repositories',
            'plugin_repositories',
        ]
        for lmap_attr in mappings_list_attributes:
            lmapping = getattr(self, lmap_attr, [])
            if not lmapping:
                continue
            for mapping in lmapping:
                for key, value in mapping.items():
                    if not value:
                        continue
                    mapping[key] = self._replace_properties(value, properties)

        # these attributes below are complex nested dicts and/or lists mappings

        for scope, dependencies in self.dependencies.items():
            resolved_deps = []
            # FIXME: this is missing the packaging/type and classifier
            for (group, artifact, version,), required in dependencies:
                group = self._replace_properties(group, properties)
                artifact = self._replace_properties(artifact, properties)
                version = self._replace_properties(version, properties)
                # skip weird damaged POMs such as
                # http://repo1.maven.org/maven2/net/sourceforge/findbugs/coreplugin/1.0.0/coreplugin-1.0.0.pom
                if not group or not artifact:
                    continue
                resolved_deps.append(((group, artifact, version,), required))
            self._dependencies[scope] = resolved_deps

        if TRACE:
            logger.debug('MavenPom.resolve: artifactId after resolve: {}'.format(self.artifact_id))

        # TODO: add:
        # nest dict: 'distribution_management',
        # nest list: 'mailing_lists',

    def _inherit_from_parent(self):
        """
        Update attributes using inheritance from parent attributes. For
        instance, the parent group_id is used if group_id is not defined.
        """
        # TODO: there are more attributes (all) that can be inherited
        if not self.parent:
            return
        if self.group_id is None and self.parent.group_id:
            self.group_id = self.parent.group_id
            if TRACE: logger.debug('_inherit_from_parent: group_id: {}'.format(self.parent.group_id))
        if self.version is None and self.parent.version:
            self.version = str(self.parent.version)
            if TRACE: logger.debug('_inherit_from_parent: version: {}'.format(self.parent.version))
        if not self.classifier is None and self.parent.classifier:
            self.classifier = self.parent.classifier
            if TRACE: logger.debug('_inherit_from_parent: classifier: {}'.format(self.parent.classifier))

        # FIXME: the parent may need to be resolved too?
        # special handling for URLs: see
        # http://maven.apache.org/ref/3.5.0/maven-model-builder/index.html#Inheritance_Assembly
        # Notice that the 5 URLs from the model:
        # project.url,
        # project.scm.connection, project.scm.developerConnection, project.scm.url
        # project.distributionManagement.site.url)
        # ... have a special inheritance handling: if not configured in
        # current model, the inherited value is the parent's one with
        # current artifact id appended.
        if (self.url is None
            and hasattr(self.parent, 'url')
            and getattr(self.parent, 'url', None)
            and self.artifact_id):
            # FIXME: this is not the way to join URLs parts!
            self.url = self.parent.url + self.artifact_id

        # FIXME: this is not the way to join URLs parts!
        parent_scm = getattr(self.parent, 'scm', None)
        if self.scm and parent_scm and self.artifact_id:
            ps_url = parent_scm.get('url')
            if not self.scm.get('url') and ps_url:
                # FIXME: this is not the way to join URLs parts!
                self.scm['url'] = ps_url + self.artifact_id

            ps_connection = parent_scm.get('connection')
            if not self.scm.get('connection') and ps_connection:
                # FIXME: this is not the way to join URLs parts!
                self.scm['connection'] = ps_connection + self.artifact_id

            ps_devconnection = parent_scm.get('developer_connection')
            if not self.scm.get('developer_connection') and ps_devconnection:
                # FIXME: this is not the way to join URLs parts!
                self.scm['developer_connection'] = ps_devconnection + self.artifact_id

        # TODO: distribution_management.site.url

    def _pom_factory(self, group_id, artifact_id, version):
        return ParentPom('%s:%s:pom:%s' % (group_id, artifact_id, version))

    def _get_attribute(self, xpath, xml=None):
        """Return a single value text attribute for a given xpath or None."""
        if xml is None:
            xml = self.pom_data
        attr = xml.findtext(xpath)
        val = attr and attr.strip() or None
        if TRACE:
            if 'artifactId' in xpath:
                logger.debug('MavenPom._get_attribute: xpath: {}'.format(xpath))
                logger.debug('MavenPom._get_attribute: xml: {}'.format(xml))
        return val

    def _get_attributes_list(self, xpath, xml=None):
        """Return a list of text attributes for a given xpath or empty list."""
        if xml is None:
            xml = self.pom_data
        attrs = xml.findall(xpath)
        attrs = [attr.text for attr in attrs]
        return [attr.strip() for attr in attrs if attr and attr.strip()]

    def _get_comments(self, xml=None):
        """Return a list of comment texts or an empty list."""
        if xml is None:
            xml = self.pom_data
        comments = [c.text for c in xml.xpath('//comment()')]
        return [c.strip() for c in comments if c and c.strip()]

    def _find_licenses(self):
        """Return an iterable of license mappings."""
        for lic in self.pom_data.findall('licenses/license'):
            yield dict([
                ('name', self._get_attribute('name', lic)),
                ('url', self._get_attribute('url', lic)),
                ('comments', self._get_attribute('comments', lic)),
                # arcane and seldom used
                ('distribution', self._get_attribute('distribution', lic)),
            ])

    def _find_parties(self, key='developers/developer'):
        """Return an iterable of party mappings for a given xpath."""
        for party in self.pom_data.findall(key):
            yield dict([
                ('id', self._get_attribute('id', party)),
                ('name', self._get_attribute('name', party)),
                ('email', self._get_attribute('email', party)),
                ('url', self._get_attribute('url', party)),
                ('organization', self._get_attribute('organization', party)),
                ('organization_url', self._get_attribute('organizationUrl', party)),
                ('roles', [role.findtext('.') for role in party.findall('roles/role')]),
            ])

    def _find_mailing_lists(self):
        """Return an iterable of mailing lists mappings."""
        for ml in self.pom_data.findall('mailingLists/mailingList'):
            archive_url = self._get_attribute('archive', ml)
            # TODO: add 'otherArchives/otherArchive' as lists?
            yield dict([
                ('name', self._get_attribute('name', ml)),
                ('archive_url', archive_url),
            ])

    def _find_scm(self):
        """Return a version control/scm mapping."""
        scm = self.pom_data.find('scm')
        if scm is None:
            return {}
        return dict([
            ('connection', self._get_attribute('connection', scm)),
            ('developer_connection', self._get_attribute('developer_connection', scm)),
            ('url', self._get_attribute('url', scm)),
            ('tag', self._get_attribute('tag', scm)),
        ])

    def _find_issue_management(self):
        """Return an issue management mapping."""
        imgt = self.pom_data.find('issueManagement')
        if imgt is None:
            return {}
        return dict([
            ('system', self._get_attribute('system', imgt)),
            ('url', self._get_attribute('url', imgt)),
        ])

    def _find_ci_management(self):
        """Return a CI mapping."""
        cimgt = self.pom_data.find('ciManagement')
        if cimgt is None:
            return {}
        return dict([
            ('system', self._get_attribute('system', cimgt)),
            ('url', self._get_attribute('url', cimgt)),
        ])

    def _find_repository(self, xpath, xml=None):
        """Return a repository mapping for an xpath."""
        if xml is None:
            xml = self.pom_data
        repo = xml.find(xpath)
        if repo is None:
            return {}
        return dict([
            ('id', self._get_attribute('id', repo)),
            ('name', self._get_attribute('name', repo)),
            ('url', self._get_attribute('url', repo)),
        ])

    def _find_distribution_management(self):
        """Return a distribution management mapping."""
        dmgt = self.pom_data.find('distributionManagement')
        if dmgt is None:
            return {}
        return dict([
            ('download_url', self._get_attribute('distributionManagement/downloadUrl')),
            ('site', self._find_repository('distributionManagement/site')),
            ('repository', self._find_repository('distributionManagement/repository')),
            ('snapshot_repository', self._find_repository('distributionManagement/snapshotRepository'))
        ])

    def _find_repositories(self, key='repositories/repository'):
        """Return an iterable or repository mappings for an xpath."""
        for repo in self.pom_data.findall(key):
            rep = self._find_repository('.', repo)
            if rep:
                yield rep

    def to_dict(self):
        """
        Return a mapping representing this POM.
        """
        dependencies = {}
        for scope, deps in self.dependencies.items():
            dependencies[scope] = [
                dict([
                    ('group_id', gid),
                    ('artifact_id', aid),
                    ('version', version),
                    ('required', required),
                ])
            for ((gid, aid, version), required) in deps]

        return dict([
            ('model_version', self.model_version),
            ('group_id', self.group_id),
            ('artifact_id', self.artifact_id),
            ('version', self.version),
            ('classifier', self.classifier),
            ('packaging', self.packaging),

            ('parent', self.parent.to_dict() if self.parent else {}),

            ('name', self.name),
            ('description', self.description),
            ('inception_year', self.inception_year),
            ('url', self.url),
            ('organization_name', self.organization_name),
            ('organization_url', self.organization_url),

            ('licenses', self.licenses or []),

            ('developers', self.developers or []),
            ('contributors', self.contributors or []),

            ('modules', self.modules or []),
            ('mailing_lists', self.mailing_lists),
            ('scm', self.scm),
            ('issue_management', self.issue_management),
            ('ci_management', self.ci_management),
            ('distribution_management', self.distribution_management),
            ('repositories', self.repositories),
            ('plugin_repositories', self.plugin_repositories),
            ('dependencies', dependencies or {}),
        ])


def build_property_variants(name):
    """
    Return an iterable of property variant names given a a property name.
    """
    yield name
    yield 'project.{}'.format(name)
    yield 'pom.{}'.format(name)


def _get_substring_expression(text):
    """
    Return a tuple of (text, start/end) such that:

    - if there is a substring() expression in text, the returned text
    has been stripped from it and start/end is a tuple representing
    slice indexes for the substring expression.

    - if there is no substring() expression in text, text is returned
    as-is and start/end is None.

    For example:
    >>> assert ('pom.artifactId', (8, None)) == _get_substring_expression('pom.artifactId.substring(8)')
    >>> assert ('pom.artifactId', None) == _get_substring_expression('pom.artifactId')
    """
    key, _, start_end = text.partition('.substring(')
    if not start_end:
        return text, None

    start_end = start_end.rstrip(')')
    start_end = [se.strip() for se in start_end.split(',')]

    # we cannot parse less than 1 and more than 2 slice indexes
    if len(start_end) not in (1, 2):
        return text, None

    # we cannot parse slice indexes that are not numbers
    if not all(se.isdigit() for se in start_end):
        return text, None
    start_end = [int(se) for se in start_end]

    if len(start_end) == 1:
        start = start_end[0]
        end = None
    else:
        start, end = start_end

    return key, (start, end)


def substring(s, start, end):
    """
    Return a slice of s based on start and end indexes (that can be None).
    """
    startless = start is None
    endless = end is None
    if startless and endless:
        return s
    if endless:
        return s[start:]
    if startless:
        return s[:end]
    return s[start:end]


def is_pom(location):
    """
    Return True if the file at location is highly likely to be a POM.
    """
    if (not filetype.is_file(location)
     or not location.endswith(('.pom', 'pom.xml', 'project.xml',))):

        if TRACE: logger.debug('is_pom: not a POM on name: {}'.format(location))
        return

    T = contenttype.get_type(location)
    if T.is_text:

        # check the POM version in the first 150 lines
        with io.open(location, 'rb') as pom:
            for n, line in enumerate(pom):
                if n > 150:
                    break
                if any(x in line for x in
                       (b'http://maven.apache.org/POM/4.0.0',
                        b'http://maven.apache.org/xsd/maven-4.0.0.xsd',
                        b'<modelVersion>',
                        # somehow we can still parse version 3 poms too
                        b'<pomVersion>',)
                       ):
                    return True

    if TRACE: logger.debug('is_pom: not a POM based on type: {}: {}'.format(T, location))


def has_basic_pom_attributes(pom):
    """
    Return True if a POM object has basic attributes needed to make this
    a POM.
    """
    basics = pom.model_version and pom.group_id and pom.artifact_id
    if TRACE and not basics:
        logger.debug(
            'has_basic_pom_attributes: not a POM, incomplete GAV: '
            '"{}":"{}":"{}"'.format(pom.model_version, pom.group_id, pom.artifact_id))
    return basics


def get_maven_pom(location=None, text=None, check_is_pom=False, extra_properties=None):
    """
    Return a MavenPom object from a POM file at `location` or provided as a
    `text` string.
    """
    if location and check_is_pom and not is_pom(location):
        return
    pom = MavenPom(location, text)
    if not extra_properties:
        extra_properties = {}
    # do we have a pom.properties file side-by-side?
    if location and os.path.exists(location):
        parent = fileutils.parent_directory(location)
        pom_properties = os.path.join(parent, 'pom.properties')
        if os.path.exists(pom_properties):
            with open(pom_properties) as props:
                properties = javaproperties.load(props) or {}
                if TRACE:
                    logger.debug('_get_mavenpom: properties: {}'.format(repr(properties)))
            extra_properties.update(properties)
    pom.resolve(**extra_properties)
    # TODO: we cannot do much without these??
    if check_is_pom and not has_basic_pom_attributes(pom):
        if TRACE:
            logger.debug('_get_mavenpom: has_basic_pom_attributes: {}'.format(
                has_basic_pom_attributes(pom)))
        return
    return pom


SUPPORTED_PACKAGING = set([
    u'aar',
    u'apk',
    u'gem',
    u'jar',
    u'nar',
    u'pom',
    u'so',
    u'swc',
    u'tar',
    u'tar.gz',
    u'war',
    u'xar',
    u'zip',
])


def get_dependencies(pom):
    """
    Return a list of Dependent package objects found in a MavenPom `pom` object.
    """
    dependencies = []
    for scope, deps in pom.dependencies.items():
        if TRACE:
            logger.debug('parse: dependencies.deps: {}'.format(deps))
        if scope:
            scope = scope.strip().lower()
        if not scope:
            # maven default
            scope = 'compile'

        for (dgroup_id, dartifact_id, dversion), drequired in deps:
            if TRACE:
                logger.debug('parse: dependencies.deps: {}, {}, {}, {}'.
                             format(dgroup_id, dartifact_id, dversion, drequired))
            # pymaven whart
            if dversion == 'latest.release':
                dversion = None
            dqualifiers = {}
            # FIXME: this is missing from the original Pom parser
            # classifier = dep.get('classifier')
            # if classifier:
            #     qualifiers['classifier'] = classifier
            #
            # packaging = dep.get('type')
            # if packaging and packaging != 'jar':
            #     qualifiers['packaging'] = packaging
            dep_id = models.PackageURL(
                type='maven',
                namespace=dgroup_id,
                name=dartifact_id,
                qualifiers=dqualifiers or None)
            # TODO: handle dependency management and pom type
            is_runtime = scope in ('runtime', 'compile', 'system', 'provided')
            is_optional = bool(scope in ('test',) or not drequired)
            if scope not in (('runtime', 'compile', 'system', 'provided', 'test')):
                is_runtime = True
            dep_pack = models.DependentPackage(
                purl=str(dep_id),
                requirement=dversion,
                scope=scope,
                is_runtime=is_runtime,
                is_optional=is_optional,
                is_resolved=False)
            dependencies.append(dep_pack)

    return dependencies


def get_parties(pom):
    """
    Return a list of Party object found in a MavenPom `pom` object.
    """
    parties = []
    for dev in pom.developers:
        parties.append(
            models.Party(
                type=models.party_person,
                name=dev['name'],
                role='developper',
                email=dev['email'],
                url=dev['url']))

    for cont in pom.contributors:
        parties.append(
            models.Party(
                type=models.party_person,
                name=cont['name'],
                role='contributor',
                email=cont['email'],
                url=cont['url']))

    # FIXME: we are skipping most other organization related fields, roles and the id
    party_name = pom.organization_name
    party_url = pom.organization_url
    if party_name or party_url:
        parties.append(
            models.Party(
                type=models.party_org,
                name=party_name,
                role='owner',
                url=party_url))

    return parties


def parse(location=None, text=None, check_is_pom=True, extra_properties=None):
    """
    Return a MavenPomPackage or None.
    Parse a pom file at `location` or using the provided `text` (one or
    the other but not both).
    Check if the location is a POM if `check_is_pom` is True.
    When resolving the POM, use an optional `extra_properties` mapping
    of name/value pairs to resolve properties.
    """
    pom = get_maven_pom(location, text, check_is_pom, extra_properties)
    if not pom:
        return

    if TRACE:
        logger.debug('parse: pom:.to_dict()\n{}'.format(pformat(pom.to_dict())))

    version = pom.version
    # pymaven whart
    if version == 'latest.release':
        version = None

    qualifiers = {}
    classifier = pom.classifier
    if classifier:
        qualifiers['classifier'] = classifier

    packaging = pom.packaging
    if packaging:
        extension = get_extension(packaging)
        if extension and extension not in ('jar', 'pom'):
            # we use type as in the PURL spec: this is a problematic field with
            # complex defeinition in Maven
            qualifiers['type'] = extension

    declared_license = pom.licenses

    source_packages = []
    # TODO: what does this mean????
    if not classifier and all([pom.group_id, pom.artifact_id, version]):
        spurl = PackageURL(
            type=MavenPomPackage.default_type,
            namespace=pom.group_id,
            name=pom.artifact_id,
            version=version,
            # we hardcode the source qualifier for now...
            qualifiers=dict(classifier='sources'))
        source_packages = [spurl.to_string()]

    pname = pom.name or ''
    pdesc = pom.description or ''
    if pname == pdesc:
        description = pname
    else:
        description = [d for d in (pname, pdesc) if d]
        description = '\n'.join(description)

    issue_mngt = pom.issue_management or {}
    bug_tracking_url = issue_mngt.get('url')

    scm = pom.scm or {}
    vcs_url, code_view_url = build_vcs_and_code_view_urls(scm)


    # FIXME: there are still other data to map in a Package
    package = MavenPomPackage(
        namespace=pom.group_id,
        name=pom.artifact_id,
        version=version,
        qualifiers=qualifiers or None,
        description=description or None,
        homepage_url=pom.url or None,
        declared_license=declared_license or None,
        parties=get_parties(pom),
        dependencies=get_dependencies(pom),
        source_packages=source_packages,
        bug_tracking_url=bug_tracking_url,
        vcs_url=vcs_url,
        code_view_url=code_view_url,
    )
    return package


def build_vcs_and_code_view_urls(scm):
    """
    Return a proper vcs_url and code_view_url from a Maven `scm` mapping or None.
    For example:

    >>> scm = dict(connection='scm:git:git@github.com:histogrammar/histogrammar-scala.git', tag='HEAD', url='https://github.com/histogrammar/histogrammar-scala')
    """

    vcs_url = scm.get('connection') or None
    code_view_url = scm.get('url') or None

    if code_view_url:
        cvu = normalize_vcs_url(code_view_url) or None
        if cvu:
            code_view_url = cvu

    if not vcs_url:
        if code_view_url:
            # we can craft a vcs_url in some cases
            vcs_url = code_view_url
        return vcs_url, code_view_url

    vcs_url = parse_scm_connection(vcs_url)

    # TODO: handle tag
    # vcs_tag = scm.get('tag')

    return vcs_url, code_view_url


def parse_scm_connection(scm_connection):
    """
    Return an SPDX vcs_url given a Maven `scm_connection` string or the string
    as-is if it cannot be parsed.

    See https://maven.apache.org/scm/scm-url-format.html
        scm:<scm_provider><delimiter><provider_specific_part>

    scm:git:git://server_name[:port]/path_to_repository
    scm:git:http://server_name[:port]/path_to_repository
    scm:git:https://server_name[:port]/path_to_repository
    scm:git:ssh://server_name[:port]/path_to_repository
    scm:git:file://[hostname]/path_to_repository
    """

    delimiter = '|' if '|' in scm_connection else ':'
    segments = scm_connection.split(delimiter, 2)
    if not len(segments) == 3:
        # we cannot parse this so we return it as is
        return scm_connection

    _scm, scm_tool, vcs_url = segments
    # TODO: vcs_tool is not yet supported
    normalized = normalize_vcs_url(vcs_url, vcs_tool=scm_tool)
    if normalized:
        vcs_url = normalized

    if not vcs_url.startswith(VCS_URLS):
        if not vcs_url.startswith(scm_tool):
            vcs_url = '{scm_tool}+{vcs_url}'.format(**locals())

    return vcs_url


class MavenRecognizer(object):
    """
    A package recognizer for Maven-based packages.
    """

    def __init__(self):
        return NotImplementedError()

    def recon(self, location):
        for f in  os.listdir(location):
            loc = join(location, f)
            if not filetype.is_file(loc):
                continue
            # a pom is an xml doc
            if not is_pom(location):
                continue

            if f == 'pom.xml':
                # first case: a maven pom.xml inside a META-INF directory
                # such as in META-INF/maven/log4j/log4j/pom.xml
                # the directory tree has a fixed depth
                # as is: META-INF/maven/groupid/artifactid/pom.xml
                # this will typically be inside a binary jar, so we should find
                # a typical structure above
                try:
                    gggp = dirname(dirname(dirname(dirname(loc))))
                    if fileutils.file_name(gggp) == 'META-INF':
                        # recon here: the root of the component is the parent of
                        # META-INF, return that, with a type and the POM
                        # manifest to parse.
                        pass
                except:
                    pass

                # second case: a maven pom.xml at the root of component
                # development tree we should find a few extra clues in the
                # conventional directory structure below for now we take this as
                # being the component root. return that, with a type and the POM
                # manifest to parse.

                pass
            elif f.endswith('.pom'):
                # first case: a maven repo layout
                # the jars are side-by-side with the pom
                # check if there are side-by-side artifacts
                jar = loc.replace('.pom', '.jar')
                if os.path.exists(jar):
                # return that, with a type and the POM manifest to parse.
                    pass

                # second case: a maven .pom nested in META-INF


def get_extension(packaging):
    """
    We only care for certain artifacts extension/packaging/classifier.

    Maven has some intricate interrelated values for these fields
        type, extension, packaging, classifier, language
    See http://maven.apache.org/ref/3.5.4/maven-core/artifact-handlers.html

    These are the defaults:

    type            extension   packaging    classifier   language
    --------------------------------------------------------------
    pom             = type      = type                    none
    jar             = type      = type                    java
    maven-plugin    jar         = type                    java
    ejb             jar         ejb = type                java
    ejb3            = type      ejb3 = type               java
    war             = type      = type                    java
    ear             = type      = type                    java
    rar             = type      = type                    java
    par             = type      = type                    java
    java-source     jar         = type        sources     java
    javadoc         jar         = type        javadoc     java
    ejb-client      jar         ejb           client      java
    test-jar        jar         jar           tests       java
    """

    extensions = set([
        'ejb3',
        'ear',
        'aar',
        'apk',
        'gem',
        'jar',
        'nar',
        'pom',
        'so',
        'swc',
        'tar',
        'tar.gz',
        'war',
        'xar',
        'zip'
    ])

    if packaging in extensions:
        return packaging
    else:
        return 'jar'
