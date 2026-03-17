#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict
import itertools
import logging
import re

from lxml import etree
import six

from .artifact import Artifact
from .utils import memoize
from .utils import parse_source
from .versioning import VersionRange


EMPTY_POM = """\
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>{0.group_id}</groupId>
    <artifactId>{0.artifact_id}</artifactId>
    <version>{0.version}</version>
</project>
"""

POM_PARSER = etree.XMLParser(
    recover=True,
    # we keep comments in case there is a license in the comments
    remove_comments=False,
    remove_pis=True,
    remove_blank_text=True,
    resolve_entities=False
)

PROPERTY_RE = re.compile(r'\$\{(.*?)\}')
STRIP_NAMESPACE_RE = re.compile("<project(.|\\s)*?>", re.MULTILINE)

log = logging.getLogger(__name__)


def strip_namespace(xml_text):
    """
    Return `xml_text` stripping all namespace declaration from the root project
    tag.
    """
    return STRIP_NAMESPACE_RE.sub('<project>', xml_text[xml_text.find('<project'):], count=1)


class Pom(Artifact):
    """Parse a pom file into a python object
    """

    RANGE_CHARS = ('[', '(', ']', ')')

    __slots__ = ("_client", "_parent", "_dep_mgmt", "_dependencies", "_pom_data", "_properties")

    def __init__(self, coordinate, client=None, pom_data=None, ns_aware=False):
        """Create a new Pom instance.
        `coordinate` is a coordinate string using colon separator conventions
        """
        if pom_data is not None:
            # remove all namespaces
            pom_data = strip_namespace(pom_data)
            if isinstance(pom_data, six.text_type):
                pom_data = pom_data.encode("utf-8")
            pom_data = etree.fromstring(pom_data, parser=POM_PARSER)
        self._pom_data = pom_data
        self._client = client

        # dynamic attributes
        self._parent = None
        self._dep_mgmt = None
        self._dependencies = None
        self._properties = None
        super(Pom, self).__init__(coordinate)

    def _find_deps(self, elem=None):
        if elem is None:
            elem = self.pom_data
        dependencies = OrderedDict()

        # find all non-optional, compile dependencies
        deps = _find(elem, "dependencies")
        if deps is None:
            return dependencies
        for elem in _findall(deps, "dependency"):
            group = _findtext(elem, "groupId")
            if group is None:
                log.warning("dependency element without groupId: '%s'", etree.tostring(elem))
                continue
            group = self._replace_properties(group)
            artifact = _findtext(elem, "artifactId")
            if artifact is None:
                log.warning("dependency element without artifactId: '%s'", etree.tostring(elem))
                continue
            artifact = self._replace_properties(artifact)

            if (group, artifact) in self.dependency_management:
                version, scope, optional = self.dependency_management[(group, artifact)]
            else:
                version = optional = scope = None
            local_version = _findtext(elem, "version")
            if local_version is not None:
                version = local_version
            elif version is None:
                # FIXME: Default to the latest released version if no
                # version is specified. I'm not sure if this is the
                # correct behavior, but let's try it for now.
                version = "latest.release"
            version = self._replace_properties(version)

            local_scope = _findtext(elem, "scope")
            if local_scope is not None:
                scope = local_scope
            elif scope is None:
                # if scope is None, then it should be "compile"
                scope = "compile"
            scope = scope.strip()

            local_optional = _findtext(elem, "optional")
            if local_optional is not None:
                optional = local_optional.strip()
            elif optional is None:
                # default optional to false
                optional = "false"
            # convert optional to boolean
            optional = optional == "true"
            dep = ((group, artifact, version), not optional)
            self._add_dep(dependencies, scope, dep)
        return dependencies

    def _find_dependency_management(self, elem=None):
        if elem is None:
            elem = self.pom_data
        dep_mgmt = OrderedDict()
        import_mgmt = OrderedDict()

        dependency_management = _find(elem, "dependencyManagement")
        if dependency_management is None:
            return import_mgmt
        dependencies = _find(dependency_management, "dependencies")
        if dependencies is None:
            return import_mgmt
        for elem in _findall(dependencies, "dependency"):
            group = _findtext(elem, "groupId")
            if group is None:
                log.warning("dependencyManagement/dependencies/dependency element without groupId: '%s'",
                            etree.tostring(elem))
                continue
            group = self._replace_properties(group)
            artifact = _findtext(elem, "artifactId")
            if artifact is None:
                log.warning("dependencyManagement/dependencies/dependency element without artifactId: '%s'",
                            etree.tostring(elem))
                continue
            artifact = self._replace_properties(artifact)
            version = _findtext(elem, "version")
            if version is None:
                log.warning("dependencyManagement/dependencies/dependency element without version: '%s'",
                            etree.tostring(elem))
                continue
            version = self._replace_properties(version)

            optional = _findtext(elem, "optional")
            optional = (optional is not None and optional == "true")
            scope = _findtext(elem, "scope")
            if scope is not None:
                scope = scope.strip()
            if scope == "import":
                import_pom = self._pom_factory(group, artifact, version)
                import_mgmt.update(import_pom.dependency_management)
            dep_mgmt[(group, artifact)] = (version, scope, optional)

        import_mgmt.update(dep_mgmt)
        return import_mgmt

    def _add_dep(self, dependencies, scope, dep):
        """
        Add a dep tuple to the scope of dependencies. Create a new scope as a list if needed.
        Do not add duplicate dep.
        """
        # note: we do not use a set here to keep the orginal ordering of deps
        if scope not in dependencies:
            scope_deps = dependencies[scope] = []
        else:
            scope_deps = dependencies[scope]
        if dep not in scope_deps:
            scope_deps.append(dep)

    def _find_import_deps(self):
        dependencies = OrderedDict()
        # process dependency management to find imports
        for group, artifact in self.dependency_management:
            version, scope, optional = self.dependency_management[(group, artifact)]
            if scope == "import":
                dep = ((group, artifact, version), not optional)
                self._add_dep(dependencies, scope, dep)
        return dependencies

    def _find_prerequisites(self, elem=None):
        if elem is None:
            elem = self.pom_data
        properties = OrderedDict()
        # get prerequisites
        prereqs = _find(elem, "prerequisites")
        if prereqs is None:
            return properties
        for elem in prereqs:
            tag = elem.tag
            properties['prerequisites.' + tag] = elem.text
            properties['project.prerequisites.' + tag] = elem.text
        return properties

    def _find_profiles(self, elem=None):
        if elem is None:
            elem = self.pom_data
        active_profiles = []
        default_profiles = []
        profiles = _find(elem, "profiles")
        if profiles is None:
            return default_profiles
        for p in _findall(profiles, "profile"):
            by_default = _findtext(p, "activation/activeByDefault")
            by_default = by_default is not None and by_default == "true"
            if by_default:
                default_profiles.append(p)
            else:
                jdk = _findtext(p, "activation/jdk")
                if jdk is not None:
                    # attempt some clean up
                    if (jdk.startswith('[') or jdk.startswith("![")) \
                            and jdk.endswith(','):
                        # assume they left off the )
                        jdk += ')'

                    # TODO: make the JDK version selectable
                    if jdk.startswith('!'):
                        vr = VersionRange.fromstring(jdk[1:])
                        if (vr.version and "1.8" != vr.version) \
                                or (not vr.version and "1.8" not in vr):
                            active_profiles.append(p)
                    else:
                        vr = VersionRange.fromstring(jdk)
                        if (vr.version and "1.8" == vr.version) \
                                or (not vr.version and "1.8" in vr):
                            active_profiles.append(p)
        if active_profiles:
            return active_profiles
        return default_profiles

    def _find_properties(self, elem=None):
        if elem is None:
            elem = self.pom_data
        properties = OrderedDict()
        project_properties = _find(elem, "properties")
        if project_properties is not None:
            for prop in project_properties.iterchildren():
                if prop.tag == 'property':
                    name = prop.get('name')
                    value = prop.get('value')
                else:
                    name = prop.tag
                    value = prop.text
                properties[name] = value
        return properties

    def _find_relocations(self, elem=None):
        if elem is None:
            elem = self.pom_data
        dependencies = OrderedDict()
        # process distributionManagement for relocation
        distManagement = _find(elem, "distributionManagement")
        if distManagement is None:
            return dependencies
        relocation = _find(distManagement, "relocation")
        if relocation is None:
            return dependencies
        group = _findtext(relocation, "groupId")
        if group is None:
            group = self.group_id
        else:
            group = self._replace_properties(group)

        artifact = _findtext(relocation, "artifactId")
        if artifact is None:
            artifact = self.artifact_id
        else:
            artifact = self._replace_properties(artifact)

        version = _findtext(relocation, "version")
        if version is None:
            version = self.version
        else:
            version = self._replace_properties(version)

        dep = ((group, artifact, version), True)
        self._add_dep(dependencies, "relocation", dep)

        return dependencies

    def _pom_factory(self, group, artifact, version):
        return Pom("%s:%s:pom:%s" % (group, artifact, version), self._client)

    def _replace_properties(self, text, properties=None):
        """
        Return an updated `text` replacing maven `properties`.
        """
        if properties is None:
            properties = self.properties

        def subfunc(matchobj):
            key = matchobj.group(1)
            return properties.get(key)

        result = PROPERTY_RE.sub(subfunc, text)
        while result and PROPERTY_RE.match(result):
            result = PROPERTY_RE.sub(subfunc, result)

        if not result:
            result = text
        return result.strip()

    def pick_version(self, spec, artifacts):
        """Pick a version from *versions* according to the spec

        Convert spec into maven version range and return the first version in
        *versions* that is within the range.

        :param str spec: a maven version range spec or gradle dynamic version
        :param versions: list of available versions for this artifact
        :type versions: [:py:class:`pymaven.Version`, ...]
        :return: the newest version that matches the spec
        :rtype: str or None
        """
        if spec in ("latest.release", "release"):
            for a in artifacts:
                if 'snapshot' not in str(a.version.version).lower():
                    return str(a.version)
        elif spec in ("latest.integration", "latest"):
            return str(artifacts[0].version)

        vrange = VersionRange.fromstring(spec)
        for artifact in artifacts:
            if artifact.version in vrange:
                return str(artifact.version)

    @property
    @memoize("_dependencies")
    def dependencies(self):
        dependencies = OrderedDict()
        # we depend on our parent
        if isinstance(self.parent, Pom):
            group = self.parent.group_id
            artifact = self.parent.artifact_id
            version = self.parent.version
            dep = ((group, artifact, version), True)
            self._add_dep(dependencies, "compile", dep)

        for scope, deps in itertools.chain(
                six.iteritems(self._find_import_deps()),
                six.iteritems(self._find_deps()),
                six.iteritems(self._find_relocations())):
            for dep in deps:
                self._add_dep(dependencies, scope, dep)

        for profile in self._find_profiles():
            for scope, deps in itertools.chain(
                    six.iteritems(self._find_deps(profile)),
                    six.iteritems(self._find_relocations(profile))):
                for dep in deps:
                    self._add_dep(dependencies, scope, dep)
        return dependencies

    @property
    @memoize("_dep_mgmt")
    def dependency_management(self):
        dep_mgmt = OrderedDict()
        # add parent's block first so we can override it
        if isinstance(self.parent, Pom):
            dep_mgmt.update(self.parent.dependency_management)
        dep_mgmt.update(self._find_dependency_management())
        for profile in self._find_profiles():
            dep_mgmt.update(self._find_dependency_management(profile))
        return dep_mgmt

    @property
    @memoize("_parent")
    def parent(self):
        parent = _find(self.pom_data, "parent")
        if parent is not None:
            group = _findtext(parent, "groupId").strip()
            artifact = _findtext(parent, "artifactId").strip()
            version = _findtext(parent, "version").strip()
            return self._pom_factory(group, artifact, version)

    @property
    @memoize("_pom_data")
    def pom_data(self):
        """
        Return an etree from the current POM data. If a client was provided at
        creation time, use that client to fetch the POM artifact data remotely.
        """
        if self._client is None:
            _pom_data = EMPTY_POM.format(self)
            return etree.fromstring(_pom_data.encode('utf-8'), parser=POM_PARSER)

        contents = self._client.get_artifact(self.coordinate).contents
        with contents as fh:
            contents_text = fh.read()
            if not isinstance(contents_text, six.text_type):
                contents_text = contents_text.decode('utf-8')
            contents_text = strip_namespace(contents_text)
            return etree.fromstring(contents_text, parser=POM_PARSER)

    @property
    @memoize("_properties")
    def properties(self):
        properties = {}
        if isinstance(self.parent, Pom):
            properties.update(self.parent.properties)
        if isinstance(self.parent, Artifact):
            properties['parent.groupId'] = self.parent.group_id
            properties['parent.artifactId'] = self.parent.artifact_id
            properties['parent.version'] = self.parent.version and str(self.parent.version)
            properties['project.parent.groupId'] = self.parent.group_id
            properties['project.parent.artifactId'] = self.parent.artifact_id
            properties['project.parent.version'] = self.parent.version and str(self.parent.version)
            properties['pom.parent.groupId'] = self.parent.group_id
            properties['pom.parent.artifactId'] = self.parent.artifact_id
            properties['pom.parent.version'] = self.parent.version and str(self.parent.version)

        # built-in properties
        properties['artifactId'] = self.artifact_id
        properties['groupId'] = self.group_id
        properties['version'] = self.version and str(self.version)
        properties['project.artifactId'] = self.artifact_id
        properties['project.groupId'] = self.group_id
        properties['project.version'] = self.version and str(self.version)
        properties['pom.artifactId'] = self.artifact_id
        properties['pom.groupId'] = self.group_id
        properties['pom.version'] = self.version and str(self.version)

        properties.update(self._find_properties())
        properties.update(self._find_prerequisites())

        for profile in self._find_profiles():
            profile_properties = self._find_properties(profile)
            properties.update(profile_properties)
        return properties

    def get_dependencies(self):
        return set(self.iter_dependencies())

    def get_build_dependencies(self):
        return set(self.iter_build_dependencies())

    def iter_dependencies(self):
        return itertools.chain(*self.dependencies.values())

    def iter_build_dependencies(self):
        return itertools.chain(
            (d for d, r in self.dependencies.get("compile", set()) if r),
            (d for d, r in self.dependencies.get("import", set()) if r),
            (d for d, r in self.dependencies.get("relocation", set()) if r),
            )

    @classmethod
    def parse(cls, coordinate, source, client=None):
        """Return a :ref:`Pom` object loaded with source. ``source`` can be any
        of the following:

        - a file name/path
        - a file-like object
        - a URL with the file, http(s), or ftp scheme

        :param str coordinate: the maven coordinates of the POM
        :param source: source to parse
        :param client: a :class:`MavenClient`
        :returns: a :ref:`Pom` object
        """
        fh = parse_source(source)
        return cls(coordinate, pom_data=fh.read(), client=client)

    @classmethod
    def fromstring(cls, coordinate, text, client=None):
        """Parses a POM document from a string.

        :param str coordinate: the maven coordinates of the POM
        :param str text: text to parse
        :param client: a :class:`MavenClient`
        :returns: a :ref:`Pom` object
        """
        return cls(coordinate, pom_data=text, client=client)


def _find(elem, tag):
    return elem.find(tag)


def _findall(elem, tag):
    return elem.findall(tag)


def _findtext(elem, tag):
    return elem.findtext(tag)
