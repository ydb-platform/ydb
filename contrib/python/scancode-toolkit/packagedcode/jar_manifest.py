
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import re

import attr

from commoncode.fileutils import as_posixpath
from packagedcode.utils import normalize_vcs_url
from packagedcode.maven import parse_scm_connection
from packagedcode.models import Package


"""
A JAR/WAR/EAR and OSGi MANIFEST.MF parser and handler.
See https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html
See https://www.osgi.org/bundle-headers-reference/
See https://www.osgi.org/developer/specifications/reference/#Referencecategories
See https://github.com/gradle/gradle/blob/master/subprojects/docs/src/docs/design/gradle-module-metadata-specification.md
See https://github.com/shevek/jdiagnostics/blob/master/src/main/java/org/anarres/jdiagnostics/ProductMetadata.java
"""


@attr.s()
class JavaArchive(Package):
    metafiles = ('META-INF/MANIFEST.MF',)
    extensions = ('.jar', '.war', '.ear')
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip',)
    default_type = 'jar'
    default_primary_language = 'Java'

    @classmethod
    def recognize(cls, location):
        if is_manifest(location):
            yield parse_manifest(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        if manifest_resource.path.lower().endswith('meta-inf/manifest.mf'):
            # the root is the parent of META-INF
            return manifest_resource.parent(codebase).parent(codebase)
        else:
            return manifest_resource


def is_manifest(location):
    """
    Return Trye if the file at location is a Manifest.
    """
    return as_posixpath(location).lower().endswith('meta-inf/manifest.mf')


def parse_manifest(location):
    """
    Return a Manifest parsed from the file at `location` or None if this
    cannot be parsed.         """
    mode = 'r'
    with open(location, mode) as manifest:
        return parse_manifest_data(manifest.read())


def parse_manifest_data(manifest):
    """
    Return a list of mapping, one for each manifest section (where the first
    entry is the main section) parsed from a `manifest` string.
    """
    # normalize line endings then split each section: they are separated by two LF
    lines = '\n'.join(manifest.splitlines(False))
    sections = re.split('\n\n+', lines)
    return [parse_section(s) for s in sections]


def parse_section(section):
    """
    Return a mapping of key/values for a manifest `section` string
    """
    data = {}
    for line in section.splitlines(False):
        if not line:
            continue
        if not line.startswith(' '):
            # new key/value
            key, _, value = line.partition(': ')
            data[key] = value
        else:
            # continuation of the previous value
            data[key] += line[1:]
    return data


def get_normalized_package_data(manifest_main_section):
    """
    Return a mapping of package-like data normalized from a mapping of the
    `manifest_main_section` data or None.

    Maven Archiver does this:
        Manifest-Version: 1.0
        Created-By: Apache Maven ${maven.version}
        Built-By: ${user.name}
        Build-Jdk: ${java.version}
        Specification-Title: ${project.name}
        Specification-Vendor: ${project.organization.name}
        Implementation-Title: ${project.name}
        Implementation-Vendor-Id: ${project.groupId}
        Implementation-Version: ${project.version}
        Implementation-Vendor: ${project.organization.name}
        Implementation-URL: ${project.url}
    See https://maven.apache.org/shared/maven-archiver/examples/manifest.html
    """
    if not manifest_main_section or len(manifest_main_section) == 1:
        # only a manifest version
        return

    def dget(s):
        v = manifest_main_section.get(s)
        if v and v.startswith(('%', '$', '{')):
            v = None
        return v

    built_with_gradle = bool(dget('Gradle-Version'))

    # Name, namespace, version
    #########################
    # from Eclipse OSGi
    # Bundle-SymbolicName: org.eclipse.ui.workbench.compatibility
    # Bundle-SymbolicName: org.eclipse.ui.intro.universal;singleton:=true
    b_sym_name = dget('Bundle-SymbolicName')
    if b_sym_name and ';' in b_sym_name:
        b_sym_name, _, _ = b_sym_name.partition(';')
    is_osgi_bundle = bool(b_sym_name)

    # Implementation-Title: org.apache.xerces.impl.Version
    # Implementation-Title: Apache Commons IO
    i_title = dget('Implementation-Title')
    i_title_is_id = is_id(i_title)

    # if present this is typically gid.aid (but with no clear split)
    # Extension-Name: org.apache.commons.logging
    ext_nm = dget('Extension-Name')
    if ext_nm == b_sym_name:
        ext_nm = None
    ext_nm_is_id = is_id(ext_nm)

    # Automatic-Module-Name: org.apache.commons.io
    am_nm = dget('Automatic-Module-Name')
    if am_nm == b_sym_name:
        am_nm = None
    am_nm_is_id = is_id(am_nm)

    # Name: Datalogic SDK
    nm = dget('Name')
    nm_is_id = is_id(nm)

    # this a namespace
    # Implementation-Vendor-Id: org.apache
    # Implementation-Vendor-Id: commons-io
    # Implementation-Vendor-Id: ${project.groupId}
    i_vendid = dget('Implementation-Vendor-Id')

    # Bundle-Version: 3.2.200.v20080610
    # Implementation-Version: 2.6.2
    # ImplementationVersion
    b_version = dget('Bundle-Version')
    i_version = dget('Implementation-Version') or dget('ImplementationVersion')

    # Descriptions
    #########################
    # the Bundle-Name is always a short description
    # Bundle-Name: DejaCode Toolkit
    # Bundle-Name: %pluginName
    # Bundle-Name: %fragmentName
    b_name = dget('Bundle-Name')

    # Bundle-Description: Apache Log4j 1.2
    b_desc = dget('Bundle-Description')

    s_title = dget('Specification-Title')
    if s_title in (i_title, b_name, b_desc,):
        s_title = None

    # Implementation-Title structured by Gradle if Gradle-Version: is present
    # Implementation-Title: com.netflix.hystrix#hystrix-rx-netty-metrics-stream;1.5.12
    it_namespace = it_name = it_version = None
    it_split = re.split('[#;]', i_title or '')
    if len(it_split) == 3:
        it_namespace, it_name, it_version = it_split
    has_gradle_structured_i_title = i_title_is_id and it_namespace and it_name and it_version

    # Set ns, name and version
    ##############################
    package_type = namespace = name = version = None
    descriptions = []

    # FIXME: may be we should then return each "personality"
    # we have several cases for names:
    # this is built with gradle and we have good id data
    if has_gradle_structured_i_title:
        package_type = 'maven'
        namespace = it_namespace
        name = it_name
        version = it_version
        descriptions = [nm, s_title, b_name, b_desc]

    # we have been created by maven archiver
    elif i_title and i_vendid and i_version:
        # TODO: improve name and namespace if ns is in name
        namespace = i_vendid
        name = i_title
        package_type = 'maven' if (i_title_is_id and not name.startswith(namespace)) else 'jar'
        version = i_version
        descriptions = [b_name, b_desc]

    # TODO: add case with only title + version that can still be handled if title is dotted

    # this is an OSGi bundle and we have enough to build a bundle
    elif is_osgi_bundle:
        # no namespace
        name = b_sym_name
        version = b_version
        descriptions = [b_name, b_desc]
        package_type = 'osgi'

    # we have not much data
    else:
        package_type = 'jar'
        # no namespace
        version = i_version

        if i_title_is_id:
            name = i_title
            descriptions = [s_title, nm]
        elif am_nm_is_id:
            name = am_nm
            descriptions = [s_title, i_title, nm]
        elif ext_nm_is_id:
            name = ext_nm
            descriptions = [s_title, i_title, nm]
        elif nm_is_id:
            name = nm
            descriptions = [s_title, i_title]
        else:
            name = i_title or am_nm or ext_nm or nm
            descriptions = [s_title, i_title, nm]

    descriptions = unique(descriptions)
    descriptions = [d for d in descriptions if d and d.strip() and d != name]
    description = '\n'.join(descriptions)
    if description == name:
        description = None

    # create the mapping we will return
    package = {}
    package['type'] = package_type
    package['namespace'] = namespace
    package['name'] = name
    package['version'] = version
    package['description'] = description

    # licensing
    #########################
    # Bundle-License: http://www.apache.org/licenses/LICENSE-2.0.txt
    package['declared_license'] = dget('Bundle-License')
    # Bundle-Copyright: Apache 2.0
    package['copyright'] = dget('Bundle-Copyright')

    # URLs
    #########################
    # typically homepage or DOC
    # Implementation-Url
    # Implementation-URL: http://xml.apache.org/xerces2-j/
    package['homepage_url'] = dget('Implementation-URL') or dget('Implementation-Url')

    # Bundle-DocURL: http://logging.apache.org/log4j/1.2
    package['documentation_url'] = dget('Bundle-DocURL')

    # vendor/owner/contact
    #########################
    package['parties'] = parties = []
    # Implementation-Vendor: Apache Software Foundation
    # Implementation-Vendor: The Apache Software Foundation
    i_vend = dget('Implementation-Vendor')
    if i_vend:
        parties.append(dict(role='vendor', name=i_vend))

    # Specification-Vendor: Sun Microsystems, Inc.
    s_vend = dget('Specification-Vendor')
    if s_vend == i_vend:
        s_vend = None
    if s_vend:
        parties.append(dict(role='spec-vendor', name=s_vend))

    # Bundle-Vendor: %providerName
    # Bundle-Vendor: %provider_name
    # Bundle-Vendor: Apache Software Foundation
    # Bundle-Vendor: http://supercsv.sourceforge.net/ and http://spiffyframe
    b_vend = dget('Bundle-Vendor') or dget('BundleVendor')
    if b_vend:
        v = dict(role='vendor', name=b_vend)
        if v not in parties:
            parties.append(v)

    # Module-Email: netflixoss@netflix.com
    # Module-Owner: netflixoss@netflix.com
    m_email = dget('Module-Email')
    m_owner = dget('Module-Owner')
    if m_owner:
        o = dict(role='owner', name=m_owner)
        if m_email and m_email != m_owner:
            o['email'] = m_email
        parties.append(o)

    # VCS
    # the model is <vcs_tool>+<transport>://<host_name>[/<path_to_repository>][@<revision_tag_or_branch>][#<sub_path>]
    #########################
    vcs_url = None
    code_view_url = None


    m_vcs_url = dget('Module-Origin') or ''
    if m_vcs_url.strip():
        # this block comes from Gradle?
        # Module-Origin: git@github.com:Netflix/Hystrix.git
        # Module-Source: /hystrix-contrib/hystrix-rx-netty-metrics-stream
        # Branch: master
        # Change: a7b66ca
        m_vcs_url = normalize_vcs_url(m_vcs_url)
        m_vcs_rev = dget('Change') or dget('Branch') or ''
        m_vcs_rev = m_vcs_rev.strip()
        m_vcs_rev = m_vcs_rev and ('@' + m_vcs_rev)
        m_vcs_subpath = dget('Module-Source') or ''
        m_vcs_subpath = m_vcs_subpath.strip('/').strip()
        m_vcs_subpath = m_vcs_subpath and ('#' + m_vcs_subpath.strip('/'))
        vcs_url = '{m_vcs_url}{m_vcs_rev}{m_vcs_subpath}'.format(**locals())
    else:
        # this block comes from Maven?
        # Scm-Url: http://github.com/fabric8io/kubernetes-model/kubernetes-model/
        # Scm-Connection: scm:git:https://github.com/fabric8io/zjsonpatch.git
        # Scm-Revision: ${buildNumber}
        # Scm-Revision: 4ec4abe2e7ac9e1a5e4be88e6dd09403592f9512
        s_vcs_url = dget('Scm-Url') or ''
        s_scm_connection = dget('Scm-Connection') or ''

        s_vcs_rev = dget('Scm-Revision') or ''
        s_vcs_rev = s_vcs_rev.strip()
        if s_vcs_rev:
            s_vcs_rev = '@' + s_vcs_rev

        if s_vcs_url.strip():
            code_view_url = s_vcs_url
            s_vcs_url = normalize_vcs_url(s_vcs_url)
            vcs_url = '{s_vcs_url}{s_vcs_rev}'.format(**locals())
        elif s_scm_connection.strip():
            vcs_url = parse_scm_connection(s_scm_connection)
            vcs_url = '{s_vcs_url}{s_vcs_rev}'.format(**locals())

    package['vcs_url'] = vcs_url
    package['code_view_url'] = code_view_url

    # Misc, unused for now
    #########################
    # Source:
    # Eclipse-SourceBundle: org.eclipse.jetty.websocket.api;version="9.4.12.v20180830";roots:="."
    # Deps:
    # Require-Bundle

    package['notes'] = dget('Comment')
    return package


def is_id(s):
    """
    Return True if `s` is some kind of id.
    """
    return s and ' ' not in s.strip()


def unique(objects):
    """
    Return a list of unique objects.
    """
    uniques = []
    for obj in objects:
        if obj not in uniques:
            uniques.append(obj)
    return uniques
