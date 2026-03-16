#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import re
from collections import namedtuple
from os import path


def VERSION_PATTERNS_REGEX():
    return [re.compile(x) for x in [
        # Eclipse features
        r'v\d+\.feature\_(\d+\.){1,3}\d+',

        # Common version patterns
        r'(M?(v\d+(\-|\_))?\d+\.){1,3}\d+[A-Za-z0-9]*((\.|\-|_|~)'
            r'(b|B|rc|r|v|RC|alpha|beta|BETA|M|m|pre|vm|G)?\d+((\-|\.)\d+)?)?'
            r'((\.|\-)(((alpha|dev|beta|rc|FINAL|final|pre)(\-|\_)\d+[A-Za-z]?'
            r'(\-RELEASE)?)|alpha|dev(\.\d+\.\d+)?'
            r'|beta|BETA|final|FINAL|release|fixed|(cr\d(\_\d*)?)))?',
        #
        r'[A-Za-z]?(\d+\_){1,3}\d+\_?[A-Za-z]{0,2}\d+',
        #
        r'(b|rc|r|v|RC|alpha|beta|BETA|M|m|pre|revision-)\d+(\-\d+)?',
        #
        r'current|previous|latest|alpha|beta',
        #
        r'\d{4}-\d{2}-\d{2}',
        #
        r'(\d(\-|\_)){1,2}\d',
        #
        r'\d{5,14}',
    ]]


def hint(path):
    """
    Return a version found in a ``path`` or None. Prefix the version with 'v ' if
    the version does not start with v.
    """
    for pattern in VERSION_PATTERNS_REGEX():
        segments = path.split('/')
        # skip the first path segment unless there's only one segment
        first_segment = 1 if len(segments) > 1 else 0
        interesting_segments = segments[first_segment:]
        # we iterate backwards from the end of the paths segments list
        for segment in interesting_segments[::-1]:
            version = re.search(pattern, segment)
            if version:
                v = version.group(0)
                # prefix with v space
                if not v.lower().startswith('v'):
                    v = f'v {v}'
                return v


def is_dot_num(s):
    """
    Return True if a version string `s` is semver-like and composed only of dots
    and numbers.
    """
    return s.strip(".0123456789") == "" and not s.startswith(".") and not s.endswith(".")


common_version_suffixes = (
    "final",
    "release",
    "snapshot",
    "jre",
    "android",
    "pre",
    "alpha",
    "beta",
    "rc",
)
common_dash_version_suffixes = tuple(f"-{s}" for s in common_version_suffixes)


def is_moslty_num(s):
    """
    Return True if a version string `s` is primarily composed only of dots and
    numbers, with a minority of letters.

    >>> is_moslty_num("v11r2")
    True
    """
    dot_segments = s.split(".")
    len_alpha = 0
    len_digit = 0
    first_seg = dot_segments[0].lstrip("vV")
    starts_with_digit = first_seg.isdigit()
    for dot_seg in dot_segments:
        dot_seg = dot_seg.lstrip("vV")
        for seg in re.split("([0-9]+|[a-zA-Z]+)", dot_seg):
            if seg.isdigit():
                len_digit += len(seg)
            elif seg.isalpha() and seg.lower() not in common_version_suffixes:
                len_alpha += len(seg)

    if not len_alpha and not len_digit:
        return False

    if not len_alpha:
        return True

    # we want twice more digits than alphas
    if (2 * len_alpha) < len_digit:
        return True

    if starts_with_digit and len_alpha < len_digit:
        return True

    return False


NameVersion = namedtuple('NameVersion', 'name, version')


def get_jar_nv(filename):
    """
    Return a NameVersion tuple parsed from the JAR `filename` or None.

    For example::
        >>> get_jar_nv('org.eclipse.persistence.antlr_3.2.0.v201302191141.jar')
        NameVersion(name='org.eclipse.persistence.antlr', version='3.2.0.v201302191141')
        >>> get_jar_nv('org.eclipse.persistence.antlr.jar')
        NameVersion(name='org.eclipse.persistence.antlr', version=None)
        >>> get_jar_nv('org.eclipse.persistence.core_2.4.2.v20130514-5956486.jar')
        NameVersion(name='org.eclipse.persistence.core', version='2.4.2.v20130514-5956486')

        >>> get_jar_nv('com.io7m.jareas.checkstyle-0.2.2.jar')
        NameVersion(name='com.io7m.jareas.checkstyle', version='0.2.2')

        >>> get_jar_nv('ant-contrib-1.0b3.jar')
        NameVersion(name='ant-contrib', version='1.0b3')
        >>> get_jar_nv('xpp3-1.1.3.4.C.jar')
        NameVersion(name='xpp3', version='1.1.3.4.C')
        >>> get_jar_nv('ojdbc6_v11r2.jar')
        NameVersion(name='ojdbc6', version='v11r2')

        >>> get_jar_nv('amazon-sqs-java-messaging-lib-1.0.8.jar')
        NameVersion(name='amazon-sqs-java-messaging-lib', version='1.0.8')
        >>> get_jar_nv('annotations-4.1.1.4.jar')
        NameVersion(name='annotations', version='4.1.1.4')
        >>> get_jar_nv('aws-swf-build-tools-1.10.jar')
        NameVersion(name='aws-swf-build-tools', version='1.10')
        >>> get_jar_nv('c3p0-0.9.1.1.jar')
        NameVersion(name='c3p0', version='0.9.1.1')
        >>> get_jar_nv('javax.persistence_2.0.5.v201212031355.jar')
        NameVersion(name='javax.persistence', version='2.0.5.v201212031355')
        >>> get_jar_nv('proto-google-cloud-pubsub-v1-1.95.4.jar')
        NameVersion(name='proto-google-cloud-pubsub-v1', version='1.95.4')

        >>> get_jar_nv('xpp3-1.1.4c.jar')
        NameVersion(name='xpp3', version='1.1.4c')

        >>> get_jar_nv('listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar')
        NameVersion(name='listenablefuture-9999.0-empty-to-avoid-conflict-with-guava', version=None)

        >>> get_jar_nv('aspectjweaver.jar')
        NameVersion(name='aspectjweaver', version=None)
        >>> get_jar_nv('flyway-client.jar')
        NameVersion(name='flyway-client', version=None)
        >>> get_jar_nv('jakarta.xml.bind-api.jar')
        NameVersion(name='jakarta.xml.bind-api', version=None)
        >>> get_jar_nv('javax.enterprise.concurrent.jar')
        NameVersion(name='javax.enterprise.concurrent', version=None)

        >>> get_jar_nv('netty-codec-http-4.1.53.Final.jar')
        NameVersion(name='netty-codec-http', version='4.1.53.Final')
        >>> get_jar_nv('spring-context-3.0.7.RELEASE.jar')
        NameVersion(name='spring-context', version='3.0.7.RELEASE')

        >>> get_jar_nv('guava-30.1-jre.jar')
        NameVersion(name='guava', version='30.1-jre')
        >>> get_jar_nv('guava-30.1.1-android.jar')
        NameVersion(name='guava', version='30.1.1-android')

        >>> get_jar_nv('guava-30.1.1-android.foo')

    """
    if not filename.endswith(".jar"):
        return

    basename, _extension = path.splitext(filename)

    # JAR name/version come in many flavors
    # amazon-sqs-java-messaging-lib-1.0.8.jar  is a plain name-ver
    if "_" in basename:
        # org.eclipse.persistence.antlr_3.2.0.v201302191141.jar
        name, _, version = basename.rpartition("_")
        if (
            is_dot_num(version)
            or is_moslty_num(version)
            or version.lower().endswith(common_version_suffixes)
        ):
            return NameVersion(name, version)

    if "-" in basename:
        # amazon-sqs-java-messaging-lib-1.0.8.jar
        dashname = basename
        suffix = ""
        for cs in common_dash_version_suffixes:
            if dashname.endswith(cs):
                dashname, _, suff = dashname.rpartition("-")
                suffix = f"-{suff}"
                break

        name, _, version = dashname.rpartition("-")
        if (
            is_dot_num(version)
            or is_moslty_num(version)
            or version.lower().endswith(common_version_suffixes)
        ):
            return NameVersion(name, f"{version}{suffix}")

    # no dash, no underscore means no version: org.eclipse.persistence.antlr.jar
    return NameVersion(basename, None)


def get_nupkg_nv(filename):
    """
    Return a NameVersion tuple parsed from the .nupkg NuGet archive `filename`.

    For example (taken from https://stackoverflow.com/questions/51662737/regex-to-parse-package-name-and-version-number-from-nuget-package-filenames/51662926):
        >>> get_nupkg_nv('knockoutjs.3.4.2.nupkg')
        NameVersion(name='knockoutjs', version='3.4.2')
        >>> get_nupkg_nv('log4net.2.0.8.nupkg')
        NameVersion(name='log4net', version='2.0.8')

        >>> get_nupkg_nv('runtime.tizen.4.0.0-armel.microsoft.netcore.jit.2.0.0.nupkg')
        NameVersion(name='runtime.tizen.4.0.0-armel.microsoft.netcore.jit', version='2.0.0')
        >>> get_nupkg_nv('nuget.core.2.7.0-alpha.nupkg')
        NameVersion(name='nuget.core', version='2.7.0-alpha')

        >>> get_nupkg_nv('microsoft.identitymodel.6.1.7600.16394.nupkg')
        NameVersion(name='microsoft.identitymodel', version='6.1.7600.16394')

        >>> get_nupkg_nv('guava.30.1.1.foo')
    """
    if not filename.endswith(".nupkg"):
        return

    basename, _extension = path.splitext(filename)

    # Either the last 3 or 4 segments are all digits in which case this is the
    # version. Otherwise we consider as version anything after the first all
    # digit segment starting from left.

    dot_segments = basename.split(".")
    len_dot_segments = len(dot_segments)
    if len_dot_segments > 4 and all(s.isdigit() for s in dot_segments[-4:]):
        names = dot_segments[:-4]
        versions = dot_segments[-4:]
        return NameVersion(".".join(names), ".".join(versions))

    if len_dot_segments > 3 and all(s.isdigit() for s in dot_segments[-3:]):
        names = dot_segments[:-3]
        versions = dot_segments[-3:]
        return NameVersion(".".join(names), ".".join(versions))

    names = []
    versions = []
    in_version = False
    for seg in dot_segments:
        if in_version:
            versions.append(seg)
            continue

        if not seg.isdigit():
            names.append(seg)
        else:
            versions.append(seg)
            in_version = True

    return NameVersion(".".join(names), ".".join(versions))
