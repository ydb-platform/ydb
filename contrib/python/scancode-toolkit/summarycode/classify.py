#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#



from commoncode.datautils import Boolean
from commoncode.fileset import get_matches
from plugincode.pre_scan import PreScanPlugin
from plugincode.pre_scan import pre_scan_impl
from plugincode.post_scan import PostScanPlugin
from plugincode.post_scan import post_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import PRE_SCAN_GROUP

"""
Tag files as "key" or important and top-level files.
"""

# Tracing flag
TRACE = False

def logger_debug(*args):
    pass


if TRACE:
    import logging
    import click

    class ClickHandler(logging.Handler):
        _use_stderr = True

        def emit(self, record):
            try:
                msg = self.format(record)
                click.echo(msg, err=self._use_stderr)
            except Exception:
                self.handleError(record)


    logger = logging.getLogger(__name__)
    logger.handlers = [ClickHandler()]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)


    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


@pre_scan_impl
class FileClassifier(PreScanPlugin):
    """
    Classify a file such as a COPYING file or a package manifest with a flag.
    """
    resource_attributes = dict([
        ('is_legal',
         Boolean(help='True if this file is likely a "legal", license-related'
                      'file such as a COPYING or LICENSE file.')),

        ('is_manifest',
         Boolean(help='True if this file is likely a package manifest file such '
                      'as a Maven pom.xml or an npm package.json')),

        ('is_readme',
         Boolean(help='True if this file is likely a README file.')),

        ('is_top_level',
         Boolean(help='True if this file is "top-level" file located either at '
                      'the root of a package or in a well-known common location.')),

        ('is_key_file',
         Boolean(help='True if this file is "top-level" file and either a '
                      'legal, readme or manifest file.')),

#         ('is_doc',
#          Boolean(help='True if this file is likely a documentation file.')),
#
#         ('is_test',
#          Boolean(help='True if this file is likely a test file.')),
#
#         ('is_generated',
#          Boolean(help='True if this file is likely an automatically generated file.')),
#
#         ('is_build',
#          Boolean(help='True if this file is likely a build script or file such as Makefile.')),
#
#         we have an is_data attribute
#         ('is_data',
#          Boolean(help='True if this file is likely data file.')),

    ])

    sort_order = 50

    options = [
        PluggableCommandLineOption(('--classify',),
            is_flag=True, default=False,
            help='Classify files with flags telling if the file is a legal, '
                 'or readme or test file, etc.',
            help_group=PRE_SCAN_GROUP,
            sort_order=50,
        )
    ]

    def is_enabled(self, classify, **kwargs):
        return classify

    def process_codebase(self, codebase, classify, **kwargs):
        if not classify:
            return

        # find the real root directory
        real_root = codebase.lowest_common_parent()
        if not real_root:
            real_root = codebase.root
        real_root_dist = real_root.distance(codebase)

        for resource in codebase.walk(topdown=True):
            real_dist = resource.distance(codebase) - real_root_dist
            # this means this is either a child of the root dir or the root itself.
            resource.is_top_level = (real_dist < 2)
            if resource.is_file:
                # TODO: should we do something about directories? for now we only consider files
                set_classification_flags(resource)
            resource.save(codebase)


def get_relative_path(root_path, path):
    """
    Return a path relativefrom the posix 'path' relative to a
    base path of `len_base_path` length where the base is a directory if
    `base_is_dir` True or a file otherwise.
    """
    return path[len(root_path):].lstrip('/')


@post_scan_impl
class PackageTopAndKeyFilesTagger(PostScanPlugin):
    """
    Tag resources as key or top level based on Package-type specific settings.
    """

    sort_order = 0

    def is_enabled(self, classify, **kwargs):
        return classify

    def process_codebase(self, codebase, classify, **kwargs):
        """
        Tag resource as key or top level files based on Package type-specific
        rules.
        """
        if not classify:
            logger_debug('PackageTopAndKeyFilesTagger: return')
            return

        from packagedcode import get_package_class

        if codebase.has_single_resource:
            # What if we scanned a single file and we do not have a root proper?
            return

        root_path = codebase.root.path

        has_packages = hasattr(codebase.root, 'packages')
        if not has_packages:
            # FIXME: this is not correct... we may still have cases where this
            # is wrong: e.g. a META-INF directory and we may not have a package 
            return


        for resource in codebase.walk(topdown=True):
            packages_info = resource.packages or []

            if not packages_info:
                continue
            if not resource.has_children():
                continue

            descendants = None

            for package_info in packages_info:
                package_class = get_package_class(package_info)
                extra_root_dirs = package_class.extra_root_dirs()
                extra_key_files = package_class.extra_key_files()
                if TRACE:
                    logger_debug('PackageTopAndKeyFilesTagger: extra_root_dirs:', extra_root_dirs)
                    logger_debug('PackageTopAndKeyFilesTagger: extra_key_files:', extra_key_files)

                if not (extra_root_dirs or extra_key_files):
                    # FIXME: this is not correct!
                    # we may still have other files under the actual root.
                    continue

                if not descendants:
                    descendants = {
                        get_relative_path(root_path, r.path): r
                                   for r in resource.descendants(codebase)}

                    if TRACE:
                        logger_debug('PackageTopAndKeyFilesTagger: descendants')
                        for rpath, desc in descendants.items():
                            logger_debug('rpath:', rpath, 'desc:', desc)

                for rpath, desc in descendants.items():
                    if extra_root_dirs and get_matches(rpath, extra_root_dirs):
                        if TRACE:
                            logger_debug('PackageTopAndKeyFilesTagger: get_matches for:', rpath, desc)
                        desc.is_top_level = True
                        if desc.is_file:
                            set_classification_flags(desc)
                        desc.save(codebase)

                        for child in desc.children(codebase):
                            if TRACE:
                                logger_debug('PackageTopAndKeyFilesTagger: set is_top_level for:', child)

                            child.is_top_level = True
                            if child.is_file:
                                set_classification_flags(child)
                            child.save(codebase)

                    if extra_key_files and get_matches(rpath, extra_key_files):
                        desc.is_key_file = True
                        desc.save(codebase)


LEGAL_STARTS_ENDS = (
    'copying',
    'copyright',
    'copyrights',

    'copyleft',
    'notice',
    'license',
    'licenses',
    'licence',
    'licences',
    'licensing',
    'licencing',

    'legal',
    'eula',
    'agreement',
    'copyleft',
    'patent',
    'patents',
)


_MANIFEST_ENDS = {
    '.about': 'ABOUT file',
    '/bower.json': 'bower',
    '/project.clj': 'clojure',
    '.podspec': 'cocoapod',
    '/composer.json': 'composer',
    '/description': 'cran',
    '/elm-package.json': 'elm',
    '/+compact_manifest': 'freebsd',
    '+manifest': 'freebsd',
    '.gemspec': 'gem',
    '/metadata': 'gem',
    # the extracted metadata of a gem archive
    '/metadata.gz-extract': 'gem',
    '/build.gradle': 'gradle',
    '/project.clj': 'clojure',
    '.pom': 'maven',
    '/pom.xml': 'maven',

    '.cabal': 'haskell',
    '/haxelib.json': 'haxe',
    '/package.json': 'npm',
    '.nuspec': 'nuget',
    '.pod': 'perl',
    '/meta.yml': 'perl',
    '/dist.ini': 'perl',

    '/pipfile': 'pypi',
    '/setup.cfg': 'pypi',
    '/setup.py': 'pypi',
    '/PKG-INFO': 'pypi',
    '/pyproject.toml': 'pypi', 
    '.spec': 'rpm',
    '/cargo.toml': 'rust',
    '.spdx': 'spdx',
    '/dependencies': 'generic',

    # note that these two cannot be top-level for now
    'debian/copyright': 'deb',
    'meta-inf/manifest.mf': 'maven',

    # TODO: Maven also has sometimes a pom under META-INF/
    # 'META-INF/manifest.mf': 'JAR and OSGI',

}


MANIFEST_ENDS = tuple(_MANIFEST_ENDS)


README_STARTS_ENDS = (
    'readme',
)


def set_classification_flags(resource,
        _LEGAL=LEGAL_STARTS_ENDS,
        _MANIF=MANIFEST_ENDS,
        _README=README_STARTS_ENDS):
    """
    Set classification flags on the `resource` Resource
    """
    name = resource.name.lower()
    path = resource.path.lower()

    resource.is_legal = is_legal = name.startswith(_LEGAL) or name.endswith(_LEGAL)
    resource.is_readme = is_readme = name.startswith(_README) or name.endswith(_README)
    resource.is_manifest = is_manifest = path.endswith(_MANIF)
    resource.is_key_file = (resource.is_top_level
                            and (is_readme or is_legal or is_manifest))
    return resource
