import collections
import logging

import build.build_opts
import core.yarg
import devtools.ya.test.opts as test_opts
from devtools.ya.package import const

logger = logging.getLogger(__name__)


COMMON_SUBGROUP = core.yarg.Group('Common', 1)
TAR_SUBGROUP = core.yarg.Group('Tar', 2)
DEB_SUBGROUP = core.yarg.Group('Debian', 3)
DOCKER_SUBGROUP = core.yarg.Group('Docker', 4)
AAR_SUBGROUP = core.yarg.Group('Aar', 5)
RPM_SUBGROUP = core.yarg.Group('Rpm', 6)
NPM_SUBGROUP = core.yarg.Group('Npm', 7)
PYTHON_WHEEL_SUBGROUP = core.yarg.Group('Python wheel', 8)


class PackageOperationalOptions(core.yarg.Options):
    def __init__(self):
        self.artifactory_password_path = None
        self.build_debian_scripts = False
        self.build_only = False
        self.change_log = None
        self.cleanup = True
        self.codec = None
        self.convert = None
        self.custom_data_root = None
        self.custom_tests_data_root = None
        self.debian_distribution = 'unstable'
        self.debian_upload_token = None  # please, do not remove, we really need it in opensource nebius ya
        self.docker_no_cache = False
        self.docker_push_image = False
        self.docker_remote_image_version = None
        self.docker_use_remote_cache = False
        self.dump_build_targets = None
        self.dump_inputs = None
        self.ignore_fail_tests = False
        self.list_codecs = False
        self.nanny_release = None
        self.package_output = None
        self.packages = []
        self.publish_to = {}
        self.raw_package_path = None
        self.run_long_tests = False
        self.sandbox_download_protocols = []
        self.upload = False
        self.wheel_access_key_path = None
        self.wheel_secret_key_path = None

    @staticmethod
    def consumer():
        return [
            core.yarg.ArgConsumer(
                names=['--publish-to'],
                help='Publish package to the specified dist',
                hook=core.yarg.DictPutHook('publish_to'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--build-only'],
                hook=core.yarg.SetConstValueHook('build_only', True),
                visible=False,
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--change-log'],
                help='Change log text or path to the existing changelog file',
                hook=core.yarg.SetValueHook('change_log'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--new'],
                help='Use new ya package json format',
                hook=core.yarg.SetConstValueHook('convert', False),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--old'],
                help='Use old ya package json format',
                hook=core.yarg.SetConstValueHook('convert', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--tests-data-root'],
                help="Custom location for arcadia_tests_data dir, defaults to <source root>/../arcadia_tests_data",
                hook=core.yarg.SetValueHook('custom_tests_data_root'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--data-root'],
                help="Custom location for data dir, defaults to <source root>/../data",
                hook=core.yarg.SetValueHook('custom_data_root'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--artifactory-password-path'],
                help='Path to file with artifactory password',
                hook=core.yarg.SetValueHook('artifactory_password_path'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.EnvConsumer(
                'YA_ARTIFACTORY_PASSWORD_PATH',
                help='Path to file with artifactory password',
                hook=core.yarg.SetValueHook('artifactory_password_path'),
            ),
            core.yarg.ArgConsumer(
                names=['--dump-arcadia-inputs'],
                help='Only dump inputs, do not build package',
                hook=core.yarg.SetValueHook('dump_inputs'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--ignore-fail-tests'],
                help='Create package, no matter tests failed or not',
                hook=core.yarg.SetConstValueHook('ignore_fail_tests', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--codec'],
                help='Codec name for uc compression',
                hook=core.yarg.SetValueHook('codec'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--codecs-list'],
                help='Show available codecs for --uc',
                hook=core.yarg.SetConstValueHook('list_codecs', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                ["-O", "--package-output"],
                help="Specifies directory for package output",
                hook=core.yarg.SetValueHook('package_output'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.FreeArgConsumer(
                help='Package description file name(s)',
                hook=core.yarg.SetValueHook(name='packages'),
            ),
            core.yarg.ArgConsumer(
                ['--sandbox-download-protocol'],
                help='Sandbox download protocols comma-separated (default: http,http_tgz)',
                hook=core.yarg.SetValueHook(
                    'sandbox_download_protocols', transform=lambda val: [_f for _f in val.split(",") if _f]
                ),
                visible=False,
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--wheel-repo-access-key'],
                help='Path to access key for wheel repository',
                hook=core.yarg.SetValueHook('wheel_access_key_path'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=PYTHON_WHEEL_SUBGROUP,
            ),
            core.yarg.EnvConsumer(
                'YA_WHEEL_REPO_ACCESS_KEY_PATH',
                help='Path to access key for wheel repository',
                hook=core.yarg.SetValueHook('wheel_access_key_path'),
            ),
            core.yarg.ArgConsumer(
                names=['--wheel-repo-secret-key'],
                help='Path to secret key for wheel repository',
                hook=core.yarg.SetValueHook('wheel_secret_key_path'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=PYTHON_WHEEL_SUBGROUP,
            ),
            core.yarg.EnvConsumer(
                'YA_WHEEL_SECRET_KEY_PATH',
                help='Path to secret key for wheel repository',
                hook=core.yarg.SetValueHook('wheel_secret_key_path'),
            ),
            core.yarg.ArgConsumer(
                names=['--raw-package-path'],
                help="Custom path for raw-package (implies --raw-package)",
                hook=core.yarg.SetValueHook('raw_package_path'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--no-cleanup'],
                help='Do not clean the temporary directory',
                hook=core.yarg.SetConstValueHook('cleanup', False),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--build-debian-scripts'],
                hook=core.yarg.SetConstValueHook('build_debian_scripts', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--debian-distribution'],
                help='Debian distribution',
                hook=core.yarg.SetValueHook('debian_distribution'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.EnvConsumer(
                'YA_DEBIAN_UPLOAD_TOKEN',
                help='Iam token or path to iam token for nebiuscloud debian repository',
                hook=core.yarg.SetValueHook('debian_upload_token'),
            ),
            core.yarg.ArgConsumer(
                names=['--docker-push'],
                help='Push docker image to registry',
                hook=core.yarg.SetConstValueHook('docker_push_image', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-no-cache'],
                help='Disable docker cache',
                hook=core.yarg.SetConstValueHook('docker_no_cache', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ConfigConsumer("docker_no_cache"),
            core.yarg.ArgConsumer(
                names=['--dump-build-targets'],
                hook=core.yarg.SetValueHook('dump_build_targets'),
                visible=False,
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-use-remote-cache'],
                help='Use image from registry as cache source',
                hook=core.yarg.SetConstValueHook('docker_use_remote_cache', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-remote-image-version'],
                help='Specify image version to be used as cache source',
                hook=core.yarg.SetValueHook('docker_remote_image_version'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--nanny-release'],
                help='Notify nanny about new release',
                hook=core.yarg.SetValueHook(
                    'nanny_release', transform=lambda s: s.upper(), values=const.NANNY_RELEASE_TYPES
                ),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                ['--upload'],
                help='Upload created package to sandbox',
                hook=core.yarg.SetConstValueHook('upload', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
        ]

    def postprocess(self):
        if self.convert is not None:
            logger.warning("Package format will be detected automatically, no need to use --new and --old")
        if self.nanny_release and not self.docker_push_image:
            raise core.yarg.ArgsValidatingException("Using --nanny-release without --docker-push is pointless")

    def postprocess2(self, params):
        if params.raw_package_path and not params.raw_package:
            params.raw_package = True
        # old params compatibility
        if getattr(params, 'run_long_tests', False):
            params.run_tests = test_opts.RunTestOptions.RunAllTests


class PackageCustomizableOptions(core.yarg.Options):
    """
    Don't add parameters here by default, otherwise user could use them in package.json.
    For more info see https://docs.yandex-team.ru/ya-make/usage/ya_package/json#params
    """

    deb_compression_levels = collections.OrderedDict(
        sorted(
            {
                'none': 0,
                'low': 3,
                'medium': 6,
                'high': 9,
            }.items(),
            key=lambda i: i[1],
        )
    )

    def __init__(self):
        self.arch_all = False
        self.artifactory = None
        self.compress_archive = True
        self.compression_filter = None
        self.compression_level = None
        self.create_dbg = False
        self.custom_version = None
        self.debian_arch = None
        self.debian_compression_level = None
        self.debian_compression_type = 'gzip'
        self.docker_add_host = []
        self.docker_build_arg = {}
        self.docker_build_network = None
        self.docker_platform = None
        self.docker_registry = "registry.yandex.net"
        self.docker_repository = ""
        self.docker_save_image = False
        self.docker_secrets = []
        self.docker_target = None
        self.dupload_max_attempts = 1
        self.dupload_no_mail = False
        self.ensure_package_published = False
        self.force_dupload = False
        self.format = None
        self.full_strip = False
        self.key = None
        self.overwrite_read_only_files = False
        self.raw_package = False
        self.resource_attrs = {}
        self.resource_type = "YA_PACKAGE"
        self.sandbox_task_id = 0
        self.sign = True
        self.sloppy_deb = False
        self.store_debian = True
        self.strip = False
        self.wheel_platform = ""
        self.wheel_python3 = False

    @staticmethod
    def consumer():
        return [
            core.yarg.ArgConsumer(
                names=['--strip'],
                help='Strip binaries (only debug symbols: "strip -g")',
                hook=core.yarg.SetConstValueHook('strip', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--full-strip'],
                help='Strip binaries',
                hook=core.yarg.SetConstValueHook('full_strip', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--set-sandbox-task-id'],
                visible=False,
                help='Use the provided task id for the package version if needed',
                hook=core.yarg.SetValueHook('sandbox_task_id', int),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--wheel-platform'],
                visible=True,
                help='Set wheel package platform',
                hook=core.yarg.SetValueHook('wheel_platform'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=PYTHON_WHEEL_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--key'],
                help='The key to use for signing',
                hook=core.yarg.SetValueHook('key'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--debian'],
                help='Build debian package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.DEBIAN),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--tar'],
                help='Build tarball package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.TAR),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--no-compression'],
                help="Don't compress tar archive (for --tar only)",
                hook=core.yarg.SetConstValueHook('compress_archive', False),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--create-dbg'],
                help='Create separate package with debug info (works only in case of --strip or --full-strip)',
                hook=core.yarg.SetConstValueHook('create_dbg', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                ["--compression-filter"],
                help="Specifies compression filter (gzip/zstd)",
                hook=core.yarg.SetValueHook('compression_filter'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                ["--compression-level"],
                help="Specifies compression level (0-9 for gzip [6 is default], 0-22 for zstd [3 is default])",
                hook=core.yarg.SetValueHook('compression_level', transform=lambda s: int(s)),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker'],
                help='Build docker',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.DOCKER),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--rpm'],
                help='Build rpm package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.RPM),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=RPM_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--aar'],
                help='Build aar package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.AAR),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=AAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--npm'],
                help='Build npm package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.NPM),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=NPM_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--wheel'],
                help='Build wheel package',
                hook=core.yarg.SetConstValueHook('format', const.PackageFormat.WHEEL),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=PYTHON_WHEEL_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--wheel-python3'],
                help='use python3 when building wheel package',
                hook=core.yarg.SetConstValueHook('wheel_python3', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=PYTHON_WHEEL_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--artifactory'],
                help='Build package and upload it to artifactory',
                hook=core.yarg.SetConstValueHook("artifactory", True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-add-host'],
                help='Docker --add-host',
                hook=core.yarg.SetAppendHook('docker_add_host'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-secret'],
                help='Same as Docker --secret. You can pass few secrets at the same time',
                hook=core.yarg.SetAppendHook('docker_secrets'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-registry'],
                help='Docker registry',
                hook=core.yarg.SetValueHook('docker_registry'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-repository'],
                help='Specify private repository',
                hook=core.yarg.SetValueHook('docker_repository'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-save-image'],
                help='Save docker image to archive',
                hook=core.yarg.SetConstValueHook('docker_save_image', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-network'],
                help='--network parameter for `docker build` command',
                hook=core.yarg.SetValueHook('docker_build_network'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-platform'],
                help='Specify platform for docker build (require buildx)',
                hook=core.yarg.SetValueHook('docker_platform'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-build-arg'],
                help='--build-arg parameter for `docker build` command, set it in the <key>=<value> form',
                hook=core.yarg.DictPutHook('docker_build_arg'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--docker-target'],
                help='Specifying target build stage (--target)',
                hook=core.yarg.SetValueHook('docker_target'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DOCKER_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--raw-package'],
                help="Used with --tar to get package content without tarring",
                hook=core.yarg.SetConstValueHook('raw_package', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=TAR_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--sloppy-and-fast-debian'],
                help="Fewer checks and no compression when building debian package",
                hook=core.yarg.SetConstValueHook('sloppy_deb', True),
                visible=False,
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--not-sign-debian'],
                help='Do not sign debian package',
                hook=core.yarg.SetConstValueHook('sign', False),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--custom-version'],
                help='Custom package version',
                hook=core.yarg.SetValueHook('custom_version'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--debian-arch'],
                help='Debian arch (passed to debuild as `-a`)',
                hook=core.yarg.SetValueHook('debian_arch'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--arch-all'],
                help='Use "Architecture: all" in debian',
                hook=core.yarg.SetConstValueHook('arch_all', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--force-dupload'],
                help='dupload --force',
                hook=core.yarg.SetConstValueHook('force_dupload', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['-z', '--debian-compression'],
                help="deb-file compresson level ({})".format(
                    ", ".join(list(PackageCustomizableOptions.deb_compression_levels.keys()))
                ),
                hook=core.yarg.SetValueHook(
                    'debian_compression_level', values=list(PackageCustomizableOptions.deb_compression_levels.keys())
                ),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['-Z', '--debian-compression-type'],
                help="deb-file compression type used when building deb-file (allowed types: {}, gzip (default), xz, bzip2, lzma, none)".format(
                    const.DEBIAN_HOST_DEFAULT_COMPRESSION_LEVEL
                ),
                hook=core.yarg.SetValueHook('debian_compression_type'),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--dont-store-debian'],
                help="Save debian package in a separate archive",
                hook=core.yarg.SetConstValueHook('store_debian', False),
                visible=False,
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                ['--upload-resource-type'],
                help='Created resource type',
                hook=core.yarg.SetValueHook('resource_type'),
                group=build.build_opts.SANDBOX_UPLOAD_OPT_GROUP,
            ),
            core.yarg.ArgConsumer(
                ['--upload-resource-attr'],
                help='Resource attr, set it in the <name>=<value> form',
                hook=core.yarg.DictPutHook(name='resource_attrs'),
                group=build.build_opts.SANDBOX_UPLOAD_OPT_GROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--dupload-max-attempts'],
                help='How many times try to run dupload if it fails',
                hook=core.yarg.SetValueHook('dupload_max_attempts', int),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--dupload-no-mail'],
                help='dupload --no-mail',
                hook=core.yarg.SetConstValueHook('dupload_no_mail', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=DEB_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--overwrite-read-only-files'],
                help='Overwrite read-only files in package',
                hook=core.yarg.SetConstValueHook('overwrite_read_only_files', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
            core.yarg.ArgConsumer(
                names=['--ensure-package-published'],
                help='Ensure that package is available in the repository',
                hook=core.yarg.SetConstValueHook('ensure_package_published', True),
                group=core.yarg.PACKAGE_OPT_GROUP,
                subgroup=COMMON_SUBGROUP,
            ),
        ]

    def postprocess(self):
        if self.debian_compression_level is not None:
            self.debian_compression_level = self.deb_compression_levels[self.debian_compression_level]
        if self.create_dbg:
            if not self.full_strip:
                self.strip = True
        if self.compression_filter not in (None, 'gzip', 'zstd'):
            raise core.yarg.ArgsValidatingException(
                "Using unsupported compression filter: {}".format(self.compression_filter)
            )


class InterimOptions(core.yarg.Options):
    Visible = False

    def __init__(self):
        self.verify_patterns_usage = True

    # All this options
    #  - !! should never be available in YA_PACKAGE sandbox task !!
    #  - will be removed when work is done
    def consumer(self):
        return [
            core.yarg.ArgConsumer(
                names=['--fixme-CHEMODAN-80080'],
                help='See CHEMODAN-80080 and DEVTOOLSSUPPORT-12411 for more info',
                hook=core.yarg.SetConstValueHook('verify_patterns_usage', False),
                visible=self.Visible,
            ),
        ]
