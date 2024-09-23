import os
import sys
import json
import signal
import shutil
import tempfile
import logging
import argparse
import subprocess
import warnings
from urllib3.exceptions import HTTPWarning

from ydb.tools.cfg.walle import NopHostsInformationProvider
from ydb.tools.ydbd_slice import nodes, handlers, cluster_description
from ydb.tools.ydbd_slice.kube import handlers as kube_handlers, docker

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=HTTPWarning)


logger = logging.getLogger(__name__)


HELP = '''
\033[92mKiKiMR Developer's Slice Deployment Tool\x1b[0m

See examples of cluster.yaml here
    https://cs.yandex-team.ru/#!,kikimr%%2F.*%%2Fcluster.yaml,,arcadia
And some explanation about cluster.yaml's format here
    https://wiki.yandex-team.ru/kikimr/techdoc/confdeploy/configuration/
Guide for Kubernetes Developer Slice could be found here
    https://docs.yandex-team.ru/ydb-tech/ops/devslice-user-guide-k8s-baremetal-host
Guide for ad-hoc Kubernetes operations could be found here
    https://docs.yandex-team.ru/ydb-tech/ops/kubernetes/howto/

\033[96mCommands for Traditional Developer's Slices\x1b[0m

\033[95minstall\033[94m - full install process from scratch:
    %(prog)s install cluster.yaml --arcadia

\033[95mupdate\033[94m - only update configs and  kikimr executable on cluster:
    %(prog)s update cluster.yaml --arcadia

\033[95mexplain\033[94m - explain cluster description:
    %(prog)s explain cluster.yaml --out-cfg cfg

\033[95mstart\033[94m - start instances on cluster:
    %(prog)s start cluster.yaml

\033[95mstop\033[94m - stop instances on cluster:
    %(prog)s stop cluster.yaml

\033[95mclear\033[94m - clear cluster:
    %(prog)s clear cluster.yaml

\033[95mformat\033[94m - format cluster:
    %(prog)s format cluster.yaml

Use option --nodes to instant particular nodes subset form cluster nodes.
Example, update only kikimr0999 according cluster.yaml setting:
    %(prog)s update cluster.yaml --hosts kikimr0999 --arcadia

Use components specification to choose active component.
Example, update only kikimr:
    %(prog)s update cluster.yaml kikimr --arcadia

Example, update only kikimr binary:
    %(prog)s update cluster.yaml kikimr=bin --arcadia

Example, update only kikimr configs:
    %(prog)s update cluster.yaml kikimr=cfg --arcadia

Example, install part by part:
    %(prog)s install cluster.yaml kikimr --arcadia
    %(prog)s install cluster.yaml dynamic_slots --arcadia

Example, stop only kikimr at one node:
    %(prog)s stop cluster.yaml kikimr --hosts kikimr0111

Example, stop only kikimr at the cluster:
    %(prog)s stop cluster.yaml kikimr

Example, stop/start all slots at the cluster:
    %(prog)s stop cluster.yaml dynamic_slots
    %(prog)s start cluster.yaml dynamic_slots

And so on. Feel free to combine component arguments with all modes.
And use hosts specification to reduce active nodes set.

\033[96mCommands for Kubernetes Developer's Slices\x1b[0m

\033[95mdocker-build\033[94m - command to build dev docker image.

\033[36mDev images uses special docker-registry and tag by default,
example: cr.yandex/crpbo4q9lbgkn85vr1rm/ydb:<login>-latest.\033[94m

Example, build dev docker image:
    %(prog)s docker-build

Example, build dev docker image with specific build args:
    %(prog)s docker-build --build_args -j 50

\033[95mkube-generate\033[94m - command to generate manifests for new slice in current directory or directory,
specified using -p option.

Example, create Kubernetes manifests for 8 node cluster with block-4-2 erasure using nodes with
cpu104_soc2_mem512G_net25G_4nvme flavor:
    mkdir directory_with_my_manifests
    cd directory_with_my_manifests
    %(prog)s kube-generate -n myslice -v node_flavor=cpu104_soc2_mem512G_net25G_4nvme

\033[36mAll kube-* commands must be executed in directory with Kubernetes manifest files or pointed to such directory
using -p option.\033[94m

\033[95mkube-install\033[94m - command to setup new or completely redeploy existing YDB Slice in Kubernetes.
All existing objects related to your mainfest files will be deleted first.

Example, create Kubernetes objects using your latest dev image (cr.yandex/crpbo4q9lbgkn85vr1rm/ydb:<login>-latest):
    cd directory_with_my_manifests
    %(prog)s kube-install

Example, create Kubernetes objects using your latest dev image (cr.yandex/crpbo4q9lbgkn85vr1rm/ydb:<login>-latest) and
wait for all slice objects to become Ready:
    cd directory_with_my_manifests
    %(prog)s kube-install -w

Example, create kubernetes objects using dev image with specific tag (cr.yandex/crpbo4q9lbgkn85vr1rm/ydb:somebody-1):
    cd directory_with_my_manifests
    %(prog)s kube-install -t somebody-1

Example, create kubernetes objects using your locally build image, specify
image name (cr.yandex/crpl7ipeu79oseqhcgn2/ydb:23.2.11):
    cd directory_with_my_manifests
    %(prog)s kube-install -i cr.yandex/crpl7ipeu79oseqhcgn2/ydb:23.3.11

Example, create kubernetes objects using existing release image with
specific tag (cr.yandex/crpl7ipeu79oseqhcgn2/ydb:23.2.10:
    cd directory_with_my_manifests
    %(prog)s kube-install --use-prebuilt-image -i cr.yandex/crpl7ipeu79oseqhcgn2/ydb:23.2.10

Example, create kubernetes objects, force-rebuild image:
    cd directory_with_my_manifests
    %(prog)s kube-install --force-rebuild --build_args -j 50

\033[95mkube-update\033[94m - command to update existing YDB Slice in kubernetes.

Example, update kubernetes objects using your latest dev image (cr.yandex/crpbo4q9lbgkn85vr1rm/ydb:<login>-latest):
    cd directory_with_my_manifests
    %(prog)s kube-update

Example, update all storage objects only:
    cd directory_with_my_manifests
    %(prog)s kube-update -c storage

Example, update specific database object only:
    cd directory_with_my_manifests
    %(prog)s kube-update -c database:database1,database2

Example, update specific database object only and wait for this Database object to become Ready:
    cd directory_with_my_manifests
    %(prog)s kube-update -c database:database1,database2 -w

Command supports all docker-related build options.
Example, update kubernetes objects, force-rebuild image:
    cd directory_with_my_manifests
    %(prog)s kube-update --force-rebuild --build_args -j 50

\033[95mkube-stop\033[94m - command to stop nodes by removing Storage and Database objects from Kubernetes cluster.

Example, stop all pods:
    cd directory_with_my_manifests
    %(prog)s kube-stop

Example, stop mydatabase pods:
    cd directory_with_my_manifests
    %(prog)s kube-stop -c database:mydatabase

\033[95mkube-start\033[94m - command to start nodes by creating Storage and Database objects in Kubernetes cluster.

Example, start all pods:
    cd directory_with_my_manifests
    %(prog)s kube-start

Example, start mydatabase pods:
    cd directory_with_my_manifests
    %(prog)s kube-start -c database:mydatabase

Example, start mydatabase pods and wait for Database object to become Ready:
    cd directory_with_my_manifests
    %(prog)s kube-start -c database:mydatabase -w

\033[95mkube-restart\033[94m - command to restart nodes by deleting pods in Kuberetes cluster.

Example, restart all pods:
    cd directory_with_my_manifests
    %(prog)s kube-restart

Example, restart mydatabase pods:
    cd directory_with_my_manifests
    %(prog)s kube-restart -c database:mydatabase

\033[95mkube-nodes\033[94m - command to list NodeClaim nodes.

Example, list all slice nodes:
    cd directory_with_my_manifests
    %(prog)s kube-nodes

Example, save all slice nodes in file and use this file to run remote commands on nodes:
    cd directory_with_my_manifests
    %(prog)s kube-nodes > nodelist
    pssh run-e -p 30 -H L@nodelist 'unified_agent select -s kikimr -S now-10m'

\033[95mkube-format\033[94m - command to stop nodes (like with kube-stop command), format drives on hosts,
reserved by your NodeClaims, start nodes (like with kube-start command).

Example:
    cd directory_with_my_manifests
    %(prog)s kube-format

Example, wait for Storage and Database object to become Ready:
    cd directory_with_my_manifests
    %(prog)s kube-format -w

\033[95mkube-clear\033[94m - command to stop nodes (like with kube-stop command), format drives on hosts,
reserved by your NodeClaims.

Example:
    cd directory_with_my_manifests
    %(prog)s kube-clear

\033[95mkube-uninstall\033[94m - command to stop nodes (like with kube-stop command), format drives on hosts,
reserved by your NodeClaims, delete your NodeClaims.

Example:
    cd directory_with_my_manifests
    %(prog)s kube-uninstall

\x1b[0m
'''


YDBD_EXECUTABLE = 'ydb/apps/ydbd/ydbd'


class Terminate(BaseException):
    @staticmethod
    def handler(signum, frame):
        logger.debug('got SIGTERM signal, terminating')
        raise Terminate(signum, frame)


def safe_load_cluster_details(cluster_yaml, walle_provider):
    try:
        cluster_details = cluster_description.ClusterDetails(cluster_yaml, walle_provider)
    except IOError as io_err:
        print('', file=sys.stderr)
        print("unable to open YAML params as a file, check args", file=sys.stderr)
        print("origin exception was %s" % io_err, file=sys.stderr)
        sys.exit(2)
    else:
        return cluster_details


def deduce_components_from_args(args, cluster_details):
    dynamic_enabled = bool(cluster_details.databases) or bool(cluster_details.dynamic_slots)

    components = ['kikimr']
    if dynamic_enabled:
        components.append('dynamic_slots')

    result = dict()

    for item in args.components:
        name, val = item.rsplit('=') if '=' in item else (item, None)
        assert name == 'all' or name in components, \
            "component <%s> not in allowed set of components [%s]" % (name, ", ".join(components))

        if name == 'dynamic_slots':
            assert val is None
        else:
            assert val in ('cfg', 'bin', 'none', None)

        val = [val] if val is not None else []
        if name in result:
            result[name] += val
        else:
            result[name] = val

    if 'all' in args.components:
        result = {item: [] for item in components}

    if 'kikimr' in result and len(result['kikimr']) == 0:
        result['kikimr'] = ['bin', 'cfg']

    if 'dynamic_slots' in result:
        result['dynamic_slots'] = ['all']

    logger.debug("active components is '%s'", result)
    return result


def deduce_nodes_from_args(args, walle_provider, ssh_user):
    cluster_hosts = safe_load_cluster_details(args.cluster, walle_provider).hosts_names
    result = cluster_hosts

    if args.nodes is not None:
        result = []
        for orig_host in cluster_hosts:
            for manual_host in args.nodes:
                if orig_host.startswith(manual_host):
                    result.append(orig_host)
                    break

    if not result:
        sys.exit("unable to deduce hosts")

    logger.info("use nodes '%s'", result)
    return nodes.Nodes(result, args.dry_run, ssh_user=ssh_user)


def ya_build(arcadia_root, artifact, opts, dry_run):
    project, _ = os.path.split(artifact)
    ya_bin = os.path.join(arcadia_root, 'ya')
    project_path = os.path.join(arcadia_root, project)
    bin_path = os.path.join(arcadia_root, artifact)

    cmd = [ya_bin, 'make'] + opts + [project_path]
    logger.info("run command '%s'", cmd)
    if not dry_run:
        subprocess.check_call(cmd)
        logger.debug(bin_path)
        assert os.path.isfile(bin_path)

    return bin_path


def ya_package_docker(arcadia_root, opts, pkg_path, image):
    registry = docker.DOCKER_IMAGE_REGISTRY
    repository = docker.DOCKER_IMAGE_REPOSITORY
    ya_bin = os.path.join(arcadia_root, 'ya')
    path = os.path.join(arcadia_root, pkg_path)

    image_name, tag = image.rsplit(':', 1)

    cmd = [
        ya_bin, 'package'
    ] + opts + [
        '--docker',
        '--docker-network', 'host',
        '--docker-registry', registry,
        '--docker-repository', repository,
        '--custom-version', tag,
        path,
    ]
    logger.info("run command '%s'", cmd)
    subprocess.check_call(cmd)
    try:
        with open('packages.json') as file:
            img_data = json.load(file)
        logger.info('successfully built image: %s', img_data)
        built_image = img_data[0]['docker_image']
        built_image_name, _ = built_image.rsplit(':', 1)
        if built_image_name != image_name:
            logger.debug('tagging image from "%s" to "%s"', built_image_name, image_name)
            docker.docker_tag(built_image, image)
        return built_image
    except Exception as e:
        logger.error('failed to get image details from packages.json file, error: %s', str(e))
        raise


def arcadia_root(begin_path='.'):
    path = os.path.realpath(begin_path)
    while not path == '/':
        mark = os.path.join(path, '.arcadia.root')
        if os.path.exists(mark):
            return path
        path = os.path.dirname(path)
    sys.exit("unable to find arcadia root")


def deduce_kikimr_bin_from_args(args):
    if args.kikimr is not None:
        path = os.path.abspath(args.kikimr)
    elif args.arcadia:
        root = arcadia_root()
        path = ya_build(root, YDBD_EXECUTABLE, args.build_args, args.dry_run)
    else:
        sys.exit("unable to deduce kikimr bin")

    if 'LD_LIBRARY_PATH' not in os.environ:
        os.environ['LD_LIBRARY_PATH'] = os.path.dirname(path)

    compressed_path = args.kikimr_lz4

    logger.info("use kikimr bin '%s'", path)
    return path, compressed_path


def deduce_temp_dir_from_args(args):
    permits = 0o755

    if args.temp_dir is not None:
        temp_dir = args.temp_dir
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir, permits)
        assert os.path.isdir(temp_dir)
    else:
        temp_dir = tempfile.mkdtemp()
        assert os.path.isdir(temp_dir)
        os.chmod(temp_dir, permits)

    logger.info("use tmp dir '%s'", temp_dir)
    return temp_dir


def direct_nodes_args():
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "-H",
        "--hosts",
        metavar="HOST",
        dest='nodes',
        nargs='+',
        help="set of nodes as is"
    )
    return args


def cluster_description_args():
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "cluster",
        metavar="YAML",
        help="cluster description in yaml format"
    )
    return args


def log_args():
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "--clear_logs",
        dest='clear_logs',
        action='store_true',
        help="stop rsyslogd and erase all kikimr logs"
    )
    return args


def binaries_args():
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "--kikimr",
        metavar="BIN",
        default=None,
        help="explicit path to kikimr"
    )
    args.add_argument(
        "--kikimr-lz4",
        metavar="PATH",
        help="explicit path to compressed kikimr binary file used for transfer acceleration"
    )
    args.add_argument(
        "--arcadia",
        action='store_true',
        help="build all binaries from arcadia, figure out root by finding .arcadia.root upstairs"
    )
    args.add_argument(
        "--build_args",
        metavar="BUILD_ARGS",
        default=['-r'],
        nargs=argparse.REMAINDER,
        help="remaining arguments are treated as arguments to 'ya make' tool (only valid if --arcadia is provided)"
    )
    return args


def component_args():
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "components",
        metavar="COMPONENT",
        default=['all'],
        nargs="*",
        help="specify components to work with, "
             "multiple choice from: 'all', 'kikimr[={bin|cfg}]', "
             "'dynamic_slots'"
             "'all' is default",
    )
    return args


def ssh_args():
    current_user = os.environ["USER"]
    args = argparse.ArgumentParser(add_help=False)
    args.add_argument(
        "--ssh-user",
        metavar="SSH_USER",
        default=current_user,
        help="user for ssh interaction with slice. Default value is $USER "
             "(which equals {user} now)".format(user=current_user),
    )
    return args


def add_explain_mode(modes, walle_provider):
    def _run(args):
        logger.debug("run func explain with cmd args is '%s'", args)

        cluster_details = safe_load_cluster_details(args.cluster, walle_provider)
        components = deduce_components_from_args(args, cluster_details)

        kikimr_bin, kikimr_compressed_bin = deduce_kikimr_bin_from_args(args)

        if not os.path.exists(args.out_cfg):
            os.mkdir(args.out_cfg, 0o755)
        assert os.path.isdir(args.out_cfg)

        configuration = cluster_description.Configurator(
            cluster_details,
            args.out_cfg,
            kikimr_bin,
            kikimr_compressed_bin,
            walle_provider
        )

        if 'kikimr' in components:
            static = configuration.create_static_cfg()
            logger.debug("static cfg: %s", static)

            dynamic = configuration.create_dynamic_cfg()
            logger.debug("dynamic cfg: %s", dynamic)

    mode = modes.add_parser(
        "explain",
        parents=[cluster_description_args(), binaries_args(), component_args()],
        description="Just dump generated cfg into --out-cfg."
    )
    mode.add_argument(
        "--out-cfg",
        metavar="DIR",
        required=True,
        help=""
    )
    mode.set_defaults(handler=_run)


def dispatch_run(func, args, walle_provider):
    logger.debug("run func '%s' with cmd args is '%s'", func.__name__, args)

    cluster_details = safe_load_cluster_details(args.cluster, walle_provider)
    components = deduce_components_from_args(args, cluster_details)

    nodes = deduce_nodes_from_args(args, walle_provider, args.ssh_user)

    temp_dir = deduce_temp_dir_from_args(args)
    clear_tmp = not args.dry_run and args.temp_dir is None

    kikimr_bin, kikimr_compressed_bin = deduce_kikimr_bin_from_args(args)

    configurator = cluster_description.Configurator(
        cluster_details,
        out_dir=temp_dir,
        kikimr_bin=kikimr_bin,
        kikimr_compressed_bin=kikimr_compressed_bin,
        walle_provider=walle_provider
    )

    v = vars(args)
    clear_logs = v.get('clear_logs')
    yav_version = v.get('yav_version')
    slice = handlers.Slice(
        components,
        nodes,
        cluster_details,
        configurator,
        clear_logs,
        yav_version,
        walle_provider,
    )
    func(slice)

    if clear_tmp:
        logger.debug("remove temp dirs '%s'", temp_dir)
        shutil.rmtree(temp_dir)


def add_install_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_install, args, walle_provider)

    mode = modes.add_parser(
        "install",
        conflict_handler='resolve',
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), log_args(), ssh_args()],
        description="Full installation of the cluster from scratch. "
                    "You can use --hosts to specify particular hosts. But it is tricky."
    )
    mode.set_defaults(handler=_run)


def add_update_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_update, args, walle_provider)

    mode = modes.add_parser(
        "update",
        conflict_handler='resolve',
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), log_args(), ssh_args()],
        description="Minor cluster update, just binary and cfg. No additional configuration is performed."
                    "Stop all kikimr instances at the nodes, sync binary and cfg, start the instances. "
                    "Use --hosts to specify particular hosts."
    )
    mode.set_defaults(handler=_run)


def add_update_raw_configs(modes, walle_provider):
    def _run(args):

        dispatch_run(lambda self: handlers.Slice.slice_update_raw_configs(self, args.raw_cfg), args, walle_provider)

    mode = modes.add_parser(
        "update-raw-cfg",
        conflict_handler='resolve',
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), ssh_args()],
        description=""
    )
    mode.add_argument(
        "--raw-cfg",
        metavar="DIR",
        required=True,
        help="",
    )
    mode.set_defaults(handler=_run)


def add_stop_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_stop, args, walle_provider)

    mode = modes.add_parser(
        "stop",
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), ssh_args()],
        description="Stop kikimr static instaneces at the nodes. "
                    "If option components specified, try to stop particular component. "
                    "Use --hosts to specify particular hosts."
    )
    mode.set_defaults(handler=_run)


def add_start_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_start, args, walle_provider)

    mode = modes.add_parser(
        "start",
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), ssh_args()],
        description="Start all kikimr instances at the nodes. "
                    "If option components specified, try to start particular component. "
                    "Otherwise only kikimr-multi-all will be started. "
                    "Use --hosts to specify particular hosts."
    )
    mode.set_defaults(handler=_run)


def add_clear_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_clear, args, walle_provider)

    mode = modes.add_parser(
        "clear",
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), ssh_args()],
        description="Stop all kikimr instances at the nodes, format all kikimr drivers, shutdown dynamic slots. "
                    "And don't start nodes afrer it. "
                    "Use --hosts to specify particular hosts."
    )
    mode.set_defaults(handler=_run)


def add_format_mode(modes, walle_provider):
    def _run(args):
        dispatch_run(handlers.Slice.slice_format, args, walle_provider)

    mode = modes.add_parser(
        "format",
        parents=[direct_nodes_args(), cluster_description_args(), binaries_args(), component_args(), ssh_args()],
        description="Stop all kikimr instances at the nodes, format all kikimr drivers at the nodes, start the instances. "
                    "If you call format for all cluster, you will spoil it. "
                    "Additional dynamic configuration will required after it. "
                    "If you call format for few nodes, cluster will regenerate after it. "
                    "Use --hosts to specify particular hosts."

    )
    mode.set_defaults(handler=_run)


#
# docker and kube scenarios
def build_and_push_docker_image(build_args, docker_package, build_ydbd, image, force_rebuild):
    if docker_package is None:
        docker_package = docker.DOCKER_IMAGE_YDBD_PACKAGE_SPEC

    logger.debug(f'using docker package spec: {docker_package}')

    image_details = docker.docker_inspect(image)

    if image_details is None:
        logger.debug('ydb image %s is not present on host, building', image)
        root = arcadia_root()
        ya_package_docker(root, build_args, docker_package, image)
    elif force_rebuild:
        logger.debug('ydb image %s is already present on host, rebuilding', image)
        root = arcadia_root()
        ya_package_docker(root, build_args, docker_package, image)
    else:
        logger.debug('ydb image %s is already present on host, using existing image', image)

    docker.docker_push(image)


def add_arguments_docker_build_with_remainder(mode, add_force_rebuild=False):
    group = mode.add_argument_group('docker build options')
    if add_force_rebuild:
        group.add_argument(
            '-f', '--force-rebuild',
            help='Force rebuild docker image even if it is already present on host.',
            action='store_true',
        )
    group.add_argument(
        '-d', '--docker-package',
        help='Optional: path to docker package description file relative from ARCADIA_ROOT.',
    )
    group.add_argument(
        '-i', '--image',
        help='Optional: docker image name and tag to mark image after build. Conflicts with "-t" argument.',
    )
    group.add_argument(
        '-t', '--tag',
        help='Optional: docker image tag to mark image after build. Conflicts with "-i" argument. Default is {user}-latest.',
    )
    group.add_argument(
        "--build_args",
        metavar="BUILD_ARGS",
        default=['-r'],
        nargs=argparse.REMAINDER,
        help="remaining arguments are treated as arguments to 'ya package' tool"
    )


def add_docker_build_mode(modes):
    def _run(args):
        logger.debug("starting docker-build cmd with args '%s'", args)
        try:
            image = docker.get_image_from_args(args)
            build_and_push_docker_image(args.build_args, args.docker_package, False, image, force_rebuild=True)

            logger.info('docker-build finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "docker-build",
        parents=[],
        description="Build YDB docker image."
    )
    add_arguments_docker_build_with_remainder(mode, add_force_rebuild=False)
    mode.set_defaults(handler=_run)


def add_kube_generate_mode(modes):
    def _run(args):
        logger.debug("starting kube-generate cmd with args '%s'", args)
        try:
            if args.user is None:
                args.user = docker.get_user()

            template_vars = {}
            for item in args.template_vars:
                key, value = item.split('=')
                template_vars[key] = value

            kube_handlers.slice_generate(args.path, args.user, args.name, args.template, template_vars)

            logger.info('kube-generate finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-generate",
        parents=[],
        description="Setup new or completely redeploy existing YDB Slice in Kubernetes."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-n', '--name',
        help='Slice name.',
        required=True,
    )
    mode.add_argument(
        '-t', '--template',
        help='Slice manifest templates for quick start.',
        choices=('8-node-block-4-2',),
        default='8-node-block-4-2',
    )
    mode.add_argument(
        '-v', '--template-vars',
        help='Slice manifest template variables for quick start. Example: ',
        action='append',
        default=[],
    )
    mode.add_argument(
        '--user',
        help='Slice user login. Default: get from $USER environ.',
        default=None,
    )
    mode.set_defaults(handler=_run)


def add_kube_install_mode(modes):
    def _run(args):
        logger.debug("starting kube-install cmd with args '%s'", args)
        try:
            image = docker.get_image_from_args(args)
            if not args.use_prebuilt_image:
                build_and_push_docker_image(args.build_args, args.docker_package, False, image, force_rebuild=args.force_rebuild)

            manifests = kube_handlers.get_all_manifests(args.path)
            kube_handlers.manifests_ydb_set_image(args.path, manifests, image)
            kube_handlers.slice_install(args.path, manifests, args.wait_ready, args.dynamic_config_type)

            logger.info('kube-install finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-install",
        parents=[],
        description="Setup new or completely redeploy existing YDB Slice in Kubernetes."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-w', '--wait-ready',
        help='Wait for ydb objects ready state. Default: false',
        action='store_true',
    )
    mode.add_argument(
        '--use-prebuilt-image',
        help='Do not build docker image, just specify image name in manifests.',
        action='store_true',
    )
    mode.add_argument(
        '--dynamic-config-type',
        help='Upload dynamic config with specified type',
        choices=['both', 'proto', 'yaml', 'none'],
        default='both',
    )
    add_arguments_docker_build_with_remainder(mode, add_force_rebuild=True)
    mode.set_defaults(handler=_run)


def add_kube_update_mode(modes):
    def _run(args):
        logger.debug("starting kube-update cmd with args '%s'", args)
        try:
            image = docker.get_image_from_args(args)
            if not args.use_prebuilt_image:
                build_and_push_docker_image(args.build_args, args.docker_package, False, image, force_rebuild=args.force_rebuild)

            manifests = kube_handlers.get_all_manifests(args.path)
            manifests = kube_handlers.manifests_ydb_filter_components(args.path, manifests, args.components)
            kube_handlers.manifests_ydb_set_image(args.path, manifests, image)
            kube_handlers.slice_update(args.path, manifests, args.wait_ready, args.dynamic_config_type)

            logger.info('kube-update finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-update",
        parents=[],
        description="Update existing YDB Slice in kubernetes."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-c', '--components',
        help=('Selector for specific components to perform action. '
              'Example: "storage:mystorage;database:mydatabase1,mydatabase2".'),
        type=kube_handlers.parse_components_selector,
    )
    mode.add_argument(
        '-w', '--wait-ready',
        help='Wait for ydb objects ready state. Default: false',
        action='store_true',
    )
    mode.add_argument(
        '--use-prebuilt-image',
        help='Do not build docker image, just specify image name in manifests.',
        action='store_true',
    )
    mode.add_argument(
        '--dynamic-config-type',
        help='Upload dynamic config with specified type',
        choices=['both', 'proto', 'yaml', 'none'],
        default='both',
    )
    add_arguments_docker_build_with_remainder(mode, add_force_rebuild=True)
    mode.set_defaults(handler=_run)


def add_kube_stop_mode(modes):
    def _run(args):
        logger.debug("starting kube-stop cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            manifests = kube_handlers.manifests_ydb_filter_components(args.path, manifests, args.components)
            kube_handlers.slice_stop(args.path, manifests)

            logger.info('kube-stop finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-stop",
        parents=[],
        description="Stop nodes by removing Storage and Database objects from Kubernetes cluster."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-c', '--components',
        help=('Selector for specific components to perform action. '
              'Example: "storage:mystorage;database:mydatabase1,mydatabase2".'),
        type=kube_handlers.parse_components_selector,
    )
    mode.set_defaults(handler=_run)


def add_kube_start_mode(modes):
    def _run(args):
        logger.debug("starting kube-start cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            manifests = kube_handlers.manifests_ydb_filter_components(args.path, manifests, args.components)
            kube_handlers.slice_start(args.path, manifests, args.wait_ready, args.dynamic_config_type)

            logger.info('kube-start finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-start",
        parents=[],
        description="Start nodes by creating Storage and Database objects in Kubernetes cluster."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-c', '--components',
        help=('Selector for specific components to perform action. '
              'Example: "storage:mystorage;database:mydatabase1,mydatabase2".'),
        type=kube_handlers.parse_components_selector,
    )
    mode.add_argument(
        '-w', '--wait-ready',
        help='Wait for ydb objects ready state. Default: false',
        action='store_true',
    )
    mode.add_argument(
        '--dynamic-config-type',
        help='Upload dynamic config with specified type',
        choices=['both', 'proto', 'yaml', 'none'],
        default='both',
    )
    mode.set_defaults(handler=_run)


def add_kube_restart_mode(modes):
    def _run(args):
        logger.debug("starting kube-restart cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            manifests = kube_handlers.manifests_ydb_filter_components(args.path, manifests, args.components)
            kube_handlers.slice_restart(args.path, manifests)

            logger.info('kube-restart finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-restart",
        parents=[],
        description="Restart nodes by deleting pods in Kuberetes cluster."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-c', '--components',
        help=('Selector for specific components to perform action. '
              'Example: "storage:mystorage;database:mydatabase1,mydatabase2".'),
        type=kube_handlers.parse_components_selector,
    )
    mode.set_defaults(handler=_run)


def add_kube_nodes_mode(modes):
    def _run(args):
        logger.debug("starting kube-nodes cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            kube_handlers.slice_nodes(args.path, manifests)

            logger.info('kube-nodes finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-nodes",
        parents=[],
        description=("List slice nodes.")
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.set_defaults(handler=_run)


def add_kube_format_mode(modes):
    def _run(args):
        logger.debug("starting kube-format cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            kube_handlers.slice_format(args.path, manifests, args.wait_ready, args.dynamic_config_type)

            logger.info('kube-format finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-format",
        parents=[],
        description=("Stop nodes (like with kube-stop command), format drives on hosts, reserved by your NodeClaims, "
                     "start nodes (like with kube-start command).")
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.add_argument(
        '-w', '--wait-ready',
        help='Wait for ydb objects ready state. Default: false',
        action='store_true',
    )
    mode.add_argument(
        '--dynamic-config-type',
        help='Upload dynamic config with specified type',
        choices=['both', 'proto', 'yaml', 'none'],
        default='both',
    )
    mode.set_defaults(handler=_run)


def add_kube_clear_mode(modes):
    def _run(args):
        logger.debug("starting kube-clear cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            kube_handlers.slice_clear(args.path, manifests)

            logger.info('kube-clear finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-clear",
        parents=[],
        description="Stop nodes (like with kube-stop command), format drives on hosts, reserved by your NodeClaims."
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.set_defaults(handler=_run)


def add_kube_uninstall_mode(modes):
    def _run(args):
        logger.debug("starting kube-uninstall cmd with args '%s'", args)
        try:
            manifests = kube_handlers.get_all_manifests(args.path)
            kube_handlers.slice_uninstall(args.path, manifests)

            logger.info('kube-uninstall finished')
        except RuntimeError as e:
            logger.error(e.args[0])
            sys.exit(1)

    mode = modes.add_parser(
        "kube-uninstall",
        parents=[],
        description=("Stop nodes (like with kube-stop command), format drives on hosts, reserved by your NodeClaims, "
                     "delete your NodeClaims.")
    )
    mode.add_argument(
        '-p', '--path',
        help='Path to project directory with kubernetes manifests. Default: $PWD.',
        default='.',
    )
    mode.set_defaults(handler=_run)


def main(walle_provider=None):
    try:
        signal.signal(signal.SIGTERM, Terminate.handler)

        log_formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-39s %(funcName)s: %(message)s')
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(log_formatter)
        logging.getLogger().addHandler(log_handler)
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger('kikimr.tools.kikimr_slice').setLevel(logging.DEBUG)
        logging.getLogger('ya.test').setLevel(logging.WARNING)
        logging.getLogger('kubernetes').setLevel(logging.ERROR)
        logging.getLogger('urllib3').setLevel(logging.ERROR)

        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawTextHelpFormatter,
            description=HELP,
        )
        parser.add_argument(
            '--log-level',
            metavar="LEVEL",
            choices=['debug', 'info', 'error'],
            default='debug',
            help='root logger level'
        )
        parser.add_argument(
            '--dry-run',
            default=False,
            action='store_true',
            help='do not touch the cluster only print debug'
        )
        parser.add_argument(
            '--temp-dir',
            metavar="DIR",
            default=None,
            help=''
        )
        parser.add_argument(
            "--yav-version",
            metavar="VERSION",
            default="ver-01gswscgce37hdbqyssjm3nd7x",
            help=''
        )

        modes = parser.add_subparsers()
        walle_provider = walle_provider or NopHostsInformationProvider()
        add_start_mode(modes, walle_provider)
        add_stop_mode(modes, walle_provider)
        add_install_mode(modes, walle_provider)
        add_update_mode(modes, walle_provider)
        add_update_raw_configs(modes, walle_provider)
        add_clear_mode(modes, walle_provider)
        add_format_mode(modes, walle_provider)
        add_explain_mode(modes, walle_provider)
        add_docker_build_mode(modes)
        add_kube_generate_mode(modes)
        add_kube_install_mode(modes)
        add_kube_update_mode(modes)
        add_kube_stop_mode(modes)
        add_kube_start_mode(modes)
        add_kube_restart_mode(modes)
        add_kube_nodes_mode(modes)
        add_kube_format_mode(modes)
        add_kube_clear_mode(modes)
        add_kube_uninstall_mode(modes)

        args = parser.parse_args()
        logging.root.setLevel(args.log_level.upper())
        args.handler(args)

    except KeyboardInterrupt:
        sys.exit('\nStopped by KeyboardInterrupt.')
    except Terminate:
        sys.exit('\nTerminated.')
