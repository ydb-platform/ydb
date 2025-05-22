import os
import random
import re
import subprocess
import six

import yaml
import logging
import argparse
import yatest.common

from six.moves import input
import build.plugins.lib.test_const as const
import library.python.fs as fs
import library.python.testing.recipe


logger = logging.getLogger(__name__)


class DockerComposeRecipeException(Exception):

    def __init__(self, msg):
        super(DockerComposeRecipeException, self).__init__("[[bad]]{}[[rst]]".format(msg))


def avoid_env_interpolation(env=None):
    # For more info see
    # - https://docs.docker.com/compose/compose-file/12-interpolation
    # - https://github.com/docker/compose/issues/9704
    # - https://st.yandex-team.ru/YA-1810

    env = env or os.environ
    replaced = []

    for key, val in env.items():
        if "$" in val:
            replaced.append(key)
            env[key] = re.sub(r"((^\$$)|([^\$]\$$)|(\$[^\$\{A-Za-z]))", r"\1$", val)

    if replaced:
        logger.debug("Replacing $ with $$ to avoid incorrect interpolation for %s. Fore more info see https://docs.docker.com/compose/compose-file/12-interpolation", replaced)
    return env


def start(argv):
    avoid_env_interpolation(env=os.environ)

    args = _parse_args(argv)

    yml_file, cwd = get_compose_file_and_cwd(argv)
    _verify_compose_file(yml_file)

    env = _setup_env()

    docker_compose = get_docker_compose()

    recipe_config = _get_recipe_config(args)

    deprecated_context = _get_docker_deprecated_context(args)

    docker_context = recipe_config.get('context', deprecated_context)
    if docker_context:
        context_map = _create_context(docker_context, os.path.dirname(yml_file), yatest.common.work_path("docker_context_root"))
        env.update(context_map)

    test_host_name = _get_test_host(args, recipe_config)
    if test_host_name:
        yml_file = _setup_test_host(test_host_name, yml_file, env)
        library.python.testing.recipe.set_env("DONT_CREATE_TEST_PROCESS_GROUP", "1")  # XXX find out why docker-compose hangs in other case
        library.python.testing.recipe.set_env(
            "TEST_COMMAND_WRAPPER", " ".join([docker_compose, "-f", yml_file, "exec", "-T", test_host_name])
        )

    library.python.testing.recipe.set_env("DOCKER_COMPOSE_FILE", yml_file)

    networks = _get_networks(recipe_config)
    if networks:
        subnets = set()

        for net_name, net_config in six.iteritems(networks):
            while True:
                subnet = "fc00:420:%04x::/48" % random.randrange(16**4)
                if subnet not in subnets:
                    break

            subnets.add(subnet)

            net_args = ["docker", "network", "create", "--subnet", subnet]
            if net_config and net_config.get("ipv6", False):
                net_args.append("--ipv6")
            net_args.append(net_name)

            yatest.common.execute(net_args, cwd=cwd)

    yatest.common.execute([docker_compose, "-f", yml_file, "--log-level", "DEBUG", "--no-ansi", "up", "-d", "--build",
                           "--force-recreate"],
                          cwd=cwd, env=env)


def stop(argv):
    avoid_env_interpolation(env=os.environ)

    if yatest.common.get_param("docker-pause"):
        library.python.testing.recipe.tty()
        try:
            input("\ndocker_compose will stop, press <Enter> to continue")
        except KeyboardInterrupt:
            pass

    docker_compose = get_docker_compose()
    yaml, cwd = get_compose_file_and_cwd(argv)
    args = _parse_args(argv)
    recipe_config = _get_recipe_config(args)
    failed_containers = []
    containers = _get_containers(yaml)

    try:
        containers_ids_res = yatest.common.execute([docker_compose, "-f", yaml, "--log-level", "DEBUG", "--no-ansi", "ps", "-q"], cwd=cwd, stdout=subprocess.PIPE, text=True)
        if containers_ids_res.exit_code != 0:
            raise DockerComposeRecipeException("'docker-compose ps' returned {}'".format(containers_ids_res.exit_code))

        containers_ids = str.splitlines(containers_ids_res.std_out)
        if len(containers_ids) == 0:
            raise DockerComposeRecipeException("'docker-compose ps' output is empty '{}'".format(containers_ids_res.std_out))

        for container_id in containers_ids:
            container_id_status_res = yatest.common.execute(["docker", "ps", "-a", "--filter", "id=" + container_id, "--format", "{{.Status}}\t{{.Names}}"], cwd=cwd)

            if container_id_status_res.exit_code != 0:
                raise DockerComposeRecipeException("'docker ps' returned {}'".format(container_id_status_res.exit_code))

            status_line, container_name = six.ensure_str(container_id_status_res.std_out).split('\t')
            if "Up" in status_line or "Exited (0)" in status_line:
                continue
            failed_containers.append(container_name)

        yatest.common.execute([docker_compose, "-f", yaml, "--log-level", "DEBUG", "--no-ansi", "stop"], cwd=cwd)

        _dump_container_logs(containers, _get_requested_paths(yaml, recipe_config))

    finally:
        if not yatest.common.context.test_debug:
            yatest.common.execute([get_docker_compose(), "-f", yaml, "--log-level", "DEBUG", "--no-ansi", "down", "--rmi", "local"], cwd=cwd)

        networks = _get_networks(recipe_config)
        if networks:
            for net_name in six.iterkeys(networks):
                yatest.common.execute(["docker", "network", "rm", net_name], cwd=cwd)

        if len(failed_containers) > 0:
            raise DockerComposeRecipeException("Has failed containers: {}".format(", ".join(failed_containers)))


def get_compose_file_and_cwd(args):
    if "DOCKER_COMPOSE_FILE" in os.environ:
        yaml_file = os.environ["DOCKER_COMPOSE_FILE"]
    else:
        args = _parse_args(args)
        if args.compose_file and args.compose_file != "$DOCKER_COMPOSE_FILE":
            yaml_file = yatest.common.source_path(args.compose_file)
        else:
            yaml_file = yatest.common.test_source_path("docker-compose.yml")
    return yaml_file, os.path.dirname(yaml_file)


def get_docker_compose():
    docker_compose = yatest.common.build_path("library/recipes/docker_compose/bin/docker-compose")
    if not os.path.exists(docker_compose):
        raise DockerComposeRecipeException("cannot find docker_compose by build_path '{}'".format(docker_compose))
    os.chmod(docker_compose, 0o755)
    return docker_compose


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--recipe-config-file", help="Path recipe config yml file (Arcadia related)")
    parser.add_argument("--compose-file", help="Path to docker-compose.yml file (Arcadia related)")
    parser.add_argument("--context-file", help="Path to docker-context.yml file (Arcadia related)")
    parser.add_argument("--test-host", help="Name of service in docker-compose file that will host test execution")
    return parser.parse_args(argv)


def _get_recipe_config(args):
    if args.recipe_config_file and args.recipe_config_file != "$RECIPE_CONFIG_FILE":
        config_path = yatest.common.source_path(args.recipe_config_file)
        if not os.path.exists(config_path):
            raise DockerComposeRecipeException("Cannot find specified recipe config file '{}'".format(args.recipe_config_file))
        with open(config_path) as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    return {}


def _get_docker_deprecated_context(args):
    # XXX: to be removed when all recipes use new config file
    if args.context_file and args.context_file != "$DOCKER_CONTEXT_FILE":
        context_file = yatest.common.source_path(args.context_file)
        if not os.path.exists(context_file):
            raise DockerComposeRecipeException("Cannot find context file by {}".format(context_file))
        with open(context_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    return None


def _setup_env():
    env = os.environ.copy()
    env["CURRENT_USER"] = "{}:{}".format(os.getuid(),  os.getgid())
    # Setup extra env.vars. to be able to pass coverage dir to the docker
    for name in const.COVERAGE_ENV_VARS:
        if name in env:
            env["{}_DIRNAME".format(name.rsplit('_', 1)[0])] = os.path.dirname(env[name])
    return env


def _create_context(context, init_dir, context_root):

    def copy_files(src, dst):
        if os.path.isfile(src):
            fs.copy_file(src, dst)
        else:
            if not os.path.exists(dst):
                os.makedirs(dst)

            for name in os.listdir(src):
                copy_files(os.path.join(src, name), os.path.join(dst, name))

    context_map = {}

    fs.ensure_dir(context_root)
    for context_name in context:
        _verify_context_name(context_name)
        context_dir = os.path.join(context_root, context_name)
        copy_files(init_dir, context_dir)
        for item in context[context_name]:
            if len(item.keys()) != 1 or len(item.values()) != 1:
                raise DockerComposeRecipeException("Context item should be in form of <source>:<destination> item")
            source_path, target_path = next(iter(six.iteritems(item)))
            if source_path.startswith("build://"):
                source_path = yatest.common.build_path(source_path[len("build://"):])
            elif source_path.startswith("arcadia://"):
                source_path = yatest.common.source_path(source_path[len("arcadia://"):])
            else:
                raise DockerComposeRecipeException("Source path should start with 'build://' or 'arcadia://'")
            target_path = os.path.join(context_dir, target_path.lstrip("/"))
            fs.ensure_dir(os.path.dirname(target_path))
            copy_files(source_path, target_path)

        context_map[context_name] = context_dir

    return context_map


def _verify_context_name(name):
    assert re.match("^[a-zA-Z0-9]+$", name), "Context name '{}' has incorrect symbols".format(name)


def _verify_compose_file(file_path):
    with open(file_path) as f:
        data = yaml.safe_load(f)
    known_images = set()
    for service, settings in six.iteritems(data["services"]):
        if "image" in settings:
            image = settings["image"]
            if "build" not in settings and "@sha256" not in image and image not in known_images:
                message = "Using image without specified sha256 (e.g. redis:alpine@sha256:66ccc75f079ab9059c900e9545bbd271bff78a66f94b45827e6901f57fb973f1) and build section is not supported"
                logger.error(message)
                raise DockerComposeRecipeException(message)
            known_images.add(image)


def _get_networks(config):
    return config.get("networks")


def _get_test_host(args, config):
    def get_from_args():
        if args.test_host and args.test_host != "$DOCKER_TEST_HOST":
            return args.test_host
        return None
    return config.get('test-host', get_from_args())


def _setup_test_host(test_host_name, yml_path, env):
    with open(yml_path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    for service, settings in six.iteritems(data["services"]):
        if service == test_host_name:
            overwritten_yml_path = yatest.common.work_path("docker_compose_for_test.yml")
            if "env" not in settings:
                settings["environment"] = []
            for env_key, env_value in six.iteritems(env):
                settings["environment"].append("{}={}".format(env_key, env_value))

            if "volumes" not in settings:
                settings["volumes"] = []

            if "command" in settings:
                raise DockerComposeRecipeException("Test hosting service '{}' has `command` section which is not supported by testing framework".format(test_host_name))
            settings["command"] = "sleep 3600"

            settings["tty"] = False

            if "user" in settings:
                raise DockerComposeRecipeException("Test hosting service '{}' has `user` section which is not supported by testing framework".format(test_host_name))
            settings["user"] = "$CURRENT_USER"

            if "build" in settings:
                conext = settings["build"].get("context")
                if conext == ".":
                    settings["build"]["context"] = os.path.dirname(yml_path)

            bind_paths = [
                yatest.common.build_path(),
                os.environ.get("ORIGINAL_SOURCE_ROOT", yatest.common.source_path()),
            ]

            if yatest.common.runtime.context.test_tool_path:
                bind_paths.append(yatest.common.runtime.context.test_tool_path)

            for k in [
                "PORT_SYNC_PATH",
                "OS_SDK_ROOT_RESOURCE_GLOBAL",
                "LLD_ROOT_RESOURCE_GLOBAL",
                "ASAN_SYMBOLIZER_PATH",
                "LSAN_SYMBOLIZER_PATH",
                "MSAN_SYMBOLIZER_PATH",
                "UBSAN_SYMBOLIZER_PATH",
                "TMPDIR",
            ]:
                if k in os.environ:
                    p = os.environ[k]
                    if p not in bind_paths and os.path.exists(p):
                        bind_paths.append(p)

            for bind_path in bind_paths:
                settings["volumes"].append({
                    "type": "bind",
                    "source": bind_path,
                    "target": bind_path,
                })
                real_bind_path = os.path.realpath(bind_path)
                if real_bind_path != bind_path:
                    settings["volumes"].append({
                        "type": "bind",
                        "source": real_bind_path,
                        "target": real_bind_path,
                    })

            with open(overwritten_yml_path, "w") as f:
                yaml.dump(data, f)

            return overwritten_yml_path

    raise DockerComposeRecipeException("Service with name '{}' was not found to be setup as a host for running test".format(test_host_name))


def _dump_container_logs(containers, requested_paths):
    # add links to container logs
    container_logs = yatest.common.output_path("containers")
    if not os.path.exists(container_logs):
        os.makedirs(container_logs)

    for container_id, container_name in six.iteritems(containers):
        try:
            output_path = os.path.join(container_logs, container_name)
            os.makedirs(output_path)
            for output_type in ['std_out', 'std_err']:
                output_log_path = os.path.join(output_path, "container_{}.log".format(output_type))
                res = yatest.common.execute(["docker", "logs", container_id])
                with open(output_log_path, "w") as f:
                    f.write(six.ensure_str(getattr(res, output_type)))

            for path in requested_paths.get(container_id, []):
                try:
                    yatest.common.execute(["docker", "cp", "-L", "{}:{}".format(container_id, path), output_path])
                except yatest.common.ExecutionError:
                    logging.exception("Error while copying %s from %s", path, container_name)

        except Exception:
            logger.exception("Error collecting container's log with name {} and id {}".format(container_name, container_id))


def _get_requested_paths(yaml_path, config):
    requested_logs = {}
    if config:
        for service_name, logs in six.iteritems(config.get("save", {})):
            try:
                res = yatest.common.execute([get_docker_compose(), "-f", yaml_path, "ps", "-q", service_name])
                requested_logs[six.ensure_str(res.std_out).strip()] = logs
            except yatest.common.ExecutionError:
                logging.exception("Error while trying to find docker compose service by name: %s", service_name)
    return requested_logs


def _get_containers(yaml_path):
    res = yatest.common.execute([get_docker_compose(), "-f", yaml_path, "ps", "-q"])
    return {container_id: _get_container_name(container_id) for container_id in filter(None, six.ensure_str(res.std_out).split("\n"))}


def _get_container_name(container_id):
    res = yatest.common.execute(["docker", "inspect", "--format={{.Name}}", container_id])
    container_name = six.ensure_str(res.std_out).strip("/").strip()
    return container_name
