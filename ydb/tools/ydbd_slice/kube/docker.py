import os
import sys
import json
import logging
import subprocess


logger = logging.getLogger(__name__)


DOCKER_IMAGE_YDBD_PACKAGE_SPEC = 'ydb/deploy/docker/debug/pkg.json'
DOCKER_IMAGE_REGISTRY = 'cr.yandex'
DOCKER_IMAGE_REPOSITORY = 'crpbo4q9lbgkn85vr1rm'
DOCKER_IMAGE_NAME = 'ydb'
DOCKER_IMAGE_FULL_NAME = '%s/%s/%s' % (DOCKER_IMAGE_REGISTRY, DOCKER_IMAGE_REPOSITORY, DOCKER_IMAGE_NAME)


def get_user():
    try:
        return os.environ['USER'].lower()
    except KeyError:
        print('', file=sys.stderr)
        print("unable to get USER env var", file=sys.stderr)
        print("please specify USER env var or use '--tag' argument", file=sys.stderr)
        sys.exit(2)


def get_image_from_args(args):
    if args.image is not None and args.tag is not None:
        print('', file=sys.stderr)
        print("image and tag arguments are mutually exclusive", file=sys.stderr)
        print("please specify either image or tag argument", file=sys.stderr)
        sys.exit(2)

    if args.image is not None:
        return args.image
    else:
        if args.tag is None:
            user = get_user()
            tag = '%s-latest' % user
        else:
            tag = args.tag
        return "%s:%s" % (DOCKER_IMAGE_FULL_NAME, tag)


def docker_tag(old, new):
    proc = subprocess.Popen(['docker', 'tag', old, new], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        _, stderr = proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise RuntimeError("docker tag: timed out")
    if proc.returncode != 0:
        raise RuntimeError("docker tag: failed with code %d, error: %s" % (proc.returncode, stderr))


def docker_inspect(obj):
    proc = subprocess.Popen(['docker', 'inspect', obj], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise RuntimeError("docker inspect: timed out")
    if proc.returncode == 1 and 'No such object' in stderr:
        logger.debug('docker inspect: object %s not found', obj)
        return None
    elif proc.returncode != 0:
        raise RuntimeError("docker inspect: failed with code %d, error: %s" % (proc.returncode, stderr))
    else:
        return json.loads(stdout)


def docker_push(image):
    proc = subprocess.Popen(['docker', 'push', image], text=True)
    proc.communicate()
    if proc.returncode != 0:
        logger.error(f'command failed: docker push {image}')
        sys.exit(1)
