from pathlib import Path

from wasabi import msg

from ..util import load_project_config, logger
from .main import Arg, app
from .remote_storage import RemoteStorage, get_command_hash, get_content_hash


@app.command("push")
def project_push_cli(
    # fmt: off
    remote: str = Arg("default", help="Name or path of remote storage"),
    project_dir: Path = Arg(Path.cwd(), help="Location of project directory. Defaults to current working directory.", exists=True, file_okay=False),
    # fmt: on
):
    """Persist outputs to a remote storage. You can alias remotes in your
    project.yml by mapping them to storage paths. A storage can be anything that
    the smart_open library can upload to, e.g. AWS, Google Cloud Storage, SSH,
    local directories etc.

    DOCS: https://github.com/explosion/weasel/tree/main/docs/cli.md#arrow_up-push
    """
    for output_path, url in project_push(project_dir, remote):
        if url is None:
            msg.info(f"Skipping {output_path}")
        else:
            msg.good(f"Pushed {output_path} to {url}")


def project_push(project_dir: Path, remote: str):
    """Persist outputs to a remote storage. You can alias remotes in your project.yml
    by mapping them to storage paths. A storage can be anything that the smart_open
    library can upload to, e.g. gcs, aws, ssh, local directories etc
    """
    config = load_project_config(project_dir)
    if remote in config.get("remotes", {}):
        remote = config["remotes"][remote]
    storage = RemoteStorage(project_dir, remote)
    for cmd in config.get("commands", []):
        logger.debug("CMD: %s", cmd["name"])
        deps = [project_dir / dep for dep in cmd.get("deps", [])]
        if any(not dep.exists() for dep in deps):
            logger.debug("Dependency missing. Skipping %s outputs", cmd["name"])
            continue
        cmd_hash = get_command_hash(
            "", "", [project_dir / dep for dep in cmd.get("deps", [])], cmd["script"]
        )
        logger.debug("CMD_HASH: %s", cmd_hash)
        for output_path in cmd.get("outputs", []):
            output_loc = project_dir / output_path
            if output_loc.exists() and _is_not_empty_dir(output_loc):
                url = storage.push(
                    output_path,
                    command_hash=cmd_hash,
                    content_hash=get_content_hash(output_loc),
                )
                logger.debug(
                    "URL: %s for output %s with cmd_hash %s", url, output_path, cmd_hash
                )
                yield output_path, url


def _is_not_empty_dir(loc: Path):
    if not loc.is_dir():
        return True
    elif any(_is_not_empty_dir(child) for child in loc.iterdir()):
        return True
    else:
        return False
