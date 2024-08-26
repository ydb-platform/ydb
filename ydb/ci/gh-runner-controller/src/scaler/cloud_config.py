import pkgutil

import yaml


def create_userdata(repo_url, gh_token, runner_name, runner_labels, ssh_keys, agent_mirror_url_prefix):
    runner_username = "runner"

    install_script = pkgutil.get_data(__name__, "scripts/install_runner.sh")

    runner_labels = ",".join(runner_labels)

    env_file = f"""
RUNNER_USERNAME="{runner_username}"
REPO_URL="{repo_url}"
GITHUB_TOKEN="{gh_token}"
RUNNER_NAME="{runner_name}"
RUNNER_LABELS="{runner_labels}"
AGENT_MIRROR_URL_PREFIX="{agent_mirror_url_prefix}"
""".strip().encode(
        "utf8"
    )

    cloud_cfg = {
        "package_update": False,
        "package_upgrade": False,
        "system_info": {
            "default_user": {
                "name": runner_username,
                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                "groups": ["sudo", "adm", "docker"],
                "shell": "/bin/bash",
            },
        },
        "write_files": [
            {
                "content": install_script,
                "path": "/install_runner.sh",
                "owner": "root:root",
                "permissions": "0755",
            },
            {
                "content": env_file,
                "path": "/install_runner.env",
                "owner": "root:root",
                "permissions": "0600",
            },
        ],
        "runcmd": [
            "bash /install_runner.sh",
            "rm -f /install_runner.env /install_runner.sh",
        ],
    }
    if ssh_keys:
        cloud_cfg["ssh_authorized_keys"] = ssh_keys

    return f"#cloud-config\n{yaml.dump(cloud_cfg)}"
