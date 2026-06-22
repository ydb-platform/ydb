import argparse
import json

import random
from scaler.config import Config
from scaler.gh import Github
from scaler.cloud_config import create_userdata
from scaler.yc import YandexCloudProvider

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf", help="yaml app config", required=True)
    parser.add_argument("--preset", help="Name of preset", required=True)
    parser.add_argument("--vm-name", help="Name of vm", required=True)
    parser.add_argument("--label", help="github runner label", required=True)
    parser.add_argument("--disable-update", help="Disable agent update", action="store_true")
    args = parser.parse_args()

    runner_name = args.vm_name
    labels = [args.label]
    preset_name = args.preset

    cfg = Config.load(args.conf)

    yc_auth_sa_key = None

    if cfg.yc_auth_sa_key:
        with open(cfg.yc_auth_sa_key, "r") as fp:
            yc_auth_sa_key = json.load(fp)

    yc = YandexCloudProvider(cfg.yc_auth_use_metadata, yc_auth_sa_key)

    cfg.late_discover_from_yc(yc)

    yc.set_config(cfg)

    gh = Github(cfg.gh_repo, cfg.gh_token)
    new_runner_token = gh.get_new_runner_token()

    user_data = create_userdata(gh.html_url, new_runner_token, runner_name, labels, cfg.ssh_keys,
                                cfg.agent_mirror_url_prefix, preset_name, disable_update=args.disable_update)
    placement = random.choice(cfg.yc_zones)
    zone_id = placement['zone_id']
    subnet_id = placement['subnet_id']

    print(f"start runner {runner_name} in {zone_id} ({labels})")

    yc.start_vm(zone_id, subnet_id, runner_name, preset_name, user_data, {})


if __name__ == "__main__":
    main()
