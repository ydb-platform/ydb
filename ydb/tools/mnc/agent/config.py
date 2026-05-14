import os
from pathlib import Path


mnc_home = "/Berkanavt/multinode_home"
user = os.environ.get("USER")
if user:
    mnc_home = f"/home/{user}/multinode_home"


def ensure_mnc_home():
    path = Path(mnc_home)
    path.mkdir(parents=True, exist_ok=True)
    return str(path)

