import os

mnc_home = '/Berkanavt/multinode_home'
user = os.environ.get('USER')
if user:
    mnc_home = f'/home/{user}/multinode_home'


def ensure_mnc_home():
    if not os.path.exists(mnc_home):
        os.makedirs(mnc_home)
    return mnc_home
