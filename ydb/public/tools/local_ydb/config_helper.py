# -*- coding: utf-8 -*-
"""Helper module for local_ydb to provide minimal configuration template."""

import os
from importlib_resources import read_binary


def get_minimal_config_path():
    """
    Returns the path to the minimal YAML configuration template for local_ydb.
    
    This is a minimal configuration optimized for Docker images, with:
    - Auto-configuration for actor system (removed explicit actor_system_config)
    - Only essential sections included
    - Removed redundant configurations like PQ, SQS, Federated Query, etc.
    """
    # Create a temporary file with the minimal config
    import tempfile
    import yaml
    
    data = read_binary(__package__, "resources/minimal_yaml.yml")
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    
    # Create a temporary file that will be used by the config generator
    # We return the template content directly since it will be processed by KikimrConfigGenerator
    fd, path = tempfile.mkstemp(suffix='.yml', prefix='local_ydb_config_')
    os.write(fd, data)
    os.close(fd)
    return path
