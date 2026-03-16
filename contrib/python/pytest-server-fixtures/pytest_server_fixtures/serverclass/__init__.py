"""
Implementation of how a server fixture will run.
"""
# flake8: noqa
from __future__ import absolute_import

def create_server(server_class, **kwargs):
    if server_class == 'thread':
        from .thread import ThreadServer
        return ThreadServer(
            cmd=kwargs["cmd_local"],
            get_args=kwargs["get_args"],
            env=kwargs["env"],
            workspace=kwargs["workspace"],
            cwd=kwargs["cwd"],
            listen_hostname=kwargs["listen_hostname"],
        )

    if server_class == 'docker':
        from .docker import DockerServer
        return DockerServer(
            server_type=kwargs["server_type"],
            cmd=kwargs["cmd"],
            get_args=kwargs["get_args"],
            env=kwargs["env"],
            image=kwargs["image"],
            labels=kwargs["labels"],
        )

    if server_class == 'kubernetes':
        from .kubernetes import KubernetesServer
        return KubernetesServer(
            server_type=kwargs["server_type"],
            cmd=kwargs["cmd"],
            get_args=kwargs["get_args"],
            env=kwargs["env"],
            image=kwargs["image"],
            labels=kwargs["labels"],
        )
