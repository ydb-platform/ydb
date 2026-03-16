# Pamela: yet another Python wrapper for PAM

There seems to be a glut of Python wrappers for PAM that have since been abandoned.
This repo merges two separate efforts:

- [gnosek/python-pam](https://github.com/gnosek/python-pam)
  - adds wrappers for a few more calls, e.g. opening sessions
  - raises PamError on failure instead of returning False, with informative error messages
- [simplepam](https://github.com/leonnnn/python3-simplepam)
  - adds Python 3 support
  - resets credentials after authentication, apparently for kerberos users

## Why?

Both projects appear to be abandoned, with no response to issues or pull requests in at least a year, and I need it for [JupyterHub](https://github.com/jupyter/jupyterhub).

## Use it

Install:

    pip install pamela

Test:

    python -m pamela -a `whoami`
