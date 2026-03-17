import svn.local
import svn.remote


def get_client(url_or_path, *args, **kwargs):
    if url_or_path[0] == '/':
        return svn.local.LocalClient(url_or_path, *args, **kwargs)
    else:
        return svn.remote.RemoteClient(url_or_path, *args, **kwargs)
