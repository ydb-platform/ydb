import tempfile
import pkgutil


class _CertManager(object):
    def __init__(self):
        self._file = tempfile.NamedTemporaryFile(prefix='fb_ca_chain_bundle.', suffix='.crt')
        self._file.write(pkgutil.get_data(__package__, 'fb_ca_chain_bundle.crt'))

    def get_cert_file(self):
        return self._file.name


cert_manager = _CertManager()
