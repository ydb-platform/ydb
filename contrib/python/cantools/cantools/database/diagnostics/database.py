import logging

from .formats import cdd

LOGGER = logging.getLogger(__name__)


class Database:
    """This class contains all DIDs.

    The factory functions :func:`load()<cantools.database.load()>`,
    :func:`load_file()<cantools.database.load_file()>` and
    :func:`load_string()<cantools.database.load_string()>` returns
    instances of this class.

    """

    def __init__(self,
                 dids=None):
        self._name_to_did = {}
        self._identifier_to_did = {}
        self._dids = dids or []
        self.refresh()

    @property
    def dids(self):
        """A list of DIDs in the database.

        """

        return self._dids

    def add_cdd(self, fp):
        """Read and parse CDD data from given file-like object and add the
        parsed data to the database.

        """

        self.add_cdd_string(fp.read())

    def add_cdd_file(self, filename, encoding='utf-8'):
        """Open, read and parse CDD data from given file and add the parsed
        data to the database.

        `encoding` specifies the file encoding.

        """

        with open(filename, encoding=encoding, errors='replace') as fin:
            self.add_cdd(fin)

    def add_cdd_string(self, string):
        """Parse given CDD data string and add the parsed data to the
        database.

        """

        database = cdd.load_string(string)
        self._dids = database.dids
        self.refresh()

    def _add_did(self, did):
        """Add given DID to the database.

        """

        if did.name in self._name_to_did:
            LOGGER.warning("Overwriting DID with name '%s' in the "
                           "name to DID dictionary.",
                           did.name)

        if did.identifier in self._identifier_to_did:
            LOGGER.warning(
                "Overwriting DID '%s' with '%s' in the identifier to DID "
                "dictionary because they have identical identifiers 0x%x.",
                self._identifier_to_did[did.identifier].name,
                did.name,
                did.identifier)

        self._name_to_did[did.name] = did
        self._identifier_to_did[did.identifier] = did

    def get_did_by_name(self, name):
        """Find the DID object for given name `name`.

        """

        return self._name_to_did[name]

    def get_did_by_identifier(self, identifier):
        """Find the DID object for given identifier `identifier`.

        """

        return self._identifier_to_did[identifier]

    def refresh(self):
        """Refresh the internal database state.

        This method must be called after modifying any DIDs in the
        database to refresh the internal lookup tables used when
        encoding and decoding DIDs.

        """

        self._name_to_did = {}
        self._identifier_to_did = {}

        for did in self._dids:
            did.refresh()
            self._add_did(did)

    def __repr__(self):
        lines = []

        for did in self._dids:
            lines.append(repr(did))

            for data in did.datas:
                lines.append('  ' + repr(data))

            lines.append('')

        return '\n'.join(lines)
