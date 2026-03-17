# -*- coding: utf-8 -*-

class CrxHeaderInfo:
    """
    CRX file header basic info.
    """

    def __init__(self, crx_id, public_key):
        """
        Create CrxHeaderInfo with file_hash, crx_id and public_key.
        :param crx_id: crx id (encoded in base16 using the characters [a-p]).
        :param public_key: public key (PEM format, without the header/footer).
        """
        self.crx_id = crx_id
        self.public_key = public_key

    def __str__(self):
        return 'CrxHeaderInfo(crx_id=%s, public_key=%s)' % (self.crx_id, self.public_key)

    def __eq__(self, other):
        if not isinstance(other, CrxHeaderInfo):
            return False
        return self.crx_id == other.crx_id and self.public_key == other.public_key
