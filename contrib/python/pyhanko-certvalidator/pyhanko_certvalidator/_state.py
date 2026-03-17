from typing import Optional

from asn1crypto import x509
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.util import ConsList


class ValProcState:
    def __init__(
        self,
        *,
        cert_path_stack: ConsList[ValidationPath],
        ee_name_override: Optional[str] = None,
        is_side_validation: bool = False,
    ):
        if cert_path_stack.head is None:
            raise ValueError("Empty path stack")
        self.index: int = 0
        self.ee_name_override = ee_name_override
        self.is_side_validation = bool(
            is_side_validation or cert_path_stack.tail
        )
        self.cert_path_stack = cert_path_stack

    @property
    def path_len(self):
        """
        Length of the path being validated.

        .. note::
            This is the path length in the sense of RFC 5280, i.e.
            the root doesn't count.
        """
        from pyhanko_certvalidator.path import ValidationPath

        path = self.cert_path_stack.head
        assert isinstance(path, ValidationPath)
        return path.pkix_len

    @property
    def is_ee_cert(self) -> bool:
        return self.index == self.path_len

    def check_path_verif_recursion(self, ee_cert: x509.Certificate):
        """
        Helper method to avoid recursion in indirect CRL validation.
        There are some questionable-but-technically-valid CA setups where
        a CRL issuer is authorised to assert its own revocation status,
        which could cause a naive implementation to recurse.
        """
        from pyhanko_certvalidator.path import ValidationPath

        path: ValidationPath
        for path in self.cert_path_stack:
            cert = path.get_ee_cert_safe()
            if cert and cert.sha256 == ee_cert.sha256:
                return path
        return None

    def describe_cert(self, def_interm=False, never_def=False):
        """
        :return:
            A unicode string describing the position of a certificate
            in the chain
        """

        prefix = not never_def
        if self.index < 1 and self.ee_name_override is None:
            # catchall default
            result = "certificate"
        elif not self.is_ee_cert:
            prefix &= def_interm
            result = f'intermediate certificate {self.index}'
        elif self.ee_name_override is not None:
            result = self.ee_name_override
        else:
            result = 'end-entity certificate'
        if prefix:
            return "the " + result
        else:
            return result
