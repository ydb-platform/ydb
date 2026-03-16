from typing import Optional

from asn1crypto import cms, core, x509

__all__ = [
    'Target',
    'TargetCert',
    'Targets',
    'SequenceOfTargets',
    'AttrSpec',
    'AAControls',
]


class TargetCert(core.Sequence):
    _fields = [
        ('target_certificate', cms.IssuerSerial),
        ('target_name', x509.GeneralName, {'optional': True}),
        ('cert_digest_info', cms.ObjectDigestInfo, {'optional': True}),
    ]


class Target(core.Choice):
    _alternatives = [
        ('target_name', x509.GeneralName, {'explicit': 0}),
        ('target_group', x509.GeneralName, {'explicit': 1}),
        ('target_cert', TargetCert, {'explicit': 2}),
    ]


class Targets(core.SequenceOf):
    _child_spec = Target


# Blame X.509...
class SequenceOfTargets(core.SequenceOf):
    _child_spec = Targets


class AttrSpec(core.SequenceOf):
    _child_spec = cms.AttCertAttributeType


class AAControls(core.Sequence):
    _fields = [
        ('path_len_constraint', core.Integer, {'optional': True}),
        ('permitted_attrs', AttrSpec, {'optional': True, 'implicit': 0}),
        ('excluded_attrs', AttrSpec, {'optional': True, 'implicit': 1}),
        ('permit_unspecified', core.Boolean, {'default': True}),
    ]

    def accept(self, attr_id: cms.AttCertAttributeType) -> bool:
        attr_id_str = attr_id.native
        excluded = self['excluded_attrs'].native
        if excluded is not None:
            excluded = frozenset(excluded)
        if excluded is not None and attr_id_str in excluded:
            return False
        permitted = self['permitted_attrs'].native
        if permitted is not None:
            permitted = frozenset(permitted)
        if permitted is not None and attr_id_str in permitted:
            return True
        return bool(self['permit_unspecified'])

    @classmethod
    def read_extension_value(
        cls, cert: x509.Certificate
    ) -> Optional['AAControls']:
        # handle AA controls (not natively supported by asn1crypto, so
        # not available as an attribute).
        try:
            return next(
                ext['extn_value'].parsed
                for ext in cert['tbs_certificate']['extensions']
                if ext['extn_id'].native == 'aa_controls'
            )
        except StopIteration:
            return None


# patch in attribute certificate extensions
# Note: unlike in Certomancer, we don't do this one conditionally, since
# we need the actual Python types to agree with what we export

ext_map = x509.ExtensionId._map
ext_specs = x509.Extension._oid_specs

ext_map['2.5.29.55'] = 'target_information'
ext_specs['target_information'] = SequenceOfTargets

ext_map['2.5.29.56'] = 'no_rev_avail'
ext_specs['no_rev_avail'] = core.Null

ext_map['1.3.6.1.5.5.7.1.6'] = 'aa_controls'
ext_specs['aa_controls'] = AAControls

ext_map['1.3.6.1.5.5.7.1.4'] = 'audit_identity'
ext_specs['audit_identity'] = core.OctetString
