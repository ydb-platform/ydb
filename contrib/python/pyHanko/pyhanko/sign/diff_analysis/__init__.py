from .commons import qualify
from .form_rules_api import (
    FieldComparisonContext,
    FieldComparisonSpec,
    FieldMDPRule,
    FormUpdate,
    FormUpdatingRule,
)
from .policies import (
    DEFAULT_DIFF_POLICY,
    NO_CHANGES_DIFF_POLICY,
    StandardDiffPolicy,
)
from .policy_api import (
    DiffPolicy,
    DiffResult,
    ModificationLevel,
    SuspiciousModification,
)
from .rules.file_structure_rules import (
    CatalogModificationRule,
    ObjectStreamRule,
    XrefStreamRule,
)
from .rules.form_field_rules import (
    BaseFieldModificationRule,
    DSSCompareRule,
    GenericFieldModificationRule,
    SigFieldCreationRule,
    SigFieldModificationRule,
)
from .rules.metadata_rules import DocInfoRule, MetadataUpdateRule
from .rules_api import QualifiedWhitelistRule, ReferenceUpdate, WhitelistRule

# export list for compatibility with the old module

__all__ = [
    'ModificationLevel',
    'SuspiciousModification',
    'QualifiedWhitelistRule',
    'WhitelistRule',
    'qualify',
    'ReferenceUpdate',
    'DocInfoRule',
    'DSSCompareRule',
    'MetadataUpdateRule',
    'CatalogModificationRule',
    'ObjectStreamRule',
    'XrefStreamRule',
    'FormUpdatingRule',
    'FormUpdate',
    'FieldMDPRule',
    'FieldComparisonSpec',
    'FieldComparisonContext',
    'GenericFieldModificationRule',
    'SigFieldCreationRule',
    'SigFieldModificationRule',
    'BaseFieldModificationRule',
    'DiffPolicy',
    'StandardDiffPolicy',
    'DEFAULT_DIFF_POLICY',
    'NO_CHANGES_DIFF_POLICY',
    'DiffResult',
]
