from office365.runtime.client_value import ClientValue


class ComplianceTagInfo(ClientValue):
    def __init__(
        self,
        is_record=None,
        is_regulatory=None,
        should_keep=None,
        tag_name=None,
        unified_rule_id=None,
        unified_tag_id=None,
    ):
        """
        :param bool is_record:
        :param bool is_regulatory:
        :param bool should_keep:
        :param str tag_name:
        :param str unified_rule_id:
        :param str unified_tag_id:
        """
        self.IsRecord = is_record
        self.IsRegulatory = is_regulatory
        self.ShouldKeep = should_keep
        self.TagName = tag_name
        self.UnifiedRuleId = unified_rule_id
        self.UnifiedTagId = unified_tag_id

    @property
    def entity_type_name(self):
        return "SP.ComplianceFoundation.Models.ComplianceTagInfo"
