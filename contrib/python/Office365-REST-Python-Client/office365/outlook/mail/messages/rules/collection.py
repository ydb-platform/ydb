from office365.entity_collection import EntityCollection
from office365.outlook.mail.messages.rules.rule import MessageRule


class MessageRuleCollection(EntityCollection[MessageRule]):
    def __init__(self, context, resource_path=None):
        super(MessageRuleCollection, self).__init__(context, MessageRule, resource_path)

    def add(self, display_name, sequence, actions, **kwargs):
        """
        Create a messageRule object by specifying a set of conditions and actions.
        Outlook carries out those actions if an incoming message in the user's Inbox meets the specified conditions.
        This API is available in the following national cloud deployments.

        :param str display_name: The display name of the rule.
        :param int sequence: Indicates the order in which the rule is executed, among other rules.
        :param MessageRuleActions actions: Actions to be taken on a message when the corresponding conditions,
            if any, are fulfilled.
        """
        props = {
            "displayName": display_name,
            "sequence": sequence,
            "actions": actions.to_json(),
            **kwargs,
        }
        return super(MessageRuleCollection, self).add(**props)
