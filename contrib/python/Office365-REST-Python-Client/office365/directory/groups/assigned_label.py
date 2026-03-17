from office365.runtime.client_value import ClientValue


class AssignedLabel(ClientValue):
    """
    Represents a sensitivity label assigned to a Microsoft 365 group. Sensitivity labels allow administrators
    to enforce specific group settings on a group by assigning a classification to the group (such as Confidential,
    Highly Confidential or General). Sensitivity labels are published by administrators in Microsoft 365 Security and
    Compliance Center as part of Microsoft Purview Information Protection capabilities. For more information about
    sensitivity labels, see Sensitivity labels overview.
    """
