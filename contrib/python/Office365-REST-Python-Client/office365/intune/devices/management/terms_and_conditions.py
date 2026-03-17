from office365.entity import Entity


class TermsAndConditions(Entity):
    """
    A termsAndConditions entity represents the metadata and contents of a given Terms and Conditions (T&C) policy.
    T&C policies’ contents are presented to users upon their first attempt to enroll into Intune and subsequently
    upon edits where an administrator has required re-acceptance. They enable administrators to communicate
    the provisions to which a user must agree in order to have devices enrolled into Intune.
    """
