class PersonalizationScope:
    """Specifies the personalization scope for the LimitedWebPartManager object"""

    User = 0
    """Personalization data that is user-specific, as well as personalization data that applies to all users,
    is loaded for all Web Parts on a Web Part Page that support personalization data. Only personalization data that
    is user-specific is saved on the Web Part Page. When referring to the scope associated
    with a Web Parts control property, User scope indicates that the property can only load and store data applicable
    to all users when running on a page in Shared scope. However, when the property's control is running on a page
    in User scope, the property's per-user and all-user data will be loaded and merged. In this case, though,
    only per-user data will be saved when a page is running in User scope."""

    Shared = 1
    """
    Personalization data that applies to all users is loaded for all Web Parts on a Web Part Page and is available
    to be saved on the Web Part Page. When referring to the scope associated with a Web Parts control property,
    Shared scope indicates that the property normally only allows loading or saving of data associated with all users.
    """
