from .part import Part
from .primitives import Int, Long


class Stat(Part):
    """
    Znode stat structure

    Contains attributes:

    - **created_zxid** The zxid of the change that created this znode.
    - **last_modified_zxid** The zxid of the change that last modified
      this znode.
    - **created** The time in milliseconds from epoch when this znode
      was created.
    - **modified** The time in milliseconds from epoch when this znode
      was last modified.
    - **version** The number of changes to the data of this znode.
    - **child_version** The number of changes to the children of this znode.
    - **acl_version** The number of changes to the ACL of this znode.
    - **ephemeral_owner** The session id of the owner of this znode
      if the znode is an ephemeral node. If it is not an ephemeral node,
      it will be zero.
    - **data_length** The length of the data field of this znode.
    - **num_children** The number of children of this znode.
    - **last_modified_children** The zxid of the change that last modified
      this znode children.

    """

    parts = (
        ('created_zxid', Long),
        ('last_modified_zxid', Long),
        ('created', Long),
        ('modified', Long),
        ('version', Int),
        ('child_version', Int),
        ('acl_version', Int),
        ('ephemeral_owner', Long),
        ('data_length', Int),
        ('num_children', Int),
        ('last_modified_children', Long),
    )


class StatPersisted(Part):
    """ """

    parts = (
        ('created_zxid', Long),
        ('last_modified_zxid', Long),
        ('created', Long),
        ('modified', Long),
        ('version', Int),
        ('child_version', Int),
        ('acl_version', Int),
        ('ephemeral_owner', Long),
        ('last_modified_children', Long),
    )
