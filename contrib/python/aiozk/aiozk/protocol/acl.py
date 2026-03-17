from .part import Part
from .primitives import Int, UString, Vector
from .request import Request
from .response import Response
from .stat import Stat


class ID(Part):
    parts = (
        ('scheme', UString),
        ('id', UString),
    )


class ACL(Part):
    """
    ACL object. Used to control access to its znodes.

    Do not create this object directly, use ``aiozk,ACL.make()`` instead.
    """

    READ_PERM = 1 << 0
    WRITE_PERM = 1 << 1
    CREATE_PERM = 1 << 2
    DELETE_PERM = 1 << 3
    ADMIN_PERM = 1 << 4

    parts = (
        ('perms', Int),
        ('id', ID),
    )

    @classmethod
    def make(cls, scheme, id, read=False, write=False, create=False, delete=False, admin=False):
        """
        Create ACL

        :param str scheme: ACL scheme, one of following:

            - **world** has a single id, anyone, that represents **anyone**.
            - **auth** doesn't use any id, represents any authenticated user.
            - **digest** uses a username:password string to generate MD5 hash
              which is then used as an ACL ID identity.
            - **host** uses the client host name as an ACL ID identity.
            - **ip** uses the client host IP or CIDR as an ACL ID identity.

        :param str id: ACL ID identity.

        :param bool read: Permission, can get data from a node
          and list its children

        :param bool write: Permission, can set data for a node

        :param bool create: Permission, can create a child node

        :param bool delete: Permission, can delete a child node

        :param bool admin: Permission, can set permissions

        :return: ACL instance
        :rtype: aiozk.ACL
        """
        instance = cls(id=ID(scheme=scheme, id=id))
        instance.set_perms(read, write, create, delete, admin)

        return instance

    def set_perms(self, read, write, create, delete, admin):
        perms = 0
        if read:
            perms |= self.READ_PERM
        if write:
            perms |= self.WRITE_PERM
        if create:
            perms |= self.CREATE_PERM
        if delete:
            perms |= self.DELETE_PERM
        if admin:
            perms |= self.ADMIN_PERM

        self.perms = perms


WORLD_READABLE = ACL.make(scheme='world', id='anyone', read=True, write=False, create=False, delete=False, admin=False)

AUTHED_UNRESTRICTED = ACL.make(scheme='auth', id='', read=True, write=True, create=True, delete=True, admin=True)

UNRESTRICTED_ACCESS = ACL.make(scheme='world', id='anyone', read=True, write=True, create=True, delete=True, admin=True)


class GetACLRequest(Request):
    opcode = 6
    parts = (('path', UString),)


class GetACLResponse(Response):
    opcode = 6
    parts = (
        ('acl', Vector.of(ACL)),
        ('stat', Stat),
    )


class SetACLRequest(Request):
    opcode = 7
    parts = (
        ('path', UString),
        ('acl', Vector.of(ACL)),
        ('version', Int),
    )


class SetACLResponse(Response):
    opcode = 7
    parts = (('stat', Stat),)
