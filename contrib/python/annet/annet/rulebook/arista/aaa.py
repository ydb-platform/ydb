from dataclasses import dataclass

from annet.annlib.types import Op


@dataclass
class UserConfig:
    name: str
    privilege: int
    role: str
    secret_type: str
    secret: str


def _parse_user_config(config_line):
    """Convert a user config line into a dataclass. Config example:

    username someuser privilege 15 role network-admin secret sha512 $6$....

    privilege could be omitted if equal to 1
    role could be omitted
    secret could be omitted, 'nopassword' is provided instead
    """
    splstr = config_line.split()
    name = splstr[1]
    priv = 1
    role = ""
    secret_type = ""
    secret = ""
    if "privilege" in splstr:
        pos = splstr.index("privilege")
        priv = int(splstr[pos + 1])
    if "role" in splstr:
        pos = splstr.index("role")
        role = splstr[pos + 1]
    if "secret" in splstr:
        pos = splstr.index("secret")
        secret_type = splstr[pos + 1]
        secret = splstr[pos + 2]
    return UserConfig(name=name, privilege=priv, role=role, secret_type=secret_type, secret=secret)


def user(key, diff, **_):
    if diff[Op.ADDED] and not diff[Op.REMOVED]:
        for add in diff[Op.ADDED]:
            yield (True, add["row"], None)
    elif diff[Op.REMOVED] and not diff[Op.ADDED]:
        for _ in diff[Op.REMOVED]:
            yield (False, f"no username {key[0]}", None)
    else:
        for num, add in enumerate(diff[Op.ADDED]):
            new_user = _parse_user_config(add["row"])
            old_user = _parse_user_config(diff[Op.REMOVED][num]["row"])
            if new_user.privilege != old_user.privilege:
                yield (True, f"username {key[0]} privilege {new_user.privilege}", None)
            if new_user.role != old_user.role and not new_user.role:
                yield (True, f"no username {key[0]} role", None)
            elif new_user.role != old_user.role:
                yield (True, f"username {key[0]} role {new_user.role}", None)
            if new_user.secret != old_user.secret and not new_user.secret:
                yield (True, f"username {key[0]} nopassword", None)
            elif new_user.secret != old_user.secret:
                yield (True, f"username {key[0]} secret {new_user.secret_type} {new_user.secret}", None)
