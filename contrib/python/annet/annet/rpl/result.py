from enum import Enum


class ResultType(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    NEXT = "next"
    NEXT_POLICY = "next_policy"
