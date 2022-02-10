class Warning(Exception):
    pass


class Error(Exception):
    def __init__(self, message, issues=None, status=None):

        pretty_issues = _pretty_issues(issues)
        message = message if pretty_issues is None else pretty_issues

        super(Error, self).__init__(message)
        self.issues = issues
        self.message = message
        self.status = status


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def _pretty_issues(issues):
    if issues is None:
        return None

    children_messages = [_get_messages(issue, root=True) for issue in issues]

    if None in children_messages:
        return None

    return "\n" + "\n".join(children_messages)


def _get_messages(issue, max_depth=100, indent=2, depth=0, root=False):
    if depth >= max_depth:
        return None
    margin_str = " " * depth * indent
    pre_message = ""
    children = ""
    if issue.issues:
        collapsed_messages = []
        while not root and len(issue.issues) == 1:
            collapsed_messages.append(issue.message)
            issue = issue.issues[0]
        if collapsed_messages:
            pre_message = margin_str + ", ".join(collapsed_messages) + "\n"
            depth += 1
            margin_str = " " * depth * indent
        else:
            pre_message = ""

        children_messages = [
            _get_messages(iss, max_depth=max_depth, indent=indent, depth=depth + 1)
            for iss in issue.issues
        ]

        if None in children_messages:
            return None

        children = "\n".join(children_messages)

    return (
        pre_message
        + margin_str
        + issue.message
        + "\n"
        + margin_str
        + "severity level: "
        + str(issue.severity)
        + "\n"
        + margin_str
        + "issue code: "
        + str(issue.issue_code)
        + "\n"
        + children
    )
