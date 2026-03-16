from ncclient.operations import OperationError
from ncclient.xml_ import *

from ncclient.operations.rpc import RPC

def global_operations(node):
    """Instantiate an SR OS global operation action element

    Args:
        node: A string representing the top-level action for a
            given global operation.
    Returns:
        A tuple of 'lxml.etree._Element' values.  The first value
        represents the top-level YANG action element and the second
        represents the caller supplied initial node.
    """
    parent, child = yang_action('global-operations',
            attrs={'xmlns': SROS_GLOBAL_OPS_NS})
    ele = sub_ele(child, node)
    return (parent, ele)

class MdCliRawCommand(RPC):
    def request(self, command=None):
        node, raw_cmd_node = global_operations('md-cli-raw-command')
        sub_ele(raw_cmd_node, 'md-cli-input-line').text = command
        self._huge_tree = True
        return self._request(node)


class Commit(RPC):
    "`commit` RPC. Depends on the `:candidate` capability, and the `:confirmed-commit`."

    DEPENDS = [':candidate']

    def request(self, confirmed=False, timeout=None, persist=None, persist_id=None, comment=None):
        """Commit the candidate configuration as the device's new current configuration. Depends on the `:candidate` capability.

        A confirmed commit (i.e. if *confirmed* is `True`) is reverted if there is no followup commit within the *timeout* interval. If no timeout is specified the confirm timeout defaults to 600 seconds (10 minutes). A confirming commit may have the *confirmed* parameter but this is not required. Depends on the `:confirmed-commit` capability.

        *confirmed* whether this is a confirmed commit

        *timeout* specifies the confirm timeout in seconds

        *persist* make the confirmed commit survive a session termination, and set a token on the ongoing confirmed commit

        *persist_id* value must be equal to the value given in the <persist> parameter to the original <commit> operation.

        *comment* a descriptive text comment that will be associated with the commit action. This is useful for logging or audit purposes.
        """

        node = new_ele("commit")
        if comment and comment.strip():
            sub_ele(node, "comment",
                    attrs={'xmlns': "urn:nokia.com:sros:ns:yang:sr:ietf-netconf-augments"}).text = comment

        if persist and persist_id:
            raise OperationError("Invalid operation as persist cannot be present with persist-id")
        if confirmed:
            self._assert(":confirmed-commit")
            sub_ele(node, "confirmed")
            if timeout is not None:
                sub_ele(node, "confirm-timeout").text = timeout
            if persist is not None:
                sub_ele(node, "persist").text = persist
        if persist_id:
            sub_ele(node, "persist-id").text = persist_id

        return self._request(node)
