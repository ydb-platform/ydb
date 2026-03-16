"""Datastore simulator, custom actions."""


def device_reset(_registers, _inx, _cell):
    """Use example custom action."""


custom_actions_dict = {
    "umg804_reset": device_reset,
}
