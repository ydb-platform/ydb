from typing import Optional

from annet.adapters.netbox.common.models import NetboxDevice
from annet.rpl import R, Route, RouteMap


routemap = RouteMap[NetboxDevice]()


def find_loopback(device: NetboxDevice) -> Optional[str]:
    for iface in device.interfaces:
        if iface.name.lower().startswith("lo"):
            return iface.name
    return None


SOME_CONDITION = (R.rd.has_any("RD_EXAMPLE1")) & (R.protocol == "bgp")


@routemap
def example1(device: NetboxDevice, route: Route):
    # condition can be referenced as global constant
    with route(SOME_CONDITION, number=1, name="n1") as rule:
        rule.set_local_pref(100)
        rule.set_metric(100)
        rule.add_metric(200)
        rule.community.set("COMMUNITY_EXAMPLE_ADD")
        rule.as_path.set(12345, "123456")
        rule.allow()

    loopback = find_loopback(device)
    if loopback:  # rules can be generated dynamically
        with route(
            R.community.has("COMMUNITY_EXAMPLE_REMOVE"),
            R.interface == loopback,  # conditions can reference calculated values
            number=2,
            name="n2",
        ) as rule:
            rule.set_local_pref(100)
            rule.add_metric(200)
            rule.community.add("COMMUNITY_EXAMPLE_ADD")
            rule.community.remove("COMMUNITY_EXAMPLE_REMOVE")
            rule.allow()
    # default rule with no conditions
    with route(number=100, name="last") as rule:
        rule.deny()


@routemap
def example2(device: NetboxDevice, route: Route):
    with route(R.as_path_filter("ASP_EXAMPLE"), number=3, name="n3") as rule:
        rule.deny()
    with route(R.match_v6("IPV6_LIST_EXAMPLE"), number=4, name="n4") as rule:
        rule.allow()
    with route(R.match_v4("IPV4_LIST_EXAMPLE", or_longer=(29, 48)), number=5, name="n5") as rule:
        rule.allow()

    with route(R.as_path_length >= 1, R.as_path_length <= 20, number=6, name="n6") as rule:
        rule.allow()
    with route(R.extcommunity_rt.has("EXTCOMMUNITY_EXAMPLE_REMOVE"), number=7, name="n7") as rule:
        rule.extcommunity_rt.remove("EXTCOMMUNITY_EXAMPLE_REMOVE")
        rule.deny()


# this policy is not used in mesh model, so will be filtered out
@routemap
def unused(device: NetboxDevice, route: Route):
    with route(number=3, name="n3") as rule:
        rule.deny()
