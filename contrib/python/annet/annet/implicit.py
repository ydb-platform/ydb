from collections import OrderedDict as odict

from annet.annlib.rbparser import syntax


def config(config_tree, rules):
    implicit_config_tree = odict()
    for row, rule in rules.items():
        matched_lines = [line for line in config_tree.keys() if rule["regexp"].match(line)]
        if rule["type"] != "ignore":
            if not any(matched_lines) and row not in config_tree:
                implicit_config_tree[row] = odict()
        for line in matched_lines:
            implicit_config_tree[line] = config(config_tree[line], rule["children"])
    return implicit_config_tree


def compile_rules(device):
    return compile_tree(_implicit_tree(device))


def compile_tree(tree):
    rules = odict()
    for _, attrs in tree.items():
        rule = {
            "type": attrs["type"],
            "children": compile_tree(attrs["children"]) if attrs.get("children") else odict(),
            "regexp": attrs["params"]["regexp"] or syntax.compile_row_regexp(attrs["row"]),
        }
        rules[attrs["row"]] = rule
    return rules


def _implicit_tree(device):
    text = ""
    if device.hw.Huawei:
        if device.hw.Huawei.CE:
            text = """
                stp mode mstp
                stp enable
                undo ntp server disable
                undo telnet server disable
                undo dhcp enable
                !user-interface con *
                    user privilege level 3
                !user-interface vty ~
                    protocol inbound all
                netconf
                port link-flap trigger error-down
            """
        elif device.hw.Huawei.NE:
            text = """
                 !bgp *
                     !ipv4-family unicast
                         undo synchronization
                     !ipv6-family unicast
                         undo synchronization
                 !user-interface con *
                     user privilege level 3
                 !user-interface vty ~
                     protocol inbound all
                 aaa
                    undo user-password complexity-check
                 netconf
                 """
        elif device.hw.Huawei.Quidway.S5700.S5735I:
            text = """
                !interface (?!Vlanif).*
                    port link-type access %regexp=port link-type .*
                interface NULL0
            """
        elif device.hw.Huawei.Quidway.S2x:
            text = """
                !interface (?!Vlanif).*
                    port link-type hybrid %regexp=port link-type .*
                interface NULL0
            """
        else:
            text = """
                stp mode mstp
                !interface X?GigabitEthernet*
                    bpdu enable
                netconf
            """
    elif device.hw.Arista:
        # This part of configuration will not be visible in configuration
        text = "\n".join(
            (
                "ip load-sharing trident fields ipv6 destination-port source-ip ingress-interface destination-ip "
                "source-port flow-label",
                "ip load-sharing trident fields ip source-ip source-port destination-ip destination-port "
                "ingress-interface",
            )
        )
    elif device.hw.Nexus:
        text = r"""
                # This part of configuration will not be visible in configuration if enabled
                snmp-server enable traps link linkDown
                snmp-server enable traps link linkUp
        """
        if (
            device.hw.Nexus.N3x.N3432
            or (device.hw.Nexus.N9x.N9500 and "spine1" in device.tags)
            or device.hw.Nexus.N9x.N9316
            or device.hw.Cisco.Nexus.N9x.N9364
        ):
            text += r"""
                # SVI
                !interface Vlan*
                    !shutdown
                !interface mgmt[0-9]*
                    no shutdown
                    mtu 1500
                !interface Ethernet1*
                    no shutdown
                # Лупбеки
                !interface */Loopback[0-9.]+/
                    no shutdown
                # Агрегаты
                !interface */port-channel[0-9.]+/
                    no shutdown
                # BGP
                !router bgp *
                    !neighbor *
                        no shutdown
            """

        elif device.hw.Nexus.N3x:
            # Cisco Nexus has some specific related to "shutdown" command
            # Behavior is cheked on Cisco Nexus 3132Q 6.0(2)U6(7)
            text += r"""
                # SVI
                !interface Vlan*
                    !shutdown
                !interface mgmt[0-9]*
                    no shutdown
                # Physical and NOT splitted interfaces and subifs
                !interface */Ethernet1\/[0-9.]*/
                    no shutdown
                # Physical and NOT splitted interfaces and subifs
                !interface */Ethernet1\/[0-9]+\/[0-9.]+/
                    # только explicit
                # Loopbacks
                !interface */Loopback[0-9.]+/
                    no shutdown
                # Port-Channels
                !interface */port-channel[0-9.]+/
                    no shutdown
                # BGP
                !router bgp *
                    !neighbor *
                        no shutdown
            """
    elif device.hw.Cisco:
        # C2900/C3500/C3600/AIR does not support the MTU on a per-interface basis
        if (
            device.hw.Cisco.Catalyst.C2900
            or device.hw.Cisco.Catalyst.C3500
            or device.hw.Cisco.Catalyst.C3600
            or device.hw.Cisco.AIR
        ):
            text += r"""
                !interface */\S*Ethernet\S+/
                    no shutdown
                !interface */Loopback[0-9.]+/
                    no shutdown
                !interface */port-channel[0-9.]+/
                    no shutdown
            """
        elif device.hw.Cisco.XR:
            text += r"""
                !interface */\S*Ethernet\S+/
                    mtu 1500
                    no shutdown
                !interface */Loopback[0-9.]+/
                    no shutdown
                !interface */port-channel[0-9.]+/
                    mtu 1500
                    no shutdown
            """
        else:
            text += r"""
                !interface */\S*Ethernet\S+/
                    mtu 1500
                    no shutdown
                !interface */Loopback[0-9.]+/
                    mtu 1500
                    no shutdown
                !interface */port-channel[0-9.]+/
                    mtu 1500
                    no shutdown
            """
        if device.hw.Cisco.Catalyst:
            # this configuration is not visible in running-config when enabled
            # no vstack and system mtu are default values
            text += r"""
                # line console aaa config
                !line con 0
                    authorization exec default
                no vstack
                system mtu routing 1500
                system mtu jumbo 9000
            """

    return parse_text(text)


def parse_text(text):
    return syntax.parse_text(
        text,
        {
            "regexp": {
                "default": None,
                "validator": lambda x: syntax.compile_row_regexp(x),
            },
        },
    )
