import ipaddress
import re
from typing import Dict, Any, Union

from .parse import BaseParser, IPBaseKeys
from .servers import IPv4Allocations


class RIRParser(BaseParser):
    date_keys = (
        IPBaseKeys.ORG_REG_DATE,
        IPBaseKeys.ORG_UPDATED,
        IPBaseKeys.REG_DATE,
        IPBaseKeys.UPDATED,
    )

    # ARIN is used as "base" expression list
    reg_expressions = {
        IPBaseKeys.NET_RANGE: r"NetRange: *(.+)",
        IPBaseKeys.CIDR: r"CIDR: *(.+)",
        IPBaseKeys.NET_NAME: r"NetName: *(.+)",
        IPBaseKeys.NET_HANDLE: r"NetHandle: *(.+)",
        IPBaseKeys.NET_TYPE: r"NetType: *(.+)",
        IPBaseKeys.PARENT: r"Parent: *(.+)",
        IPBaseKeys.ORIGIN_AS: r"OriginAS: *(.+)",
        IPBaseKeys.ORGANIZATION: r"Organization: *(.+)",
        IPBaseKeys.REG_DATE: r"RegDate: *(.+)",
        IPBaseKeys.UPDATED: r"Updated: *(.+)",
        IPBaseKeys.RDAP_IP_REF: r"Updated: *.+\nRef: *(.+)",
        IPBaseKeys.ORG_NAME: r"OrgName: *(.+)",
        IPBaseKeys.ORG_ID: r"OrgId: *(.+)",
        IPBaseKeys.ORG_ADDRESS: r"Address: *(.+)",
        IPBaseKeys.ORG_CITY: r"City: *(.+)",
        IPBaseKeys.ORG_STATE: r"StateProv: *(.+)",
        IPBaseKeys.ORG_COUNTRY: r"Country: *(.+)",
        IPBaseKeys.ORG_ZIPCODE: r"PostalCode: *(.+)",
        IPBaseKeys.ORG_REG_DATE: r"Country: *.+\nRegDate: *(.+)",
        IPBaseKeys.ORG_UPDATED: r"Country: *.+\nRegDate: *.+\nUpdated: *(.+)",
        IPBaseKeys.ORG_RDAP_REF: r"Country: *.+\nRegDate: *.+\nUpdated: *.+\nRef: *(.+)",
        IPBaseKeys.ABUSE_HANDLE: r"OrgAbuseHandle: *(.+)",
        IPBaseKeys.ABUSE_NAME: r"OrgAbuseName: *(.+)",
        IPBaseKeys.ABUSE_PHONE: r"OrgAbusePhone: *(.+)",
        IPBaseKeys.ABUSE_EMAIL: r"OrgAbuseEmail: *(.+)",
        IPBaseKeys.ABUSE_ADDRESS: r"",  # not for ARIN, but for other RIRs
        IPBaseKeys.ABUSE_RDAP_REF: r"OrgAbuseRef: *(.+)",
        IPBaseKeys.ROUTING_HANDLE: r"OrgRoutingHandle: *(.+)",
        IPBaseKeys.ROUTING_NAME: r"OrgRoutingName: *(.+)",
        IPBaseKeys.ROUTING_PHONE: r"OrgRoutingPhone: *(.+)",
        IPBaseKeys.ROUTING_EMAIL: r"OrgRoutingEmail: *(.+)",
        IPBaseKeys.ROUTING_ADDRESS: r"",  # not for ARIN, but for other RIRs
        IPBaseKeys.ROUTING_RDAP_REF: r"OrgRoutingRef: *(.+)",
        IPBaseKeys.TECH_HANDLE: r"OrgTechHandle: *(.+)",
        IPBaseKeys.TECH_NAME: r"OrgTechName: *(.+)",
        IPBaseKeys.TECH_ADDRESS: r"",  # not for ARIN, but for other RIRs
        IPBaseKeys.TECH_PHONE: r"OrgTechPhone: *(.+)",
        IPBaseKeys.TECH_EMAIL: r"OrgTechEmail: *(.+)",
    }


class NumberParser:
    def __init__(self):
        self.servers = IPv4Allocations()

    def parse(
        self,
        blob: str,
        ip: Union[ipaddress.IPv4Address],
    ) -> dict[IPBaseKeys, Any]:
        _, server = self.servers.get_servers(ip)
        parser = self._init_parser(server)
        return parser.parse(blob)

    @staticmethod
    def _init_parser(rir_server: str) -> RIRParser:
        if rir_server == "whois.arin.net":
            return ARINParser()
        elif rir_server == "whois.afrinic.net":
            return AFRINICParser()
        elif rir_server == "whois.apnic.net":
            return APNICParser()
        elif rir_server == "whois.lacnic.net":
            return LACNICParser()
        elif rir_server == "whois.ripe.net":
            return RIPEParser()
        else:
            return ARINParser()  # default


class ARINParser(RIRParser):  # default
    def __init__(self): ...


class AFRINICParser(RIRParser):
    _afrinic = {
        IPBaseKeys.NET_RANGE: r"inetnum: *(.+)",
        IPBaseKeys.NET_NAME: r"netname: *(.+)",
        IPBaseKeys.NET_TYPE: r"status: *(.+)",
        IPBaseKeys.ORIGIN_AS: r"origin: *(.+)",
        IPBaseKeys.PARENT: r"parent: *(.+)",
        IPBaseKeys.ORGANIZATION: r"descr: *(.+)",
        IPBaseKeys.UPDATED: r"last-modified: *(.+)",
        IPBaseKeys.ORG_NAME: r"descr: *(.+)",
        IPBaseKeys.ORG_COUNTRY: r"country: *(.+)",
        IPBaseKeys.ABUSE_HANDLE: r"abuse-c: *(.+)",
        IPBaseKeys.ABUSE_NAME: r"",
        IPBaseKeys.ABUSE_ADDRESS: r"",
        IPBaseKeys.ABUSE_PHONE: r"",
        IPBaseKeys.ABUSE_EMAIL: r"",
        IPBaseKeys.ROUTING_HANDLE: r"admin-c: *(.+)",
        IPBaseKeys.ROUTING_NAME: r"",
        IPBaseKeys.ROUTING_ADDRESS: r"",
        IPBaseKeys.ROUTING_PHONE: r"",
        IPBaseKeys.ROUTING_EMAIL: r"",
        IPBaseKeys.TECH_HANDLE: r"tech-c: *(.+)",
        IPBaseKeys.TECH_NAME: r"",
        IPBaseKeys.TECH_ADDRESS: r"",
        IPBaseKeys.TECH_PHONE: r"",
        IPBaseKeys.TECH_EMAIL: r"",
    }

    _contact_fields = {
        "phone": r"phone: *(.+)",
        "name": r"(?:role|person): *(.+)",
        "address": r"address: *(.+)",
    }

    def __init__(self):
        self.update_reg_expressions(self._afrinic)

    def parse(self, blob: str) -> Dict[IPBaseKeys, Any]:
        parser_output = super().parse(blob)
        contact_field_fills = (
            ("abuse", parser_output.get(IPBaseKeys.ABUSE_HANDLE)),
            ("tech", parser_output.get(IPBaseKeys.TECH_HANDLE)),
            ("routing", parser_output.get(IPBaseKeys.ROUTING_HANDLE)),
        )
        # parse the contact info by looking up each "nic-hdl"
        for prefix, handle in contact_field_fills:
            if handle:
                pattern = re.compile(
                    r"(?:role|person):.+\n(?:.+\n){{1,}}nic-hdl: *{nic_hdl}\n(?:.+\n){{1,}}".format(
                        nic_hdl=handle
                    ),
                    flags=re.I,
                )
                contact_blob = pattern.search(blob)
                if contact_blob:
                    for field, field_regex in self._contact_fields.items():
                        key = getattr(IPBaseKeys, f"{prefix}_{field}".upper())
                        parser_output[key] = self.find_match(
                            field_regex, contact_blob.group()
                        )
        return parser_output


class APNICParser(RIRParser):
    _apnic = {
        IPBaseKeys.NET_RANGE: r"inetnum: *(.+)",
        IPBaseKeys.NET_NAME: r"netname: *(.+)",
        IPBaseKeys.NET_TYPE: r"status: *(.+)",
        IPBaseKeys.ORGANIZATION: r"descr: *(.+)",
        IPBaseKeys.UPDATED: r"last-modified: *(.+)",
        IPBaseKeys.ORG_NAME: r"irt: *(.+)",
        IPBaseKeys.ORG_ADDRESS: r"address: *(.+)",
        IPBaseKeys.ORG_COUNTRY: r"country: *(.+)",
        IPBaseKeys.ORG_UPDATED: r"irt:.+\n(?:.+\n){1,}last-modified: *(.+)",
        IPBaseKeys.ABUSE_HANDLE: r"abuse-c: *(.+)",
        IPBaseKeys.ABUSE_NAME: r"",
        IPBaseKeys.ABUSE_PHONE: r"",
        IPBaseKeys.ABUSE_EMAIL: r"",
        IPBaseKeys.ROUTING_HANDLE: r"admin-c: *(.+)",
        IPBaseKeys.ROUTING_NAME: r"",
        IPBaseKeys.ROUTING_PHONE: r"",
        IPBaseKeys.ROUTING_EMAIL: r"",
        IPBaseKeys.TECH_HANDLE: r"tech-c: *(.+)",
        IPBaseKeys.TECH_NAME: r"",
        IPBaseKeys.TECH_PHONE: r"",
        IPBaseKeys.TECH_EMAIL: r"",
    }

    _contact_fields = {
        "email": r"e-mail: *(.+)",
        "phone": r"phone: *(.+)",
        "name": r"(?:role|person): *(.+)",
        "address": r"address: *(.+)",
    }

    def __init__(self):
        self.update_reg_expressions(self._apnic)

    def parse(self, blob: str) -> Dict[IPBaseKeys, Any]:
        parser_output = super().parse(blob)
        contact_field_fills = (
            ("abuse", parser_output.get(IPBaseKeys.ABUSE_HANDLE)),
            ("tech", parser_output.get(IPBaseKeys.TECH_HANDLE)),
            ("routing", parser_output.get(IPBaseKeys.ROUTING_HANDLE)),
        )
        # parse the contact info by looking up each "nic-hdl"
        for prefix, handle in contact_field_fills:
            if handle:
                pattern = re.compile(
                    r"(?:role|person):.+\n(?:.+\n){{0,}}nic-hdl: *{nic_hdl}\n(?:.+\n){{1,}}".format(
                        nic_hdl=handle
                    ),
                    flags=re.I,
                )
                contact_blob = pattern.search(blob)
                if contact_blob:
                    for field, field_regex in self._contact_fields.items():
                        key = getattr(IPBaseKeys, f"{prefix}_{field}".upper())
                        if field == "address":
                            addresses = self.find_match(
                                field_regex, contact_blob.group(), many=True
                            )
                            parser_output[key] = (
                                ", ".join(addresses) if addresses else ""
                            )
                        else:
                            parser_output[key] = self.find_match(
                                field_regex, contact_blob.group()
                            )

        return parser_output


class LACNICParser(RIRParser):
    _lacnic = {
        IPBaseKeys.NET_RANGE: r"inetnum: *(.+)",
        IPBaseKeys.CIDR: r"inetrev: *(.+)",
        IPBaseKeys.NET_HANDLE: r"ownerid: *(.+)",
        IPBaseKeys.NET_TYPE: r"status: *(.+)",
        IPBaseKeys.ORGANIZATION: r"owner: *(.+)",
        IPBaseKeys.REG_DATE: r"created: *(.+)",
        IPBaseKeys.UPDATED: r"changed: *(.+)",
        IPBaseKeys.ORG_NAME: r"owner: *(.+)",
        IPBaseKeys.ORG_ID: r"ownerid: *(.+)",
        IPBaseKeys.ORG_ADDRESS: r"owner:.+\n(?:.+\n){1,}address: *(.+)",
        IPBaseKeys.ORG_COUNTRY: r"owner:.+\n(?:.+\n){1,}country: *(.+)",
        IPBaseKeys.ORG_REG_DATE: r"created: *(.+)",
        IPBaseKeys.ORG_UPDATED: r"changed: *(.+)",
        IPBaseKeys.ABUSE_HANDLE: r"abuse-c: *(.+)",
        IPBaseKeys.ABUSE_NAME: r"",  # handled in `parse`
        IPBaseKeys.ABUSE_PHONE: r"",
        IPBaseKeys.ABUSE_EMAIL: r"",
        IPBaseKeys.ROUTING_HANDLE: r"owner-c: *(.+)",
        IPBaseKeys.ROUTING_NAME: r"",
        IPBaseKeys.ROUTING_PHONE: r"",
        IPBaseKeys.ROUTING_EMAIL: r"",
        IPBaseKeys.TECH_HANDLE: r"tech-c: *(.+)",
        IPBaseKeys.TECH_NAME: r"",
        IPBaseKeys.TECH_PHONE: r"",
        IPBaseKeys.TECH_EMAIL: r"",
    }

    _contact_fields = {
        "email": r"e-mail: *(.+)",
        "phone": r"phone: *(.+)",
        "name": r"(?:role|person): *(.+)",
        "address": r"address: *(.+)",
    }

    def __init__(self):
        self.update_reg_expressions(self._lacnic)

    def parse(self, blob: str) -> Dict[IPBaseKeys, Any]:
        parser_output = super().parse(blob)
        contact_field_fills = (
            ("abuse", parser_output.get(IPBaseKeys.ABUSE_HANDLE)),
            ("tech", parser_output.get(IPBaseKeys.TECH_HANDLE)),
            ("routing", parser_output.get(IPBaseKeys.ROUTING_HANDLE)),
        )
        # parse contact info using each "nic-hdl"
        for prefix, handle in contact_field_fills:
            pattern = re.compile(
                r"nic-hdl(?:.){{0,}}:*{nic_hdl}\n(?:.+\n){{1,}}".format(nic_hdl=handle),
                flags=re.I,
            )
            contact_blob = pattern.search(blob)
            if contact_blob:
                for field, field_regex in self._contact_fields.items():
                    key = getattr(IPBaseKeys, f"{prefix}_{field}".upper())
                    if field == "address":
                        addresses = self.find_match(
                            field_regex, contact_blob.group(), many=True
                        )
                        parser_output[key] = ", ".join(addresses) if addresses else ""
                    else:
                        parser_output[key] = self.find_match(
                            field_regex, contact_blob.group()
                        )

        return parser_output


class RIPEParser(RIRParser):
    _ripe = {
        IPBaseKeys.NET_RANGE: r"inetnum: *(.+)",
        IPBaseKeys.NET_NAME: r"netname: *(.+)",
        IPBaseKeys.NET_TYPE: r"status: *(.+)",
        IPBaseKeys.ORIGIN_AS: r"origin: *(.+)",
        IPBaseKeys.ORGANIZATION: r"descr: *(.+)",
        IPBaseKeys.REG_DATE: r"created: *(.+)",
        IPBaseKeys.UPDATED: r"last-modified: *(.+)",
        IPBaseKeys.ABUSE_HANDLE: r"abuse-c: *(.+)",
        IPBaseKeys.ABUSE_NAME: r"",
        IPBaseKeys.ABUSE_PHONE: r"",
        IPBaseKeys.ABUSE_EMAIL: r"",
        IPBaseKeys.ROUTING_HANDLE: r"admin-c: *(.+)",
        IPBaseKeys.ROUTING_NAME: r"",
        IPBaseKeys.ROUTING_PHONE: r"",
        IPBaseKeys.ROUTING_EMAIL: r"",
        IPBaseKeys.TECH_HANDLE: r"tech-c: *(.+)",
        IPBaseKeys.TECH_NAME: r"",
        IPBaseKeys.TECH_PHONE: r"",
        IPBaseKeys.TECH_EMAIL: r"",
    }

    _contact_fields = {
        "email": r"abuse-mailbox: *(.+)",
        "phone": r"phone: *(.+)",
        "name": r"(?:role|person): *(.+)",
        "address": r"address: *(.+)",
    }

    def __init__(self):
        self.update_reg_expressions(self._ripe)

    def parse(self, blob: str) -> Dict[IPBaseKeys, Any]:
        parser_output = super().parse(blob)
        contact_field_fills = (
            ("abuse", parser_output.get(IPBaseKeys.ABUSE_HANDLE)),
            ("tech", parser_output.get(IPBaseKeys.TECH_HANDLE)),
            ("routing", parser_output.get(IPBaseKeys.ROUTING_HANDLE)),
        )
        # parse the contact info by looking up each "nic-hdl"
        for prefix, handle in contact_field_fills:
            if handle:
                pattern = re.compile(
                    r"(?:role|person):.+\n(?:.+\n){{1,}}nic-hdl: *{nic_hdl}\n(?:.+\n){{1,}}".format(
                        nic_hdl=handle
                    ),
                    flags=re.I,
                )
                contact_blob = pattern.search(blob)
                if contact_blob:
                    for field, field_regex in self._contact_fields.items():
                        key = getattr(IPBaseKeys, f"{prefix}_{field}".upper())
                        if field == "address":
                            addresses = self.find_match(
                                field_regex, contact_blob.group(), many=True
                            )
                            parser_output[key] = (
                                ", ".join(addresses) if addresses else ""
                            )
                        else:
                            parser_output[key] = self.find_match(
                                field_regex, contact_blob.group()
                            )

        return parser_output
