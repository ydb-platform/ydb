import re
from enum import Enum
from datetime import datetime
from typing import Dict, List, Any, Union

from dateutil.parser import parse, ParserError
from dateutil import tz


class TLDBaseKeys(str, Enum):
    DOMAIN_NAME = "domain_name"

    CREATED = "created"
    UPDATED = "updated"
    EXPIRES = "expires"

    REGISTRAR = "registrar"
    REGISTRAR_IANA_ID = "registrar_iana_id"
    REGISTRAR_URL = "registrar_url"
    REGISTRAR_ABUSE_EMAIL = "registrar_abuse_email"
    REGISTRAR_ABUSE_PHONE = "registrar_abuse_phone"

    REGISTRANT_NAME = "registrant_name"
    REGISTRANT_ORGANIZATION = "registrant_organization"
    REGISTRANT_ADDRESS = "registrant_address"
    REGISTRANT_CITY = "registrant_city"
    REGISTRANT_STATE = "registrant_state"
    REGISTRANT_COUNTRY = "registrant_country"
    REGISTRANT_ZIPCODE = "registrant_zipcode"
    REGISTRANT_PHONE = "registrant_phone"
    REGISTRANT_FAX = "registrant_fax"
    REGISTRANT_EMAIL = "registrant_email"

    ADMIN_NAME = "admin_name"
    ADMIN_ID = "admin_id"
    ADMIN_ORGANIZATION = "admin_organization"
    ADMIN_ADDRESS = "admin_address"
    ADMIN_CITY = "admin_city"
    ADMIN_STATE = "admin_state"
    ADMIN_COUNTRY = "admin_country"
    ADMIN_ZIPCODE = "admin_zipcode"
    ADMIN_PHONE = "admin_phone"
    ADMIN_FAX = "admin_fax"
    ADMIN_EMAIL = "admin_email"

    BILLING_NAME = "billing_name"
    BILLING_ID = "billing_id"
    BILLING_ORGANIZATION = "billing_organization"
    BILLING_ADDRESS = "billing_address"
    BILLING_CITY = "billing_city"
    BILLING_STATE = "billing_state"
    BILLING_COUNTRY = "billing_country"
    BILLING_ZIPCODE = "billing_zipcode"
    BILLING_PHONE = "billing_phone"
    BILLING_FAX = "billing_fax"
    BILLING_EMAIL = "billing_email"

    TECH_NAME = "tech_name"
    TECH_ID = "tech_id"
    TECH_ORGANIZATION = "tech_organization"
    TECH_ADDRESS = "tech_address"
    TECH_CITY = "tech_city"
    TECH_STATE = "tech_state"
    TECH_COUNTRY = "tech_country"
    TECH_ZIPCODE = "tech_zipcode"
    TECH_PHONE = "tech_phone"
    TECH_FAX = "tech_fax"
    TECH_EMAIL = "tech_email"

    DNSSEC = "dnssec"
    STATUS = "status"
    NAME_SERVERS = "name_servers"

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


class IPBaseKeys(str, Enum):
    NET_RANGE = "net_range"
    CIDR = "cidr"
    NET_NAME = "net_name"
    NET_TYPE = "net_type"
    NET_HANDLE = "net_handle"
    PARENT = "parent"
    ORIGIN_AS = "origin_as"
    ORGANIZATION = "organization"
    REG_DATE = "registered_date"
    UPDATED = "updated_date"
    RDAP_IP_REF = "rdap_ref"

    ORG_NAME = "org_name"
    ORG_ID = "org_id"
    ORG_ADDRESS = "org_address"
    ORG_CITY = "org_city"
    ORG_STATE = "org_state"
    ORG_COUNTRY = "org_country"
    ORG_ZIPCODE = "org_zipcode"
    ORG_REG_DATE = "org_registered_date"
    ORG_UPDATED = "org_updated_date"
    ORG_RDAP_REF = "org_rdap_ref"

    ABUSE_HANDLE = "abuse_handle"
    ABUSE_NAME = "abuse_name"
    ABUSE_PHONE = "abuse_phone"
    ABUSE_EMAIL = "abuse_email"
    ABUSE_ADDRESS = "abuse_address"
    ABUSE_RDAP_REF = "abuse_rdap_ref"

    ROUTING_HANDLE = "routing_handle"
    ROUTING_NAME = "routing_name"
    ROUTING_PHONE = "routing_phone"
    ROUTING_EMAIL = "routing_email"
    ROUTING_ADDRESS = "routing_address"
    ROUTING_RDAP_REF = "routing_rdap_ref"

    TECH_HANDLE = "tech_handle"
    TECH_NAME = "tech_name"
    TECH_PHONE = "tech_phone"
    TECH_ADDRESS = "tech_address"
    TECH_EMAIL = "tech_email"

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


def convert_whodap_keys(parser_output: dict) -> dict:
    # keys in both, but with mismatched names
    conversions = [
        (TLDBaseKeys.EXPIRES, "expires_date", False),
        (TLDBaseKeys.UPDATED, "updated_date", False),
        (TLDBaseKeys.CREATED, "created_date", False),
        (TLDBaseKeys.NAME_SERVERS, "nameservers", False),
        (TLDBaseKeys.REGISTRAR_ABUSE_EMAIL, "abuse_email", False),
        (TLDBaseKeys.REGISTRAR_ABUSE_PHONE, "abuse_phone", False),
        (TLDBaseKeys.TECH_EMAIL, "technical_email", False),
        (TLDBaseKeys.TECH_ADDRESS, "technical_address", False),
        (TLDBaseKeys.TECH_ORGANIZATION, "technical_organization", False),
        (TLDBaseKeys.TECH_NAME, "technical_name", False),
        (TLDBaseKeys.TECH_PHONE, "technical_phone", False),
        (TLDBaseKeys.TECH_FAX, "technical_fax", False),
        (TLDBaseKeys.REGISTRAR, "registrar_name", False),
    ]
    for asyncwhois_key, whodap_key, keep in conversions:
        if keep:
            parser_output[asyncwhois_key] = parser_output.get(whodap_key)
        else:
            parser_output[asyncwhois_key] = parser_output.pop(whodap_key)
    # asyncwhois keys not in whodap
    non_whodap_keys = [
        TLDBaseKeys.ADMIN_ID,
        TLDBaseKeys.ADMIN_CITY,
        TLDBaseKeys.ADMIN_STATE,
        TLDBaseKeys.ADMIN_COUNTRY,
        TLDBaseKeys.ADMIN_ZIPCODE,
        TLDBaseKeys.BILLING_ID,
        TLDBaseKeys.BILLING_CITY,
        TLDBaseKeys.BILLING_STATE,
        TLDBaseKeys.BILLING_COUNTRY,
        TLDBaseKeys.BILLING_ZIPCODE,
        TLDBaseKeys.TECH_ID,
        TLDBaseKeys.TECH_CITY,
        TLDBaseKeys.TECH_STATE,
        TLDBaseKeys.TECH_COUNTRY,
        TLDBaseKeys.TECH_ZIPCODE,
        TLDBaseKeys.REGISTRANT_CITY,
        TLDBaseKeys.REGISTRANT_STATE,
        TLDBaseKeys.REGISTRANT_COUNTRY,
        TLDBaseKeys.REGISTRANT_ZIPCODE,
        TLDBaseKeys.REGISTRAR_IANA_ID,
        TLDBaseKeys.REGISTRAR_URL,
        TLDBaseKeys.DNSSEC,
    ]
    for key in non_whodap_keys:
        parser_output[key] = None
    # whodap keys not in asyncwhois
    non_asyncwhois_keys = [
        "registrar_email",
        "registrar_phone",
        "registrar_address",
        "registrar_fax",
    ]
    for key in non_asyncwhois_keys:
        parser_output.pop(key)
    return parser_output


class BaseParser:
    reg_expressions = {}

    date_keys = ()
    multiple_match_keys = ()

    # For handling special cases in TLD parser classes
    known_date_formats = []
    # Extra formats that dateutil might not figure out
    extra_date_formats = [
        "%Y-%m-%dT%H:%M:%SZ[%Z]",  # 2007-01-26T19:10:31Z[UTC]
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 2018-12-01T16:17:30.568Z
        "%Y-%m-%dT%H:%M:%S%zZ",  # 1970-01-01T02:00:00+02:00Z
        "%Y-%m-%dt%H:%M:%S.%fz",  # 2007-01-26t19:10:31.00z
        "%Y-%m-%d %H:%M:%SZ",  # 2000-08-22 18:55:20Z
        "before %b-%Y",  # before aug-1996
    ]
    # Additional timezone info for dateutil
    timezone_info = {
        "KST": tz.gettz("Asia/Seoul"),  # Korea Standard Time UTC+9
        "JST": tz.gettz("Asia/Tokyo"),  # Japan Standard Time UTC+9
        "EEST": tz.gettz("Europe/Athens"),  # Eastern European Summertime UTC+3
    }

    def update_reg_expressions(self, expressions_update: Dict[str, Any]) -> None:
        """
        Updates the `reg_expressions` dictionary
        :param expressions_update: dict of keys/regexes to update
        """
        self.reg_expressions.update(expressions_update)

    def parse(self, blob: str) -> Dict[Union[IPBaseKeys, TLDBaseKeys], Any]:
        """
        Iterates over the `reg_expressions` dictionary attempting to use each regex to extract values
        from `blob`, the output from the whois server.

        Assumes that the keys and regular expressions are formatted in the output `blob` such
        that a re.findall operation will work correctly. If this is not the case, you should implement
        your own version of this function in the appropriate BaseParser child class.

        :param blob: the output from the whois server
        :return: dictionary of parsed key/value pairs
        """
        parsed_output = {}
        for key, regex in self.reg_expressions.items():
            if not regex:
                parsed_output[key] = None
            else:
                many = key in self.multiple_match_keys
                parsed_output[key] = self.find_match(regex, blob, many=many)
                if key in self.date_keys and parsed_output.get(key, None):
                    parsed_output[key] = self._parse_date(parsed_output.get(key))
        return parsed_output

    def find_match(
        self,
        regex: str,
        blob: str,
        flags: re.RegexFlag = re.IGNORECASE,
        many: bool = False,
    ) -> Union[str, List[str], None]:
        """
        Performs the given regex operation on the raw output `blob`

        :param regex: the regex to use against blob
        :param blob: the raw output from the whois server
        :param flags: the optional flags to pass to the `re` method
        :param many: if True this function will use re.findall for many matches else re.search for single match
        """
        if many:
            matches = re.findall(regex, blob, flags=flags)
            return [self._process(m) for m in matches if m]
        else:
            match = re.search(regex, blob, flags=flags)
            if match:
                return self._process(match.group(1))
            return None

    def find_multiline_match(self, start: str, blob: str) -> List[str]:
        """
        Used to find multiple lines related to a single key within the
        WHOIS query response. Assumes the values are on a newline below
        the key, and that a blank line separates the last value from the
        next key or end of the text.

        Example:
        -------
        example_blob = '''
            Name servers:
                 ns1.google.com
                 ns2.google.com
                 ns3.google.com
                 ns4.google.com

        '''
        find_multiline_match('Name servers:\n', example_blob)
        # would return... ['ns1.google.com', 'ns2.google.com', 'ns3.google.com', 'ns4.google.com']

        :param start: a key that identifies where to begin the multiline search in blob
        :param blob: the whois query text
        :return: a list of values as strings
        """
        matches = []
        regex_string = start + r"\s+([A-Za-z0-9\.\s]+\n\n)"
        multiline_match = re.search(regex_string, blob, re.DOTALL | re.IGNORECASE)
        if multiline_match:
            matches = self._process_many(multiline_match.group(1))
        return matches

    def _parse_date(self, date_string: str) -> Union[datetime, str]:
        """
        Attempts to convert the given date string to a datetime.datetime object
        otherwise returns the input `date_string`
        :param date_string: a date string
        :return: a datetime.datetime object
        """

        def _datetime_or_none(dt_string: str, dt_format: str) -> Union[datetime, None]:
            try:
                return datetime.strptime(dt_string, dt_format)
            except ValueError:
                return None

        # first, try the known formats
        for date_format in self.known_date_formats:
            if date := _datetime_or_none(date_string, date_format):
                return date
        # next, try dateutil.parse
        try:
            clean_date_string = re.sub(r"\(([^)]+)\)", r"\1", date_string).strip()
            return parse(clean_date_string, tzinfos=self.timezone_info)
        except ParserError:
            pass
        # finally, try extra formats
        for date_format in self.extra_date_formats:
            if date := _datetime_or_none(date_string, date_format):
                return date
        # no luck parsing
        return date_string

    def _process_many(self, match: str) -> List[str]:
        if "\n" in match:
            match = match.split("\n")
            matches = [self._process(m) for m in match if m]
            return [m for m in matches if m]  # remove empty strings
        else:
            match = self._process(match)
            return [match] if match else []  # remove empty strings

    @staticmethod
    def _process(match: str) -> str:
        if match:
            return match.rstrip("\r").rstrip("\n").lstrip("\t").lstrip().rstrip()
