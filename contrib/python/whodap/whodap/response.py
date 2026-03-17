from datetime import datetime
from json import dumps, loads
from types import SimpleNamespace
from typing import Dict, Any, List, Union, Optional

from .utils import WHOISKeys, RDAPVCardKeys
from .errors import RDAPConformanceException

REDACTED = "REDACTED FOR PRIVACY"


class RDAPResponse(SimpleNamespace):
    """
    Base class representing an RDAP Response
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return self.to_json(indent=2)

    def __repr__(self):
        return self.to_json(indent=2)

    @staticmethod
    def _convert_date(ds: str) -> Union[str, datetime]:
        """
        Utility for converting known RDAP date strings
        into Python datetime objects.

        :param ds: a date string
        :return: a datetime object or the original string
        """
        known_rdap_formats = (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            # https://stackoverflow.com/questions/53291250/python-3-6-datetime-strptime-returns-error-while-python-3-7-works-well
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        for date_format in known_rdap_formats:
            try:
                # try every date format used by RDAP
                v = datetime.strptime(ds, date_format)
                return v
            except ValueError:
                continue
        # return original date string if unsuccessful
        return ds

    def _convert_list(self, ls: List[Any]) -> List[Any]:
        """
        Iterates over the given list checking for nested RDAPResponses;
        Recursively calls itself when it encounters another list otherwise
        converts RDAPResponses and appends values.

        :param ls: any list
        :return: another list
        """
        converted = []
        for obj in ls:
            if isinstance(obj, type(self)):
                converted.append(self._convert_self_to_dict(obj))
            elif isinstance(obj, list):
                converted.append(self._convert_list(obj))
            else:
                converted.append(obj)
        return converted

    def _convert_self_to_dict(self, rdr: "RDAPResponse") -> Dict[str, Any]:
        """
        Converts the RDAPResponse to a dictionary.
        Recursively calls itself to convert nested RDAPResponse.

        :param rdr: an instance of RDAPResponse
        :return: the RDAPResponse converted to a dictionary
        """
        converted = {}
        for key, value in rdr.__dict__.items():
            if isinstance(value, list):
                converted[key] = self._convert_list(value)
            elif isinstance(value, type(self)):
                converted[key] = self._convert_self_to_dict(value)
            else:
                converted[key] = value
        return converted

    @classmethod
    def from_json(cls, json: Union[str, bytes]):
        """
        Initializes an instance of DomainResponse from the
        JSON output of an RDAP HTTP query.

        :param json: JSON Response from the RDAP server
        :return: an RDAPResponse
        """
        return loads(json, object_hook=lambda d: cls(**d))

    def to_json(self, **kwargs) -> str:
        """
        Converts the DomainResponse to a JSON string.

        :param kwargs: arguments to be passed to `json.dumps`
        :return: JSON string
        """
        if not kwargs.get("default"):
            kwargs["default"] = self._encoder
        return dumps(self.to_dict(), **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the DomainResponse to a dictionary.
        """
        return self._convert_self_to_dict(self)

    @staticmethod
    def _encoder(x: Any):
        """
        JSON encoding helper for "datetime" objects.

        :param x: Any
        :return: "datetime" as an iso formatted string or Any
        """
        if isinstance(x, datetime):
            return x.isoformat()
        return x

    def _flatten_list(self, ls: List[Any]):
        """
        Recursively flattens the input list.

        :param ls: any list
        :return: a flattened list
        """
        flattened = []
        for i in ls:
            if isinstance(i, list):
                flattened.extend(self._flatten_list(i))
            else:
                flattened.append(i)
        return flattened


class DomainResponse(RDAPResponse):
    def __getattribute__(self, item):
        """
        Converts and returns an "eventDate" value to a datetime object;
        otherwise returns the value of the given attribute
        """
        val = super().__getattribute__(item)
        if item == "eventDate" and val:
            return self._convert_date(val)
        return val

    def to_whois_json(self, **kwargs) -> str:
        """
        Converts the DomainResponse to a WHOIS JSON string.

        :param kwargs: arguments to be passed to `json.dumps`
        :return: JSON string
        """
        if not kwargs.get("default"):
            kwargs["default"] = self._encoder
        return dumps(self.to_whois_dict(), **kwargs)

    def to_whois_dict(
        self, strict: bool = False
    ) -> Dict[WHOISKeys, Union[str, List[str], datetime, None]]:
        """
        Returns the DomainResponse as "flattened" WHOIS dictionary;
        does not modify the original DomainResponse object.

        :param strict: If True, raises an RDAPConformanceException if
          the given RDAP response is incorrectly formatted. Otherwise,
          if False, the method will attempt to parse the RDAP response
          without raising any exception.
        :return: dict with WHOIS keys
        """
        flat = {}

        # traverse and extract information from RDAP fields
        if getattr(self, "nameservers", None):
            flat_nameservers = {"nameservers": []}
            for obj in self.nameservers:
                if hasattr(obj, "ldhName"):
                    flat_nameservers["nameservers"].append(obj.ldhName)
                # if hostnames are not given, try ipv4 addresses
                elif hasattr(obj, "ipAddresses"):
                    if hasattr(obj.ipAddresses, "v4"):
                        flat_nameservers["nameservers"].extend(obj.ipAddresses.v4)

            flat.update(flat_nameservers)

        if getattr(self, "status", None):
            flat.update({"status": self.status})

        if getattr(self, "events", None):
            flat.update(self._flat_dates(self.events))

        try:
            # most common issues stem from nested "entities"
            if getattr(self, "entities", None):
                # _flat_entities will raise an RDAPConformanceException if strict=True
                flat.update(self._flat_entities(self.entities, strict))
        except (TypeError, KeyError, ValueError):
            # handle other edge-cases that make this method "explode"
            if strict:
                raise RDAPConformanceException("Could not parse the response.")

        # convert dict keys over to "WHOISKeys"
        flat = self._construct_flat_dict(flat)

        # add domain name
        flat[WHOISKeys.DOMAIN_NAME] = self.ldhName

        # add dnssec after ensuring that it exists
        if (
            getattr(self, "secureDNS", None)
            and getattr(self.secureDNS, "delegationSigned", None) is not None
        ):
            flat[WHOISKeys.DNSSEC] = (
                "unsigned" if not self.secureDNS.delegationSigned else "signed"
            )

        return flat

    @staticmethod
    def _flat_dates(events: List[SimpleNamespace]) -> Dict[str, datetime]:
        """
        Returns the list of events as a flattened dict of date keys and values

        :param events: list of "events" from the RDAP response
        :return: dictionary of "date" key value pairs
        """
        dates = dict([(event.eventAction, event.eventDate) for event in events])
        return dates

    @staticmethod
    def _check_valid_entities(obj: Any) -> Optional[RDAPConformanceException]:
        # todo: this method will be removed in the future; replaced
        #  by a formal "validation" feature for the library or an implementation
        #  of the icann-tool: https://github.com/icann/rdap-conformance-tool
        if not isinstance(obj, list):
            return RDAPConformanceException(
                f"entities type={type(obj)} is not an Array"
            )

    @staticmethod
    def _check_valid_vcardArray(obj: Any) -> Optional[RDAPConformanceException]:  # noqa
        # todo: this method will be removed in the future; replaced
        #  by a formal "validation" feature for the library or an implementation
        #  of the icann-tool: https://github.com/icann/rdap-conformance-tool
        if not isinstance(obj, list):
            return RDAPConformanceException(
                f"vcardArray type={type(obj)} is not an Array"
            )
        elif len(obj) < 2:
            return RDAPConformanceException("vcardArray is incorrectly formatted")
        elif obj[0] != "vcard" and not isinstance(obj[-1], list):
            return RDAPConformanceException("vcardArray is incorrectly formatted")

    @staticmethod
    def _check_valid_vcard(obj: Any) -> Optional[RDAPConformanceException]:
        # todo: this method will be removed in the future; replaced
        #  by a formal "validation" feature for the library or an implementation
        #  of the icann-tool: https://github.com/icann/rdap-conformance-tool
        if not isinstance(obj, list):
            return RDAPConformanceException(f"vCard type={type(obj)} is not an Array")
        elif len(obj) < 4:
            return RDAPConformanceException("vCard array length is less than < 4")

    def _flat_entities(
        self, entities: List[SimpleNamespace], strict: bool
    ) -> Dict[str, Dict[str, str]]:
        # validate that entities is at least iterable
        conformance_check_exc = self._check_valid_entities(entities)
        if conformance_check_exc is not None:
            if strict:
                raise conformance_check_exc
            else:
                # skip because entity parsing is likely to fail
                return {}
        # attempt to parse entities
        entities_dict = {}
        for entity in entities:
            ent_dict = {}
            # check for redacted information
            mark_redacted = False
            if hasattr(entity, "remarks"):
                for remark in entity.remarks:
                    if hasattr(remark, "title") and "redact" in remark.title.lower():
                        mark_redacted = True
            # check for nested entities
            if hasattr(entity, "entities"):
                # recursive call for nested entities
                ent_dict = self._flat_entities(entity.entities, strict)
                entities_dict.update(ent_dict)
            # iterate through vCard array
            if hasattr(entity, "vcardArray"):
                # basic validation for vcard
                conformance_check_exc = self._check_valid_vcardArray(entity.vcardArray)
                if conformance_check_exc is not None:
                    if strict:
                        raise conformance_check_exc
                    else:
                        # skip because vcardArray parsing is likely to fail
                        continue
                else:
                    # parse vcards
                    for vcard in entity.vcardArray[-1]:
                        conformance_check_exc = self._check_valid_vcard(vcard)
                        if conformance_check_exc is not None:
                            if strict:
                                raise conformance_check_exc
                            else:
                                # skip because vcard parsing is likely to fail
                                continue
                        else:
                            # vCard represents information about an individual or entity.
                            vcard_type = vcard[0]
                            vcard_value = vcard[-1]
                            # check for organization
                            if vcard_type == RDAPVCardKeys.ORG:
                                ent_dict["org"] = vcard_value
                            # check for email
                            elif vcard_type == RDAPVCardKeys.EMAIL:
                                ent_dict["email"] = vcard_value
                            # check for email
                            elif vcard_type == RDAPVCardKeys.CONTACT:
                                ent_dict["contact-uri"] = vcard_value
                            # check for name
                            elif vcard_type == RDAPVCardKeys.FN:
                                ent_dict["name"] = vcard_value
                            # check for address
                            elif vcard_type == RDAPVCardKeys.ADR:
                                values = self._flatten_list(vcard_value)
                                address_string = ", ".join([v for v in values if v])
                                ent_dict["address"] = address_string.lstrip()
                            # check for contact
                            elif vcard_type == RDAPVCardKeys.TEL:
                                # check the "type" of "tel" vcard (either voice or fax):
                                # vcard looks like: ['tel', {"type": "voice"}, 'uri', 'tel:0000000']
                                #               or: ['tel', {"type": ["voice"]}, 'uri', 'tel:0000000']
                                if hasattr(vcard[1], "type"):
                                    contact_type = vcard[1].to_dict().get("type")
                                    if (
                                        contact_type == "voice"
                                        or "voice" in contact_type
                                    ):
                                        ent_dict["phone"] = vcard_value
                                    elif contact_type == "fax" or "fax" in contact_type:
                                        ent_dict["fax"] = vcard_value
                                else:
                                    ent_dict["phone"] = vcard_value

            # add roles for this entity
            if hasattr(entity, "roles"):
                for role in entity.roles:
                    if mark_redacted:
                        for key in ("address", "phone", "name", "org", "email", "fax"):
                            if not ent_dict.get(key):
                                ent_dict[key] = REDACTED
                    # save the information under this "role"
                    entities_dict[role.lower()] = ent_dict

        # return parsed entities dict
        return entities_dict

    @staticmethod
    def _construct_flat_dict(parsed: Dict[str, Any]) -> Dict[WHOISKeys, Any]:
        converted = {
            WHOISKeys.ABUSE_EMAIL: parsed.get("abuse", {}).get("email")
            or parsed.get("abuse", {}).get("contact-uri"),
            WHOISKeys.ABUSE_PHONE: parsed.get("abuse", {}).get("phone"),
            WHOISKeys.ADMIN_NAME: parsed.get("administrative", {}).get("name"),
            WHOISKeys.ADMIN_ORG: parsed.get("administrative", {}).get("org"),
            WHOISKeys.ADMIN_EMAIL: parsed.get("administrative", {}).get("email"),
            WHOISKeys.ADMIN_ADDRESS: parsed.get("administrative", {}).get("address"),
            WHOISKeys.ADMIN_PHONE: parsed.get("administrative", {}).get("phone"),
            WHOISKeys.ADMIN_FAX: parsed.get("administrative", {}).get("fax"),
            WHOISKeys.BILLING_NAME: parsed.get("billing", {}).get("name"),
            WHOISKeys.BILLING_ORG: parsed.get("billing", {}).get("org"),
            WHOISKeys.BILLING_EMAIL: parsed.get("billing", {}).get("email"),
            WHOISKeys.BILLING_ADDRESS: parsed.get("billing", {}).get("address"),
            WHOISKeys.BILLING_PHONE: parsed.get("billing", {}).get("phone"),
            WHOISKeys.BILLING_FAX: parsed.get("billing", {}).get("fax"),
            WHOISKeys.REGISTRANT_NAME: parsed.get("registrant", {}).get("name"),
            WHOISKeys.REGISTRANT_ORG: parsed.get("registrant", {}).get("org"),
            WHOISKeys.REGISTRANT_EMAIL: parsed.get("registrant", {}).get("email"),
            WHOISKeys.REGISTRANT_ADDRESS: parsed.get("registrant", {}).get("address"),
            WHOISKeys.REGISTRANT_PHONE: parsed.get("registrant", {}).get("phone"),
            WHOISKeys.REGISTRANT_FAX: parsed.get("registrant", {}).get("fax"),
            WHOISKeys.REGISTRAR_NAME: parsed.get("registrar", {}).get("name"),
            WHOISKeys.REGISTRAR_EMAIL: parsed.get("registrar", {}).get("email"),
            WHOISKeys.REGISTRAR_ADDRESS: parsed.get("registrar", {}).get("address"),
            WHOISKeys.REGISTRAR_PHONE: parsed.get("registrar", {}).get("phone"),
            WHOISKeys.REGISTRAR_FAX: parsed.get("registrar", {}).get("fax"),
            WHOISKeys.TECHNICAL_NAME: parsed.get("technical", {}).get("name"),
            WHOISKeys.TECHNICAL_ORG: parsed.get("technical", {}).get("org"),
            WHOISKeys.TECHNICAL_EMAIL: parsed.get("technical", {}).get("email"),
            WHOISKeys.TECHNICAL_ADDRESS: parsed.get("technical", {}).get("address"),
            WHOISKeys.TECHNICAL_PHONE: parsed.get("technical", {}).get("phone"),
            WHOISKeys.TECHNICAL_FAX: parsed.get("technical", {}).get("fax"),
            WHOISKeys.CREATED_DATE: parsed.get("registration"),
            WHOISKeys.UPDATED_DATE: parsed.get("last update")
            or parsed.get("last changed"),
            WHOISKeys.EXPIRES_DATE: parsed.get("expiration"),
            WHOISKeys.STATUS: parsed.get("status"),
            WHOISKeys.NAMESERVERS: parsed.get("nameservers"),
        }
        return converted


class IPv4Response(RDAPResponse):
    # IPv4Response has no specific parser utils at this time
    ...


class IPv6Response(RDAPResponse):
    # IPv6Response has no specific parser utils at this time
    ...


class ASNResponse(RDAPResponse):
    # ASNClient has no specific parser utils at this time
    ...
