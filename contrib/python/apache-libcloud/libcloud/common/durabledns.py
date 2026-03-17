# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from typing import Dict, List
from xml.etree import ElementTree as ET  # noqa

from libcloud.common.base import XmlResponse, ConnectionUserAndKey

# API HOST to connect
API_HOST = "durabledns.com"


def _schema_builder(urn_nid, method, attributes):
    """
    Return a xml schema used to do an API request.

    :param urn_nid: API urn namespace id.
    :type urn_nid: type: ``str``

    :param method: API method.
    :type method: type: ``str``

    :param attributes: List of attributes to include.
    :type attributes: ``list`` of ``str``

    rtype: :class:`Element`
    """
    soap = ET.Element("soap:Body", {"xmlns:m": "https://durabledns.com/services/dns/%s" % method})
    urn = ET.SubElement(soap, "urn:{}:{}".format(urn_nid, method))
    # Attributes specification
    for attribute in attributes:
        ET.SubElement(urn, "urn:{}:{}".format(urn_nid, attribute))
    return soap


SCHEMA_BUILDER_MAP = {
    "list_zones": {
        "urn_nid": "listZoneswsdl",
        "method": "listZones",
        "attributes": ["apiuser", "apikey"],
    },
    "list_records": {
        "urn_nid": "listRecordswsdl",
        "method": "listRecords",
        "attributes": ["apiuser", "apikey", "zonename"],
    },
    "get_zone": {
        "urn_nid": "getZonewsdl",
        "method": "getZone",
        "attributes": ["apiuser", "apikey", "zonename"],
    },
    "get_record": {
        "urn_nid": "getRecordwsdl",
        "method": "getRecord",
        "attributes": ["apiuser", "apikey", "zonename", "recordid"],
    },
    "create_zone": {
        "urn_nid": "createZonewsdl",
        "method": "createZone",
        "attributes": [
            "apiuser",
            "apikey",
            "zonename",
            "ns",
            "mbox",
            "refresh",
            "retry",
            "expire",
            "minimum",
            "ttl",
            "xfer",
            "update_acl",
        ],
    },
    "create_record": {
        "urn_nid": "createRecordwsdl",
        "method": "createRecord",
        "attributes": [
            "apiuser",
            "apikey",
            "zonename",
            "name",
            "type",
            "data",
            "aux",
            "ttl",
            "ddns_enabled",
        ],
    },
    "update_zone": {
        "urn_nid": "updateZonewsdl",
        "method": "updateZone",
        "attributes": [
            "apiuser",
            "apikey",
            "zonename",
            "ns",
            "mbox",
            "refresh",
            "retry",
            "expire",
            "minimum",
            "ttl",
            "xfer",
            "update_acl",
        ],
    },
    "update_record": {
        "urn_nid": "updateRecordwsdl",
        "method": "updateRecord",
        "attributes": [
            "apiuser",
            "apikey",
            "zonename",
            "id",
            "name",
            "aux",
            "data",
            "ttl",
            "ddns_enabled",
        ],
    },
    "delete_zone": {
        "urn_nid": "deleteZonewsdl",
        "method": "deleteZone",
        "attributes": ["apiuser", "apikey", "zonename"],
    },
    "delete_record": {
        "urn_nid": "deleteRecordwsdl",
        "method": "deleteRecord",
        "attributes": ["apiuser", "apikey", "zonename", "id"],
    },
}


class DurableDNSException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "DurableDNSException {} {}".format(self.code, self.message)


class DurableResponse(XmlResponse):
    errors = []  # type: List[Dict]
    objects = []  # type: List[Dict]

    def __init__(self, response, connection):
        super().__init__(response=response, connection=connection)

        self.objects, self.errors = self.parse_body_and_error()
        if self.errors:
            raise self._make_excp(self.errors[0])

    def parse_body_and_error(self):
        """
        Used to parse body from httplib.HttpResponse object.
        """
        objects = []
        errors = []
        error_dict = {}
        extra = {}
        zone_dict = {}
        record_dict = {}
        xml_obj = self.parse_body()

        # pylint: disable=no-member
        envelop_body = list(xml_obj)[0]
        method_resp = list(envelop_body)[0]
        # parse the xml_obj
        # handle errors
        if "Fault" in method_resp.tag:
            fault = [fault for fault in list(method_resp) if fault.tag == "faultstring"][0]
            error_dict["ERRORMESSAGE"] = fault.text.strip()
            error_dict["ERRORCODE"] = self.status
            errors.append(error_dict)

        # parsing response from listZonesResponse
        if "listZonesResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            for element in answer:
                zone_dict["id"] = list(element)[0].text
                objects.append(zone_dict)
                # reset the zone_dict
                zone_dict = {}
        # parse response from listRecordsResponse
        if "listRecordsResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            for element in answer:
                for child in list(element):
                    if child.tag == "id":
                        record_dict["id"] = child.text.strip()
                objects.append(record_dict)
                # reset the record_dict for later usage
                record_dict = {}
        # parse response from getZoneResponse
        if "getZoneResponse" in method_resp.tag:
            for child in list(method_resp):
                if child.tag == "origin":
                    zone_dict["id"] = child.text.strip()
                    zone_dict["domain"] = child.text.strip()
                elif child.tag == "ttl":
                    zone_dict["ttl"] = int(child.text.strip())
                elif child.tag == "retry":
                    extra["retry"] = int(child.text.strip())
                elif child.tag == "expire":
                    extra["expire"] = int(child.text.strip())
                elif child.tag == "minimum":
                    extra["minimum"] = int(child.text.strip())
                else:
                    if child.text:
                        extra[child.tag] = child.text.strip()
                    else:
                        extra[child.tag] = ""
                    zone_dict["extra"] = extra
            objects.append(zone_dict)
        # parse response from getRecordResponse
        if "getRecordResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            for child in list(method_resp):
                if child.tag == "id" and child.text:
                    record_dict["id"] = child.text.strip()
                elif child.tag == "name" and child.text:
                    record_dict["name"] = child.text.strip()
                elif child.tag == "type" and child.text:
                    record_dict["type"] = child.text.strip()
                elif child.tag == "data" and child.text:
                    record_dict["data"] = child.text.strip()
                elif child.tag == "aux" and child.text:
                    record_dict["aux"] = child.text.strip()
                elif child.tag == "ttl" and child.text:
                    record_dict["ttl"] = child.text.strip()
            if not record_dict:
                error_dict["ERRORMESSAGE"] = "Record does not exist"
                error_dict["ERRORCODE"] = 404
                errors.append(error_dict)
            objects.append(record_dict)
            record_dict = {}
        if "createZoneResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            if answer.tag == "return" and answer.text:
                record_dict["id"] = answer.text.strip()
            objects.append(record_dict)
        # catch Record does not exists error when deleting record
        if "deleteRecordResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            if "Record does not exists" in answer.text.strip():
                errors.append({"ERRORMESSAGE": answer.text.strip(), "ERRORCODE": self.status})
        # parse response in createRecordResponse
        if "createRecordResponse" in method_resp.tag:
            answer = list(method_resp)[0]
            record_dict["id"] = answer.text.strip()
            objects.append(record_dict)
            record_dict = {}

        return (objects, errors)

    def parse_body(self):
        # A problem arise in the api response because there are undeclared
        # xml namespaces. In order to fix that at the moment, we use the
        # _fix_response method to clean up since we won't always have lxml
        # library.
        self._fix_response()
        body = super().parse_body()
        return body

    def success(self):
        """
        Used to determine if the request was successful.
        """
        return len(self.errors) == 0

    def _make_excp(self, error):
        return DurableDNSException(error["ERRORCODE"], error["ERRORMESSAGE"])

    def _fix_response(self):
        items = re.findall('<ns1:.+ xmlns:ns1="">', self.body, flags=0)
        for item in items:
            parts = item.split(" ")
            prefix = parts[0].replace("<", "").split(":")[1]
            new_item = "<" + prefix + ">"
            close_tag = "</" + parts[0].replace("<", "") + ">"
            new_close_tag = "</" + prefix + ">"
            self.body = self.body.replace(item, new_item)
            self.body = self.body.replace(close_tag, new_close_tag)


class DurableConnection(ConnectionUserAndKey):
    host = API_HOST
    responseCls = DurableResponse

    def add_default_params(self, params):
        params["user_id"] = self.user_id
        params["key"] = self.key
        return params

    def add_default_headers(self, headers):
        headers["Content-Type"] = "text/xml"
        headers["Content-Encoding"] = "gzip; charset=ISO-8859-1"
        return headers
