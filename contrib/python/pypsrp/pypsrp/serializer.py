# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import base64
import binascii
import logging
import re
import typing
import uuid
import xml.etree.ElementTree as ET
from copy import copy
from queue import Empty, Queue

from cryptography.hazmat.primitives.padding import PKCS7

from pypsrp._utils import to_bytes, to_string, to_unicode
from pypsrp.complex_objects import (
    ApartmentState,
    Color,
    CommandMetadataCount,
    CommandOrigin,
    ComplexObject,
    Coordinates,
    CultureInfo,
    DictionaryMeta,
    GenericComplexObject,
    HostMethodIdentifier,
    InformationalRecord,
    KeyInfoDotNet,
    ListMeta,
    ObjectMeta,
    ParameterMetadata,
    PipelineResultTypes,
    ProgressRecordType,
    PSCredential,
    PSThreadOptions,
    QueueMeta,
    RemoteStreamOptions,
    SessionStateEntryVisibility,
    Size,
    StackMeta,
)
from pypsrp.exceptions import SerializationError
from pypsrp.messages import (
    DebugRecord,
    ErrorRecordMessage,
    InformationRecord,
    VerboseRecord,
    WarningRecord,
)

log = logging.getLogger(__name__)


class Serializer(object):
    def __init__(self) -> None:
        self.obj_id = 0
        self.obj: typing.Dict = {}
        self.tn_id = 0
        self.tn: typing.Dict = {}

        self.cipher: typing.Any = None
        # Finds C0, C1 and surrogate pairs in a unicode string for us to
        # encode according to the PSRP rules
        self._serial_str = re.compile("[\u0000-\u001f\u007f-\u009f\U00010000-\U0010ffff]")

        # to support surrogate UTF-16 pairs we need to use a UTF-16 regex
        # so we can replace the UTF-16 string representation with the actual
        # UTF-16 byte value and then decode that
        self._deserial_str = re.compile(b"\\x00_\\x00x([\\0\\w]{8})\\x00_")

    def serialize(
        self,
        value: typing.Any,
        metadata: typing.Optional[ObjectMeta] = None,
        parent: typing.Optional[ET.Element] = None,
        clear: bool = True,
    ) -> typing.Optional[ET.Element]:
        """
        Serializes a raw value or class into an XML Element that can be sent
        over to the remote host.

        :param value: The value to serialize
        :param metadata: Any extra metadata to control how to serialize the
            value, if None then the value will be inferred by the type
        :param parent: Whether to append the element onto a parent element
        :param clear: Whether to clear the Obj and TN reference map, this
            should only be True when initially calling serialize
        :return: The XML Element from the serializied value
        """
        if clear:
            self._clear()

        if isinstance(value, ET.Element):
            if metadata is not None and metadata.name is not None:
                value.attrib["N"] = metadata.name

            if parent is not None:
                parent.append(value)

            return value

        metadata = metadata or ObjectMeta()
        if metadata.tag == "*":
            if isinstance(value, TaggedValue):
                metadata.tag = value.tag
                value = value.value
            else:
                metadata.tag = self._get_tag_from_value(value)

        pack_function: typing.Callable[[ObjectMeta, typing.Any], ET.Element] = {  # type: ignore[assignment] # Not sure why
            # primitive types
            "S": lambda m, d: self._serialize_string(d),
            "ToString": lambda d: self._serialize_string(d),
            "C": lambda m, d: str(ord(d)),
            "B": lambda m, d: str(d).lower(),
            "DT": lambda m, d: None,
            "TS": lambda m, d: str(d),
            "By": lambda m, d: str(d),
            "SB": lambda m, d: str(d),
            "U16": lambda m, d: str(d),
            "I16": lambda m, d: str(d),
            "U32": lambda m, d: str(d),
            "I32": lambda m, d: str(d),
            "U64": lambda m, d: str(d),
            "I64": lambda m, d: str(d),
            "Sg": lambda m, d: str(d),
            "Db": lambda m, d: str(d),
            "D": lambda m, d: str(d),
            "BA": lambda m, d: to_string(base64.b64encode(d)),
            "G": lambda m, d: str(d),
            "URI": lambda m, d: self._serialize_string(d),
            "Version": lambda m, d: str(d),
            "XD": lambda m, d: self._serialize_string(d),
            "SBK": lambda m, d: self._serialize_string(d),
            "SS": lambda m, d: self._serialize_secure_string(d),
            "Obj": self._serialize_obj,
            "ObjDynamic": self._serialize_dynamic_obj,
            "LST": self._serialize_lst,
            "IE": self._serialize_ie,
            "QUE": self._serialize_que,
            "STK": self._serialize_stk,
            "DCT": self._serialize_dct,
        }[
            metadata.tag
        ]

        if value is None:
            if metadata.optional:
                return None
            element = ET.Element("Nil")
        else:
            element_value = pack_function(metadata, value)
            if isinstance(element_value, str):
                element = ET.Element(metadata.tag)
                element.text = element_value
            else:
                element = element_value

        if metadata.name is not None:
            element.attrib["N"] = metadata.name

        if parent is not None:
            parent.append(element)

        return element

    def deserialize(
        self,
        element: typing.Union[ET.Element, str],
        metadata: typing.Optional[ObjectMeta] = None,
        clear: bool = True,
    ) -> typing.Any:
        if clear:
            self._clear()

        if isinstance(element, str):
            element_string = element
            try:
                element = ET.fromstring(element)
            except ET.ParseError as err:
                log.warning("Failed to parse data '%s' as XML, return raw xml: %s" % (element_string, str(err)))
                return element_string
        else:
            xml_string = ET.tostring(element, encoding="utf-8", method="xml")
            element_string = to_string(xml_string)

        metadata = metadata or ObjectMeta()
        if metadata.tag == "*":
            metadata.tag = element.tag

        # get the object types so we store the TN Ref ids for later use
        obj_types = self._get_types_from_obj(element)

        # check if it is a primitive object
        unpack_function: typing.Optional[typing.Callable[[ET.Element], typing.Any]] = {
            # Primitive types
            "S": lambda d: self._deserialize_string(d.text),
            "ToString": lambda d: self._deserialize_string(d.text),
            "C": lambda d: chr(int(d.text)),
            "B": lambda d: d.text.lower() == "true",
            "DT": lambda d: d.text,
            "TS": lambda d: d.text,
            "By": lambda d: int(d.text),
            "SB": lambda d: int(d.text),
            "U16": lambda d: int(d.text),
            "I16": lambda d: int(d.text),
            "U32": lambda d: int(d.text),
            "I32": lambda d: int(d.text),
            "U64": lambda d: int(d.text),
            "I64": lambda d: int(d.text),
            "Sg": lambda d: float(d.text),
            "Db": lambda d: float(d.text),
            "D": lambda d: d.text,  # TODO: deserialize this
            "BA": lambda d: base64.b64decode(d.text),
            "G": lambda d: uuid.UUID(d.text),
            "URI": lambda d: self._deserialize_string(d.text),
            "Nil": lambda d: None,
            "Version": lambda d: d.text,
            "XD": lambda d: self._deserialize_string(d.text),
            "SBK": lambda d: self._deserialize_string(d.text),
            "SS": lambda d: self._deserialize_secure_string(d),
            # references an object already deserialized
            "Ref": lambda d: self.obj[d.attrib["RefId"]],
        }.get(element.tag)

        if unpack_function is not None:
            return unpack_function(element)

        # not a primitive object, so try and decode the complex object
        if type(metadata) == ObjectMeta and metadata.object is None:
            structures = {
                "Selected.Microsoft.PowerShell.Commands.GenericMeasureInfo": ObjectMeta(
                    "Obj", object=CommandMetadataCount
                ),
                "System.Array": ListMeta(),
                "System.Collections.ArrayList": ListMeta(),
                "System.Collections.Hashtable": DictionaryMeta(),
                "System.Collections.Generic.List": ListMeta(),
                "System.Collections.Queue": QueueMeta(),
                "System.Collections.Stack": StackMeta(),
                "System.ConsoleColor": ObjectMeta("Obj", object=Color),
                "System.Management.Automation.CommandOrigin": ObjectMeta("Obj", object=CommandOrigin),
                "System.Management.Automation.DebugRecord": ObjectMeta("Obj", object=DebugRecord),
                "System.Management.Automation.ErrorRecord": ObjectMeta("Obj", object=ErrorRecordMessage),
                "System.Management.Automation.Host.Coordinates": ObjectMeta("Obj", object=Coordinates),
                "System.Management.Automation.Host.KeyInfo": ObjectMeta("Obj", object=KeyInfoDotNet),
                "System.Management.Automation.Host.Size": ObjectMeta("Obj", object=Size),
                "System.Management.Automation.InformationalRecord": ObjectMeta("Obj", object=InformationalRecord),
                "System.Management.Automation.InformationRecord": ObjectMeta("Obj", object=InformationRecord),
                "System.Management.Automation.ParameterMetadata": ObjectMeta("Obj", object=ParameterMetadata),
                "System.Management.Automation.ProgressRecordType": ObjectMeta("Obj", object=ProgressRecordType),
                "System.Management.Automation.PSBoundParametersDictionary": DictionaryMeta(),
                "System.Management.Automation.PSCredential": ObjectMeta("Obj", object=PSCredential),
                "System.Management.Automation.PSObject": ObjectMeta("ObjDynamic", object=GenericComplexObject),
                "System.Management.Automation.PSPrimitiveDictionary": DictionaryMeta(),
                "System.Management.Automation.PSTypeName": ObjectMeta("S"),
                "System.Management.Automation.Remoting.RemoteHostMethodId": ObjectMeta(
                    "Obj", object=HostMethodIdentifier
                ),
                "System.Management.Automation.Runspaces.ApartmentState": ObjectMeta("Obj", object=ApartmentState),
                "System.Management.Automation.Runspaces.PipelineResultTypes": ObjectMeta(
                    "Obj", object=PipelineResultTypes
                ),
                "System.Management.Automation.Runspaces.PSThreadOptions": ObjectMeta("Obj", object=PSThreadOptions),
                "System.Management.Automation.Runspaces.RemoteStreamOptions": ObjectMeta(
                    "Obj", object=RemoteStreamOptions
                ),
                "System.Management.Automation.SessionStateEntryVisibility": ObjectMeta(
                    "Obj", object=SessionStateEntryVisibility
                ),
                "System.Management.Automation.VerboseRecord": ObjectMeta("Obj", object=VerboseRecord),
                "System.Management.Automation.WarningRecord": ObjectMeta("Obj", object=WarningRecord),
                "System.Globalization.CultureInfo": ObjectMeta("Obj", object=CultureInfo),
                # Fallback to the GenericComplexObject
                "System.Object": ObjectMeta("ObjDynamic", object=GenericComplexObject),
                # Primitive types
                "System.String": ObjectMeta("S"),
                "System.Char": ObjectMeta("C"),
                "System.Boolean": ObjectMeta("B"),
                "System.DateTime": ObjectMeta("DT"),
                # None: ObjectMeta("TS"), # duration timespan
                "System.Byte": ObjectMeta("By"),
                "System.SByte": ObjectMeta("SB"),
                "System.UInt16": ObjectMeta("U16"),
                "System.Int16": ObjectMeta("I16"),
                "System.UInt32": ObjectMeta("U32"),
                "System.Int32": ObjectMeta("I32"),
                "System.UInt64": ObjectMeta("U64"),
                "System.Int64": ObjectMeta("I64"),
                "System.Single": ObjectMeta("Sg"),
                "System.Double": ObjectMeta("Db"),
                "System.Decimal": ObjectMeta("D"),
                # None: ObjectMeta("BA"), # Byte array base64 encoded
                "System.Guid": ObjectMeta("G"),
                "System.Uri": ObjectMeta("URI"),
                "System.Version": ObjectMeta("Version"),
                "System.Xml.XmlDocument": ObjectMeta("XD"),
                "System.Management.Automation.ScriptBlock": ObjectMeta("SBK"),
                "System.Security.SecureString": ObjectMeta("SS"),
            }

            # fallback to GenericComplexObject if no types were defined
            if metadata.tag == "Obj" and len(obj_types) == 0:
                obj_types = ["System.Object"]

            metadata = None
            for obj_type in obj_types:
                if obj_type.startswith("Deserialized.System."):
                    obj_type = obj_type[13:]

                is_list = False
                if obj_type.endswith("[]"):
                    obj_type = obj_type[0:-2]
                    is_list = True
                elif obj_type.startswith("System.Collections.Generic.List`1[["):
                    list_info = obj_type[35:-1]
                    obj_type = list_info.split(",")[0]
                    is_list = True
                elif obj_type.startswith("System.Collections.ObjectModel.Collection`1[["):
                    list_info = obj_type[45:-1]
                    obj_type = list_info.split(",")[0]
                    is_list = True
                elif obj_type.startswith("System.Collections.ObjectModel.ReadOnlyCollection`1[["):
                    list_info = obj_type[53:-1]
                    obj_type = list_info.split(",")[0]
                    is_list = True
                elif obj_type.startswith("System.Collections.Generic.Dictionary`2[["):
                    dict_meta = obj_type[41:-2].split("],[")
                    key_type = structures.get(dict_meta[0].split(",")[0], ObjectMeta())
                    value_type = structures.get(dict_meta[1].split(",")[0], ObjectMeta())
                    metadata = DictionaryMeta(dict_key_meta=key_type, dict_value_meta=value_type)
                    break

                obj_meta = structures.get(obj_type)
                if obj_meta is not None:
                    metadata = obj_meta
                    if is_list:
                        metadata = ListMeta(list_value_meta=metadata)
                    break

        # we were unable to find the complex object type so just return the
        # element
        obj: typing.Any
        if metadata is None:
            obj = element_string
        elif metadata.tag == "Obj":
            obj = self._deserialize_obj(element, metadata)
        elif metadata.tag == "ObjDynamic":
            obj = self._deserialize_dynamic_obj(element, metadata)
        elif metadata.tag == "LST":
            obj = self._deserialize_lst(element, metadata)
        elif metadata.tag == "QUE":
            obj = self._deserialize_que(element)
        elif metadata.tag == "STK":
            obj = self._deserialize_stk(element)
        elif metadata.tag == "DCT":
            obj = self._deserialize_dct(element)
        else:
            log.warning("Unknown metadata tag type '%s', failed to deserialize object" % metadata.tag)
            obj = element_string

        if element.tag == "Obj":
            self.obj[element.attrib["RefId"]] = obj

        if isinstance(obj, ComplexObject):
            obj._xml = element_string

        return obj

    def _get_tag_from_value(
        self,
        value: typing.Any,
    ) -> str:
        # Get's the XML tag based on the value type, this is a simple list
        # and explicit tagging is recommended.

        value_type = type(value)
        if value_type == int:
            return "I32"
        elif value_type == bool:
            return "B"
        elif value_type == float:
            return "Sg"
        elif value_type == str:
            return "S"
        elif value_type == bytes:
            # This will only occur in Python 3 as a byte string in Python 2 is
            # a str. If users on that platform want a BA then they need to
            # explicitly set the metadata themselves
            return "BA"
        elif value_type == uuid.UUID:
            return "G"
        elif value_type == list:
            return "LST"
        elif value_type == dict:
            return "DCT"
        elif isinstance(value, Queue):
            return "QUE"
        elif isinstance(value, GenericComplexObject):
            return "ObjDynamic"
        elif isinstance(value, ComplexObject):
            return "Obj"
        else:
            # catch all, this probably isn't right but will not throw an
            # error
            return "S"

    def _serialize_obj(
        self,
        metadata: ObjectMeta,
        value: typing.Any,
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())

        if len(value._types) > 0:
            self._create_tn(obj, value._types)

        to_string_value = value._to_string
        if to_string_value is not None:
            ET.SubElement(obj, "ToString").text = self._serialize_string(to_string_value)

        for attr, property_meta in value._property_sets:
            attr_value = getattr(value, attr)
            self._create_obj(obj, attr_value, meta=property_meta)

        def serialize_prop(parent: str, properties: typing.Tuple[typing.Tuple[str, ObjectMeta], ...]) -> None:
            if len(properties) == 0:
                return
            parent_et = ET.SubElement(obj, parent)
            for attr, property_meta in properties:
                attr_value = getattr(value, attr)
                self._create_obj(parent_et, attr_value, meta=property_meta)

        serialize_prop("MS", value._extended_properties)
        serialize_prop("Props", value._adapted_properties)

        return obj

    def _serialize_dynamic_obj(
        self,
        metadata: ObjectMeta,
        value: typing.Any,
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())
        self.obj[obj.attrib["RefId"]] = value

        if len(value.types) > 0:
            self._create_tn(obj, value.types)

        if value.to_string is not None:
            ET.SubElement(obj, "ToString").text = self._serialize_string(value.to_string)

        for prop in value.property_sets:
            self._create_obj(obj, prop)

        def set_properties(element: str, prop_name: str) -> None:
            prop_keys = list(getattr(value, prop_name).keys())
            if len(prop_keys) == 0:
                return

            parent = ET.SubElement(obj, element)
            prop_keys.sort()
            for key in prop_keys:
                prop = getattr(value, prop_name)[key]
                self._create_obj(parent, prop, key=key)

        set_properties("MS", "extended_properties")
        set_properties("Props", "adapted_properties")

        return obj

    def _serialize_que(
        self,
        metadata: QueueMeta,
        values: Queue,
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())
        if not isinstance(metadata, QueueMeta):
            metadata = QueueMeta(name=metadata.name, optional=metadata.optional)
        self._create_tn(obj, metadata.list_types)

        que = ET.SubElement(obj, "QUE")
        while True:
            try:
                value = values.get(block=False)
                self.serialize(value, metadata.list_value_meta, parent=que, clear=False)
            except Empty:
                break

        return obj

    def _serialize_stk(
        self,
        metadata: StackMeta,
        values: typing.List,
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())
        self._create_tn(obj, metadata.list_types)

        stk = ET.SubElement(obj, "STK")
        while True:
            try:
                value = values.pop()
                self.serialize(value, metadata.list_value_meta, parent=stk, clear=False)
            except IndexError:
                break

        return obj

    def _serialize_ie(
        self,
        metadata: ListMeta,
        values: typing.List,
    ) -> ET.Element:
        return self._serialize_lst(metadata, values, tag="IE")

    def _serialize_lst(
        self,
        metadata: ListMeta,
        values: typing.List,
        tag: str = "LST",
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())
        if not isinstance(metadata, ListMeta):
            metadata = ListMeta(name=metadata.name, optional=metadata.optional)
        self._create_tn(obj, metadata.list_types)

        lst = ET.SubElement(obj, tag)
        for value in iter(values):
            entry_meta = copy(metadata.list_value_meta)
            self.serialize(value, entry_meta, parent=lst, clear=False)

        return obj

    def _serialize_dct(
        self,
        metadata: DictionaryMeta,
        values: typing.Dict,
    ) -> ET.Element:
        obj = ET.Element("Obj", RefId=self._get_obj_id())
        if not isinstance(metadata, DictionaryMeta):
            metadata = DictionaryMeta(name=metadata.name, optional=metadata.optional)
        self._create_tn(obj, metadata.dict_types)

        dct = ET.SubElement(obj, "DCT")

        # allow dicts to be defined as a tuple so that the order is kept
        iterator: typing.Iterable[typing.Tuple[typing.Any, typing.Any]]
        if isinstance(values, tuple):
            iterator = values
        else:
            iterator = values.items()

        for key, value in iterator:
            en = ET.SubElement(dct, "En")
            key_meta = copy(metadata.dict_key_meta)
            value_meta = copy(metadata.dict_value_meta)
            self.serialize(key, key_meta, parent=en, clear=False)
            self.serialize(value, value_meta, parent=en, clear=False)

        return obj

    def _serialize_string(
        self,
        value: typing.Optional[str],
    ) -> typing.Optional[str]:
        if value is None:
            return None

        def rplcr(matchobj):
            surrogate_char = matchobj.group(0)
            byte_char = to_bytes(surrogate_char, encoding="utf-16-be")
            hex_char = to_unicode(binascii.hexlify(byte_char)).upper()
            hex_split = [hex_char[i : i + 4] for i in range(0, len(hex_char), 4)]

            return "".join(["_x%s_" % i for i in hex_split])

        # before running the translation we need to make sure _ before x is
        # encoded, normally _ isn't encoded except when preceding x
        string_value = to_unicode(value)

        # The MS-PSRP docs don't state this but the _x0000_ matcher is case insensitive so we need to make sure we
        # escape _X as well as _x.
        string_value = re.sub("(?i)_(x)", "_x005F_\\1", string_value)
        string_value = re.sub(self._serial_str, rplcr, string_value)

        return string_value

    def _serialize_secure_string(
        self,
        value: str,
    ) -> str:
        if self.cipher is None:
            raise SerializationError("Cannot generate secure string as cipher is not initialised")

        # convert the string to a UTF-16 byte string as that is what is
        # expected in Windows. If a byte string (native string in Python 2) was
        # passed in, the sender must make sure it is a valid UTF-16
        # representation and not UTF-8 or else the server will fail to decrypt
        # the secure string in most cases
        string_bytes = to_bytes(value, encoding="utf-16-le")

        padder = PKCS7(self.cipher.algorithm.block_size).padder()
        padded_data = padder.update(string_bytes) + padder.finalize()

        encryptor = self.cipher.encryptor()
        ss_value = encryptor.update(padded_data) + encryptor.finalize()
        ss_string = to_string(base64.b64encode(ss_value))

        return ss_string

    def _deserialize_obj(
        self,
        element: ET.Element,
        metadata: ObjectMeta,
    ) -> typing.Any:
        obj = metadata.object()
        self.obj[element.attrib["RefId"]] = obj

        to_string_value = element.find("ToString")
        if to_string_value is not None:
            obj._to_string = self._deserialize_string(to_string_value.text)

        def deserialize_property(prop_tag: str, properties: typing.Tuple[typing.Tuple[str, ObjectMeta], ...]) -> None:
            for attr, property_meta in properties:
                if attr == "invocation_info":
                    a = ""
                property_name = "Unknown"
                property_filter = ""
                if property_meta.name is not None:
                    property_name = property_meta.name
                    property_filter = "[@N='%s']" % property_meta.name

                tags = [property_meta.tag]
                # The below tags are actually seen as Obj in the parent element
                if property_meta.tag in ["DCT", "LST", "IE", "QUE", "STK", "ObjDynamic"]:
                    tags = ["Obj", "Ref"]

                val = None
                for tag in tags:
                    val = element.find("%s%s%s" % (prop_tag, tag, property_filter))
                    if val is not None:
                        break

                if val is None and not property_meta.optional:
                    val = element.find("%sNil%s" % (prop_tag, property_filter))
                    if val is None:
                        obj_name = str(obj) if obj._to_string is not None else "Unknown"
                        err_msg = "Mandatory return value for '%s' was not found on object %s" % (
                            property_name,
                            obj_name,
                        )
                        raise SerializationError(err_msg)
                    val = None
                elif val is not None:
                    val = self.deserialize(val, property_meta, clear=False)

                setattr(obj, attr, val)

        deserialize_property("", obj._property_sets)
        deserialize_property("Props/", obj._adapted_properties)
        deserialize_property("MS/", obj._extended_properties)

        return obj

    def _deserialize_dynamic_obj(
        self,
        element: ET.Element,
        metadata: ObjectMeta,
    ) -> typing.Any:
        obj = metadata.object()
        self.obj[element.attrib["RefId"]] = obj

        for obj_property in element:
            if obj_property.tag == "TN":
                for obj_type in obj_property:
                    obj.types.append(obj_type.text)
                self.tn[obj_property.attrib["RefId"]] = obj.types
            elif obj_property.tag == "TNRef":
                obj.types = self.tn[obj_property.attrib["RefId"]]
            elif obj_property.tag == "Props":
                for adapted_property in obj_property:
                    key = adapted_property.attrib["N"]
                    value = self.deserialize(adapted_property, clear=False)
                    obj.adapted_properties[key] = value
            elif obj_property.tag == "MS":
                for extended_property in obj_property:
                    key = extended_property.attrib["N"]
                    value = self.deserialize(extended_property, clear=False)
                    obj.extended_properties[key] = value
            elif obj_property.tag == "ToString":
                value = self.deserialize(obj_property, clear=False)
                obj.to_string = value
            else:
                value = self.deserialize(obj_property, clear=False)
                obj.property_sets.append(value)

        return obj

    def _deserialize_lst(
        self,
        element: ET.Element,
        metadata: typing.Optional[ObjectMeta] = None,
    ) -> typing.List:
        value_meta = getattr(metadata, "list_value_meta", None)

        list_value = list(self._deserialize_list_values(element, "LST", value_meta=value_meta))

        return list_value

    def _deserialize_que(
        self,
        element: ET.Element,
    ) -> Queue:
        queue: Queue = Queue()

        for entry in self._deserialize_list_values(element, "QUE"):
            queue.put(entry)

        return queue

    def _deserialize_stk(
        self,
        element: ET.Element,
    ) -> typing.List:
        # no native Stack object in Python so just use a list
        stack = list(self._deserialize_list_values(element, "STK"))

        return stack

    def _deserialize_list_values(
        self,
        element: ET.Element,
        list_type: str,
        value_meta: typing.Optional[ObjectMeta] = None,
    ) -> typing.Iterable[typing.Any]:
        entries = element.find(list_type)
        if entries is None:
            return

        for entry in entries:
            entry_value = self.deserialize(entry, metadata=value_meta, clear=False)
            yield entry_value

    def _deserialize_dct(
        self,
        element: ET.Element,
    ) -> typing.Dict:
        dictionary = {}
        entries = element.findall("DCT/En")
        for entry in entries:
            key = entry.find("*[@N='Key']")
            value = entry.find("*[@N='Value']")

            key = self.deserialize(key if key is not None else "", clear=False)
            value = self.deserialize(value if value is not None else "", clear=False)
            dictionary[key] = value

        return dictionary

    def _deserialize_string(
        self,
        value: typing.Optional[str],
    ) -> str:
        if value is None:
            return ""

        def rplcr(matchobj):
            # The matched object is the UTF-16 byte representation of the UTF-8
            # hex string value. We need to decode the byte str to unicode and
            # then unhexlify that hex string to get the actual bytes of the
            # _x****_ value, e.g.
            # group(0) == b"\x00_\x00x\x000\x000\x000\x00A\x00_"
            # group(1) == b"\x000\x000\x000\x00A"
            # unicode (from utf-16-be) == u"000A"
            # returns b"\x00\x0A"
            match_hex = matchobj.group(1)
            hex_string = to_unicode(match_hex, encoding="utf-16-be")
            return binascii.unhexlify(hex_string)

        # need to ensure we start with a unicode representation of the string
        # so that we can get the actual UTF-16 bytes value from that string
        unicode_value = to_unicode(value)
        unicode_bytes = to_bytes(unicode_value, encoding="utf-16-be")
        bytes_value = re.sub(self._deserial_str, rplcr, unicode_bytes)
        return to_unicode(bytes_value, encoding="utf-16-be")

    def _deserialize_secure_string(self, value: ET.Element) -> typing.Union[ET.Element, str]:
        if self.cipher is None:
            # cipher is not set up so we can't decrypt the string, just return
            # the raw element
            return value

        ss_string = base64.b64decode(value.text or "")
        decryptor = self.cipher.decryptor()
        decrypted_bytes = decryptor.update(ss_string) + decryptor.finalize()

        unpadder = PKCS7(self.cipher.algorithm.block_size).unpadder()
        unpadded_bytes = unpadder.update(decrypted_bytes) + unpadder.finalize()
        decrypted_string = to_unicode(unpadded_bytes, "utf-16-le")

        return decrypted_string

    def _clear(self) -> None:
        self.obj_id = 0
        self.obj = {}
        self.tn = {}
        self.tn_id = 0

    def _get_obj_id(self) -> str:
        ref_id = str(self.obj_id)
        self.obj_id += 1
        return ref_id

    def _get_types_from_obj(
        self,
        element: ET.Element,
    ) -> typing.List[str]:
        obj_types = [e.text or "" for e in element.findall("TN/T")]

        if len(obj_types) > 0:
            ref_id = element.find("TN").attrib["RefId"]  # type: ignore[union-attr] # Mandated by the spec
            self.tn[ref_id] = obj_types

        tn_ref = element.find("TNRef")
        if tn_ref is not None:
            ref_id = tn_ref.attrib["RefId"]
            obj_types = self.tn[ref_id]

        return obj_types

    def _create_tn(
        self,
        parent: ET.Element,
        types: typing.List[str],
    ) -> None:
        main_type = types[0]
        ref_id = self.tn.get(main_type, None)
        if ref_id is None:
            ref_id = self.tn_id
            self.tn_id += 1
            self.tn[main_type] = ref_id

            tn = ET.SubElement(parent, "TN", RefId=str(ref_id))
            for type_name in types:
                ET.SubElement(tn, "T").text = type_name
        else:
            ET.SubElement(parent, "TNRef", RefId=str(ref_id))

    def _create_obj(
        self,
        parent: ET.Element,
        obj: typing.Any,
        key: typing.Optional[str] = None,
        meta: typing.Optional[ObjectMeta] = None,
    ) -> None:
        if isinstance(obj, ComplexObject):
            for ref, value in self.obj.items():
                if value == obj:
                    sub_element = ET.SubElement(parent, "Ref", RefId=ref)
                    if key is not None:
                        sub_element.attrib["N"] = key
                    return

        if meta is None:
            meta = ObjectMeta(name=key)
        self.serialize(obj, metadata=meta, parent=parent, clear=False)


class TaggedValue(object):
    def __init__(
        self,
        tag: str,
        value: typing.Any,
    ) -> None:
        self.tag = tag
        self.value = value
