#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMPv2-CONF (https://www.pysnmp.com/pysnmp)
#

(MibNode,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibNode")


class ObjectGroup(MibNode):
    status = "current"
    objects = ()
    description = ""
    reference = ""

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getObjects(self):
        return getattr(self, "objects", ())

    def setObjects(self, *args, **kwargs):
        if kwargs.get("append"):
            self.objects += args
        else:
            self.objects = args
        return self

    def getDescription(self):
        return getattr(self, "description", "")

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """\
OBJECT-GROUP
  OBJECTS {{ {} }}
  STATUS "{}"
  DESCRIPTION "{}"
""".format(
            ", ".join([x for x in self.getObjects()]),
            self.status,
            self.getDescription(),
        )

    ## compatibility with legacy code
    def set_objects(self, *args, **kwargs):
        return self.setObjects(*args, **kwargs)

    def set_description(self, v):
        return self.setDescription(v)

    def set_status(self, v):
        return self.setStatus(v)

    def set_reference(self, v):
        return self.setReference(v)


class NotificationGroup(MibNode):
    status = "current"
    objects = ()
    description = ""
    reference = ""

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getObjects(self):
        return getattr(self, "objects", ())

    def setObjects(self, *args, **kwargs):
        if kwargs.get("append"):
            self.objects += args
        else:
            self.objects = args
        return self

    def getDescription(self):
        return getattr(self, "description", "")

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """\
NOTIFICATION-GROUP
  NOTIFICATIONS {{ {} }}
  STATUS "{}"
  DESCRIPTION "{}"
""".format(
            ", ".join([x for x in self.getObjects()]),
            self.getStatus(),
            self.getDescription(),
        )

    ## compatibility with legacy code
    def set_objects(self, *args, **kwargs):
        return self.setObjects(*args, **kwargs)

    def set_description(self, v):
        return self.setDescription(v)

    def set_status(self, v):
        return self.setStatus(v)

    def set_reference(self, v):
        return self.setReference(v)


class ModuleCompliance(MibNode):
    status = "current"
    objects = ()
    description = ""

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getObjects(self):
        return getattr(self, "objects", ())

    def setObjects(self, *args, **kwargs):
        if kwargs.get("append"):
            self.objects += args
        else:
            self.objects = args
        return self

    def getDescription(self):
        return getattr(self, "description", "")

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """\
MODULE-COMPLIANCE
  STATUS "{}"
  DESCRIPTION "{}"
  OBJECT {{ {} }}
""".format(
            self.getStatus(),
            self.getDescription(),
            ", ".join([x for x in self.getObjects()]),
        )

    ## compatibility with legacy code
    def set_objects(self, *args, **kwargs):
        return self.setObjects(*args, **kwargs)

    def set_status(self, v):
        return self.setStatus(v)

    def set_description(self, v):
        return self.setDescription(v)


class AgentCapabilities(MibNode):
    status = "current"
    description = ""
    reference = ""
    productRelease = ""

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getDescription(self):
        return getattr(self, "description", "")

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def getProductRelease(self):
        return self.productRelease

    def setProductRelease(self, v):
        self.productRelease = v
        return self

    # TODO: implement the rest of properties

    def asn1Print(self):
        return """\
AGENT-CAPABILITIES
  PRODUCT-RELEASE "{}"
  STATUS "{}"
  DESCRIPTION "{}"
""".format(
            self.getProductRelease(), self.getStatus(), self.getDescription()
        )


mibBuilder.export_symbols(
    "SNMPv2-CONF",
    ObjectGroup=ObjectGroup,
    NotificationGroup=NotificationGroup,
    ModuleCompliance=ModuleCompliance,
    AgentCapabilities=AgentCapabilities,
)
