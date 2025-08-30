import os
import sys
import json
import re

from jinja2 import Environment, FileSystemLoader

templateFile = sys.argv[1]
(templateDir, templateFilename) = os.path.split(templateFile)

jsonFile = sys.argv[2]
headerOutFile = sys.argv[3]
declOutFile = sys.argv[4]
defsOutFile = sys.argv[5]

env = Environment(loader=FileSystemLoader(templateDir))
template = env.get_template(templateFilename)

json_data = open(jsonFile)
model = json.load(json_data)
json_data.close()

# Fill properties
for node in model["Nodes"]:
    aux = node["aux"] = {}

    aux["stubName"] = node["Name"] + "Stub"
    aux["stubMaybeName"] = node["Name"] + "MaybeStub"
    aux["stubBuilderName"] = node["Name"] + "BuilderStub"
    aux["stubBuilderAliasName"] = node["Name"] + "Builder"

    if "Builder" not in node:
        node["Builder"] = {}

    if "ListBase" in node:
        node["Base"] = "TListBase<{0}>".format(node["ListBase"])
        node["Builder"]["Kind"] = "List"
        node["Builder"]["ListItemType"] = node["ListBase"]

    if "VarArgBase" in node:
        node["Base"] = "TVarArgCallable<{0}>".format(node["VarArgBase"])
        node["Builder"]["Kind"] = "List"
        node["Builder"]["ListItemType"] = node["VarArgBase"]

    if "Children" in node:
        for child in node["Children"]:
            childAux = child["aux"] = {}
            childAux["holderName"] = child["Name"] + "Holder"
            if "Optional" not in child:
                child["Optional"] = False

    if node["Base"] == model["FreeArgCallableBase"]:
        aux["isFinal"] = True

# Traverse tree
nodesMap = {}
for node in model["Nodes"]:
    nodesMap[node["Name"]] = node

    aux = node["aux"]
    descendants = aux["descendants"] = set()
    acscendants = aux["acscendants"] = set()

for node in model["Nodes"]:
    curNode = node
    while curNode["Base"] in nodesMap:
        parent = nodesMap[curNode["Base"]]

        if "isFinal" in parent["aux"] and parent["aux"]["isFinal"]:
            raise Exception("Node " + node["Name"] + " inherits final node " + parent["Name"])

        parent["aux"]["descendants"].add(node["Name"])
        node["aux"]["acscendants"].add(parent["Name"])
        curNode = parent

# Fill global properties
for node in model["Nodes"]:
    aux = node["aux"]

    # Determine builders kind
    if "Generate" not in node["Builder"]:
        node["Builder"]["Generate"] = "Auto"

    def hasBuilderKind(node):
        return "Builder" in node and "Kind" in node["Builder"]

    def isListBuilder(node):
        return hasBuilderKind(node) and node["Builder"]["Kind"] == "List"

    hasAscList = False
    for ascName in sorted(aux["acscendants"]):
        asc = nodesMap[ascName]
        if isListBuilder(asc):
            hasAscList = True
            ascListItemType = asc["Builder"]["ListItemType"]
            break

    if hasBuilderKind(node):
        if hasAscList and not isListBuilder(node):
            raise Exception("Invalid builder kind in " + node["Name"])
        if hasAscList and ascListItemType != node["Builder"]["ListItemType"]:
            raise Exception("Invalid builder list item type in " + node["Name"])
    else:
        if hasAscList:
            node["Builder"]["Kind"] = "List"
            node["Builder"]["ListItemType"] = ascListItemType
        else:
            node["Builder"]["Kind"] = "FreeArg" if node["Base"] == model["FreeArgCallableBase"] else "Node"

    aux["generateBuilderStub"] = node["Builder"]["Generate"] != "None" and node["Builder"]["Kind"] != "List"
    aux["generateBuilder"] = node["Builder"]["Generate"] == "Auto"

    # Get all children
    allChildren = []

    if "Children" in node:
        allChildren.extend(node["Children"])

    for ascName in sorted(aux["acscendants"]):
        if "Children" in nodesMap[ascName]:
            allChildren.extend(nodesMap[ascName]["Children"])

    allChildren = sorted(allChildren, key=lambda c: c["Index"])
    aux["allChildren"] = allChildren

    # Make sure indices are OK
    optionalArgs = False
    aux["allChildrenCount"] = len(allChildren)
    aux["fixedChildrenCount"] = 0
    for index, child in enumerate(allChildren):
        if child["Index"] != index:
            raise Exception("Missing child #" + str(index) + " in " + node["Name"])
        if not child["Optional"] and optionalArgs:
            raise Exception("Child #" + str(index) + " should be optional in " + node["Name"])
        optionalArgs = child["Optional"]
        if not child["Optional"]:
            aux["fixedChildrenCount"] += 1

    if "Match" in node and node["Match"]["Type"] == "CallableBase":
        namesToMatch = aux["namesToMatch"] = []
        for descName in sorted(aux["descendants"]):
            desc = nodesMap[descName]
            if desc["Match"]["Type"] == "Callable":
                namesToMatch.append(desc["Match"]["Name"])

    def parseTypename(typename):
        usages = []
        match = re.match("(.*)(<([^,]*)>)+", typename)
        if match:
            usages.append((match.group(1), "template<typename> class {0}".format(match.group(1))))
            usages.append((match.group(3), "typename {0}".format(match.group(3))))
        elif typename == model["FreeArgCallableBase"]:
            usages.append((typename, "template<const size_t> class {0}".format(typename)))
        else:
            usages.append((typename, "typename {0}".format(typename)))

        return usages

    usages = []
    usagesSet = set()
    declarations = []

    def addUsages(typename):
        typeUsages = parseTypename(typename)
        for usage in typeUsages:
            if usage[0] not in usagesSet:
                usagesSet.add(usage[0])
                usages.append(usage[0])
                declarations.append(usage[1])

    addUsages(model["NodeRootType"])
    addUsages(node["Base"])
    if ("Children" in node):
        for child in node["Children"]:
            addUsages(child["Type"])
    aux["usages"] = usages
    aux["typenames"] = declarations

    usages = []
    usagesSet = set()
    declarations = []
    addUsages(model["NodeRootType"])
    addUsages(node["Base"])
    for child in aux["allChildren"]:
        addUsages(child["Type"])
    aux["builderUsages"] = usages
    aux["builderTypenames"] = declarations

    if node["Base"] == model["FreeArgCallableBase"]:
        node["Base"] = "{0}<{1}>".format(node["Base"], len(aux["allChildren"]))


headerOutput = template.render(generator=__file__, genType="Header", model=model, nodes=model["Nodes"])
with open(headerOutFile, "w") as fh:
    fh.write(headerOutput)

declOutput = template.render(generator=__file__, genType="Declarations", model=model, nodes=model["Nodes"])
with open(declOutFile, "w") as fh:
    fh.write(declOutput)

defsOutput = template.render(generator=__file__, genType="Definitions", model=model, nodes=model["Nodes"])
with open(defsOutFile, "w") as fh:
    fh.write(defsOutput)
