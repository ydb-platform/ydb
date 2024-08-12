#include "viewer_bsgroupinfo.h"
#include "viewer_nodeinfo.h"
#include "viewer_pdiskinfo.h"
#include "viewer_sysinfo.h"
#include "viewer_tabletinfo.h"
#include "viewer_vdiskinfo.h"
#include "json_handlers.h"

namespace NKikimr::NViewer {

YAML::Node GetWhiteboardRequestParameters() {
    return YAML::Load(R"___(
            - name: node_id
              in: query
              description: node identifier
              required: false
              type: integer
            - name: merge
              in: query
              description: merge information from nodes
              required: false
              type: boolean
            - name: group
              in: query
              description: group information by field
              required: false
              type: string
            - name: all
              in: query
              description: return all possible key combinations (for enums only)
              required: false
              type: boolean
            - name: filter
              in: query
              description: filter information by field
              required: false
              type: string
            - name: alive
              in: query
              description: request from alive (connected) nodes only
              required: false
              type: boolean
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: retries
              in: query
              description: number of retries
              required: false
              type: integer
            - name: retry_period
              in: query
              description: retry period in ms
              required: false
              type: integer
              default: 500
            - name: static
              in: query
              description: request from static nodes only
              required: false
              type: boolean
            - name: since
              in: query
              description: filter by update time
              required: false
              type: string
            )___");
}

void InitViewerBSGroupInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "Storage groups information",
        .Description = "Returns information about storage groups"
    });
    yaml.SetParameters(GetWhiteboardRequestParameters());
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonBSGroupInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/bsgroupinfo", new TJsonHandler<TJsonBSGroupInfo>(yaml));
    TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::InitMerger();
}

void InitViewerNodeInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "Interconnect information",
        .Description = "Returns information about node connections"
    });
    yaml.SetParameters(GetWhiteboardRequestParameters());
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonNodeInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/nodeinfo", new TJsonHandler<TJsonNodeInfo>(yaml));
    TWhiteboardInfo<NKikimrWhiteboard::TEvNodeStateResponse>::InitMerger();
}

void InitViewerPDiskInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "PDisk information",
        .Description = "Returns information about PDisks"
    });
    yaml.SetParameters(GetWhiteboardRequestParameters());
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonPDiskInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/pdiskinfo", new TJsonHandler<TJsonPDiskInfo>(yaml));
}

void InitViewerSysInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "System information",
        .Description = "Returns system information"
    });
    yaml.SetParameters(GetWhiteboardRequestParameters());
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonSysInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/sysinfo", new TJsonHandler<TJsonSysInfo>(yaml));
}

void InitViewerTabletInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "Tablet information",
        .Description = "Returns information about tablets"
    });
    yaml.AddParameter({
        .Name = "database",
        .Description = "database name",
        .Type = "string",
    });
    yaml.AddParameter({
        .Name = "node_id",
        .Description = "node identifier",
        .Type = "integer",
    });
    yaml.AddParameter({
        .Name = "path",
        .Description = "schema path",
        .Type = "string",
    });
    yaml.AddParameter({
        .Name = "merge",
        .Description = "merge information from nodes",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "group",
        .Description = "group information by field",
        .Type = "string",
    });
    yaml.AddParameter({
        .Name = "all",
        .Description = "return all possible key combinations (for enums only)",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "filter",
        .Description = "filter information by field",
        .Type = "string",
    });
    yaml.AddParameter({
        .Name = "alive",
        .Description = "request from alive (connected) nodes only",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "enums",
        .Description = "convert enums to strings",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "ui64",
        .Description = "return ui64 as number",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "timeout",
        .Description = "timeout in ms",
        .Type = "integer",
    });
    yaml.AddParameter({
        .Name = "retries",
        .Description = "number of retries",
        .Type = "integer",
    });
    yaml.AddParameter({
        .Name = "retry_period",
        .Description = "retry period in ms",
        .Type = "integer",
        .Default = "500",
    });
    yaml.AddParameter({
        .Name = "static",
        .Description = "request from static nodes only",
        .Type = "boolean",
    });
    yaml.AddParameter({
        .Name = "since",
        .Description = "filter by update time",
        .Type = "string",
    });
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonTabletInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/tabletinfo", new TJsonHandler<TJsonTabletInfo>(yaml));
}

void InitViewerVDiskInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "VDisk information",
        .Description = "Returns information about VDisks"
    });
    yaml.SetParameters(GetWhiteboardRequestParameters());
    yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TJsonVDiskInfo::TResponseType>());
    jsonHandlers.AddHandler("/viewer/vdiskinfo", new TJsonHandler<TJsonVDiskInfo>(yaml));
}

}
