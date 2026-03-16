import { GridPlot } from "../models/plots";
import type { Tool } from "../models/tools/tool";
import type { ToolLike } from "../models/tools/tool_proxy";
import { ToolProxy } from "../models/tools/tool_proxy";
import { UIElement } from "../models/ui/ui_element";
import type { SizingMode, Location } from "../core/enums";
export type GridPlotOpts = {
    toolbar_location?: Location | null;
    merge_tools?: boolean;
    sizing_mode?: SizingMode;
    width?: number;
    height?: number;
};
export type MergeFn = (cls: typeof Tool, group: Tool[]) => Tool | ToolProxy<Tool> | null;
export declare function group_tools(tools: ToolLike<Tool>[], merge?: MergeFn, ignore?: Set<string>): ToolLike<Tool>[];
export type GridPlotItem = UIElement | null | undefined;
export declare function gridplot(children: GridPlotItem[], options: GridPlotOpts & {
    ncols: number;
}): GridPlot;
export declare function gridplot(children: GridPlotItem[][], options?: GridPlotOpts): GridPlot;
//# sourceMappingURL=gridplot.d.ts.map