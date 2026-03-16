import { ToolProxy } from "./tool_proxy";
import type { Tool, ToolView } from "./tool";
import type * as p from "../../core/properties";
export declare namespace ToolGroup {
    type Attrs<T extends Tool> = p.AttrsOf<Props<T>>;
    type Props<T extends Tool> = ToolProxy.Props<T> & {
        show_count: p.Property<boolean>;
    };
}
export interface ToolGroup<T extends Tool> extends ToolGroup.Attrs<T> {
}
export declare class ToolGroup<T extends Tool> extends ToolProxy<T> {
    properties: ToolGroup.Props<T>;
    __view_type__: ToolView;
    constructor(attrs?: Partial<ToolGroup.Attrs<T>>);
    get tooltip(): string;
}
//# sourceMappingURL=tool_group.d.ts.map