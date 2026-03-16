import { ActionTool, ActionToolView } from "./action_tool";
import type { MenuItemLike } from "../../ui/menus";
import type * as p from "../../../core/properties";
export declare class SaveToolView extends ActionToolView {
    model: SaveTool;
    protected _export(): Promise<Blob>;
    copy(): Promise<void>;
    save(name: string): Promise<void>;
    open(): Promise<void>;
    doit(action?: "save" | "copy" | "open"): void;
}
export declare namespace SaveTool {
    type Attrs = p.AttrsOf<Props>;
    type Props = ActionTool.Props & {
        filename: p.Property<string | null>;
    };
}
export interface SaveTool extends SaveTool.Attrs {
}
export declare class SaveTool extends ActionTool {
    properties: SaveTool.Props;
    __view_type__: SaveToolView;
    constructor(attrs?: Partial<SaveTool.Attrs>);
    tool_name: string;
    tool_icon: string;
    get menu(): MenuItemLike[];
}
//# sourceMappingURL=save_tool.d.ts.map