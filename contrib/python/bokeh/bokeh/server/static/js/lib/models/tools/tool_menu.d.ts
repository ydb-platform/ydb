import { Menu, MenuView } from "../ui/menus/menu";
import type { MenuItemLike } from "../ui/menus/menu";
import { Toolbar } from "./toolbar";
import type * as p from "../../core/properties";
export declare class ToolMenuView extends MenuView {
    model: ToolMenu;
    protected _compute_menu_items(): MenuItemLike[];
    connect_signals(): void;
}
export declare namespace ToolMenu {
    type Attrs = p.AttrsOf<Props>;
    type Props = Menu.Props & {
        toolbar: p.Property<Toolbar>;
    };
}
export interface ToolMenu extends ToolMenu.Attrs {
}
export declare class ToolMenu extends Menu {
    properties: ToolMenu.Props;
    __view_type__: ToolMenuView;
    constructor(attrs?: Partial<ToolMenu.Attrs>);
}
//# sourceMappingURL=tool_menu.d.ts.map