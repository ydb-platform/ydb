import { UIElement, UIElementView } from "../ui_element";
import { MenuItem } from "./menu_item";
import { DividerItem } from "./divider_item";
import type * as p from "../../../core/properties";
import type { XY } from "../../../core/util/bbox";
import type { StyleSheetLike } from "../../../core/dom";
import type { ViewStorage, IterViews } from "../../../core/build_views";
export declare const MenuItemLike: import("../../../core/kinds").Kinds.Or<[MenuItem, DividerItem, null]>;
export type MenuItemLike = typeof MenuItemLike["__type__"];
export declare class MenuView extends UIElementView {
    model: Menu;
    protected _menu_views: ViewStorage<Menu>;
    children(): IterViews;
    private _menu_items;
    get menu_items(): MenuItemLike[];
    protected _compute_menu_items(): MenuItemLike[];
    protected _update_menu_items(): void;
    get is_empty(): boolean;
    initialize(): void;
    lazy_initialize(): Promise<void>;
    connect_signals(): void;
    prevent_hide?: (event: MouseEvent) => boolean;
    protected _open: boolean;
    get is_open(): boolean;
    protected _item_click: (item: MenuItem) => void;
    protected _on_mousedown: (event: MouseEvent) => void;
    protected _on_keydown: (event: KeyboardEvent) => void;
    protected _on_blur: () => void;
    remove(): void;
    protected _listen(): void;
    protected _unlisten(): void;
    stylesheets(): StyleSheetLike[];
    render(): void;
    protected _show_submenu(target: HTMLElement): void;
    show(at: XY): boolean;
    hide(): void;
}
export declare namespace Menu {
    type Attrs = p.AttrsOf<Props>;
    type Props = UIElement.Props & {
        items: p.Property<MenuItemLike[]>;
        reversed: p.Property<boolean>;
    };
}
export interface Menu extends Menu.Attrs {
}
export declare class Menu extends UIElement {
    properties: Menu.Props;
    __view_type__: MenuView;
    constructor(attrs?: Partial<Menu.Attrs>);
}
//# sourceMappingURL=menu.d.ts.map