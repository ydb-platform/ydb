import type { StyleSheetLike } from "../dom";
import { ClassList } from "../dom";
import type { Orientation } from "../enums";
import type { MenuItemLike, MenuItem } from "../../models/ui/menus";
import type { IconLike } from "../../models/common/kinds";
export type ScreenPoint = {
    left?: number;
    right?: number;
    top?: number;
    bottom?: number;
};
export type At = ScreenPoint | {
    left_of: HTMLElement;
} | {
    right_of: HTMLElement;
} | {
    below: HTMLElement;
} | {
    above: HTMLElement;
};
export type MenuEntry = {
    icon?: IconLike;
    label?: string;
    tooltip?: string;
    class?: string;
    content?: HTMLElement;
    custom?: HTMLElement;
    checked?: () => boolean;
    action?: () => void;
    disabled?: () => boolean;
};
export type MenuItemLike_ = MenuItemLike | MenuEntry;
export type MenuOptions = {
    target: HTMLElement;
    orientation?: Orientation;
    reversed?: boolean;
    labels?: boolean;
    prevent_hide?: (event: MouseEvent) => boolean;
    extra_styles?: StyleSheetLike[];
};
export declare class ContextMenu {
    readonly items: MenuItemLike_[];
    readonly el: HTMLElement;
    readonly shadow_el: ShadowRoot;
    protected _open: boolean;
    get is_open(): boolean;
    get can_open(): boolean;
    readonly target: HTMLElement;
    readonly orientation: Orientation;
    readonly reversed: boolean;
    readonly labels: boolean;
    readonly prevent_hide?: (event: MouseEvent) => boolean;
    readonly extra_styles: StyleSheetLike[];
    readonly class_list: ClassList;
    constructor(items: MenuItemLike_[], options: MenuOptions);
    protected _item_click: (entry: MenuEntry | MenuItem) => void;
    protected _on_mousedown: (event: MouseEvent) => void;
    protected _on_keydown: (event: KeyboardEvent) => void;
    protected _on_blur: () => void;
    remove(): void;
    protected _listen(): void;
    protected _unlisten(): void;
    protected _position(at: At): void;
    stylesheets(): StyleSheetLike[];
    empty(): void;
    render(): void;
    show(at?: At): void;
    hide(): void;
    toggle(at?: At): void;
}
//# sourceMappingURL=menus.d.ts.map