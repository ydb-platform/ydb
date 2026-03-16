import { Model } from "../../../model";
import type { Menu } from "./menu";
import { IconLike } from "../../common/kinds";
import type { CallbackLike1 } from "../../../core/util/callbacks";
import type * as p from "../../../core/properties";
type ActionCallback = CallbackLike1<Menu, {
    item: MenuItem;
}>;
export declare namespace MenuItem {
    type Attrs = p.AttrsOf<Props>;
    type Props = Model.Props & {
        checked: p.Property<(() => boolean) | boolean | null>;
        icon: p.Property<IconLike | null>;
        label: p.Property<string>;
        tooltip: p.Property<string | null>;
        shortcut: p.Property<string | null>;
        menu: p.Property<Menu | null>;
        disabled: p.Property<(() => boolean) | boolean>;
        action: p.Property<ActionCallback | null>;
    };
}
export interface MenuItem extends MenuItem.Attrs {
}
export declare class MenuItem extends Model {
    properties: MenuItem.Props;
    constructor(attrs?: Partial<MenuItem.Attrs>);
}
export {};
//# sourceMappingURL=menu_item.d.ts.map