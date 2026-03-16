import { MenuItem } from "./menu_item";
import type * as p from "../../../core/properties";
/** @deprecated use MenuItem.checkable */
export declare namespace CheckableItem {
    type Attrs = p.AttrsOf<Props>;
    type Props = MenuItem.Props;
}
/** @deprecated use MenuItem.checkable */
export interface CheckableItem extends CheckableItem.Attrs {
}
/** @deprecated use MenuItem.checkable */
export declare class CheckableItem extends MenuItem {
    properties: CheckableItem.Props;
    constructor(attrs?: Partial<CheckableItem.Attrs>);
}
//# sourceMappingURL=checkable_item.d.ts.map