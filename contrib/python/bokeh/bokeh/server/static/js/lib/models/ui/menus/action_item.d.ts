import { MenuItem } from "./menu_item";
import type * as p from "../../../core/properties";
/** @deprecated use MenuItem */
export declare namespace ActionItem {
    type Attrs = p.AttrsOf<Props>;
    type Props = MenuItem.Props;
}
/** @deprecated use MenuItem */
export interface ActionItem extends ActionItem.Attrs {
}
/** @deprecated use MenuItem */
export declare class ActionItem extends MenuItem {
    properties: ActionItem.Props;
    constructor(attrs?: Partial<ActionItem.Attrs>);
}
//# sourceMappingURL=action_item.d.ts.map