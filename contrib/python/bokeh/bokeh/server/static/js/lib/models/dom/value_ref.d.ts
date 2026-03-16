import { Placeholder, PlaceholderView, Formatter } from "./placeholder";
import type { Formatters } from "./placeholder";
import { CustomJS } from "../callbacks/customjs";
import type { ColumnarDataSource } from "../sources/columnar_data_source";
import type { Index } from "../../core/util/templating";
import type { SyncExecutableLike } from "../../core/util/callbacks";
import type * as p from "../../core/properties";
import type { PlainObject } from "../../core/types";
import type { Model } from "../../model";
export type FilterArgs = {
    value: unknown;
    field: string;
    row: {
        [key: string]: unknown;
    };
    data_source: ColumnarDataSource;
    vars: PlainObject;
};
export declare const FilterDef: import("../../core/kinds").Kinds.Or<[(args_0: FilterArgs) => boolean, CustomJS]>;
export type FilterDef = SyncExecutableLike<Model, [FilterArgs], boolean> | CustomJS;
export declare class ValueRefView extends PlaceholderView {
    model: ValueRef;
    connect_signals(): void;
    lazy_initialize(): Promise<void>;
    protected _update_filter(): Promise<void>;
    update(data_source: ColumnarDataSource, index: Index | null, vars: PlainObject, _formatters?: Formatters): void;
}
export declare namespace ValueRef {
    type Attrs = p.AttrsOf<Props>;
    type Props = Placeholder.Props & {
        field: p.Property<string>;
        format: p.Property<string | null>;
        formatter: p.Property<Formatter>;
        filter: p.Property<FilterDef | FilterDef[] | null>;
    };
}
export interface ValueRef extends ValueRef.Attrs {
}
export declare class ValueRef extends Placeholder {
    properties: ValueRef.Props;
    __view_type__: ValueRefView;
    constructor(attrs?: Partial<ValueRef.Attrs>);
}
//# sourceMappingURL=value_ref.d.ts.map