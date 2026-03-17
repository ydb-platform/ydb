import { Model } from "../../model";
import { Node } from "../coordinates/node";
import { Styles } from "../dom/styles";
import { StyleSheet as BaseStyleSheet } from "../dom/stylesheets";
import { DOMComponentView } from "../../core/dom_view";
import type { StyleSheet, StyleSheetLike } from "../../core/dom";
import { InlineStyleSheet } from "../../core/dom";
import type * as p from "../../core/properties";
import { Dict } from "../../core/kinds";
export declare const StylesLike: import("../../core/kinds").Kinds.Or<[import("../../core/types").Dict<string | null>, Styles]>;
export type StylesLike = typeof StylesLike["__type__"];
export declare const StyleSheets: import("../../core/kinds").Kinds.List<string | BaseStyleSheet | import("../../core/types").Dict<Styles | import("../../core/types").Dict<string | null>>>;
export type StyleSheets = typeof StyleSheets["__type__"];
export declare const CSSVariables: import("../../core/kinds").Kinds.Dict<string | Node>;
export type CSSVariables = typeof CSSVariables["__type__"];
export declare abstract class StyledElementView extends DOMComponentView {
    model: StyledElement;
    /**
     * Computed styles applied to self.
     */
    readonly style: InlineStyleSheet;
    /**
     * Computed styles append by the parent.
     */
    readonly parent_style: InlineStyleSheet;
    computed_stylesheets(): InlineStyleSheet[];
    connect_signals(): void;
    protected _css_classes(): Iterable<string>;
    protected _css_variables(): Iterable<[string, string]>;
    user_stylesheets(): StyleSheetLike[];
    protected _user_stylesheets(): Iterable<StyleSheet>;
    protected _apply_html_attributes(): void;
    protected _apply_styles(): void;
}
export declare namespace StyledElement {
    type Attrs = p.AttrsOf<Props>;
    type Props = Model.Props & {
        html_attributes: p.Property<Dict<string>>;
        html_id: p.Property<string | null>;
        css_classes: p.Property<string[]>;
        css_variables: p.Property<CSSVariables>;
        styles: p.Property<StylesLike>;
        stylesheets: p.Property<StyleSheets>;
    };
}
export interface StyledElement extends StyledElement.Attrs {
}
export declare abstract class StyledElement extends Model {
    properties: StyledElement.Props;
    __view_type__: StyledElementView;
    constructor(attrs?: Partial<StyledElement.Attrs>);
}
//# sourceMappingURL=styled_element.d.ts.map