import type { CategoricalMapper } from "./categorical_mapper";
import { ColorMapper } from "./color_mapper";
import type { Factor } from "../ranges/factor_range";
import type * as p from "../../core/properties";
import type { Arrayable, uint32 } from "../../core/types";
export declare namespace CategoricalColorMapper {
    type Attrs = p.AttrsOf<Props>;
    type Props = ColorMapper.Props & CategoricalMapper.Props;
}
export interface CategoricalColorMapper extends CategoricalColorMapper.Attrs {
}
export declare class CategoricalColorMapper extends ColorMapper {
    properties: CategoricalColorMapper.Props;
    constructor(attrs?: Partial<CategoricalColorMapper.Attrs>);
    protected _v_compute(data: Arrayable<Factor | number | null>, values: Arrayable<uint32>, palette: Arrayable<uint32>, { nan_color }: {
        nan_color: uint32;
    }): void;
}
//# sourceMappingURL=categorical_color_mapper.d.ts.map