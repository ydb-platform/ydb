import { BBox } from "./bbox";
import type { OutputBackend } from "../enums";
export declare const exportable: unique symbol;
export interface Exportable {
    [exportable]: boolean;
    export(type?: "auto" | "png" | "svg", hidpi?: boolean): CanvasLayer;
    readonly bbox: BBox;
}
export declare function is_Exportable<T>(obj: T): obj is T & Exportable;
export type CanvasPatternRepetition = "repeat" | "repeat-x" | "repeat-y" | "no-repeat";
export type Context2d = {
    createPattern(image: CanvasImageSource, repetition: CanvasPatternRepetition | null): CanvasPattern | null;
    readonly layer: CanvasLayer;
    rect_bbox(bbox: BBox): void;
} & CanvasRenderingContext2D;
export declare class CanvasLayer {
    readonly backend: OutputBackend;
    readonly hidpi: boolean;
    private readonly _canvas;
    get canvas(): HTMLCanvasElement;
    private readonly _ctx;
    get ctx(): Context2d;
    private readonly _el;
    get el(): HTMLElement;
    private _pixel_ratio;
    get pixel_ratio(): number;
    bbox: BBox;
    constructor(backend: OutputBackend, hidpi: boolean);
    get pixel_ratio_changed(): boolean;
    resize(width: number, height: number): void;
    private get target();
    undo_transform(fn: (ctx: Context2d) => void): void;
    prepare(): Context2d;
    clear(): void;
    finish(): void;
    to_blob(): Promise<Blob>;
}
//# sourceMappingURL=canvas.d.ts.map