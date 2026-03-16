import type { NDDataType } from "../types";
export declare function b64encode(data: Uint8Array): string;
export declare function b64decode(data: string): Uint8Array<ArrayBuffer>;
export declare function buffer_to_base64(buffer: ArrayBufferLike): string;
export declare function base64_to_buffer(base64: string): ArrayBufferLike;
export declare function swap(buffer: ArrayBufferLike, dtype: NDDataType): void;
//# sourceMappingURL=buffer.d.ts.map