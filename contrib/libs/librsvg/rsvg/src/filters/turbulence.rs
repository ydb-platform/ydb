use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::error::*;
use crate::node::{CascadedValues, Node};
use crate::parse_identifiers;
use crate::parsers::{NumberOptionalNumber, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::{
    ImageSurfaceDataExt, Pixel, PixelOps,
    shared_surface::{ExclusiveImageSurface, SurfaceType},
};
use crate::util::clamp;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, InputRequirements, Primitive, PrimitiveParams,
    ResolvedPrimitive,
};

/// Limit the `numOctaves` parameter to avoid unbounded CPU consumption.
///
/// <https://drafts.fxtf.org/filter-effects/#element-attrdef-feturbulence-numoctaves>
const MAX_OCTAVES: i32 = 9;

/// Enumeration of the tile stitching modes.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
enum StitchTiles {
    Stitch,
    #[default]
    NoStitch,
}

/// Enumeration of the noise types.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
enum NoiseType {
    FractalNoise,
    #[default]
    Turbulence,
}

/// The `feTurbulence` filter primitive.
#[derive(Default)]
pub struct FeTurbulence {
    base: Primitive,
    params: Turbulence,
}

/// Resolved `feTurbulence` primitive for rendering.
#[derive(Clone)]
pub struct Turbulence {
    base_frequency: NumberOptionalNumber<f64>,
    num_octaves: i32,
    seed: f64,
    stitch_tiles: StitchTiles,
    type_: NoiseType,
    color_interpolation_filters: ColorInterpolationFilters,
}

impl Default for Turbulence {
    /// Constructs a new `Turbulence` with empty properties.
    #[inline]
    fn default() -> Turbulence {
        Turbulence {
            base_frequency: NumberOptionalNumber(0.0, 0.0),
            num_octaves: 1,
            seed: 0.0,
            stitch_tiles: Default::default(),
            type_: Default::default(),
            color_interpolation_filters: Default::default(),
        }
    }
}

impl ElementTrait for FeTurbulence {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.base.parse_no_inputs(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "baseFrequency") => {
                    set_attribute(&mut self.params.base_frequency, attr.parse(value), session);
                }
                expanded_name!("", "numOctaves") => {
                    set_attribute(&mut self.params.num_octaves, attr.parse(value), session);
                    if self.params.num_octaves > MAX_OCTAVES {
                        let n = self.params.num_octaves;
                        rsvg_log!(
                            session,
                            "ignoring numOctaves={n}, setting it to {MAX_OCTAVES}"
                        );
                        self.params.num_octaves = MAX_OCTAVES;
                    }
                }
                // Yes, seed needs to be parsed as a number and then truncated.
                expanded_name!("", "seed") => {
                    set_attribute(&mut self.params.seed, attr.parse(value), session);
                }
                expanded_name!("", "stitchTiles") => {
                    set_attribute(&mut self.params.stitch_tiles, attr.parse(value), session);
                }
                expanded_name!("", "type") => {
                    set_attribute(&mut self.params.type_, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

// Produces results in the range [1, 2**31 - 2].
// Algorithm is: r = (a * r) mod m
// where a = 16807 and m = 2**31 - 1 = 2147483647
// See [Park & Miller], CACM vol. 31 no. 10 p. 1195, Oct. 1988
// To test: the algorithm should produce the result 1043618065
// as the 10,000th generated number if the original seed is 1.
const RAND_M: i32 = 2147483647; // 2**31 - 1
const RAND_A: i32 = 16807; // 7**5; primitive root of m
const RAND_Q: i32 = 127773; // m / a
const RAND_R: i32 = 2836; // m % a

fn setup_seed(mut seed: i32) -> i32 {
    if seed <= 0 {
        seed = -(seed % (RAND_M - 1)) + 1;
    }
    if seed > RAND_M - 1 {
        seed = RAND_M - 1;
    }
    seed
}

fn random(seed: i32) -> i32 {
    let mut result = RAND_A * (seed % RAND_Q) - RAND_R * (seed / RAND_Q);
    if result <= 0 {
        result += RAND_M;
    }
    result
}

const B_SIZE: usize = 0x100;
const PERLIN_N: i32 = 0x1000;

#[derive(Clone, Copy)]
struct NoiseGenerator {
    base_frequency: (f64, f64),
    num_octaves: i32,
    stitch_tiles: StitchTiles,
    type_: NoiseType,

    tile_width: f64,
    tile_height: f64,

    lattice_selector: [usize; B_SIZE + B_SIZE + 2],
    gradient: [[[f64; 2]; B_SIZE + B_SIZE + 2]; 4],
}

#[derive(Clone, Copy)]
struct StitchInfo {
    width: usize, // How much to subtract to wrap for stitching.
    height: usize,
    wrap_x: usize, // Minimum value to wrap.
    wrap_y: usize,
}

impl NoiseGenerator {
    fn new(
        seed: i32,
        base_frequency: (f64, f64),
        num_octaves: i32,
        type_: NoiseType,
        stitch_tiles: StitchTiles,
        tile_width: f64,
        tile_height: f64,
    ) -> Self {
        let mut rv = Self {
            base_frequency,
            num_octaves,
            type_,
            stitch_tiles,

            tile_width,
            tile_height,

            lattice_selector: [0; B_SIZE + B_SIZE + 2],
            gradient: [[[0.0; 2]; B_SIZE + B_SIZE + 2]; 4],
        };

        let mut seed = setup_seed(seed);

        for k in 0..4 {
            for i in 0..B_SIZE {
                rv.lattice_selector[i] = i;
                for j in 0..2 {
                    seed = random(seed);
                    rv.gradient[k][i][j] =
                        ((seed % (B_SIZE + B_SIZE) as i32) - B_SIZE as i32) as f64 / B_SIZE as f64;
                }
                let s = (rv.gradient[k][i][0] * rv.gradient[k][i][0]
                    + rv.gradient[k][i][1] * rv.gradient[k][i][1])
                    .sqrt();
                rv.gradient[k][i][0] /= s;
                rv.gradient[k][i][1] /= s;
            }
        }
        for i in (1..B_SIZE).rev() {
            let k = rv.lattice_selector[i];
            seed = random(seed);
            let j = seed as usize % B_SIZE;
            rv.lattice_selector[i] = rv.lattice_selector[j];
            rv.lattice_selector[j] = k;
        }
        for i in 0..B_SIZE + 2 {
            rv.lattice_selector[B_SIZE + i] = rv.lattice_selector[i];
            for k in 0..4 {
                for j in 0..2 {
                    rv.gradient[k][B_SIZE + i][j] = rv.gradient[k][i][j];
                }
            }
        }

        rv
    }

    fn noise2(&self, color_channel: usize, vec: [f64; 2], stitch_info: Option<StitchInfo>) -> f64 {
        #![allow(clippy::many_single_char_names)]

        const BM: usize = 0xff;

        let s_curve = |t| t * t * (3. - 2. * t);
        let lerp = |t, a, b| a + t * (b - a);

        let t = vec[0] + f64::from(PERLIN_N);
        let mut bx0 = t as usize;
        let mut bx1 = bx0 + 1;
        let rx0 = t.fract();
        let rx1 = rx0 - 1.0;
        let t = vec[1] + f64::from(PERLIN_N);
        let mut by0 = t as usize;
        let mut by1 = by0 + 1;
        let ry0 = t.fract();
        let ry1 = ry0 - 1.0;

        // If stitching, adjust lattice points accordingly.
        if let Some(stitch_info) = stitch_info {
            if bx0 >= stitch_info.wrap_x {
                bx0 -= stitch_info.width;
            }
            if bx1 >= stitch_info.wrap_x {
                bx1 -= stitch_info.width;
            }
            if by0 >= stitch_info.wrap_y {
                by0 -= stitch_info.height;
            }
            if by1 >= stitch_info.wrap_y {
                by1 -= stitch_info.height;
            }
        }
        bx0 &= BM;
        bx1 &= BM;
        by0 &= BM;
        by1 &= BM;
        let i = self.lattice_selector[bx0];
        let j = self.lattice_selector[bx1];
        let b00 = self.lattice_selector[i + by0];
        let b10 = self.lattice_selector[j + by0];
        let b01 = self.lattice_selector[i + by1];
        let b11 = self.lattice_selector[j + by1];
        let sx = s_curve(rx0);
        let sy = s_curve(ry0);
        let q = self.gradient[color_channel][b00];
        let u = rx0 * q[0] + ry0 * q[1];
        let q = self.gradient[color_channel][b10];
        let v = rx1 * q[0] + ry0 * q[1];
        let a = lerp(sx, u, v);
        let q = self.gradient[color_channel][b01];
        let u = rx0 * q[0] + ry1 * q[1];
        let q = self.gradient[color_channel][b11];
        let v = rx1 * q[0] + ry1 * q[1];
        let b = lerp(sx, u, v);
        lerp(sy, a, b)
    }

    fn turbulence(&self, color_channel: usize, point: [f64; 2], tile_x: f64, tile_y: f64) -> f64 {
        let mut stitch_info = None;
        let mut base_frequency = self.base_frequency;

        // Adjust the base frequencies if necessary for stitching.
        if self.stitch_tiles == StitchTiles::Stitch {
            // When stitching tiled turbulence, the frequencies must be adjusted
            // so that the tile borders will be continuous.
            if base_frequency.0 != 0.0 {
                let freq_lo = (self.tile_width * base_frequency.0).floor() / self.tile_width;
                let freq_hi = (self.tile_width * base_frequency.0).ceil() / self.tile_width;
                if base_frequency.0 / freq_lo < freq_hi / base_frequency.0 {
                    base_frequency.0 = freq_lo;
                } else {
                    base_frequency.0 = freq_hi;
                }
            }
            if base_frequency.1 != 0.0 {
                let freq_lo = (self.tile_height * base_frequency.1).floor() / self.tile_height;
                let freq_hi = (self.tile_height * base_frequency.1).ceil() / self.tile_height;
                if base_frequency.1 / freq_lo < freq_hi / base_frequency.1 {
                    base_frequency.1 = freq_lo;
                } else {
                    base_frequency.1 = freq_hi;
                }
            }

            // Set up initial stitch values.
            let width = (self.tile_width * base_frequency.0 + 0.5) as usize;
            let height = (self.tile_height * base_frequency.1 + 0.5) as usize;
            stitch_info = Some(StitchInfo {
                width,
                wrap_x: (tile_x * base_frequency.0) as usize + PERLIN_N as usize + width,
                height,
                wrap_y: (tile_y * base_frequency.1) as usize + PERLIN_N as usize + height,
            });
        }

        let mut sum = 0.0;
        let mut vec = [point[0] * base_frequency.0, point[1] * base_frequency.1];
        let mut ratio = 1.0;
        for _ in 0..self.num_octaves {
            if self.type_ == NoiseType::FractalNoise {
                sum += self.noise2(color_channel, vec, stitch_info) / ratio;
            } else {
                sum += (self.noise2(color_channel, vec, stitch_info)).abs() / ratio;
            }
            vec[0] *= 2.0;
            vec[1] *= 2.0;
            ratio *= 2.0;
            if let Some(stitch_info) = stitch_info.as_mut() {
                // Update stitch values. Subtracting PerlinN before the multiplication and
                // adding it afterward simplifies to subtracting it once.
                stitch_info.width *= 2;
                stitch_info.wrap_x = 2 * stitch_info.wrap_x - PERLIN_N as usize;
                stitch_info.height *= 2;
                stitch_info.wrap_y = 2 * stitch_info.wrap_y - PERLIN_N as usize;
            }
        }
        sum
    }
}

impl Turbulence {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        let bounds: IRect = bounds_builder.compute(ctx).clipped.into();

        let affine = ctx.paffine().invert().unwrap();

        let seed = clamp(
            self.seed.trunc(), // per the spec, round towards zero
            f64::from(i32::MIN),
            f64::from(i32::MAX),
        ) as i32;

        // "Negative values are unsupported" -> set to the initial value which is 0.0
        //
        // https://drafts.fxtf.org/filter-effects/#element-attrdef-feturbulence-basefrequency
        //
        // Later in the algorithm, the base_frequency gets multiplied by the coordinates within the
        // tile.  So, limit the base_frequency to avoid overflow later.  We impose an arbitrary
        // upper limit for the frequency.  If it crosses that limit, we consider it invalid
        // and revert back to the initial value.  See bug #1115.
        let base_frequency = {
            let NumberOptionalNumber(base_freq_x, base_freq_y) = self.base_frequency;

            let x = if base_freq_x > 32768.0 {
                0.0
            } else {
                base_freq_x.max(0.0)
            };

            let y = if base_freq_y > 32768.0 {
                0.0
            } else {
                base_freq_y.max(0.0)
            };

            (x, y)
        };

        let noise_generator = NoiseGenerator::new(
            seed,
            base_frequency,
            self.num_octaves,
            self.type_,
            self.stitch_tiles,
            f64::from(bounds.width()),
            f64::from(bounds.height()),
        );

        // The generated color values are in the color space determined by
        // color-interpolation-filters.
        let surface_type = SurfaceType::from(self.color_interpolation_filters);

        let mut surface = ExclusiveImageSurface::new(
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
            surface_type,
        )?;

        surface.modify(&mut |data, stride| {
            for y in bounds.y_range() {
                for x in bounds.x_range() {
                    let point = affine.transform_point(f64::from(x), f64::from(y));
                    let point = [point.0, point.1];

                    let generate = |color_channel| {
                        let v = noise_generator.turbulence(
                            color_channel,
                            point,
                            f64::from(x - bounds.x0),
                            f64::from(y - bounds.y0),
                        );

                        let v = match self.type_ {
                            NoiseType::FractalNoise => (v * 255.0 + 255.0) / 2.0,
                            NoiseType::Turbulence => v * 255.0,
                        };

                        (clamp(v, 0.0, 255.0) + 0.5) as u8
                    };

                    let pixel = Pixel {
                        r: generate(0),
                        g: generate(1),
                        b: generate(2),
                        a: generate(3),
                    }
                    .premultiply();

                    data.set_pixel(stride, pixel, x as u32, y as u32);
                }
            }
        });

        Ok(FilterOutput {
            surface: surface.share()?,
            bounds,
        })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        InputRequirements::default()
    }
}

impl FilterEffect for FeTurbulence {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let values = cascaded.get();

        let mut params = self.params.clone();
        params.color_interpolation_filters = values.color_interpolation_filters();

        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Turbulence(params),
        }])
    }
}

impl Parse for StitchTiles {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "stitch" => StitchTiles::Stitch,
            "noStitch" => StitchTiles::NoStitch,
        )?)
    }
}

impl Parse for NoiseType {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "fractalNoise" => NoiseType::FractalNoise,
            "turbulence" => NoiseType::Turbulence,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turbulence_rng() {
        let mut r = 1;
        r = setup_seed(r);

        for _ in 0..10_000 {
            r = random(r);
        }

        assert_eq!(r, 1043618065);
    }
}
