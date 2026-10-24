/* Pango
 * pango-matrix.c: Matrix manipulation routines
 *
 * Copyright (C) 2000, 2006 Red Hat Software
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#include "config.h"
#include <stdlib.h>
#include <math.h>

#include "pango-matrix.h"
#include "pango-impl-utils.h"

G_DEFINE_BOXED_TYPE (PangoMatrix, pango_matrix,
                     pango_matrix_copy,
                     pango_matrix_free);

/**
 * pango_matrix_copy:
 * @matrix: (nullable): a `PangoMatrix`
 *
 * Copies a `PangoMatrix`.
 *
 * Return value: (nullable): the newly allocated `PangoMatrix`
 *
 * Since: 1.6
 */
PangoMatrix *
pango_matrix_copy (const PangoMatrix *matrix)
{
  PangoMatrix *new_matrix;

  if (matrix == NULL)
    return NULL;

  new_matrix = g_slice_new (PangoMatrix);

  *new_matrix = *matrix;

  return new_matrix;
}

/**
 * pango_matrix_free:
 * @matrix: (nullable): a `PangoMatrix`, may be %NULL
 *
 * Free a `PangoMatrix`.
 *
 * Since: 1.6
 */
void
pango_matrix_free (PangoMatrix *matrix)
{
  if (matrix == NULL)
    return;

  g_slice_free (PangoMatrix, matrix);
}

/**
 * pango_matrix_translate:
 * @matrix: a `PangoMatrix`
 * @tx: amount to translate in the X direction
 * @ty: amount to translate in the Y direction
 *
 * Changes the transformation represented by @matrix to be the
 * transformation given by first translating by (@tx, @ty)
 * then applying the original transformation.
 *
 * Since: 1.6
 */
void
pango_matrix_translate (PangoMatrix *matrix,
			double       tx,
			double       ty)
{
  g_return_if_fail (matrix != NULL);

  matrix->x0  = matrix->xx * tx + matrix->xy * ty + matrix->x0;
  matrix->y0  = matrix->yx * tx + matrix->yy * ty + matrix->y0;
}

/**
 * pango_matrix_scale:
 * @matrix: a `PangoMatrix`
 * @scale_x: amount to scale by in X direction
 * @scale_y: amount to scale by in Y direction
 *
 * Changes the transformation represented by @matrix to be the
 * transformation given by first scaling by @sx in the X direction
 * and @sy in the Y direction then applying the original
 * transformation.
 *
 * Since: 1.6
 */
void
pango_matrix_scale (PangoMatrix *matrix,
		    double       scale_x,
		    double       scale_y)
{
  g_return_if_fail (matrix != NULL);

  matrix->xx *= scale_x;
  matrix->xy *= scale_y;
  matrix->yx *= scale_x;
  matrix->yy *= scale_y;
}

/**
 * pango_matrix_rotate:
 * @matrix: a `PangoMatrix`
 * @degrees: degrees to rotate counter-clockwise
 *
 * Changes the transformation represented by @matrix to be the
 * transformation given by first rotating by @degrees degrees
 * counter-clockwise then applying the original transformation.
 *
 * Since: 1.6
 */
void
pango_matrix_rotate (PangoMatrix *matrix,
		     double       degrees)
{
  PangoMatrix tmp;
  gdouble r, s, c;

  g_return_if_fail (matrix != NULL);

  r = degrees * (G_PI / 180.);
  s = sin (r);
  c = cos (r);

  tmp.xx = c;
  tmp.xy = s;
  tmp.yx = -s;
  tmp.yy = c;
  tmp.x0 = 0;
  tmp.y0 = 0;

  pango_matrix_concat (matrix, &tmp);
}

/**
 * pango_matrix_concat:
 * @matrix: a `PangoMatrix`
 * @new_matrix: a `PangoMatrix`
 *
 * Changes the transformation represented by @matrix to be the
 * transformation given by first applying transformation
 * given by @new_matrix then applying the original transformation.
 *
 * Since: 1.6
 */
void
pango_matrix_concat (PangoMatrix       *matrix,
		     const PangoMatrix *new_matrix)
{
  PangoMatrix tmp;

  g_return_if_fail (matrix != NULL);

  tmp = *matrix;

  matrix->xx = tmp.xx * new_matrix->xx + tmp.xy * new_matrix->yx;
  matrix->xy = tmp.xx * new_matrix->xy + tmp.xy * new_matrix->yy;
  matrix->yx = tmp.yx * new_matrix->xx + tmp.yy * new_matrix->yx;
  matrix->yy = tmp.yx * new_matrix->xy + tmp.yy * new_matrix->yy;
  matrix->x0  = tmp.xx * new_matrix->x0 + tmp.xy * new_matrix->y0 + tmp.x0;
  matrix->y0  = tmp.yx * new_matrix->x0 + tmp.yy * new_matrix->y0 + tmp.y0;
}

/**
 * pango_matrix_get_font_scale_factor:
 * @matrix: (nullable): a `PangoMatrix`, may be %NULL
 *
 * Returns the scale factor of a matrix on the height of the font.
 *
 * That is, the scale factor in the direction perpendicular to the
 * vector that the X coordinate is mapped to.  If the scale in the X
 * coordinate is needed as well, use [method@Pango.Matrix.get_font_scale_factors].
 *
 * Return value: the scale factor of @matrix on the height of the font,
 *   or 1.0 if @matrix is %NULL.
 *
 * Since: 1.12
 */
double
pango_matrix_get_font_scale_factor (const PangoMatrix *matrix)
{
  double yscale;
  pango_matrix_get_font_scale_factors (matrix, NULL, &yscale);
  return yscale;
}

/**
 * pango_matrix_get_font_scale_factors:
 * @matrix: (nullable): a `PangoMatrix`
 * @xscale: (out) (optional): output scale factor in the x direction
 * @yscale: (out) (optional): output scale factor perpendicular to the x direction
 *
 * Calculates the scale factor of a matrix on the width and height of the font.
 *
 * That is, @xscale is the scale factor in the direction of the X coordinate,
 * and @yscale is the scale factor in the direction perpendicular to the
 * vector that the X coordinate is mapped to.
 *
 * Note that output numbers will always be non-negative.
 *
 * Since: 1.38
 **/
void
pango_matrix_get_font_scale_factors (const PangoMatrix *matrix,
				     double *xscale, double *yscale)
{
/*
 * Based on cairo-matrix.c:_cairo_matrix_compute_scale_factors()
 *
 * Copyright 2005, Keith Packard
 */
  double major = 1., minor = 1.;

  if (matrix)
    {
      double x = matrix->xx;
      double y = matrix->yx;
      major = sqrt (x*x + y*y);

      if (major)
	{
	  double det = matrix->xx * matrix->yy - matrix->yx * matrix->xy;

	  /*
	   * ignore mirroring
	   */
	  if (det < 0)
	    det = - det;

	  minor = det / major;
	}
      else
        minor = 0.;
    }

  if (xscale)
    *xscale = major;
  if (yscale)
    *yscale = minor;
}

/**
 * pango_matrix_get_slant_ratio:
 * @matrix: a `PangoMatrix`
 *
 * Gets the slant ratio of a matrix.
 *
 * For a simple shear matrix in the form:
 *
 *     1 λ
 *     0 1
 *
 * this is simply λ.
 *
 * Returns: the slant ratio of @matrix
 *
 * Since: 1.50
 */
double
pango_matrix_get_slant_ratio (const PangoMatrix *matrix)
{
  double x0, y0;
  double x1, y1;

  x0 = 0;
  y0 = 1;
  pango_matrix_transform_distance (matrix, &x0, &y0);

  x1 = 1;
  y1 = 0;
  pango_matrix_transform_distance (matrix, &x1, &y1);

  return (x0 * x1 + y0 * y1) / (x0 * x0 + y0 * y0);
}

/**
 * pango_matrix_transform_distance:
 * @matrix: (nullable): a `PangoMatrix`
 * @dx: (inout): in/out X component of a distance vector
 * @dy: (inout): in/out Y component of a distance vector
 *
 * Transforms the distance vector (@dx,@dy) by @matrix.
 *
 * This is similar to [method@Pango.Matrix.transform_point],
 * except that the translation components of the transformation
 * are ignored. The calculation of the returned vector is as follows:
 *
 * ```
 * dx2 = dx1 * xx + dy1 * xy;
 * dy2 = dx1 * yx + dy1 * yy;
 * ```
 *
 * Affine transformations are position invariant, so the same vector
 * always transforms to the same vector. If (@x1,@y1) transforms
 * to (@x2,@y2) then (@x1+@dx1,@y1+@dy1) will transform to
 * (@x1+@dx2,@y1+@dy2) for all values of @x1 and @x2.
 *
 * Since: 1.16
 */
void
pango_matrix_transform_distance (const PangoMatrix *matrix,
				 double            *dx,
				 double            *dy)
{
  if (matrix)
    {
      double new_x, new_y;

      new_x = (matrix->xx * *dx + matrix->xy * *dy);
      new_y = (matrix->yx * *dx + matrix->yy * *dy);

      *dx = new_x;
      *dy = new_y;
    }
}

/**
 * pango_matrix_transform_point:
 * @matrix: (nullable): a `PangoMatrix`
 * @x: (inout): in/out X position
 * @y: (inout): in/out Y position
 *
 * Transforms the point (@x, @y) by @matrix.
 *
 * Since: 1.16
 **/
void
pango_matrix_transform_point (const PangoMatrix *matrix,
			      double            *x,
			      double            *y)
{
  if (matrix)
    {
      pango_matrix_transform_distance (matrix, x, y);

      *x += matrix->x0;
      *y += matrix->y0;
    }
}

/**
 * pango_matrix_transform_rectangle:
 * @matrix: (nullable): a `PangoMatrix`
 * @rect: (inout) (optional): in/out bounding box in Pango units
 *
 * First transforms @rect using @matrix, then calculates the bounding box
 * of the transformed rectangle.
 *
 * This function is useful for example when you want to draw a rotated
 * @PangoLayout to an image buffer, and want to know how large the image
 * should be and how much you should shift the layout when rendering.
 *
 * If you have a rectangle in device units (pixels), use
 * [method@Pango.Matrix.transform_pixel_rectangle].
 *
 * If you have the rectangle in Pango units and want to convert to
 * transformed pixel bounding box, it is more accurate to transform it first
 * (using this function) and pass the result to pango_extents_to_pixels(),
 * first argument, for an inclusive rounded rectangle.
 * However, there are valid reasons that you may want to convert
 * to pixels first and then transform, for example when the transformed
 * coordinates may overflow in Pango units (large matrix translation for
 * example).
 *
 * Since: 1.16
 */
void
pango_matrix_transform_rectangle (const PangoMatrix *matrix,
				  PangoRectangle    *rect)
{
  int i;
  double quad_x[4], quad_y[4];
  double dx1, dy1;
  double dx2, dy2;
  double min_x, max_x;
  double min_y, max_y;

  if (!rect || !matrix)
    return;

  quad_x[0] = pango_units_to_double (rect->x);
  quad_y[0] = pango_units_to_double (rect->y);
  pango_matrix_transform_point (matrix, &quad_x[0], &quad_y[0]);

  dx1 = pango_units_to_double (rect->width);
  dy1 = 0;
  pango_matrix_transform_distance (matrix, &dx1, &dy1);
  quad_x[1] = quad_x[0] + dx1;
  quad_y[1] = quad_y[0] + dy1;

  dx2 = 0;
  dy2 = pango_units_to_double (rect->height);
  pango_matrix_transform_distance (matrix, &dx2, &dy2);
  quad_x[2] = quad_x[0] + dx2;
  quad_y[2] = quad_y[0] + dy2;

  quad_x[3] = quad_x[0] + dx1 + dx2;
  quad_y[3] = quad_y[0] + dy1 + dy2;

  min_x = max_x = quad_x[0];
  min_y = max_y = quad_y[0];

  for (i=1; i < 4; i++) {
      if (quad_x[i] < min_x)
	  min_x = quad_x[i];
      else if (quad_x[i] > max_x)
	  max_x = quad_x[i];

      if (quad_y[i] < min_y)
	  min_y = quad_y[i];
      else if (quad_y[i] > max_y)
	  max_y = quad_y[i];
  }

  rect->x      = pango_units_from_double (min_x);
  rect->y      = pango_units_from_double (min_y);
  rect->width  = pango_units_from_double (max_x) - rect->x;
  rect->height = pango_units_from_double (max_y) - rect->y;
}

/**
 * pango_matrix_transform_pixel_rectangle:
 * @matrix: (nullable): a `PangoMatrix`
 * @rect: (inout) (optional): in/out bounding box in device units
 *
 * First transforms the @rect using @matrix, then calculates the bounding box
 * of the transformed rectangle.
 *
 * This function is useful for example when you want to draw a rotated
 * @PangoLayout to an image buffer, and want to know how large the image
 * should be and how much you should shift the layout when rendering.
 *
 * For better accuracy, you should use [method@Pango.Matrix.transform_rectangle]
 * on original rectangle in Pango units and convert to pixels afterward
 * using [func@extents_to_pixels]'s first argument.
 *
 * Since: 1.16
 */
void
pango_matrix_transform_pixel_rectangle (const PangoMatrix *matrix,
					PangoRectangle    *rect)
{
  int i;
  double quad_x[4], quad_y[4];
  double dx1, dy1;
  double dx2, dy2;
  double min_x, max_x;
  double min_y, max_y;

  if (!rect || !matrix)
    return;

  quad_x[0] = rect->x;
  quad_y[0] = rect->y;
  pango_matrix_transform_point (matrix, &quad_x[0], &quad_y[0]);

  dx1 = rect->width;
  dy1 = 0;
  pango_matrix_transform_distance (matrix, &dx1, &dy1);
  quad_x[1] = quad_x[0] + dx1;
  quad_y[1] = quad_y[0] + dy1;

  dx2 = 0;
  dy2 = rect->height;
  pango_matrix_transform_distance (matrix, &dx2, &dy2);
  quad_x[2] = quad_x[0] + dx2;
  quad_y[2] = quad_y[0] + dy2;

  quad_x[3] = quad_x[0] + dx1 + dx2;
  quad_y[3] = quad_y[0] + dy1 + dy2;

  min_x = max_x = quad_x[0];
  min_y = max_y = quad_y[0];

  for (i=1; i < 4; i++)
    {
      if (quad_x[i] < min_x)
        min_x = quad_x[i];
      else if (quad_x[i] > max_x)
        max_x = quad_x[i];

      if (quad_y[i] < min_y)
        min_y = quad_y[i];
      else if (quad_y[i] > max_y)
        max_y = quad_y[i];
    }

  rect->x      = floor (min_x);
  rect->y      = floor (min_y);
  rect->width  = ceil (max_x - rect->x);
  rect->height = ceil (max_y - rect->y);
}
