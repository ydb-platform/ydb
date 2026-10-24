/* Pango
 * pango-gravity.c: Gravity routines
 *
 * Copyright (C) 2006, 2007 Red Hat Software
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

#include "pango-gravity.h"

#include <math.h>

/**
 * pango_gravity_to_rotation:
 * @gravity: gravity to query, should not be %PANGO_GRAVITY_AUTO
 *
 * Converts a `PangoGravity` value to its natural rotation in radians.
 *
 * Note that [method@Pango.Matrix.rotate] takes angle in degrees, not radians.
 * So, to call [method@Pango.Matrix,rotate] with the output of this function
 * you should multiply it by (180. / G_PI).
 *
 * Return value: the rotation value corresponding to @gravity.
 *
 * Since: 1.16
 */
double
pango_gravity_to_rotation (PangoGravity gravity)
{
  double rotation;

  g_return_val_if_fail (gravity != PANGO_GRAVITY_AUTO, 0);

  switch (gravity)
    {
      default:
      case PANGO_GRAVITY_AUTO: /* shut gcc up */
      case PANGO_GRAVITY_SOUTH:	rotation =  0;		break;
      case PANGO_GRAVITY_NORTH:	rotation =  G_PI;	break;
      case PANGO_GRAVITY_EAST:	rotation = -G_PI_2;	break;
      case PANGO_GRAVITY_WEST:	rotation = +G_PI_2;	break;
    }

  return rotation;
}

/**
 * pango_gravity_get_for_matrix:
 * @matrix: (nullable): a `PangoMatrix`
 *
 * Finds the gravity that best matches the rotation component
 * in a `PangoMatrix`.
 *
 * Return value: the gravity of @matrix, which will never be
 * %PANGO_GRAVITY_AUTO, or %PANGO_GRAVITY_SOUTH if @matrix is %NULL
 *
 * Since: 1.16
 */
PangoGravity
pango_gravity_get_for_matrix (const PangoMatrix *matrix)
{
  PangoGravity gravity;
  double x;
  double y;

  if (!matrix)
    return PANGO_GRAVITY_SOUTH;

  x = matrix->xy;
  y = matrix->yy;

  if (fabs (x) > fabs (y))
    gravity = x > 0 ? PANGO_GRAVITY_WEST : PANGO_GRAVITY_EAST;
  else
    gravity = y < 0 ? PANGO_GRAVITY_NORTH : PANGO_GRAVITY_SOUTH;

  return gravity;
}



typedef enum
{
  PANGO_VERTICAL_DIRECTION_NONE,
  PANGO_VERTICAL_DIRECTION_TTB,
  PANGO_VERTICAL_DIRECTION_BTT
} PangoVerticalDirection;

typedef struct {
  /* PangoDirection */
  guint8 horiz_dir;		/* Orientation in horizontal context */

  /* PangoVerticalDirection */
  guint8 vert_dir;		/* Orientation in vertical context */

  /* PangoGravity */
  guint8 preferred_gravity;	/* Preferred context gravity */

  /* gboolean */
  guint8 wide;			/* Whether script is mostly wide.
				 * Wide characters are upright (ie.
				 * not rotated) in foreign context */
} PangoScriptProperties;

#define NONE PANGO_VERTICAL_DIRECTION_NONE
#define TTB  PANGO_VERTICAL_DIRECTION_TTB
#define BTT  PANGO_VERTICAL_DIRECTION_BTT

#define LTR  PANGO_DIRECTION_LTR
#define RTL  PANGO_DIRECTION_RTL
#define WEAK PANGO_DIRECTION_WEAK_LTR

#define S PANGO_GRAVITY_SOUTH
#define E PANGO_GRAVITY_EAST
#define W PANGO_GRAVITY_WEST

const PangoScriptProperties script_properties[] =
  {				/* ISO 15924 code */
      {LTR, NONE, S, FALSE},	/* Zyyy */
      {LTR, NONE, S, FALSE},	/* Qaai */
      {RTL, NONE, S, FALSE},	/* Arab */
      {LTR, NONE, S, FALSE},	/* Armn */
      {LTR, NONE, S, FALSE},	/* Beng */
      {LTR, TTB,  E, TRUE },	/* Bopo */
      {LTR, NONE, S, FALSE},	/* Cher */
      {LTR, NONE, S, FALSE},	/* Qaac */
      {LTR, NONE, S, FALSE},	/* Cyrl (Cyrs) */
      {LTR, NONE, S, FALSE},	/* Dsrt */
      {LTR, NONE, S, FALSE},	/* Deva */
      {LTR, NONE, S, FALSE},	/* Ethi */
      {LTR, NONE, S, FALSE},	/* Geor (Geon, Geoa) */
      {LTR, NONE, S, FALSE},	/* Goth */
      {LTR, NONE, S, FALSE},	/* Grek */
      {LTR, NONE, S, FALSE},	/* Gujr */
      {LTR, NONE, S, FALSE},	/* Guru */
      {LTR, TTB,  E, TRUE },	/* Hani */
      {LTR, TTB,  E, TRUE },	/* Hang */
      {RTL, NONE, S, FALSE},	/* Hebr */
      {LTR, TTB,  E, TRUE },	/* Hira */
      {LTR, NONE, S, FALSE},	/* Knda */
      {LTR, TTB,  E, TRUE },	/* Kana */
      {LTR, NONE, S, FALSE},	/* Khmr */
      {LTR, NONE, S, FALSE},	/* Laoo */
      {LTR, NONE, S, FALSE},	/* Latn (Latf, Latg) */
      {LTR, NONE, S, FALSE},	/* Mlym */
      {WEAK,TTB,  W, FALSE},	/* Mong */
      {LTR, NONE, S, FALSE},	/* Mymr */
      {LTR, BTT,  W, FALSE},	/* Ogam */
      {LTR, NONE, S, FALSE},	/* Ital */
      {LTR, NONE, S, FALSE},	/* Orya */
      {LTR, NONE, S, FALSE},	/* Runr */
      {LTR, NONE, S, FALSE},	/* Sinh */
      {RTL, NONE, S, FALSE},	/* Syrc (Syrj, Syrn, Syre) */
      {LTR, NONE, S, FALSE},	/* Taml */
      {LTR, NONE, S, FALSE},	/* Telu */
      {RTL, NONE, S, FALSE},	/* Thaa */
      {LTR, NONE, S, FALSE},	/* Thai */
      {LTR, NONE, S, FALSE},	/* Tibt */
      {LTR, NONE, S, FALSE},	/* Cans */
      {LTR, TTB,  S, TRUE },	/* Yiii */
      {LTR, NONE, S, FALSE},	/* Tglg */
      {LTR, NONE, S, FALSE},	/* Hano */
      {LTR, NONE, S, FALSE},	/* Buhd */
      {LTR, NONE, S, FALSE},	/* Tagb */

      /* Unicode-4.0 additions */
      {LTR, NONE, S, FALSE},	/* Brai */
      {RTL, NONE, S, FALSE},	/* Cprt */
      {LTR, NONE, S, FALSE},	/* Limb */
      {LTR, NONE, S, FALSE},	/* Osma */
      {LTR, NONE, S, FALSE},	/* Shaw */
      {LTR, NONE, S, FALSE},	/* Linb */
      {LTR, NONE, S, FALSE},	/* Tale */
      {LTR, NONE, S, FALSE},	/* Ugar */

      /* Unicode-4.1 additions */
      {LTR, NONE, S, FALSE},	/* Talu */
      {LTR, NONE, S, FALSE},	/* Bugi */
      {LTR, NONE, S, FALSE},	/* Glag */
      {LTR, NONE, S, FALSE},	/* Tfng */
      {LTR, NONE, S, FALSE},	/* Sylo */
      {LTR, NONE, S, FALSE},	/* Xpeo */
      {RTL, NONE, S, FALSE},	/* Khar */

      /* Unicode-5.0 additions */
      {LTR, NONE, S, FALSE},	/* Zzzz */
      {LTR, NONE, S, FALSE},	/* Bali */
      {LTR, NONE, S, FALSE},	/* Xsux */
      {RTL, NONE, S, FALSE},	/* Phnx */
      {LTR, NONE, S, FALSE},	/* Phag */
      {RTL, NONE, S, FALSE},    /* Nkoo */

      /* Unicode-5.1 additions */
      {LTR, NONE, S, FALSE},	/* Kali */
      {LTR, NONE, S, FALSE},	/* Lepc */
      {LTR, NONE, S, FALSE},	/* Rjng */
      {LTR, NONE, S, FALSE},	/* Sund */
      {LTR, NONE, S, FALSE},	/* Saur */
      {LTR, NONE, S, FALSE},	/* Cham */
      {LTR, NONE, S, FALSE},	/* Olck */
      {LTR, NONE, S, FALSE},	/* Vaii */
      {LTR, NONE, S, FALSE},	/* Cari */
      {LTR, NONE, S, FALSE},	/* Lyci */
      {RTL, NONE, S, FALSE},	/* Lydi */

      /* Unicode-5.2 additions */
      {RTL, NONE, S, FALSE},	/* Avst */
      {LTR, NONE, S, FALSE},	/* Bamu */
      {LTR, NONE, S, FALSE},	/* Egyp */
      {RTL, NONE, S, FALSE},	/* Armi */
      {RTL, NONE, S, FALSE},	/* Phli */
      {RTL, NONE, S, FALSE},	/* Prti */
      {LTR, NONE, S, FALSE},	/* Java */
      {LTR, NONE, S, FALSE},	/* Kthi */
      {LTR, NONE, S, FALSE},	/* Lisu */
      {LTR, NONE, S, FALSE},	/* Mtei */
      {RTL, NONE, S, FALSE},	/* Sarb */
      {RTL, NONE, S, FALSE},	/* Orkh */
      {RTL, TTB,  S, FALSE},	/* Samr */
      {LTR, NONE, S, FALSE},	/* Lana */
      {LTR, NONE, S, FALSE},	/* Tavt */

      /* Unicode-6.0 additions */
      {LTR, NONE, S, FALSE},	/* Batk */
      {LTR, NONE, S, FALSE},	/* Brah */
      {RTL, NONE, S, FALSE},	/* Mand */

      /* Unicode-6.1 additions */
      {LTR, NONE, S, FALSE},	/* Cakm */
      {RTL, NONE, S, FALSE},	/* Merc */
      {RTL, NONE, S, FALSE},	/* Mero */
      {LTR, NONE, S, FALSE},	/* Plrd */
      {LTR, NONE, S, FALSE},	/* Shrd */
      {LTR, NONE, S, FALSE},	/* Sora */
      {LTR, NONE, S, FALSE},	/* Takr */

      /* Unicode-7.0 additions */
      {LTR, NONE, S, FALSE},	/* Bass */
      {LTR, NONE, S, FALSE},	/* Aghb */
      {LTR, NONE, S, FALSE},	/* Dupl */
      {LTR, NONE, S, FALSE},	/* Elba */
      {LTR, NONE, S, FALSE},	/* Gran */
      {LTR, NONE, S, FALSE},	/* Khoj */
      {LTR, NONE, S, FALSE},	/* Sind */
      {LTR, NONE, S, FALSE},	/* Lina */
      {LTR, NONE, S, FALSE},	/* Mahj */
      {RTL, NONE, S, FALSE},	/* Mani */
      {RTL, NONE, S, FALSE},	/* Mend */
      {LTR, NONE, S, FALSE},	/* Modi */
      {LTR, NONE, S, FALSE},	/* Mroo */
      {RTL, NONE, S, FALSE},	/* Nbat */
      {RTL, NONE, S, FALSE},	/* Narb */
      {LTR, NONE, S, FALSE},	/* Perm */
      {LTR, NONE, S, FALSE},	/* Hmng */
      {RTL, NONE, S, FALSE},	/* Palm */
      {LTR, NONE, S, FALSE},	/* Pauc */
      {RTL, NONE, S, FALSE},	/* Phlp */
      {LTR, NONE, S, FALSE},	/* Sidd */
      {LTR, NONE, S, FALSE},	/* Tirh */
      {LTR, NONE, S, FALSE},	/* Wara */

      /* Unicode-8.0 additions */
      {LTR, NONE, S, FALSE},	/* Ahom */
      {LTR, NONE, S, FALSE},	/* Hluw */
      {RTL, NONE, S, FALSE},	/* Hatr */
      {LTR, NONE, S, FALSE},    /* Mult */
      {LTR, NONE, S, FALSE},	/* Hung */
      {LTR, NONE, S, FALSE},	/* Sgnw */

      /* Unicode-9.0 additions */
      {RTL, NONE, S, FALSE},	/* Adlm */
      {LTR, NONE, S, FALSE},	/* Bhks */
      {LTR, NONE, S, FALSE},	/* Marc */
      {LTR, NONE, S, FALSE},	/* Newa */
      {LTR, NONE, S, FALSE},	/* Osge */
      {LTR, NONE, S, FALSE},	/* Tang */

      /* Unicode-10.0 additions */
      {LTR, NONE, S, FALSE},	/* Gonm */
      {LTR, NONE, S, FALSE},	/* Nshu */
      {LTR, NONE, S, FALSE},	/* Soyo */
      {LTR, NONE, S, FALSE},	/* Zanb */

      /* Unicode-11.0 additions */
      {LTR, NONE, S, FALSE},	/* Dogr */
      {LTR, NONE, S, FALSE},	/* Gong */
      {RTL, NONE, S, FALSE},	/* Rohg */
      {LTR, NONE, S, FALSE},	/* Maka */
      {LTR, NONE, S, FALSE},	/* Medf */
      {RTL, NONE, S, FALSE},	/* Sogo */
      {RTL, NONE, S, FALSE},	/* Sogd */

      /* Unicode-12.0 additions */
      {RTL, NONE, S, FALSE},	/* Elym */
      {LTR, NONE, S, FALSE},	/* Nand */
      {LTR, NONE, S, FALSE},	/* Rohg */
      {LTR, NONE, S, FALSE},	/* Wcho */

      /* Unicode-13.0 additions */
      {RTL, NONE, S, FALSE},	/* Chrs */
      {LTR, NONE, S, FALSE},	/* Diak */
      {LTR, NONE, S, FALSE},	/* Kits */
      {RTL, NONE, S, FALSE},	/* Yezi */

      {LTR, NONE, S, FALSE},    /* Cpmn */
      {RTL, NONE, S, FALSE},    /* Ougr */
      {LTR, NONE, S, FALSE},    /* Tnsa */
      {LTR, NONE, S, FALSE},    /* Toto */
      {LTR, NONE, S, FALSE},    /* Vith */
};

#undef NONE
#undef TTB
#undef BTT

#undef LTR
#undef RTL
#undef WEAK

#undef S
#undef E
#undef N
#undef W

static PangoScriptProperties
get_script_properties (PangoScript script)
{
  g_return_val_if_fail (script >= 0, script_properties[0]);

  if ((guint)script >= G_N_ELEMENTS (script_properties))
    return script_properties[0];

  return script_properties[script];
}

/**
 * pango_gravity_get_for_script:
 * @script: `PangoScript` to query
 * @base_gravity: base gravity of the paragraph
 * @hint: orientation hint
 *
 * Returns the gravity to use in laying out a `PangoItem`.
 *
 * The gravity is determined based on the script, base gravity, and hint.
 *
 * If @base_gravity is %PANGO_GRAVITY_AUTO, it is first replaced with the
 * preferred gravity of @script.  To get the preferred gravity of a script,
 * pass %PANGO_GRAVITY_AUTO and %PANGO_GRAVITY_HINT_STRONG in.
 *
 * Return value: resolved gravity suitable to use for a run of text
 * with @script
 *
 * Since: 1.16
 */
PangoGravity
pango_gravity_get_for_script (PangoScript      script,
                              PangoGravity     base_gravity,
                              PangoGravityHint hint)
{
  PangoScriptProperties props = get_script_properties (script);

  return pango_gravity_get_for_script_and_width (script, props.wide,
                                                 base_gravity, hint);
}

/**
 * pango_gravity_get_for_script_and_width:
 * @script: `PangoScript` to query
 * @wide: %TRUE for wide characters as returned by g_unichar_iswide()
 * @base_gravity: base gravity of the paragraph
 * @hint: orientation hint
 *
 * Returns the gravity to use in laying out a single character
 * or `PangoItem`.
 *
 * The gravity is determined based on the script, East Asian width,
 * base gravity, and hint,
 *
 * This function is similar to [func@Pango.Gravity.get_for_script] except
 * that this function makes a distinction between narrow/half-width and
 * wide/full-width characters also. Wide/full-width characters always
 * stand *upright*, that is, they always take the base gravity,
 * whereas narrow/full-width characters are always rotated in vertical
 * context.
 *
 * If @base_gravity is %PANGO_GRAVITY_AUTO, it is first replaced with the
 * preferred gravity of @script.
 *
 * Return value: resolved gravity suitable to use for a run of text
 * with @script and @wide.
 *
 * Since: 1.26
 */
PangoGravity
pango_gravity_get_for_script_and_width (PangoScript      script,
                                        gboolean         wide,
                                        PangoGravity     base_gravity,
                                        PangoGravityHint hint)
{
  PangoScriptProperties props = get_script_properties (script);
  gboolean vertical;

  if (G_UNLIKELY (base_gravity == PANGO_GRAVITY_AUTO))
    base_gravity = props.preferred_gravity;

  vertical = PANGO_GRAVITY_IS_VERTICAL (base_gravity);

  /* Everything is designed such that a system with no vertical support
   * renders everything correctly horizontally.  So, if not in a vertical
   * gravity, base and resolved gravities are always the same.
   *
   * Wide characters are always upright.
   */
  if (G_LIKELY (!vertical || wide))
    return base_gravity;

  /* If here, we have a narrow character in a vertical gravity setting.
   * Resolve depending on the hint.
   */
  switch (hint)
    {
    default:
    case PANGO_GRAVITY_HINT_NATURAL:
      if (props.vert_dir == PANGO_VERTICAL_DIRECTION_NONE)
	return PANGO_GRAVITY_SOUTH;
      if ((base_gravity   == PANGO_GRAVITY_EAST) ^
	  (props.vert_dir == PANGO_VERTICAL_DIRECTION_BTT))
	return PANGO_GRAVITY_SOUTH;
      else
	return PANGO_GRAVITY_NORTH;

    case PANGO_GRAVITY_HINT_STRONG:
      return base_gravity;

    case PANGO_GRAVITY_HINT_LINE:
      if ((base_gravity    == PANGO_GRAVITY_EAST) ^
	  (props.horiz_dir == PANGO_DIRECTION_RTL))
	return PANGO_GRAVITY_SOUTH;
      else
	return PANGO_GRAVITY_NORTH;
    }
}
