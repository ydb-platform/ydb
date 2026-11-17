/* Pango
 * pangofc-fontmap.c: Base fontmap type for fontconfig-based backends
 *
 * Copyright (C) 2000-2003 Red Hat, Inc.
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

/**
 * PangoFcFontMap:
 *
 * `PangoFcFontMap` is a base class for font map implementations using the
 * Fontconfig and FreeType libraries.
 *
 * It is used in the Xft and FreeType backends shipped with Pango,
 * but can also be used when creating new backends. Any backend
 * deriving from this base class will take advantage of the wide
 * range of shapers implemented using FreeType that come with Pango.
 */
#define FONTSET_CACHE_SIZE 256

#include "config.h"
#include <math.h>

#include <gio/gio.h>

#include "pango-context.h"
#include "pango-font-private.h"
#include "pango-fontmap-private.h"
#include "pangofc-fontmap-private.h"
#include "pangofc-private.h"
#include "pango-impl-utils.h"
#include "pango-enum-types.h"
#include "pango-coverage-private.h"
#include "pango-trace-private.h"
#include <hb-ft.h>


/* Overview:
 *
 * All programming is a practice in caching data.  PangoFcFontMap is the
 * major caching container of a Pango system on a Linux desktop.  Here is
 * a short overview of how it all works.
 *
 * In short, Fontconfig search patterns are constructed and a fontset loaded
 * using them.  Here is how we achieve that:
 *
 * - All FcPattern's referenced by any object in the fontmap are uniquified
 *   and cached in the fontmap.  This both speeds lookups based on patterns
 *   faster, and saves memory.  This is handled by fontmap->priv->pattern_hash.
 *   The patterns are cached indefinitely.
 *
 * - The results of a FcFontSort() are used to populate fontsets.  However,
 *   FcFontSort() relies on the search pattern only, which includes the font
 *   size but not the full font matrix.  The fontset however depends on the
 *   matrix.  As a result, multiple fontsets may need results of the
 *   FcFontSort() on the same input pattern (think rotating text).  As such,
 *   we cache FcFontSort() results in fontmap->priv->patterns_hash which
 *   is a refcounted structure.  This level of abstraction also allows for
 *   optimizations like calling FcFontMatch() instead of FcFontSort(), and
 *   only calling FcFontSort() if any patterns other than the first match
 *   are needed.  Another possible optimization would be to call FcFontSort()
 *   without trimming, and do the trimming lazily as we go.  Only pattern sets
 *   already referenced by a fontset are cached.
 *
 * - A number of most-recently-used fontsets are cached and reused when
 *   needed.  This is achieved using fontmap->priv->fontset_hash and
 *   fontmap->priv->fontset_cache.
 *
 * - All fonts created by any of our fontsets are also cached and reused.
 *   This is what fontmap->priv->font_hash does.
 *
 * - Data that only depends on the font file and face index is cached and
 *   reused by multiple fonts.  This includes coverage and cmap cache info.
 *   This is done using fontmap->priv->font_face_data_hash.
 *
 * Upon a cache_clear() request, all caches are emptied.  All objects (fonts,
 * fontsets, faces, families) having a reference from outside will still live
 * and may reference the fontmap still, but will not be reused by the fontmap.
 *
 *
 * Todo:
 *
 * - Make PangoCoverage a GObject and subclass it as PangoFcCoverage which
 *   will directly use FcCharset. (#569622)
 *
 * - Lazy trimming of FcFontSort() results.  Requires fontconfig with
 *   FcCharSetMerge().
 */

typedef enum {
  /* Initial state; Fontconfig is not initialized yet */
  DEFAULT_CONFIG_NOT_INITIALIZED,

  /* We have a thread doing Fontconfig initialization in the background */
  DEFAULT_CONFIG_INITIALIZING,

  /* FcInit() finished and its default configuration is loaded */
  DEFAULT_CONFIG_INITIALIZED
} DefaultConfig;

/* We call FcInit in a thread and set fc_initialized
 * when done, and are protected by a mutex. The thread
 * signals the cond when FcInit is done.
 */
static GMutex fc_init_mutex;
static GCond fc_init_cond;
static DefaultConfig fc_initialized = DEFAULT_CONFIG_NOT_INITIALIZED;


typedef struct _PangoFcFontFaceData PangoFcFontFaceData;
typedef struct _PangoFcFace         PangoFcFace;
typedef struct _PangoFcFamily       PangoFcFamily;
typedef struct _PangoFcFindFuncInfo PangoFcFindFuncInfo;
typedef struct _PangoFcPatterns     PangoFcPatterns;
typedef struct _PangoFcFontset      PangoFcFontset;

#define PANGO_FC_TYPE_FAMILY            (pango_fc_family_get_type ())
#define PANGO_FC_FAMILY(object)         (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_FC_TYPE_FAMILY, PangoFcFamily))
#define PANGO_FC_IS_FAMILY(object)      (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_FC_TYPE_FAMILY))

#define PANGO_FC_TYPE_FACE              (pango_fc_face_get_type ())
#define PANGO_FC_FACE(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_FC_TYPE_FACE, PangoFcFace))
#define PANGO_FC_IS_FACE(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_FC_TYPE_FACE))

#define PANGO_FC_TYPE_FONTSET           (pango_fc_fontset_get_type ())
#define PANGO_FC_FONTSET(object)        (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_FC_TYPE_FONTSET, PangoFcFontset))
#define PANGO_FC_IS_FONTSET(object)     (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_FC_TYPE_FONTSET))

struct _PangoFcFontMapPrivate
{
  GHashTable *fontset_hash;	/* Maps PangoFcFontsetKey -> PangoFcFontset  */
  GQueue *fontset_cache;	/* Recently used fontsets */

  GHashTable *font_hash;	/* Maps PangoFcFontKey -> PangoFcFont */

  GHashTable *patterns_hash;	/* Maps FcPattern -> PangoFcPatterns */

  /* pattern_hash is used to make sure we only store one copy of
   * each identical pattern. (Speeds up lookup).
   */
  GHashTable *pattern_hash;

  GHashTable *font_face_data_hash; /* Maps font file name/id -> data */

  /* List of all families available */
  PangoFcFamily **families;
  int n_families;		/* -1 == uninitialized */

  double dpi;

  /* Decoders */
  GSList *findfuncs;

  guint closed : 1;

  FcConfig *config;
  FcFontSet *fonts;

  GAsyncQueue *queue;
};

struct _PangoFcFontFaceData
{
  /* Key */
  char *filename;
  int id;            /* needed to handle TTC files with multiple faces */

  /* Data */
  FcPattern *pattern;  /* Referenced pattern that owns filename */
  PangoCoverage *coverage;
  PangoLanguage **languages;

  hb_face_t *hb_face;
};

struct _PangoFcFace
{
  PangoFontFace parent_instance;

  PangoFcFamily *family;
  char *style;
  FcPattern *pattern;

  guint fake : 1;
  guint regular : 1;
};

struct _PangoFcFamily
{
  PangoFontFamily parent_instance;

  PangoFcFontMap *fontmap;
  char *family_name;

  FcFontSet *patterns;
  PangoFcFace **faces;
  int n_faces;		/* -1 == uninitialized */

  int spacing;  /* FC_SPACING */
  gboolean variable;
};

struct _PangoFcFindFuncInfo
{
  PangoFcDecoderFindFunc findfunc;
  gpointer               user_data;
  GDestroyNotify         dnotify;
  gpointer               ddata;
};

static GType    pango_fc_family_get_type     (void);
static GType    pango_fc_face_get_type       (void);
static GType    pango_fc_fontset_get_type    (void);

static void          pango_fc_font_map_finalize      (GObject                      *object);
static PangoFont *   pango_fc_font_map_load_font     (PangoFontMap                 *fontmap,
						       PangoContext                 *context,
						       const PangoFontDescription   *description);
static PangoFontset *pango_fc_font_map_load_fontset  (PangoFontMap                 *fontmap,
						       PangoContext                 *context,
						       const PangoFontDescription   *desc,
						       PangoLanguage                *language);
static void          pango_fc_font_map_list_families (PangoFontMap                 *fontmap,
						       PangoFontFamily            ***families,
						       int                          *n_families);
static PangoFontFamily *pango_fc_font_map_get_family (PangoFontMap *fontmap,
                                                      const char   *name);

static double pango_fc_font_map_get_resolution (PangoFcFontMap *fcfontmap,
						PangoContext   *context);
static PangoFont *pango_fc_font_map_new_font   (PangoFcFontMap    *fontmap,
						PangoFcFontsetKey *fontset_key,
						FcPattern         *match);
static PangoFont * pango_fc_font_map_new_font_from_key (PangoFcFontMap    *fcfontmap,
                                                        PangoFcFontKey    *key);

static PangoFontFace *pango_fc_font_map_get_face (PangoFontMap *fontmap,
                                                  PangoFont    *font);

static void pango_fc_font_map_changed (PangoFontMap *fontmap);

static PangoFont * pango_fc_font_map_reload_font (PangoFontMap *fontmap,
                                                  PangoFont    *font,
                                                  double        scale,
                                                  PangoContext *context,
                                                  const char   *variations);

static gboolean  pango_fc_font_map_add_font_file (PangoFontMap  *fontmap,
                                                  const char    *filename,
                                                  GError       **error);

static guint    pango_fc_font_face_data_hash  (PangoFcFontFaceData *key);
static gboolean pango_fc_font_face_data_equal (PangoFcFontFaceData *key1,
					       PangoFcFontFaceData *key2);

static void               pango_fc_fontset_key_init  (PangoFcFontsetKey          *key,
						      PangoFcFontMap             *fcfontmap,
						      PangoContext               *context,
						      const PangoFontDescription *desc,
						      PangoLanguage              *language);
static PangoFcFontsetKey *pango_fc_fontset_key_copy  (const PangoFcFontsetKey *key);
static void               pango_fc_fontset_key_free  (PangoFcFontsetKey       *key);
static guint              pango_fc_fontset_key_hash  (const PangoFcFontsetKey *key);
static gboolean           pango_fc_fontset_key_equal (const PangoFcFontsetKey *key_a,
						      const PangoFcFontsetKey *key_b);

static void               pango_fc_font_key_init     (PangoFcFontKey       *key,
						      PangoFcFontMap       *fcfontmap,
						      PangoFcFontsetKey    *fontset_key,
						      FcPattern            *pattern);
static void               pango_fc_font_key_init_from_key (PangoFcFontKey       *key,
                                                           const PangoFcFontKey *orig);
static PangoFcFontKey    *pango_fc_font_key_copy     (const PangoFcFontKey *key);
static void               pango_fc_font_key_free     (PangoFcFontKey       *key);
static guint              pango_fc_font_key_hash     (const PangoFcFontKey *key);
static gboolean           pango_fc_font_key_equal    (const PangoFcFontKey *key_a,
						      const PangoFcFontKey *key_b);

static PangoFcPatterns *pango_fc_patterns_new   (FcPattern       *pat,
						 PangoFcFontMap  *fontmap);
static PangoFcPatterns *pango_fc_patterns_ref   (PangoFcPatterns *pats);
static void             pango_fc_patterns_unref (PangoFcPatterns *pats);
static FcPattern       *pango_fc_patterns_get_pattern      (PangoFcPatterns *pats);
static FcPattern       *pango_fc_patterns_get_font_pattern (PangoFcPatterns *pats,
							    int              i,
							    gboolean        *prepare);

static FcPattern *uniquify_pattern (PangoFcFontMap *fcfontmap,
				    FcPattern      *pattern);

gpointer get_gravity_class (void);

gpointer
get_gravity_class (void)
{
  static GEnumClass *class = NULL; /* MT-safe */

  if (g_once_init_enter (&class))
    g_once_init_leave (&class, (gpointer)g_type_class_ref (PANGO_TYPE_GRAVITY));

  return class;
}

static guint
pango_fc_font_face_data_hash (PangoFcFontFaceData *key)
{
  return g_str_hash (key->filename) ^ key->id;
}

static gboolean
pango_fc_font_face_data_equal (PangoFcFontFaceData *key1,
			       PangoFcFontFaceData *key2)
{
  return key1->id == key2->id &&
	 (key1 == key2 || 0 == strcmp (key1->filename, key2->filename));
}

static void
pango_fc_font_face_data_free (PangoFcFontFaceData *data)
{
  FcPatternDestroy (data->pattern);

  if (data->coverage)
    g_object_unref (data->coverage);

  g_free (data->languages);

  hb_face_destroy (data->hb_face);

  g_slice_free (PangoFcFontFaceData, data);
}

/* Fowler / Noll / Vo (FNV) Hash (http://www.isthe.com/chongo/tech/comp/fnv/)
 *
 * Not necessarily better than a lot of other hashes, but should be OK, and
 * well tested with binary data.
 */

#define FNV_32_PRIME ((guint32)0x01000193)
#define FNV1_32_INIT ((guint32)0x811c9dc5)

static guint32
hash_bytes_fnv (unsigned char *buffer,
		int            len,
		guint32        hval)
{
  while (len--)
    {
      hval *= FNV_32_PRIME;
      hval ^= *buffer++;
    }

  return hval;
}

static void
get_context_matrix (PangoContext *context,
		    PangoMatrix *matrix)
{
  const PangoMatrix *set_matrix;
  const PangoMatrix identity = PANGO_MATRIX_INIT;

  set_matrix = context ? pango_context_get_matrix (context) : NULL;
  *matrix = set_matrix ? *set_matrix : identity;
  matrix->x0 = matrix->y0 = 0.;
}

static int
get_scaled_size (PangoFcFontMap             *fcfontmap,
		 PangoContext               *context,
		 const PangoFontDescription *desc)
{
  double size = pango_font_description_get_size (desc);

  if (!pango_font_description_get_size_is_absolute (desc))
    {
      double dpi = pango_fc_font_map_get_resolution (fcfontmap, context);

      size = size * dpi / 72.;
    }

  return .5 + pango_matrix_get_font_scale_factor (pango_context_get_matrix (context)) * size;
}



struct _PangoFcFontsetKey {
  PangoFcFontMap *fontmap;
  PangoLanguage *language;
  PangoFontDescription *desc;
  PangoMatrix matrix;
  int pixelsize;
  double resolution;
  gpointer context_key;
  char *variations;
};

struct _PangoFcFontKey {
  PangoFcFontMap *fontmap;
  FcPattern *pattern;
  PangoMatrix matrix;
  gpointer context_key;
  char *variations;
};

static void
pango_fc_fontset_key_init (PangoFcFontsetKey          *key,
			   PangoFcFontMap             *fcfontmap,
			   PangoContext               *context,
			   const PangoFontDescription *desc,
			   PangoLanguage              *language)
{
  if (!language && context)
    language = pango_context_get_language (context);

  key->fontmap = fcfontmap;
  get_context_matrix (context, &key->matrix);
  key->pixelsize = get_scaled_size (fcfontmap, context, desc);
  key->resolution = pango_fc_font_map_get_resolution (fcfontmap, context);
  key->language = language;
  key->variations = g_strdup (pango_font_description_get_variations (desc));
  key->desc = pango_font_description_copy_static (desc);
  pango_font_description_unset_fields (key->desc, PANGO_FONT_MASK_SIZE | PANGO_FONT_MASK_VARIATIONS);

  if (context && PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->context_key_get)
    key->context_key = (gpointer)PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->context_key_get (fcfontmap, context);
  else
    key->context_key = NULL;
}

static gboolean
pango_fc_fontset_key_equal (const PangoFcFontsetKey *key_a,
			    const PangoFcFontsetKey *key_b)
{
  if (key_a->language == key_b->language &&
      key_a->pixelsize == key_b->pixelsize &&
      key_a->resolution == key_b->resolution &&
      ((key_a->variations == NULL && key_b->variations == NULL) ||
       (key_a->variations && key_b->variations && (strcmp (key_a->variations, key_b->variations) == 0))) &&
      pango_font_description_equal (key_a->desc, key_b->desc) &&
      0 == memcmp (&key_a->matrix, &key_b->matrix, 4 * sizeof (double)))
    {
      if (key_a->context_key)
	return PANGO_FC_FONT_MAP_GET_CLASS (key_a->fontmap)->context_key_equal (key_a->fontmap,
										key_a->context_key,
										key_b->context_key);
      else
        return key_a->context_key == key_b->context_key;
    }
  else
    return FALSE;
}

static guint
pango_fc_fontset_key_hash (const PangoFcFontsetKey *key)
{
    guint32 hash = FNV1_32_INIT;

    /* We do a bytewise hash on the doubles */
    hash = hash_bytes_fnv ((unsigned char *)(&key->matrix), sizeof (double) * 4, hash);
    hash = hash_bytes_fnv ((unsigned char *)(&key->resolution), sizeof (double), hash);

    hash ^= key->pixelsize;

    if (key->variations)
      hash ^= g_str_hash (key->variations);

    if (key->context_key)
      hash ^= PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_hash (key->fontmap,
									    key->context_key);

    return (hash ^
	    GPOINTER_TO_UINT (key->language) ^
	    pango_font_description_hash (key->desc));
}

static void
pango_fc_fontset_key_free (PangoFcFontsetKey *key)
{
  pango_font_description_free (key->desc);
  g_free (key->variations);

  if (key->context_key)
    PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_free (key->fontmap,
								  key->context_key);

  g_slice_free (PangoFcFontsetKey, key);
}

static PangoFcFontsetKey *
pango_fc_fontset_key_copy (const PangoFcFontsetKey *old)
{
  PangoFcFontsetKey *key = g_slice_new (PangoFcFontsetKey);

  key->fontmap = old->fontmap;
  key->language = old->language;
  key->desc = pango_font_description_copy (old->desc);
  key->matrix = old->matrix;
  key->pixelsize = old->pixelsize;
  key->resolution = old->resolution;
  key->variations = g_strdup (old->variations);

  if (old->context_key)
    key->context_key = PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_copy (key->fontmap,
										     old->context_key);
  else
    key->context_key = NULL;

  return key;
}

/**
 * pango_fc_fontset_key_get_language:
 * @key: the fontset key
 *
 * Gets the language member of @key.
 *
 * Returns: the language
 *
 * Since: 1.24
 */
PangoLanguage *
pango_fc_fontset_key_get_language (const PangoFcFontsetKey *key)
{
  return key->language;
}

/**
 * pango_fc_fontset_key_get_description:
 * @key: the fontset key
 *
 * Gets the font description of @key.
 *
 * Returns: the font description, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
const PangoFontDescription *
pango_fc_fontset_key_get_description (const PangoFcFontsetKey *key)
{
  return key->desc;
}

/**
 * pango_fc_fontset_key_get_matrix:
 * @key: the fontset key
 *
 * Gets the matrix member of @key.
 *
 * Returns: the matrix, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
const PangoMatrix *
pango_fc_fontset_key_get_matrix (const PangoFcFontsetKey *key)
{
  return &key->matrix;
}

/**
 * pango_fc_fontset_key_get_absolute_size:
 * @key: the fontset key
 *
 * Gets the absolute font size of @key in Pango units.
 *
 * This is adjusted for both resolution and transformation matrix.
 *
 * Returns: the pixel size of @key.
 *
 * Since: 1.24
 */
double
pango_fc_fontset_key_get_absolute_size (const PangoFcFontsetKey *key)
{
  return key->pixelsize;
}

/**
 * pango_fc_fontset_key_get_resolution:
 * @key: the fontset key
 *
 * Gets the resolution of @key
 *
 * Returns: the resolution of @key
 *
 * Since: 1.24
 */
double
pango_fc_fontset_key_get_resolution (const PangoFcFontsetKey *key)
{
  return key->resolution;
}

/**
 * pango_fc_fontset_key_get_context_key:
 * @key: the font key
 *
 * Gets the context key member of @key.
 *
 * Returns: the context key, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
gpointer
pango_fc_fontset_key_get_context_key (const PangoFcFontsetKey *key)
{
  return key->context_key;
}

/*
 * PangoFcFontKey
 */

static gboolean
pango_fc_font_key_equal (const PangoFcFontKey *key_a,
			 const PangoFcFontKey *key_b)
{
  if (key_a->pattern == key_b->pattern &&
      ((key_a->variations == NULL && key_b->variations == NULL) ||
       (key_a->variations && key_b->variations && (strcmp (key_a->variations, key_b->variations) == 0))) &&
      0 == memcmp (&key_a->matrix, &key_b->matrix, 4 * sizeof (double)))
    {
      if (key_a->context_key && key_b->context_key)
	return PANGO_FC_FONT_MAP_GET_CLASS (key_a->fontmap)->context_key_equal (key_a->fontmap,
										key_a->context_key,
										key_b->context_key);
      else
        return key_a->context_key == key_b->context_key;
    }
  else
    return FALSE;
}

static guint
pango_fc_font_key_hash (const PangoFcFontKey *key)
{
    guint32 hash = FNV1_32_INIT;

    /* We do a bytewise hash on the doubles */
    hash = hash_bytes_fnv ((unsigned char *)(&key->matrix), sizeof (double) * 4, hash);

    if (key->variations)
      hash ^= g_str_hash (key->variations);

    if (key->context_key)
      hash ^= PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_hash (key->fontmap,
									    key->context_key);

    return (hash ^ GPOINTER_TO_UINT (key->pattern));
}

static void
pango_fc_font_key_free (PangoFcFontKey *key)
{
  if (key->pattern)
    FcPatternDestroy (key->pattern);

  if (key->context_key)
    PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_free (key->fontmap,
								  key->context_key);

  g_free (key->variations);

  g_slice_free (PangoFcFontKey, key);
}

static PangoFcFontKey *
pango_fc_font_key_copy (const PangoFcFontKey *old)
{
  PangoFcFontKey *key = g_slice_new (PangoFcFontKey);

  key->fontmap = old->fontmap;
  FcPatternReference (old->pattern);
  key->pattern = old->pattern;
  key->matrix = old->matrix;
  key->variations = g_strdup (old->variations);
  if (old->context_key)
    key->context_key = PANGO_FC_FONT_MAP_GET_CLASS (key->fontmap)->context_key_copy (key->fontmap,
										     old->context_key);
  else
    key->context_key = NULL;

  return key;
}

static void
pango_fc_font_key_init_from_key (PangoFcFontKey       *key,
                                 const PangoFcFontKey *orig)
{
  key->fontmap = orig->fontmap;
  key->pattern = orig->pattern;
  key->matrix = orig->matrix;
  key->variations = orig->variations;
  key->context_key = orig->context_key;
}

static void
pango_fc_font_key_init (PangoFcFontKey    *key,
			PangoFcFontMap    *fcfontmap,
			PangoFcFontsetKey *fontset_key,
			FcPattern         *pattern)
{
  key->fontmap = fcfontmap;
  key->pattern = pattern;
  key->matrix = *pango_fc_fontset_key_get_matrix (fontset_key);
  key->variations = fontset_key->variations;
  key->context_key = pango_fc_fontset_key_get_context_key (fontset_key);
}

/* Public API */

/**
 * pango_fc_font_key_get_pattern:
 * @key: the font key
 *
 * Gets the fontconfig pattern member of @key.
 *
 * Returns: the pattern, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
const FcPattern *
pango_fc_font_key_get_pattern (const PangoFcFontKey *key)
{
  return key->pattern;
}

/**
 * pango_fc_font_key_get_matrix:
 * @key: the font key
 *
 * Gets the matrix member of @key.
 *
 * Returns: the matrix, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
const PangoMatrix *
pango_fc_font_key_get_matrix (const PangoFcFontKey *key)
{
  return &key->matrix;
}

/**
 * pango_fc_font_key_get_context_key:
 * @key: the font key
 *
 * Gets the context key member of @key.
 *
 * Returns: the context key, which is owned by @key and should not be modified.
 *
 * Since: 1.24
 */
gpointer
pango_fc_font_key_get_context_key (const PangoFcFontKey *key)
{
  return key->context_key;
}

const char *
pango_fc_font_key_get_variations (const PangoFcFontKey *key)
{
  return key->variations;
}

/*
 * PangoFcPatterns
 */

struct _PangoFcPatterns {
  PangoFcFontMap *fontmap;

  /* match and fontset are initialized in a thread,
   * and are protected by a mutex. The thread signals
   * the cond when match or fontset become available.
   */
  GMutex mutex;
  GCond cond;

  FcPattern *pattern;
  FcPattern *match;
  FcFontSet *fontset;
};

static FcFontSet *
font_set_copy (FcFontSet *fontset)
{
  FcFontSet *copy;
  int i;

  if (!fontset)
    return NULL;

  copy = malloc (sizeof (FcFontSet));
  copy->sfont = copy->nfont = fontset->nfont;
  copy->fonts = malloc (sizeof (FcPattern *) * copy->nfont);
  memcpy (copy->fonts, fontset->fonts, sizeof (FcPattern *) * copy->nfont);
  for (i = 0; i < copy->nfont; i++)
    FcPatternReference (copy->fonts[i]);

  return copy;
}

typedef enum {
  FC_INIT,
  FC_MATCH,
  FC_SORT,
  FC_END,
} FcOp;

typedef struct {
  FcOp op;
  FcConfig *config;
  FcFontSet *fonts;
  FcPattern *pattern;
  PangoFcPatterns *patterns;
} ThreadData;

static FcFontSet *pango_fc_font_map_get_config_fonts (PangoFcFontMap *fcfontmap);

static ThreadData *
thread_data_new (FcOp             op,
                 PangoFcPatterns *patterns)
{
  ThreadData *td;

  td = g_new0 (ThreadData, 1);

  td->op = op;

  if (!patterns)
    return td;

  /* We don't want the fontmap dying on us */
  g_object_ref (patterns->fontmap);

  td->patterns = pango_fc_patterns_ref (patterns);
  td->pattern = FcPatternDuplicate (patterns->pattern);
  td->config = FcConfigReference (pango_fc_font_map_get_config (patterns->fontmap));
  td->fonts = font_set_copy (pango_fc_font_map_get_config_fonts (patterns->fontmap));

  return td;
}

static void
thread_data_free (gpointer data)
{
  ThreadData *td = data;
  PangoFcFontMap *fontmap = td->patterns ? td->patterns->fontmap : NULL;

  g_clear_pointer (&td->fonts, FcFontSetDestroy);
  if (td->pattern)
    FcPatternDestroy (td->pattern);
  if (td->config)
    FcConfigDestroy (td->config);
  if (td->patterns)
    pango_fc_patterns_unref (td->patterns);
  g_free (td);

  g_clear_object (&fontmap);
}

static gpointer
init_in_thread (gpointer task_data)
{
  ThreadData *td = task_data;
  gint64 before G_GNUC_UNUSED;

  before = PANGO_TRACE_CURRENT_TIME;

  FcInit ();

  pango_trace_mark (before, "FcInit", NULL);

  g_mutex_lock (&fc_init_mutex);
  fc_initialized = DEFAULT_CONFIG_INITIALIZED;
  g_cond_broadcast (&fc_init_cond);
  g_mutex_unlock (&fc_init_mutex);

  thread_data_free (td);

  return NULL;
}

static gpointer
match_in_thread (gpointer task_data)
{
  ThreadData *td = task_data;
  FcResult result;
  FcPattern *match;
  gint64 before G_GNUC_UNUSED;

  before = PANGO_TRACE_CURRENT_TIME;

  match = FcFontSetMatch (td->config,
                          &td->fonts, 1,
                          td->pattern,
                          &result);

  pango_trace_mark (before, "FcFontSetMatch", NULL);

  g_mutex_lock (&td->patterns->mutex);
  td->patterns->match = match;
  g_cond_signal (&td->patterns->cond);
  g_mutex_unlock (&td->patterns->mutex);

  thread_data_free (td);

  return NULL;
}

static gpointer
sort_in_thread (gpointer task_data)
{
  ThreadData *td = task_data;
  FcResult result;
  FcFontSet *fontset;
  gint64 before G_GNUC_UNUSED;

  before = PANGO_TRACE_CURRENT_TIME;

  fontset = FcFontSetSort (td->config,
                           &td->fonts, 1,
                           td->pattern,
                           FcTrue,
                           NULL,
                           &result);

  pango_trace_mark (before, "FcFontSetSort", NULL);

  g_mutex_lock (&td->patterns->mutex);
  td->patterns->fontset = fontset;
  g_cond_signal (&td->patterns->cond);
  g_mutex_unlock (&td->patterns->mutex);

  thread_data_free (td);

  return NULL;
}

static gpointer
fc_thread_func (gpointer data)
{
  GAsyncQueue *queue = data;
  gboolean done = FALSE;

  while (!done)
    {
      ThreadData *td = g_async_queue_pop (queue);

      switch (td->op)
        {
        case FC_INIT:
          init_in_thread (td);
          break;

        case FC_MATCH:
          match_in_thread (td);
          break;

        case FC_SORT:
          sort_in_thread (td);
          break;

        case FC_END:
          thread_data_free (td);
          done = TRUE;
          break;

        default:
          g_assert_not_reached ();
        }
    }

  g_async_queue_unref (queue);

  pango_trace_mark (PANGO_TRACE_CURRENT_TIME, "end fontconfig thread", NULL);

  return NULL;
}

static PangoFcPatterns *
pango_fc_patterns_new (FcPattern *pat, PangoFcFontMap *fontmap)
{
  PangoFcPatterns *pats;

  pat = uniquify_pattern (fontmap, pat);
  pats = g_hash_table_lookup (fontmap->priv->patterns_hash, pat);
  if (pats)
    return pango_fc_patterns_ref (pats);

  pats = g_atomic_rc_box_new0 (PangoFcPatterns);

  pats->fontmap = fontmap;

  FcPatternReference (pat);
  pats->pattern = pat;

  g_mutex_init (&pats->mutex);
  g_cond_init (&pats->cond);

  g_async_queue_push (fontmap->priv->queue, thread_data_new (FC_MATCH, pats));
  g_async_queue_push (fontmap->priv->queue, thread_data_new (FC_SORT, pats));

  g_hash_table_insert (fontmap->priv->patterns_hash, pats->pattern, pats);

  return pats;
}

static PangoFcPatterns *
pango_fc_patterns_ref (PangoFcPatterns *pats)
{
  return g_atomic_rc_box_acquire (pats);
}

static void
free_patterns (gpointer data)
{
  PangoFcPatterns *pats = data;

  /* Only remove from fontmap hash if we are in it.  This is not necessarily
   * the case after a cache_clear() call. */
  if (pats->fontmap->priv->patterns_hash &&
      pats == g_hash_table_lookup (pats->fontmap->priv->patterns_hash, pats->pattern))
    g_hash_table_remove (pats->fontmap->priv->patterns_hash, pats->pattern);

  if (pats->pattern)
    FcPatternDestroy (pats->pattern);

  if (pats->match)
    FcPatternDestroy (pats->match);

  if (pats->fontset)
    FcFontSetDestroy (pats->fontset);

  g_cond_clear (&pats->cond);
  g_mutex_clear (&pats->mutex);
}

static void
pango_fc_patterns_unref (PangoFcPatterns *pats)
{
  g_atomic_rc_box_release_full (pats, free_patterns);
}

static FcPattern *
pango_fc_patterns_get_pattern (PangoFcPatterns *pats)
{
  return pats->pattern;
}

static gboolean
pango_fc_is_supported_font_format (FcPattern* pattern)
{
  FcResult res;
  const char *file;
  const char *fontwrapper;

  /* Patterns without FC_FILE are problematic, since our caching is based
   * on filenames.
   */
  res = FcPatternGetString (pattern, FC_FILE, 0, (FcChar8 **)(void*)&file);
  if (res != FcResultMatch)
    return FALSE;

  /* Harfbuzz supports only SFNT fonts. */
  res = FcPatternGetString (pattern, FC_FONT_WRAPPER, 0, (FcChar8 **)(void*)&fontwrapper);
  if (res != FcResultMatch)
    return FALSE;

  return strcmp (fontwrapper, "SFNT") == 0;
}

static FcFontSet *
filter_by_format (FcFontSet **sets, int nsets)
{
  FcFontSet *result;
  int set;

  result = FcFontSetCreate ();

  for (set = 0; set < nsets; set++)
    {
      FcFontSet *fontset = sets[set];
      int i;

      if (!fontset)
        continue;

      for (i = 0; i < fontset->nfont; i++)
        {
          if (!pango_fc_is_supported_font_format (fontset->fonts[i]))
            continue;

          FcPatternReference (fontset->fonts[i]);
          FcFontSetAdd (result, fontset->fonts[i]);
        }
    }

  return result;
}

static FcPattern *
pango_fc_patterns_get_font_pattern (PangoFcPatterns *pats, int i, gboolean *prepare)
{
  FcPattern *match = NULL;
  FcFontSet *fontset = NULL;

  if (i == 0)
    {
      gint64 before G_GNUC_UNUSED;
      gboolean waited = FALSE;

      before = PANGO_TRACE_CURRENT_TIME;

      g_mutex_lock (&pats->mutex);

      while (!pats->match && !pats->fontset)
        {
          waited = TRUE;
          g_cond_wait (&pats->cond, &pats->mutex);
        }

      match = pats->match;
      fontset = pats->fontset;

      g_mutex_unlock (&pats->mutex);

      if (waited)
        pango_trace_mark (before, "wait for FcFontMatch", NULL);

      if (match)
        {
          *prepare = FALSE;
          return match;
        }
    }
  else
    {
      gint64 before G_GNUC_UNUSED;
      gboolean waited = FALSE;

      before = PANGO_TRACE_CURRENT_TIME;

      g_mutex_lock (&pats->mutex);

      while (!pats->fontset)
        {
          waited = TRUE;
          g_cond_wait (&pats->cond, &pats->mutex);
        }

      fontset = pats->fontset;

      g_mutex_unlock (&pats->mutex);

      if (waited)
        pango_trace_mark (before, "wait for FcFontSort", NULL);
    }

  if (fontset)
    {
      if (i < fontset->nfont)
        {
          *prepare = TRUE;
          return fontset->fonts[i];
        }
    }

  return NULL;
}


/*
 * PangoFcFontset
 */

static void              pango_fc_fontset_finalize     (GObject                 *object);
static PangoLanguage *   pango_fc_fontset_get_language (PangoFontset            *fontset);
static  PangoFont *      pango_fc_fontset_get_font     (PangoFontset            *fontset,
							guint                    wc);
static void              pango_fc_fontset_foreach      (PangoFontset            *fontset,
							PangoFontsetForeachFunc  func,
							gpointer                 data);

struct _PangoFcFontset
{
  PangoFontset parent_instance;

  PangoFcFontsetKey *key;

  PangoFcPatterns *patterns;
  int patterns_i;

  GPtrArray *fonts;
  GPtrArray *coverages;

  GList *cache_link;
};

typedef PangoFontsetClass PangoFcFontsetClass;

G_DEFINE_TYPE (PangoFcFontset, pango_fc_fontset, PANGO_TYPE_FONTSET)

static PangoFcFontset *
pango_fc_fontset_new (PangoFcFontsetKey *key,
		      PangoFcPatterns   *patterns)
{
  PangoFcFontset *fontset;

  fontset = g_object_new (PANGO_FC_TYPE_FONTSET, NULL);

  fontset->key = pango_fc_fontset_key_copy (key);
  fontset->patterns = pango_fc_patterns_ref (patterns);

  return fontset;
}

static PangoFcFontsetKey *
pango_fc_fontset_get_key (PangoFcFontset *fontset)
{
  return fontset->key;
}

static PangoFont *
pango_fc_fontset_load_next_font (PangoFcFontset *fontset)
{
  FcPattern *pattern, *font_pattern;
  PangoFont *font;
  gboolean prepare;

  pattern = pango_fc_patterns_get_pattern (fontset->patterns);
  font_pattern = pango_fc_patterns_get_font_pattern (fontset->patterns,
						     fontset->patterns_i++,
						     &prepare);
  if (G_UNLIKELY (!font_pattern))
    return NULL;

  if (prepare)
    {
      font_pattern = FcFontRenderPrepare (fontset->key->fontmap->priv->config, pattern, font_pattern);

      if (G_UNLIKELY (!font_pattern))
	return NULL;
    }

  font = pango_fc_font_map_new_font (fontset->key->fontmap,
                                     fontset->key,
                                     font_pattern);

  if (prepare)
    FcPatternDestroy (font_pattern);

  return font;
}

static PangoFont *
pango_fc_fontset_get_font_at (PangoFcFontset *fontset,
			      unsigned int    i)
{
  while (i >= fontset->fonts->len)
    {
      PangoFont *font = pango_fc_fontset_load_next_font (fontset);
      g_ptr_array_add (fontset->fonts, font);
      g_ptr_array_add (fontset->coverages, NULL);
      if (!font)
        return NULL;
    }

  return g_ptr_array_index (fontset->fonts, i);
}

static void
pango_fc_fontset_class_init (PangoFcFontsetClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontsetClass *fontset_class = PANGO_FONTSET_CLASS (class);

  object_class->finalize = pango_fc_fontset_finalize;

  fontset_class->get_font = pango_fc_fontset_get_font;
  fontset_class->get_language = pango_fc_fontset_get_language;
  fontset_class->foreach = pango_fc_fontset_foreach;
}

static void
pango_fc_fontset_init (PangoFcFontset *fontset)
{
  fontset->fonts = g_ptr_array_new ();
  fontset->coverages = g_ptr_array_new ();
}

static void
pango_fc_fontset_finalize (GObject *object)
{
  PangoFcFontset *fontset = PANGO_FC_FONTSET (object);
  unsigned int i;

  for (i = 0; i < fontset->fonts->len; i++)
  {
    PangoFont *font = g_ptr_array_index(fontset->fonts, i);
    if (font)
      g_object_unref (font);
  }
  g_ptr_array_free (fontset->fonts, TRUE);

  for (i = 0; i < fontset->coverages->len; i++)
    {
      PangoCoverage *coverage = g_ptr_array_index (fontset->coverages, i);
      if (coverage)
	g_object_unref (coverage);
    }
  g_ptr_array_free (fontset->coverages, TRUE);

  if (fontset->key)
    pango_fc_fontset_key_free (fontset->key);

  if (fontset->patterns)
    pango_fc_patterns_unref (fontset->patterns);

  G_OBJECT_CLASS (pango_fc_fontset_parent_class)->finalize (object);
}

static PangoLanguage *
pango_fc_fontset_get_language (PangoFontset  *fontset)
{
  PangoFcFontset *fcfontset = PANGO_FC_FONTSET (fontset);

  return pango_fc_fontset_key_get_language (pango_fc_fontset_get_key (fcfontset));
}

static PangoFont *
pango_fc_fontset_get_font (PangoFontset  *fontset,
			   guint          wc)
{
  PangoFcFontset *fcfontset = PANGO_FC_FONTSET (fontset);
  PangoCoverageLevel best_level = PANGO_COVERAGE_NONE;
  PangoCoverageLevel level;
  PangoFont *font;
  PangoCoverage *coverage;
  int result = -1;
  unsigned int i;

  for (i = 0;
       pango_fc_fontset_get_font_at (fcfontset, i);
       i++)
    {
      coverage = g_ptr_array_index (fcfontset->coverages, i);

      if (coverage == NULL)
	{
	  font = g_ptr_array_index (fcfontset->fonts, i);

	  coverage = pango_font_get_coverage (font, fcfontset->key->language);
	  g_ptr_array_index (fcfontset->coverages, i) = coverage;
	}

      level = pango_coverage_get (coverage, wc);

      if (result == -1 || level > best_level)
	{
	  result = i;
	  best_level = level;
	  if (level == PANGO_COVERAGE_EXACT)
	    break;
	}
    }

  if (G_UNLIKELY (result == -1))
    return NULL;

  font = g_ptr_array_index (fcfontset->fonts, result);
  return g_object_ref (font);
}

static void
pango_fc_fontset_foreach (PangoFontset           *fontset,
			  PangoFontsetForeachFunc func,
			  gpointer                data)
{
  PangoFcFontset *fcfontset = PANGO_FC_FONTSET (fontset);
  PangoFont *font;
  unsigned int i;

  for (i = 0;
       (font = pango_fc_fontset_get_font_at (fcfontset, i));
       i++)
    {
      if ((*func) (fontset, font, data))
	return;
    }
}


/*
 * PangoFcFontMap
 */

static GType
pango_fc_font_map_get_item_type (GListModel *list)
{
  return PANGO_TYPE_FONT_FAMILY;
}

static void ensure_families (PangoFcFontMap *fcfontmap);

static guint
pango_fc_font_map_get_n_items (GListModel *list)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (list);

  ensure_families (fcfontmap);

  return fcfontmap->priv->n_families;
}

static gpointer
pango_fc_font_map_get_item (GListModel *list,
                            guint       position)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (list);

  ensure_families (fcfontmap);

  if (position < fcfontmap->priv->n_families)
    return g_object_ref (fcfontmap->priv->families[position]);

  return NULL;
}

static void
pango_fc_font_map_list_model_init (GListModelInterface *iface)
{
  iface->get_item_type = pango_fc_font_map_get_item_type;
  iface->get_n_items = pango_fc_font_map_get_n_items;
  iface->get_item = pango_fc_font_map_get_item;
}

G_DEFINE_ABSTRACT_TYPE_WITH_CODE (PangoFcFontMap, pango_fc_font_map, PANGO_TYPE_FONT_MAP,
                                  G_ADD_PRIVATE (PangoFcFontMap)
                                  G_IMPLEMENT_INTERFACE (G_TYPE_LIST_MODEL, pango_fc_font_map_list_model_init))

static void
start_fontconfig_thread (PangoFcFontMap *fcfontmap)
{
  GThread *thread;

  g_mutex_lock (&fc_init_mutex);

  thread = g_thread_new ("[pango] fontconfig", fc_thread_func, g_async_queue_ref (fcfontmap->priv->queue));
  g_thread_unref (thread);

  if (fc_initialized == DEFAULT_CONFIG_NOT_INITIALIZED)
    {
      fc_initialized = DEFAULT_CONFIG_INITIALIZING;

      g_async_queue_push (fcfontmap->priv->queue, thread_data_new (FC_INIT, NULL));
    }

  g_mutex_unlock (&fc_init_mutex);
}

static void
wait_for_fc_init (void)
{
  gint64 before G_GNUC_UNUSED;
  gboolean waited = FALSE;

  before = PANGO_TRACE_CURRENT_TIME;

  g_mutex_lock (&fc_init_mutex);
  while (fc_initialized < DEFAULT_CONFIG_INITIALIZED)
    {
      waited = TRUE;
      g_cond_wait (&fc_init_cond, &fc_init_mutex);
    }
  g_mutex_unlock (&fc_init_mutex);

  if (waited)
    pango_trace_mark (before, "wait for FcInit", NULL);
}

static void
pango_fc_font_map_init (PangoFcFontMap *fcfontmap)
{
  PangoFcFontMapPrivate *priv;

  priv = fcfontmap->priv = pango_fc_font_map_get_instance_private (fcfontmap);

  priv->n_families = -1;

  priv->font_hash = g_hash_table_new ((GHashFunc)pango_fc_font_key_hash,
				      (GEqualFunc)pango_fc_font_key_equal);

  priv->fontset_hash = g_hash_table_new_full ((GHashFunc)pango_fc_fontset_key_hash,
					      (GEqualFunc)pango_fc_fontset_key_equal,
					      NULL,
					      (GDestroyNotify)g_object_unref);
  priv->fontset_cache = g_queue_new ();

  priv->patterns_hash = g_hash_table_new (NULL, NULL);

  priv->pattern_hash = g_hash_table_new_full ((GHashFunc) FcPatternHash,
					      (GEqualFunc) FcPatternEqual,
					      (GDestroyNotify) FcPatternDestroy,
					      NULL);

  priv->font_face_data_hash = g_hash_table_new_full ((GHashFunc)pango_fc_font_face_data_hash,
						     (GEqualFunc)pango_fc_font_face_data_equal,
						     (GDestroyNotify)pango_fc_font_face_data_free,
						     NULL);
  priv->dpi = -1;

  priv->queue = g_async_queue_new ();

  start_fontconfig_thread (fcfontmap);
}

static void
pango_fc_font_map_fini (PangoFcFontMap *fcfontmap)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  int i;

  g_clear_pointer (&priv->fonts, FcFontSetDestroy);

  g_queue_free (priv->fontset_cache);
  priv->fontset_cache = NULL;

  g_hash_table_destroy (priv->fontset_hash);
  priv->fontset_hash = NULL;

  g_hash_table_destroy (priv->patterns_hash);
  priv->patterns_hash = NULL;

  g_hash_table_destroy (priv->font_hash);
  priv->font_hash = NULL;

  g_hash_table_destroy (priv->font_face_data_hash);
  priv->font_face_data_hash = NULL;

  g_hash_table_destroy (priv->pattern_hash);
  priv->pattern_hash = NULL;

  for (i = 0; i < priv->n_families; i++)
    g_object_unref (priv->families[i]);
  g_free (priv->families);
  priv->n_families = -1;
  priv->families = NULL;

  g_async_queue_push (fcfontmap->priv->queue, thread_data_new (FC_END, NULL));

  g_async_queue_unref (priv->queue);
  priv->queue = NULL;
}

static void
pango_fc_font_map_class_init (PangoFcFontMapClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontMapClass *fontmap_class = PANGO_FONT_MAP_CLASS (class);
  PangoFontMapClassPrivate *pclass;

  object_class->finalize = pango_fc_font_map_finalize;
  fontmap_class->load_font = pango_fc_font_map_load_font;
  fontmap_class->load_fontset = pango_fc_font_map_load_fontset;
  fontmap_class->list_families = pango_fc_font_map_list_families;
  fontmap_class->get_family = pango_fc_font_map_get_family;
  fontmap_class->get_face = pango_fc_font_map_get_face;
  fontmap_class->shape_engine_type = PANGO_RENDER_TYPE_FC;
  fontmap_class->changed = pango_fc_font_map_changed;

  pclass = g_type_class_get_private ((GTypeClass *) class, PANGO_TYPE_FONT_MAP);

  pclass->reload_font = pango_fc_font_map_reload_font;
  pclass->add_font_file = pango_fc_font_map_add_font_file;
}


/**
 * pango_fc_font_map_add_decoder_find_func:
 * @fcfontmap: The `PangoFcFontMap` to add this method to.
 * @findfunc: The `PangoFcDecoderFindFunc` callback function
 * @user_data: User data.
 * @dnotify: A `GDestroyNotify` callback that will be called when the
 *   fontmap is finalized and the decoder is released.
 *
 * This function saves a callback method in the `PangoFcFontMap` that
 * will be called whenever new fonts are created.
 *
 * If the function returns a `PangoFcDecoder`, that decoder will be used
 * to determine both coverage via a `FcCharSet` and a one-to-one mapping
 * of characters to glyphs. This will allow applications to have
 * application-specific encodings for various fonts.
 *
 * Since: 1.6
 */
void
pango_fc_font_map_add_decoder_find_func (PangoFcFontMap        *fcfontmap,
					 PangoFcDecoderFindFunc findfunc,
					 gpointer               user_data,
					 GDestroyNotify         dnotify)
{
  PangoFcFontMapPrivate *priv;
  PangoFcFindFuncInfo *info;

  g_return_if_fail (PANGO_IS_FC_FONT_MAP (fcfontmap));

  priv = fcfontmap->priv;

  info = g_slice_new (PangoFcFindFuncInfo);

  info->findfunc = findfunc;
  info->user_data = user_data;
  info->dnotify = dnotify;

  priv->findfuncs = g_slist_append (priv->findfuncs, info);
}

/**
 * pango_fc_font_map_find_decoder:
 * @fcfontmap: The `PangoFcFontMap` to use.
 * @pattern: The `FcPattern` to find the decoder for.
 *
 * Finds the decoder to use for @pattern.
 *
 * Decoders can be added to a font map using
 * [method@PangoFc.FontMap.add_decoder_find_func].
 *
 * Returns: (transfer full) (nullable): a newly created `PangoFcDecoder`
 *   object or %NULL if no decoder is set for @pattern.
 *
 * Since: 1.26
 */
PangoFcDecoder *
pango_fc_font_map_find_decoder (PangoFcFontMap *fcfontmap,
                                FcPattern      *pattern)
{
  GSList *l;

  g_return_val_if_fail (PANGO_IS_FC_FONT_MAP (fcfontmap), NULL);
  g_return_val_if_fail (pattern != NULL, NULL);

  for (l = fcfontmap->priv->findfuncs; l && l->data; l = l->next)
    {
      PangoFcFindFuncInfo *info = l->data;
      PangoFcDecoder *decoder;

      decoder = info->findfunc (pattern, info->user_data);
      if (decoder)
	return decoder;
    }

  return NULL;
}

static void
pango_fc_font_map_finalize (GObject *object)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (object);

  pango_fc_font_map_shutdown (fcfontmap);

  if (fcfontmap->substitute_destroy)
    fcfontmap->substitute_destroy (fcfontmap->substitute_data);

  if (fcfontmap->priv->config)
    FcConfigDestroy (fcfontmap->priv->config);

  G_OBJECT_CLASS (pango_fc_font_map_parent_class)->finalize (object);
}

/* Add a mapping from key to fcfont */
static void
pango_fc_font_map_add (PangoFcFontMap *fcfontmap,
		       PangoFcFontKey *key,
		       PangoFcFont    *fcfont)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  PangoFcFontKey *key_copy;

  key_copy = pango_fc_font_key_copy (key);
  _pango_fc_font_set_font_key (fcfont, key_copy);
  g_hash_table_insert (priv->font_hash, key_copy, fcfont);
}

static PangoFont *
pango_fc_font_map_reload_font (PangoFontMap *fontmap,
                               PangoFont    *font,
                               double        scale,
                               PangoContext *context,
                               const char   *variations)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (fontmap);
  PangoFcFont *fcfont = PANGO_FC_FONT (font);
  PangoFcFontKey key;
  FcPattern *pattern = NULL;
  double point_size;
  double pixel_size;
  PangoFont *scaled;

  pango_fc_font_key_init_from_key (&key, _pango_fc_font_get_font_key (fcfont));

  if (scale != 1.0)
    {
      pattern = FcPatternDuplicate (key.pattern);

      if (FcPatternGetDouble (pattern, FC_SIZE, 0, &point_size) != FcResultMatch)
        point_size = 13.;

      if (FcPatternGetDouble (pattern, FC_PIXEL_SIZE, 0, &pixel_size) != FcResultMatch)
        {
          double dpi;

          if (FcPatternGetDouble (pattern, FC_DPI, 0, &dpi) != FcResultMatch)
            dpi = 72.;

          pixel_size = point_size * dpi / 72.;
        }

      FcPatternRemove (pattern, FC_SIZE, 0);
      FcPatternAddDouble (pattern, FC_SIZE, point_size * scale);

      FcPatternRemove (pattern, FC_PIXEL_SIZE, 0);
      FcPatternAddDouble (pattern, FC_PIXEL_SIZE, pixel_size * scale);
    }

  if (context)
    {
      get_context_matrix (context, &key.matrix);
      if (PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->context_key_get)
        key.context_key = (gpointer) PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->context_key_get (fcfontmap, context);
    }

  if (variations)
    {
      if (!pattern)
        pattern = FcPatternDuplicate (key.pattern);

      FcPatternRemove (pattern, FC_FONT_VARIATIONS, 0);
      FcPatternAddString (pattern, FC_FONT_VARIATIONS, (FcChar8*) variations);

      key.variations = (char *) variations;
    }

  if (pattern)
    key.pattern = uniquify_pattern (fcfontmap, pattern);

  scaled = pango_fc_font_map_new_font_from_key (fcfontmap, &key);

  if (pattern)
    FcPatternDestroy (pattern);

  return scaled;
}

/* Remove mapping from fcfont->key to fcfont */
/* Closely related to shutdown_font() */
void
_pango_fc_font_map_remove (PangoFcFontMap *fcfontmap,
			   PangoFcFont    *fcfont)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  PangoFcFontKey *key;

  key = _pango_fc_font_get_font_key (fcfont);
  if (key)
    {
      /* Only remove from fontmap hash if we are in it.  This is not necessarily
       * the case after a cache_clear() call. */
      if (priv->font_hash &&
	  fcfont == g_hash_table_lookup (priv->font_hash, key))
        {
	  g_hash_table_remove (priv->font_hash, key);
	}
      _pango_fc_font_set_font_key (fcfont, NULL);
      pango_fc_font_key_free (key);
    }
}

static PangoFcFamily *
create_family (PangoFcFontMap *fcfontmap,
	       const char     *family_name,
	       int             spacing)
{
  PangoFcFamily *family = g_object_new (PANGO_FC_TYPE_FAMILY, NULL);
  family->fontmap = fcfontmap;
  family->family_name = g_strdup (family_name);
  family->spacing = spacing;
  family->variable = FALSE;
  family->patterns = FcFontSetCreate ();

  return family;
}

static gboolean
is_alias_family (const char *family_name)
{
  switch (family_name[0])
    {
    case 'c':
    case 'C':
      return (g_ascii_strcasecmp (family_name, "cursive") == 0);
    case 'f':
    case 'F':
      return (g_ascii_strcasecmp (family_name, "fantasy") == 0);
    case 'm':
    case 'M':
      return (g_ascii_strcasecmp (family_name, "monospace") == 0);
    case 's':
    case 'S':
      return (g_ascii_strcasecmp (family_name, "sans") == 0 ||
	      g_ascii_strcasecmp (family_name, "serif") == 0 ||
	      g_ascii_strcasecmp (family_name, "system-ui") == 0);
    default:
      return FALSE;
    }

  return FALSE;
}

static void
ensure_families (PangoFcFontMap *fcfontmap)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  FcFontSet *fontset;
  int i;
  int count;

  wait_for_fc_init ();

  if (priv->n_families < 0)
    {
      FcObjectSet *os = FcObjectSetBuild (FC_FAMILY, FC_SPACING, FC_STYLE, FC_WEIGHT, FC_WIDTH, FC_SLANT,
                                          FC_VARIABLE,
                                          FC_FONTFORMAT,
                                          NULL);
      FcPattern *pat = FcPatternCreate ();
      GHashTable *temp_family_hash;
      FcFontSet *fonts;

      fonts = pango_fc_font_map_get_config_fonts (fcfontmap);
      fontset = FcFontSetList (priv->config, &fonts, 1, pat, os);

      FcPatternDestroy (pat);
      FcObjectSetDestroy (os);

      priv->families = g_new (PangoFcFamily *, fontset->nfont + 4); /* 4 standard aliases */
      temp_family_hash = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

      count = 0;
      for (i = 0; i < fontset->nfont; i++)
	{
	  char *s;
	  FcResult res;
	  int spacing;
          int variable;
	  PangoFcFamily *temp_family;

	  res = FcPatternGetString (fontset->fonts[i], FC_FAMILY, 0, (FcChar8 **)(void*)&s);
	  g_assert (res == FcResultMatch);

	  temp_family = g_hash_table_lookup (temp_family_hash, s);
	  if (!is_alias_family (s) && !temp_family)
	    {
	      res = FcPatternGetInteger (fontset->fonts[i], FC_SPACING, 0, &spacing);
	      g_assert (res == FcResultMatch || res == FcResultNoMatch);
	      if (res == FcResultNoMatch)
		spacing = FC_PROPORTIONAL;

	      temp_family = create_family (fcfontmap, s, spacing);
	      g_hash_table_insert (temp_family_hash, g_strdup (s), temp_family);
	      priv->families[count++] = temp_family;
	    }

	  if (temp_family)
	    {
              variable = FALSE;
              res = FcPatternGetBool (fontset->fonts[i], FC_VARIABLE, 0, &variable);
              if (res == FcResultMatch && variable)
                temp_family->variable = TRUE;

	      FcPatternReference (fontset->fonts[i]);
	      FcFontSetAdd (temp_family->patterns, fontset->fonts[i]);
	    }
	}

      FcFontSetDestroy (fontset);
      g_hash_table_destroy (temp_family_hash);

      priv->families[count++] = create_family (fcfontmap, "Sans", FC_PROPORTIONAL);
      priv->families[count++] = create_family (fcfontmap, "Serif", FC_PROPORTIONAL);
      priv->families[count++] = create_family (fcfontmap, "Monospace", FC_MONO);
      priv->families[count++] = create_family (fcfontmap, "System-ui", FC_PROPORTIONAL);

      priv->n_families = count;
    }
}

static void
pango_fc_font_map_list_families (PangoFontMap      *fontmap,
				 PangoFontFamily ***families,
				 int               *n_families)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (fontmap);
  PangoFcFontMapPrivate *priv = fcfontmap->priv;

  if (priv->closed)
    {
      if (families)
	*families = NULL;
      if (n_families)
	*n_families = 0;

      return;
    }

  ensure_families (fcfontmap);

  if (n_families)
    *n_families = priv->n_families;

  if (families)
    *families = g_memdup2 (priv->families, priv->n_families * sizeof (PangoFontFamily *));
}

static PangoFontFamily *
pango_fc_font_map_get_family (PangoFontMap *fontmap,
                              const char   *name)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (fontmap);
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  int i;

  if (priv->closed)
    return NULL;

  ensure_families (fcfontmap);

  for (i = 0; i < priv->n_families; i++)
    {
      PangoFontFamily *family = PANGO_FONT_FAMILY (priv->families[i]);
      if (strcmp (name, pango_font_family_get_name (family)) == 0)
        return family;
    }

  return NULL;
}

static double
pango_fc_convert_weight_to_fc (PangoWeight pango_weight)
{
  return FcWeightFromOpenTypeDouble (pango_weight);
}

static int
pango_fc_convert_slant_to_fc (PangoStyle pango_style)
{
  switch (pango_style)
    {
    case PANGO_STYLE_NORMAL:
      return FC_SLANT_ROMAN;
    case PANGO_STYLE_ITALIC:
      return FC_SLANT_ITALIC;
    case PANGO_STYLE_OBLIQUE:
      return FC_SLANT_OBLIQUE;
    default:
      return FC_SLANT_ROMAN;
    }
}

static int
pango_fc_convert_width_to_fc (PangoStretch pango_stretch)
{
  switch (pango_stretch)
    {
    case PANGO_STRETCH_NORMAL:
      return FC_WIDTH_NORMAL;
    case PANGO_STRETCH_ULTRA_CONDENSED:
      return FC_WIDTH_ULTRACONDENSED;
    case PANGO_STRETCH_EXTRA_CONDENSED:
      return FC_WIDTH_EXTRACONDENSED;
    case PANGO_STRETCH_CONDENSED:
      return FC_WIDTH_CONDENSED;
    case PANGO_STRETCH_SEMI_CONDENSED:
      return FC_WIDTH_SEMICONDENSED;
    case PANGO_STRETCH_SEMI_EXPANDED:
      return FC_WIDTH_SEMIEXPANDED;
    case PANGO_STRETCH_EXPANDED:
      return FC_WIDTH_EXPANDED;
    case PANGO_STRETCH_EXTRA_EXPANDED:
      return FC_WIDTH_EXTRAEXPANDED;
    case PANGO_STRETCH_ULTRA_EXPANDED:
      return FC_WIDTH_ULTRAEXPANDED;
    default:
      return FC_WIDTH_NORMAL;
    }
}

static FcPattern *
pango_fc_make_pattern (const  PangoFontDescription *description,
		       PangoLanguage               *language,
		       int                          pixel_size,
		       double                       dpi,
                       const char                  *variations)
{
  FcPattern *pattern;
  const char *prgname;
  int slant;
  double weight;
  PangoGravity gravity;
  PangoVariant variant;
  char **families;
  int i;
  int width;

  prgname = g_get_prgname ();
  slant = pango_fc_convert_slant_to_fc (pango_font_description_get_style (description));
  weight = pango_fc_convert_weight_to_fc (pango_font_description_get_weight (description));
  width = pango_fc_convert_width_to_fc (pango_font_description_get_stretch (description));

  gravity = pango_font_description_get_gravity (description);
  variant = pango_font_description_get_variant (description);

  /* The reason for passing in FC_SIZE as well as FC_PIXEL_SIZE is
   * to work around a bug in libgnomeprint where it doesn't look
   * for FC_PIXEL_SIZE. See http://bugzilla.gnome.org/show_bug.cgi?id=169020
   *
   * Putting FC_SIZE in here slightly reduces the efficiency
   * of caching of patterns and fonts when working with multiple different
   * dpi values.
   *
   * Do not pass FC_VERTICAL_LAYOUT true as HarfBuzz shaping assumes false.
   */
  pattern = FcPatternBuild (NULL,
			    PANGO_FC_VERSION, FcTypeInteger, pango_version(),
			    FC_WEIGHT, FcTypeDouble, weight,
			    FC_SLANT,  FcTypeInteger, slant,
			    FC_WIDTH,  FcTypeInteger, width,
			    FC_VARIABLE,  FcTypeBool, FcDontCare,
			    FC_DPI, FcTypeDouble, dpi,
			    FC_SIZE,  FcTypeDouble,  pixel_size * (72. / 1024. / dpi),
			    FC_PIXEL_SIZE,  FcTypeDouble,  pixel_size / 1024.,
			    NULL);

  if (variations)
    FcPatternAddString (pattern, FC_FONT_VARIATIONS, (FcChar8*) variations);

  if (pango_font_description_get_family (description))
    {
      families = g_strsplit (pango_font_description_get_family (description), ",", -1);

      for (i = 0; families[i]; i++)
	FcPatternAddString (pattern, FC_FAMILY, (FcChar8*) families[i]);

      g_strfreev (families);
    }

  if (language)
    FcPatternAddString (pattern, FC_LANG, (FcChar8 *) pango_language_to_string (language));

  if (gravity != PANGO_GRAVITY_SOUTH)
    {
      GEnumValue *value = g_enum_get_value (get_gravity_class (), gravity);
      FcPatternAddString (pattern, PANGO_FC_GRAVITY, (FcChar8*) value->value_nick);
    }

  if (prgname)
    FcPatternAddString (pattern, PANGO_FC_PRGNAME, (FcChar8*) prgname);

  switch (variant)
    {
    case PANGO_VARIANT_SMALL_CAPS:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "smcp=1");
      break;
    case PANGO_VARIANT_ALL_SMALL_CAPS:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "smcp=1");
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "c2sc=1");
      break;
    case PANGO_VARIANT_PETITE_CAPS:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "pcap=1");
      break;
    case PANGO_VARIANT_ALL_PETITE_CAPS:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "pcap=1");
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "c2pc=1");
      break;
    case PANGO_VARIANT_UNICASE:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "unic=1");
      break;
    case PANGO_VARIANT_TITLE_CAPS:
      FcPatternAddString (pattern, FC_FONT_FEATURES, (FcChar8*) "titl=1");
      break;
    case PANGO_VARIANT_NORMAL:
      break;
    default:
      g_assert_not_reached ();
    }

  return pattern;
}

static FcPattern *
uniquify_pattern (PangoFcFontMap *fcfontmap,
		  FcPattern      *pattern)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  FcPattern *old_pattern;

  old_pattern = g_hash_table_lookup (priv->pattern_hash, pattern);
  if (old_pattern)
    {
      return old_pattern;
    }
  else
    {
      FcPatternReference (pattern);
      g_hash_table_insert (priv->pattern_hash, pattern, pattern);
      return pattern;
    }
}

static PangoFont *
pango_fc_font_map_new_font_from_key (PangoFcFontMap *fcfontmap,
                                     PangoFcFontKey *key)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  PangoFcFontMapClass *class = PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap);
  PangoFcFont *fcfont;

  if (priv->closed)
    return NULL;

  fcfont = g_hash_table_lookup (priv->font_hash, key);
  if (fcfont)
    return g_object_ref (PANGO_FONT (fcfont));

  class = PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap);

  if (class->create_font)
    fcfont = class->create_font (fcfontmap, key);
  else
    g_warning ("%s needs to implement create_font", G_OBJECT_TYPE_NAME (fcfontmap));

  if (!fcfont)
    return NULL;

  pango_fc_font_map_add (fcfontmap, key, fcfont);

  return (PangoFont *)fcfont;
}

static PangoFont *
pango_fc_font_map_new_font (PangoFcFontMap    *fcfontmap,
			    PangoFcFontsetKey *fontset_key,
			    FcPattern         *match)
{
  PangoFcFontMapClass *class;
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  FcPattern *pattern;
  PangoFcFont *fcfont;
  PangoFcFontKey key;

  if (priv->closed)
    return NULL;

  match = uniquify_pattern (fcfontmap, match);

  pango_fc_font_key_init (&key, fcfontmap, fontset_key, match);

  fcfont = g_hash_table_lookup (priv->font_hash, &key);
  if (fcfont)
    return g_object_ref (PANGO_FONT (fcfont));

  class = PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap);

  if (class->create_font)
    {
      fcfont = class->create_font (fcfontmap, &key);
    }
  else
    {
      const PangoMatrix *pango_matrix = pango_fc_fontset_key_get_matrix (fontset_key);
      FcMatrix fc_matrix, *fc_matrix_val;
      int i;

      /* Fontconfig has the Y axis pointing up, Pango, down.
       */
      fc_matrix.xx = pango_matrix->xx;
      fc_matrix.xy = - pango_matrix->xy;
      fc_matrix.yx = - pango_matrix->yx;
      fc_matrix.yy = pango_matrix->yy;

      pattern = FcPatternDuplicate (match);

      for (i = 0; FcPatternGetMatrix (pattern, FC_MATRIX, i, &fc_matrix_val) == FcResultMatch; i++)
	FcMatrixMultiply (&fc_matrix, &fc_matrix, fc_matrix_val);

      FcPatternDel (pattern, FC_MATRIX);
      FcPatternAddMatrix (pattern, FC_MATRIX, &fc_matrix);

      fcfont = class->new_font (fcfontmap, uniquify_pattern (fcfontmap, pattern));

      FcPatternDestroy (pattern);
    }

  if (!fcfont)
    return NULL;

  /* In case the backend didn't set the fontmap */
  if (!fcfont->fontmap)
    g_object_set (fcfont,
		  "fontmap", fcfontmap,
		  NULL);

  /* cache it on fontmap */
  pango_fc_font_map_add (fcfontmap, &key, fcfont);

  return (PangoFont *)fcfont;
}

static PangoFontFace *
pango_fc_font_map_get_face (PangoFontMap *fontmap,
                            PangoFont    *font)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);
  FcResult res;
  const char *s;
  PangoFontFamily *family;

  res = FcPatternGetString (fcfont->font_pattern, FC_FAMILY, 0, (FcChar8 **) &s);
  g_assert (res == FcResultMatch);

  family = pango_font_map_get_family (fontmap, s);

  res = FcPatternGetString (fcfont->font_pattern, FC_STYLE, 0, (FcChar8 **)(void*)&s);
  g_assert (res == FcResultMatch);

  return pango_font_family_get_face (family, s);
}

static void
pango_fc_default_substitute (PangoFcFontMap    *fontmap,
			     PangoFcFontsetKey *fontsetkey,
			     FcPattern         *pattern)
{
  if (PANGO_FC_FONT_MAP_GET_CLASS (fontmap)->fontset_key_substitute)
    PANGO_FC_FONT_MAP_GET_CLASS (fontmap)->fontset_key_substitute (fontmap, fontsetkey, pattern);
  else if (PANGO_FC_FONT_MAP_GET_CLASS (fontmap)->default_substitute)
    PANGO_FC_FONT_MAP_GET_CLASS (fontmap)->default_substitute (fontmap, pattern);
}

void
pango_fc_font_map_set_default_substitute (PangoFcFontMap        *fontmap,
					  PangoFcSubstituteFunc func,
					  gpointer              data,
					  GDestroyNotify        notify)
{
  if (fontmap->substitute_destroy)
    fontmap->substitute_destroy (fontmap->substitute_data);

  fontmap->substitute_func = func;
  fontmap->substitute_data = data;
  fontmap->substitute_destroy = notify;

  pango_fc_font_map_substitute_changed (fontmap);
}

void
pango_fc_font_map_substitute_changed (PangoFcFontMap *fontmap) {
  pango_fc_font_map_cache_clear(fontmap);
  pango_font_map_changed(PANGO_FONT_MAP (fontmap));
}

static double
pango_fc_font_map_get_resolution (PangoFcFontMap *fcfontmap,
				  PangoContext   *context)
{
  if (PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->get_resolution)
    return PANGO_FC_FONT_MAP_GET_CLASS (fcfontmap)->get_resolution (fcfontmap, context);

  if (fcfontmap->priv->dpi < 0)
    {
      FcResult result = FcResultNoMatch;
      FcPattern *tmp = FcPatternBuild (NULL,
				       FC_FAMILY, FcTypeString, "Sans",
				       FC_SIZE,   FcTypeDouble, 10.,
				       NULL);
      if (tmp)
	{
	  pango_fc_default_substitute (fcfontmap, NULL, tmp);
	  result = FcPatternGetDouble (tmp, FC_DPI, 0, &fcfontmap->priv->dpi);
	  FcPatternDestroy (tmp);
	}

      if (result != FcResultMatch)
	{
	  g_warning ("Error getting DPI from fontconfig, using 72.0");
	  fcfontmap->priv->dpi = 72.0;
	}
    }

  return fcfontmap->priv->dpi;
}

static FcPattern *
pango_fc_fontset_key_make_pattern (PangoFcFontsetKey *key)
{
  return pango_fc_make_pattern (key->desc,
				key->language,
				key->pixelsize,
				key->resolution,
                                key->variations);
}

static PangoFcPatterns *
pango_fc_font_map_get_patterns (PangoFontMap      *fontmap,
				PangoFcFontsetKey *key)
{
  PangoFcFontMap *fcfontmap = (PangoFcFontMap *)fontmap;
  PangoFcPatterns *patterns;
  FcPattern *pattern;

  pattern = pango_fc_fontset_key_make_pattern (key);
  pango_fc_default_substitute (fcfontmap, key, pattern);

  patterns = pango_fc_patterns_new (pattern, fcfontmap);

  FcPatternDestroy (pattern);

  return patterns;
}

static gboolean
get_first_font (PangoFontset  *fontset G_GNUC_UNUSED,
		PangoFont     *font,
		gpointer       data)
{
  *(PangoFont **)data = font;

  return TRUE;
}

static PangoFont *
pango_fc_font_map_load_font (PangoFontMap               *fontmap,
			     PangoContext               *context,
			     const PangoFontDescription *description)
{
  PangoLanguage *language;
  PangoFontset *fontset;
  PangoFont *font = NULL;

  if (context)
    language = pango_context_get_language (context);
  else
    language = NULL;

  fontset = pango_font_map_load_fontset (fontmap, context, description, language);

  if (fontset)
    {
      pango_fontset_foreach (fontset, get_first_font, &font);

      if (font)
	g_object_ref (font);

      g_object_unref (fontset);
    }

  return font;
}

static void
pango_fc_fontset_cache (PangoFcFontset *fontset,
			PangoFcFontMap *fcfontmap)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  GQueue *cache = priv->fontset_cache;

  if (fontset->cache_link)
    {
      if (fontset->cache_link == cache->head)
        return;

      /* Already in cache, move to head
       */
      if (fontset->cache_link == cache->tail)
	cache->tail = fontset->cache_link->prev;

      cache->head = g_list_remove_link (cache->head, fontset->cache_link);
      cache->length--;
    }
  else
    {
      /* Add to cache initially
       */
      if (cache->length == FONTSET_CACHE_SIZE)
	{
	  PangoFcFontset *tmp_fontset = g_queue_pop_tail (cache);
	  tmp_fontset->cache_link = NULL;
	  g_hash_table_remove (priv->fontset_hash, tmp_fontset->key);
	}

      fontset->cache_link = g_list_prepend (NULL, fontset);
    }

  g_queue_push_head_link (cache, fontset->cache_link);
}

static PangoFontset *
pango_fc_font_map_load_fontset (PangoFontMap                 *fontmap,
				PangoContext                 *context,
				const PangoFontDescription   *desc,
				PangoLanguage                *language)
{
  PangoFcFontMap *fcfontmap = (PangoFcFontMap *)fontmap;
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  PangoFcFontset *fontset;
  PangoFcFontsetKey key;

  pango_fc_fontset_key_init (&key, fcfontmap, context, desc, language);

  fontset = g_hash_table_lookup (priv->fontset_hash, &key);

  if (G_UNLIKELY (!fontset))
    {
      PangoFcPatterns *patterns = pango_fc_font_map_get_patterns (fontmap, &key);

      if (!patterns)
	return NULL;

      fontset = pango_fc_fontset_new (&key, patterns);
      g_hash_table_insert (priv->fontset_hash, pango_fc_fontset_get_key (fontset), fontset);

      pango_fc_patterns_unref (patterns);
    }

  pango_fc_fontset_cache (fontset, fcfontmap);

  pango_font_description_free (key.desc);
  g_free (key.variations);

  return g_object_ref (PANGO_FONTSET (fontset));
}

/**
 * pango_fc_font_map_cache_clear:
 * @fcfontmap: a `PangoFcFontMap`
 *
 * Clear all cached information and fontsets for this font map.
 *
 * This should be called whenever there is a change in the
 * output of the default_substitute() virtual function of the
 * font map, or if fontconfig has been reinitialized to new
 * configuration.
 *
 * Since: 1.4
 */
void
pango_fc_font_map_cache_clear (PangoFcFontMap *fcfontmap)
{
  guint removed, added;

  if (G_UNLIKELY (fcfontmap->priv->closed))
    return;

  removed = fcfontmap->priv->n_families;

  pango_fc_font_map_fini (fcfontmap);
  pango_fc_font_map_init (fcfontmap);

  ensure_families (fcfontmap);

  added = fcfontmap->priv->n_families;

  g_list_model_items_changed (G_LIST_MODEL (fcfontmap), 0, removed, added);
  if (removed != added)
    g_object_notify (G_OBJECT (fcfontmap), "n-items");

  pango_font_map_changed (PANGO_FONT_MAP (fcfontmap));
}

static void
pango_fc_font_map_changed (PangoFontMap *fontmap)
{
  /* we emit GListModel::changed in pango_fc_font_map_cache_clear() */
}

/**
 * pango_fc_font_map_config_changed:
 * @fcfontmap: a `PangoFcFontMap`
 *
 * Informs font map that the fontconfig configuration (i.e., FcConfig
 * object) used by this font map has changed.
 *
 * This currently calls [method@PangoFc.FontMap.cache_clear] which
 * ensures that list of fonts, etc will be regenerated using the
 * updated configuration.
 *
 * Since: 1.38
 */
void
pango_fc_font_map_config_changed (PangoFcFontMap *fcfontmap)
{
  pango_fc_font_map_cache_clear (fcfontmap);
}

/**
 * pango_fc_font_map_set_config: (skip)
 * @fcfontmap: a `PangoFcFontMap`
 * @fcconfig: (nullable): a `FcConfig`
 *
 * Set the `FcConfig` for this font map to use.
 *
 * The default value
 * is %NULL, which causes Fontconfig to use its global "current config".
 * You can create a new `FcConfig` object and use this API to attach it
 * to a font map.
 *
 * This is particularly useful for example, if you want to use application
 * fonts with Pango. For that, you would create a fresh `FcConfig`, add your
 * app fonts to it, and attach it to a new Pango font map.
 *
 * If @fcconfig is different from the previous config attached to the font map,
 * [method@PangoFc.FontMap.config_changed] is called.
 *
 * This function acquires a reference to the `FcConfig` object; the caller
 * does **not** need to retain a reference.
 *
 * Since: 1.38
 */
void
pango_fc_font_map_set_config (PangoFcFontMap *fcfontmap,
                              FcConfig       *fcconfig)
{
  FcConfig *oldconfig;

  g_return_if_fail (PANGO_IS_FC_FONT_MAP (fcfontmap));

  oldconfig = fcfontmap->priv->config;

  if (fcconfig)
    FcConfigReference (fcconfig);

  fcfontmap->priv->config = fcconfig;

  g_clear_pointer (&fcfontmap->priv->fonts, FcFontSetDestroy);

  if (oldconfig != fcconfig)
    pango_fc_font_map_config_changed (fcfontmap);

  if (oldconfig)
    FcConfigDestroy (oldconfig);
}

/**
 * pango_fc_font_map_get_config: (skip)
 * @fcfontmap: a `PangoFcFontMap`
 *
 * Fetches the `FcConfig` attached to a font map.
 *
 * See also: [method@PangoFc.FontMap.set_config].
 *
 * Returns: (nullable): the `FcConfig` object attached to
 *   @fcfontmap, which might be %NULL. The return value is
 *   owned by Pango and should not be freed.
 *
 * Since: 1.38
 */
FcConfig *
pango_fc_font_map_get_config (PangoFcFontMap *fcfontmap)
{
  g_return_val_if_fail (PANGO_IS_FC_FONT_MAP (fcfontmap), NULL);

  wait_for_fc_init ();

  return fcfontmap->priv->config;
}

static FcFontSet *
pango_fc_font_map_get_config_fonts (PangoFcFontMap *fcfontmap)
{
  if (fcfontmap->priv->fonts == NULL)
    {
      FcFontSet *sets[2];

      wait_for_fc_init ();

      sets[0] = FcConfigGetFonts (fcfontmap->priv->config, FcSetApplication);
      sets[1] = FcConfigGetFonts (fcfontmap->priv->config, FcSetSystem);
      fcfontmap->priv->fonts = filter_by_format (sets, 2);
    }

  return fcfontmap->priv->fonts;
}

static PangoFcFontFaceData *
pango_fc_font_map_get_font_face_data (PangoFcFontMap *fcfontmap,
				      FcPattern      *font_pattern)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  PangoFcFontFaceData key;
  PangoFcFontFaceData *data;

  if (FcPatternGetString (font_pattern, FC_FILE, 0, (FcChar8 **)(void*)&key.filename) != FcResultMatch)
    return NULL;

  if (FcPatternGetInteger (font_pattern, FC_INDEX, 0, &key.id) != FcResultMatch)
    return NULL;

  data = g_hash_table_lookup (priv->font_face_data_hash, &key);
  if (G_LIKELY (data))
    return data;

  data = g_slice_new0 (PangoFcFontFaceData);
  data->filename = key.filename;
  data->id = key.id;

  data->pattern = font_pattern;
  FcPatternReference (data->pattern);

  g_hash_table_insert (priv->font_face_data_hash, data, data);

  return data;
}

typedef struct {
  PangoCoverage parent_instance;

  FcCharSet *charset;
} PangoFcCoverage;

typedef struct {
  PangoCoverageClass parent_class;
} PangoFcCoverageClass;

GType pango_fc_coverage_get_type (void) G_GNUC_CONST;

G_DEFINE_TYPE (PangoFcCoverage, pango_fc_coverage, PANGO_TYPE_COVERAGE)

static void
pango_fc_coverage_init (PangoFcCoverage *coverage)
{
}

static PangoCoverageLevel
pango_fc_coverage_real_get (PangoCoverage *coverage,
                            int            index)
{
  PangoFcCoverage *fc_coverage = (PangoFcCoverage*)coverage;

  return FcCharSetHasChar (fc_coverage->charset, index)
         ? PANGO_COVERAGE_EXACT
         : PANGO_COVERAGE_NONE;
}

static void
pango_fc_coverage_real_set (PangoCoverage *coverage,
                            int            index,
                            PangoCoverageLevel level)
{
  PangoFcCoverage *fc_coverage = (PangoFcCoverage*)coverage;

  if (level == PANGO_COVERAGE_NONE)
    FcCharSetDelChar (fc_coverage->charset, index);
  else
    FcCharSetAddChar (fc_coverage->charset, index);
}

static PangoCoverage *
pango_fc_coverage_real_copy (PangoCoverage *coverage)
{
  PangoFcCoverage *fc_coverage = (PangoFcCoverage*)coverage;
  PangoFcCoverage *copy;

  copy = g_object_new (pango_fc_coverage_get_type (), NULL);
  copy->charset = FcCharSetCopy (fc_coverage->charset);

  return (PangoCoverage *)copy;
}

static void
pango_fc_coverage_finalize (GObject *object)
{
  PangoFcCoverage *fc_coverage = (PangoFcCoverage*)object;

  FcCharSetDestroy (fc_coverage->charset);

  G_OBJECT_CLASS (pango_fc_coverage_parent_class)->finalize (object);
}

static void
pango_fc_coverage_class_init (PangoFcCoverageClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoCoverageClass *coverage_class = PANGO_COVERAGE_CLASS (class);

  object_class->finalize = pango_fc_coverage_finalize;

  coverage_class->get = pango_fc_coverage_real_get;
  coverage_class->set = pango_fc_coverage_real_set;
  coverage_class->copy = pango_fc_coverage_real_copy;
}

PangoCoverage *
_pango_fc_font_map_get_coverage (PangoFcFontMap *fcfontmap,
				 PangoFcFont    *fcfont)
{
  PangoFcFontFaceData *data;
  FcCharSet *charset;

  data = pango_fc_font_map_get_font_face_data (fcfontmap, fcfont->font_pattern);
  if (G_UNLIKELY (!data))
    return NULL;

  if (G_UNLIKELY (data->coverage == NULL))
    {
      /*
       * Pull the coverage out of the pattern, this
       * doesn't require loading the font
       */
      if (FcPatternGetCharSet (fcfont->font_pattern, FC_CHARSET, 0, &charset) != FcResultMatch)
        return pango_coverage_new ();

      data->coverage = _pango_fc_font_map_fc_to_coverage (charset);
    }

  return g_object_ref (data->coverage);
}

/**
 * _pango_fc_font_map_fc_to_coverage:
 * @charset: `FcCharSet` to convert to a `PangoCoverage` object.
 *
 * Convert the given `FcCharSet` into a new `PangoCoverage` object.
 *
 * The caller is responsible for freeing the newly created object.
 *
 * Since: 1.6
 */
PangoCoverage  *
_pango_fc_font_map_fc_to_coverage (FcCharSet *charset)
{
  PangoFcCoverage *coverage;

  coverage = g_object_new (pango_fc_coverage_get_type (), NULL);
  coverage->charset = FcCharSetCopy (charset);

  return (PangoCoverage *)coverage;
}

static PangoLanguage **
_pango_fc_font_map_fc_to_languages (FcLangSet *langset)
{
  FcStrSet *strset;
  FcStrList *list;
  FcChar8 *s;
  GPtrArray *langs;

  langs = g_ptr_array_new ();

  strset = FcLangSetGetLangs (langset);
  list = FcStrListCreate (strset);

  FcStrListFirst (list);
  while ((s = FcStrListNext (list)))
    {
      PangoLanguage *l = pango_language_from_string ((const char *)s);
      g_ptr_array_add (langs, l);
    }

  FcStrListDone (list);
  FcStrSetDestroy (strset);

  g_ptr_array_add (langs, NULL);

  return (PangoLanguage **) g_ptr_array_free (langs, FALSE);
}

PangoLanguage **
_pango_fc_font_map_get_languages (PangoFcFontMap *fcfontmap,
                                  PangoFcFont    *fcfont)
{
  PangoFcFontFaceData *data;
  FcLangSet *langset;

  data = pango_fc_font_map_get_font_face_data (fcfontmap, fcfont->font_pattern);
  if (G_UNLIKELY (!data))
    return NULL;

  if (G_UNLIKELY (data->languages == NULL))
    {
      /*
       * Pull the languages out of the pattern, this
       * doesn't require loading the font
       */
      if (FcPatternGetLangSet (fcfont->font_pattern, FC_LANG, 0, &langset) != FcResultMatch)
        return NULL;

      data->languages = _pango_fc_font_map_fc_to_languages (langset);
    }

  return data->languages;
}

/**
 * pango_fc_font_map_create_context:
 * @fcfontmap: a `PangoFcFontMap`
 *
 * Creates a new context for this fontmap.
 *
 * This function is intended only for backend implementations deriving
 * from `PangoFcFontMap`; it is possible that a backend will store
 * additional information needed for correct operation on the `PangoContext`
 * after calling this function.
 *
 * Return value: (transfer full): a new `PangoContext`
 *
 * Since: 1.4
 *
 * Deprecated: 1.22: Use pango_font_map_create_context() instead.
 */
PangoContext *
pango_fc_font_map_create_context (PangoFcFontMap *fcfontmap)
{
  g_return_val_if_fail (PANGO_IS_FC_FONT_MAP (fcfontmap), NULL);

  return pango_font_map_create_context (PANGO_FONT_MAP (fcfontmap));
}

static void
shutdown_font (gpointer        key,
	       PangoFcFont    *fcfont,
	       PangoFcFontMap *fcfontmap)
{
  _pango_fc_font_shutdown (fcfont);

  _pango_fc_font_set_font_key (fcfont, NULL);
  pango_fc_font_key_free (key);
}

/**
 * pango_fc_font_map_shutdown:
 * @fcfontmap: a `PangoFcFontMap`
 *
 * Clears all cached information for the fontmap and marks
 * all fonts open for the fontmap as dead.
 *
 * See the shutdown() virtual function of `PangoFcFont`.
 *
 * This function might be used by a backend when the underlying
 * windowing system for the font map exits. This function is only
 * intended to be called only for backend implementations deriving
 * from `PangoFcFontMap`.
 *
 * Since: 1.4
 */
void
pango_fc_font_map_shutdown (PangoFcFontMap *fcfontmap)
{
  PangoFcFontMapPrivate *priv = fcfontmap->priv;
  int i;

  if (priv->closed)
    return;

  g_hash_table_foreach (priv->font_hash, (GHFunc) shutdown_font, fcfontmap);
  for (i = 0; i < priv->n_families; i++)
    priv->families[i]->fontmap = NULL;

  pango_fc_font_map_fini (fcfontmap);

  while (priv->findfuncs)
    {
      PangoFcFindFuncInfo *info;
      info = priv->findfuncs->data;
      if (info->dnotify)
	info->dnotify (info->user_data);

      g_slice_free (PangoFcFindFuncInfo, info);
      priv->findfuncs = g_slist_delete_link (priv->findfuncs, priv->findfuncs);
    }

  priv->closed = TRUE;
}

static PangoWeight
pango_fc_convert_weight_to_pango (double fc_weight)
{
  return FcWeightToOpenTypeDouble (fc_weight);
}

static PangoStyle
pango_fc_convert_slant_to_pango (int fc_style)
{
  switch (fc_style)
    {
    case FC_SLANT_ROMAN:
      return PANGO_STYLE_NORMAL;
    case FC_SLANT_ITALIC:
      return PANGO_STYLE_ITALIC;
    case FC_SLANT_OBLIQUE:
      return PANGO_STYLE_OBLIQUE;
    default:
      return PANGO_STYLE_NORMAL;
    }
}

static PangoStretch
pango_fc_convert_width_to_pango (int fc_stretch)
{
  switch (fc_stretch)
    {
    case FC_WIDTH_NORMAL:
      return PANGO_STRETCH_NORMAL;
    case FC_WIDTH_ULTRACONDENSED:
      return PANGO_STRETCH_ULTRA_CONDENSED;
    case FC_WIDTH_EXTRACONDENSED:
      return PANGO_STRETCH_EXTRA_CONDENSED;
    case FC_WIDTH_CONDENSED:
      return PANGO_STRETCH_CONDENSED;
    case FC_WIDTH_SEMICONDENSED:
      return PANGO_STRETCH_SEMI_CONDENSED;
    case FC_WIDTH_SEMIEXPANDED:
      return PANGO_STRETCH_SEMI_EXPANDED;
    case FC_WIDTH_EXPANDED:
      return PANGO_STRETCH_EXPANDED;
    case FC_WIDTH_EXTRAEXPANDED:
      return PANGO_STRETCH_EXTRA_EXPANDED;
    case FC_WIDTH_ULTRAEXPANDED:
      return PANGO_STRETCH_ULTRA_EXPANDED;
    default:
      return PANGO_STRETCH_NORMAL;
    }
}

/**
 * pango_fc_font_description_from_pattern:
 * @pattern: a `FcPattern`
 * @include_size: if %TRUE, the pattern will include the size from
 *   the @pattern; otherwise the resulting pattern will be unsized.
 *   (only %FC_SIZE is examined, not %FC_PIXEL_SIZE)
 *
 * Creates a `PangoFontDescription` that matches the specified
 * Fontconfig pattern as closely as possible.
 *
 * Many possible Fontconfig pattern values, such as %FC_RASTERIZER
 * or %FC_DPI, don't make sense in the context of `PangoFontDescription`,
 * so will be ignored.
 *
 * Return value: a new `PangoFontDescription`. Free with
 *   pango_font_description_free().
 *
 * Since: 1.4
 */
PangoFontDescription *
pango_fc_font_description_from_pattern (FcPattern *pattern, gboolean include_size)
{
  return font_description_from_pattern (pattern, include_size, FALSE);
}

PangoFontDescription *
font_description_from_pattern (FcPattern *pattern,
                               gboolean   include_size,
                               gboolean   shallow)
{
  PangoFontDescription *desc;
  PangoStyle style;
  PangoWeight weight;
  PangoStretch stretch;
  double size;
  PangoGravity gravity;
  PangoVariant variant;
  gboolean all_caps;
  const char *s;
  int i;
  double d;
  FcResult res;

  desc = pango_font_description_new ();

  res = FcPatternGetString (pattern, FC_FAMILY, 0, (FcChar8 **) &s);
  g_assert (res == FcResultMatch);

  if (shallow)
    pango_font_description_set_family_static (desc, s);
  else
    pango_font_description_set_family (desc, s);

  if (FcPatternGetInteger (pattern, FC_SLANT, 0, &i) == FcResultMatch)
    style = pango_fc_convert_slant_to_pango (i);
  else
    style = PANGO_STYLE_NORMAL;

  pango_font_description_set_style (desc, style);

  if (FcPatternGetDouble (pattern, FC_WEIGHT, 0, &d) == FcResultMatch)
    weight = pango_fc_convert_weight_to_pango (d);
  else
    weight = PANGO_WEIGHT_NORMAL;

  pango_font_description_set_weight (desc, weight);

  if (FcPatternGetInteger (pattern, FC_WIDTH, 0, &i) == FcResultMatch)
    stretch = pango_fc_convert_width_to_pango (i);
  else
    stretch = PANGO_STRETCH_NORMAL;

  pango_font_description_set_stretch (desc, stretch);

  variant = PANGO_VARIANT_NORMAL;
  all_caps = FALSE;

  for (int i = 0; i < 32; i++)
    {
      if (FcPatternGetString (pattern, FC_FONT_FEATURES, i, (FcChar8 **)&s) == FcResultMatch)
        {
          if (strcmp (s, "smcp=1") == 0)
            {
              if (all_caps)
                variant = PANGO_VARIANT_ALL_SMALL_CAPS;
              else
                variant = PANGO_VARIANT_SMALL_CAPS;
            }
          else if (strcmp (s, "c2sc=1") == 0)
            {
              if (variant == PANGO_VARIANT_SMALL_CAPS)
                variant = PANGO_VARIANT_ALL_SMALL_CAPS;
              else
                all_caps = TRUE;
            }
          else if (strcmp (s, "pcap=1") == 0)
            {
              if (all_caps)
                variant = PANGO_VARIANT_ALL_PETITE_CAPS;
              else
                variant = PANGO_VARIANT_PETITE_CAPS;
            }
          else if (strcmp (s, "c2pc=1") == 0)
            {
              if (variant == PANGO_VARIANT_PETITE_CAPS)
                variant = PANGO_VARIANT_ALL_PETITE_CAPS;
              else
                all_caps = TRUE;
            }
          else if (strcmp (s, "unic=1") == 0)
            {
              variant = PANGO_VARIANT_UNICASE;
            }
          else if (strcmp (s, "titl=1") == 0)
            {
              variant = PANGO_VARIANT_TITLE_CAPS;
            }
        }
      else
        break;
    }

  pango_font_description_set_variant (desc, variant);

  if (include_size && FcPatternGetDouble (pattern, FC_SIZE, 0, &size) == FcResultMatch)
    {
      FcMatrix *fc_matrix;
      double scale_factor = 1;
      volatile double scaled_size;

      if (FcPatternGetMatrix (pattern, FC_MATRIX, 0, &fc_matrix) == FcResultMatch)
        {
          PangoMatrix mat = PANGO_MATRIX_INIT;

          mat.xx = fc_matrix->xx;
          mat.xy = fc_matrix->xy;
          mat.yx = fc_matrix->yx;
          mat.yy = fc_matrix->yy;

          scale_factor = pango_matrix_get_font_scale_factor (&mat);
        }

      /* We need to use a local variable to ensure that the compiler won't
       * implicitly cast it to integer while the result is kept in registers,
       * leading to a wrong approximation in i386 (with 387 FPU)
       */
      scaled_size = scale_factor * size * PANGO_SCALE;
      pango_font_description_set_size (desc, scaled_size);
    }

  /* gravity is a bit different.  we don't want to set it if it was not set on
   * the pattern */
  if (FcPatternGetString (pattern, PANGO_FC_GRAVITY, 0, (FcChar8 **)&s) == FcResultMatch)
    {
      GEnumValue *value = g_enum_get_value_by_nick (get_gravity_class (), (char *)s);
      gravity = value->value;

      pango_font_description_set_gravity (desc, gravity);
    }

  if (include_size && FcPatternGetString (pattern, FC_FONT_VARIATIONS, 0, (FcChar8 **)&s) == FcResultMatch)
    {
      if (s && *s)
        {
          if (shallow)
            pango_font_description_set_variations_static (desc, s);
          else
            pango_font_description_set_variations (desc, s);
        }
    }

  return desc;
}

/*
 * PangoFcFace
 */

typedef PangoFontFaceClass PangoFcFaceClass;

G_DEFINE_TYPE (PangoFcFace, pango_fc_face, PANGO_TYPE_FONT_FACE)

static PangoFontDescription *
make_alias_description (PangoFcFamily *fcfamily,
			gboolean        bold,
			gboolean        italic)
{
  PangoFontDescription *desc = pango_font_description_new ();

  pango_font_description_set_family (desc, fcfamily->family_name);
  pango_font_description_set_style (desc, italic ? PANGO_STYLE_ITALIC : PANGO_STYLE_NORMAL);
  pango_font_description_set_weight (desc, bold ? PANGO_WEIGHT_BOLD : PANGO_WEIGHT_NORMAL);

  return desc;
}

static PangoFontDescription *
pango_fc_face_describe (PangoFontFace *face)
{
  PangoFcFace *fcface = PANGO_FC_FACE (face);
  PangoFcFamily *fcfamily = fcface->family;
  PangoFontDescription *desc = NULL;

  if (G_UNLIKELY (!fcfamily))
    return pango_font_description_new ();

  if (fcface->fake)
    {
      if (strcmp (fcface->style, "Regular") == 0)
	return make_alias_description (fcfamily, FALSE, FALSE);
      else if (strcmp (fcface->style, "Bold") == 0)
	return make_alias_description (fcfamily, TRUE, FALSE);
      else if (strcmp (fcface->style, "Italic") == 0)
	return make_alias_description (fcfamily, FALSE, TRUE);
      else			/* Bold Italic */
	return make_alias_description (fcfamily, TRUE, TRUE);
    }

  g_assert (fcface->pattern);
  desc = pango_fc_font_description_from_pattern (fcface->pattern, FALSE);

  return desc;
}

static const char *
pango_fc_face_get_face_name (PangoFontFace *face)
{
  PangoFcFace *fcface = PANGO_FC_FACE (face);

  return fcface->style;
}

static int
compare_ints (gconstpointer ap,
	      gconstpointer bp)
{
  int a = *(int *)ap;
  int b = *(int *)bp;

  if (a == b)
    return 0;
  else if (a > b)
    return 1;
  else
    return -1;
}

static void
pango_fc_face_list_sizes (PangoFontFace  *face,
			  int           **sizes,
			  int            *n_sizes)
{
  PangoFcFace *fcface = PANGO_FC_FACE (face);
  FcPattern *pattern;
  FcFontSet *fontset;
  FcObjectSet *objectset;
  FcFontSet *fonts;

  if (sizes)
    *sizes = NULL;
  *n_sizes = 0;
  if (G_UNLIKELY (!fcface->family || !fcface->family->fontmap))
    return;

  pattern = FcPatternCreate ();
  FcPatternAddString (pattern, FC_FAMILY, (FcChar8*)(void*)fcface->family->family_name);
  FcPatternAddString (pattern, FC_STYLE, (FcChar8*)(void*)fcface->style);

  objectset = FcObjectSetCreate ();
  FcObjectSetAdd (objectset, FC_PIXEL_SIZE);

  fonts = pango_fc_font_map_get_config_fonts (fcface->family->fontmap);
  fontset = FcFontSetList (fcface->family->fontmap->priv->config, &fonts, 1, pattern, objectset);

  if (fontset)
    {
      GArray *size_array;
      double size, dpi = -1.0;
      int i, size_i, j;

      size_array = g_array_new (FALSE, FALSE, sizeof (int));

      for (i = 0; i < fontset->nfont; i++)
	{
	  for (j = 0;
	       FcPatternGetDouble (fontset->fonts[i], FC_PIXEL_SIZE, j, &size) == FcResultMatch;
	       j++)
	    {
	      if (dpi < 0)
		dpi = pango_fc_font_map_get_resolution (fcface->family->fontmap, NULL);

	      size_i = (int) (PANGO_SCALE * size * 72.0 / dpi);
	      g_array_append_val (size_array, size_i);
	    }
	}

      g_array_sort (size_array, compare_ints);

      if (size_array->len == 0)
	{
	  *n_sizes = 0;
	  if (sizes)
	    *sizes = NULL;
	  g_array_free (size_array, TRUE);
	}
      else
	{
	  *n_sizes = size_array->len;
	  if (sizes)
	    {
	      *sizes = (int *) size_array->data;
	      g_array_free (size_array, FALSE);
	    }
	  else
	    g_array_free (size_array, TRUE);
	}

      FcFontSetDestroy (fontset);
    }
  else
    {
      *n_sizes = 0;
      if (sizes)
	*sizes = NULL;
    }

  FcPatternDestroy (pattern);
  FcObjectSetDestroy (objectset);
}

static gboolean
pango_fc_face_is_synthesized (PangoFontFace *face)
{
  PangoFcFace *fcface = PANGO_FC_FACE (face);

  return fcface->fake;
}

static PangoFontFamily *
pango_fc_face_get_family (PangoFontFace *face)
{
  PangoFcFace *fcface = PANGO_FC_FACE (face);

  return PANGO_FONT_FAMILY (fcface->family);
}

static void
pango_fc_face_finalize (GObject *object)
{
  PangoFcFace *fcface = PANGO_FC_FACE (object);

  g_free (fcface->style);
  FcPatternDestroy (fcface->pattern);

  G_OBJECT_CLASS (pango_fc_face_parent_class)->finalize (object);
}

static void
pango_fc_face_init (PangoFcFace *self)
{
}

static void
pango_fc_face_class_init (PangoFcFaceClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);

  object_class->finalize = pango_fc_face_finalize;

  class->describe = pango_fc_face_describe;
  class->get_face_name = pango_fc_face_get_face_name;
  class->list_sizes = pango_fc_face_list_sizes;
  class->is_synthesized = pango_fc_face_is_synthesized;
  class->get_family = pango_fc_face_get_family;
}


/*
 * PangoFcFamily
 */

typedef PangoFontFamilyClass PangoFcFamilyClass;

static GType
pango_fc_family_get_item_type (GListModel *list)
{
  return PANGO_TYPE_FONT_FACE;
}

static void ensure_faces (PangoFcFamily *family);

static guint
pango_fc_family_get_n_items (GListModel *list)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (list);

  ensure_faces (fcfamily);

  return (guint)fcfamily->n_faces;
}

static gpointer
pango_fc_family_get_item (GListModel *list,
                          guint       position)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (list);

  ensure_faces (fcfamily);

  if (position < fcfamily->n_faces)
    return g_object_ref (fcfamily->faces[position]);

  return NULL;
}

static void
pango_fc_family_list_model_init (GListModelInterface *iface)
{
  iface->get_item_type = pango_fc_family_get_item_type;
  iface->get_n_items = pango_fc_family_get_n_items;
  iface->get_item = pango_fc_family_get_item;
}

G_DEFINE_TYPE_WITH_CODE (PangoFcFamily, pango_fc_family, PANGO_TYPE_FONT_FAMILY,
                         G_IMPLEMENT_INTERFACE (G_TYPE_LIST_MODEL, pango_fc_family_list_model_init))

static PangoFcFace *
create_face (PangoFcFamily *fcfamily,
	     const char    *style,
	     FcPattern     *pattern,
	     gboolean       fake)
{
  PangoFcFace *face = g_object_new (PANGO_FC_TYPE_FACE, NULL);
  face->style = g_strdup (style);
  if (pattern)
    FcPatternReference (pattern);
  face->pattern = pattern;
  face->family = fcfamily;
  face->fake = fake;

  return face;
}

static int
compare_face (const void *p1, const void *p2)
{
  const PangoFcFace *f1 = *(const void **)p1;
  const PangoFcFace *f2 = *(const void **)p2;
  int w1, w2;
  int s1, s2;

  if (FcPatternGetInteger (f1->pattern, FC_WEIGHT, 0, &w1) != FcResultMatch)
    w1 = FC_WEIGHT_MEDIUM;

  if (FcPatternGetInteger (f1->pattern, FC_SLANT, 0, &s1) != FcResultMatch)
    s1 = FC_SLANT_ROMAN;

  if (FcPatternGetInteger (f2->pattern, FC_WEIGHT, 0, &w2) != FcResultMatch)
    w2 = FC_WEIGHT_MEDIUM;

  if (FcPatternGetInteger (f2->pattern, FC_SLANT, 0, &s2) != FcResultMatch)
    s2 = FC_SLANT_ROMAN;

  if (s1 != s2)
    return s1 - s2; /* roman < italic < oblique */

  return w1 - w2; /* from light to heavy */
}

static void
ensure_faces (PangoFcFamily *fcfamily)
{
  PangoFcFontMap *fcfontmap = fcfamily->fontmap;
  PangoFcFontMapPrivate *priv = fcfontmap->priv;

  if (fcfamily->n_faces < 0)
    {
      FcFontSet *fontset;
      int i;

      if (is_alias_family (fcfamily->family_name) || priv->closed)
	{
	  fcfamily->n_faces = 4;
	  fcfamily->faces = g_new (PangoFcFace *, fcfamily->n_faces);

	  i = 0;
	  fcfamily->faces[i++] = create_face (fcfamily, "Regular", NULL, TRUE);
	  fcfamily->faces[i++] = create_face (fcfamily, "Bold", NULL, TRUE);
	  fcfamily->faces[i++] = create_face (fcfamily, "Italic", NULL, TRUE);
	  fcfamily->faces[i++] = create_face (fcfamily, "Bold Italic", NULL, TRUE);
          fcfamily->faces[0]->regular = 1;
	}
      else
	{
	  enum {
	    REGULAR,
	    ITALIC,
	    BOLD,
	    BOLD_ITALIC
	  };
	  /* Regular, Italic, Bold, Bold Italic */
	  gboolean has_face [4] = { FALSE, FALSE, FALSE, FALSE };
	  PangoFcFace **faces;
	  gint num = 0;
          int regular_weight;
          int regular_idx;

	  fontset = fcfamily->patterns;

	  /* at most we have 3 additional artificial faces */
	  faces = g_new (PangoFcFace *, fontset->nfont + 3);

          regular_weight = 0;
          regular_idx = -1;

	  for (i = 0; i < fontset->nfont; i++)
	    {
	      const char *style, *font_style = NULL;
	      int weight, slant;

	      if (FcPatternGetInteger(fontset->fonts[i], FC_WEIGHT, 0, &weight) != FcResultMatch)
		weight = FC_WEIGHT_MEDIUM;

	      if (FcPatternGetInteger(fontset->fonts[i], FC_SLANT, 0, &slant) != FcResultMatch)
		slant = FC_SLANT_ROMAN;

              {
                gboolean variable;
                if (FcPatternGetBool(fontset->fonts[i], FC_VARIABLE, 0, &variable) != FcResultMatch)
                  variable = FALSE;
                if (variable) /* skip the variable face */
                  continue;
              }

	      if (FcPatternGetString (fontset->fonts[i], FC_STYLE, 0, (FcChar8 **)(void*)&font_style) != FcResultMatch)
		font_style = NULL;

              if (font_style && strcmp (font_style, "Regular") == 0)
                {
                  regular_weight = FC_WEIGHT_MEDIUM;
                  regular_idx = num;
                }

	      if (weight <= FC_WEIGHT_MEDIUM)
		{
		  if (slant == FC_SLANT_ROMAN)
		    {
		      has_face[REGULAR] = TRUE;
		      style = "Regular";
                      if (weight > regular_weight)
                        {
                          regular_weight = weight;
                          regular_idx = num;
                        }
		    }
		  else
		    {
		      has_face[ITALIC] = TRUE;
		      style = "Italic";
		    }
		}
	      else
		{
		  if (slant == FC_SLANT_ROMAN)
		    {
		      has_face[BOLD] = TRUE;
		      style = "Bold";
		    }
		  else
		    {
		      has_face[BOLD_ITALIC] = TRUE;
		      style = "Bold Italic";
		    }
		}

	      if (!font_style)
		font_style = style;
	      faces[num++] = create_face (fcfamily, font_style, fontset->fonts[i], FALSE);
	    }

	  if (has_face[REGULAR])
	    {
	      if (!has_face[ITALIC])
		faces[num++] = create_face (fcfamily, "Italic", NULL, TRUE);
	      if (!has_face[BOLD])
		faces[num++] = create_face (fcfamily, "Bold", NULL, TRUE);

	    }
	  if ((has_face[REGULAR] || has_face[ITALIC] || has_face[BOLD]) && !has_face[BOLD_ITALIC])
	    faces[num++] = create_face (fcfamily, "Bold Italic", NULL, TRUE);

          if (regular_idx != -1)
            faces[regular_idx]->regular = 1;

	  faces = g_renew (PangoFcFace *, faces, num);

          qsort (faces, num, sizeof (PangoFcFace *), compare_face);

	  fcfamily->n_faces = num;
	  fcfamily->faces = faces;
	}
    }
}

static void
pango_fc_family_list_faces (PangoFontFamily  *family,
			    PangoFontFace  ***faces,
			    int              *n_faces)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (family);

  if (faces)
    *faces = NULL;

  if (n_faces)
    *n_faces = 0;

  if (G_UNLIKELY (!fcfamily->fontmap))
    return;

  ensure_faces (fcfamily);

  if (n_faces)
    *n_faces = fcfamily->n_faces;

  if (faces)
    *faces = g_memdup2 (fcfamily->faces, fcfamily->n_faces * sizeof (PangoFontFace *));
}

static PangoFontFace *
pango_fc_family_get_face (PangoFontFamily *family,
                          const char      *name)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (family);
  int i;

  ensure_faces (fcfamily);

  for (i = 0; i < fcfamily->n_faces; i++)
    {
      PangoFontFace *face = PANGO_FONT_FACE (fcfamily->faces[i]);

      if ((name != NULL && strcmp (name, pango_font_face_get_face_name (face)) == 0) ||
          (name == NULL && PANGO_FC_FACE (face)->regular))
        return face;
    }

  return NULL;
}

static const char *
pango_fc_family_get_name (PangoFontFamily  *family)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (family);

  return fcfamily->family_name;
}

static gboolean
pango_fc_family_is_monospace (PangoFontFamily *family)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (family);

  return fcfamily->spacing == FC_MONO ||
	 fcfamily->spacing == FC_DUAL ||
	 fcfamily->spacing == FC_CHARCELL;
}

static gboolean
pango_fc_family_is_variable (PangoFontFamily *family)
{
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (family);

  return fcfamily->variable;
}

static void
pango_fc_family_finalize (GObject *object)
{
  int i;
  PangoFcFamily *fcfamily = PANGO_FC_FAMILY (object);

  g_free (fcfamily->family_name);

  for (i = 0; i < fcfamily->n_faces; i++)
    {
      fcfamily->faces[i]->family = NULL;
      g_object_unref (fcfamily->faces[i]);
    }
  FcFontSetDestroy (fcfamily->patterns);
  g_free (fcfamily->faces);

  G_OBJECT_CLASS (pango_fc_family_parent_class)->finalize (object);
}

static void
pango_fc_family_class_init (PangoFcFamilyClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);

  object_class->finalize = pango_fc_family_finalize;

  class->list_faces = pango_fc_family_list_faces;
  class->get_face = pango_fc_family_get_face;
  class->get_name = pango_fc_family_get_name;
  class->is_monospace = pango_fc_family_is_monospace;
  class->is_variable = pango_fc_family_is_variable;
}

static void
pango_fc_family_init (PangoFcFamily *fcfamily)
{
  fcfamily->n_faces = -1;
}

/**
 * pango_fc_font_map_get_hb_face: (skip)
 * @fcfontmap: a `PangoFcFontMap`
 * @fcfont: a `PangoFcFont`
 *
 * Retrieves the `hb_face_t` for the given `PangoFcFont`.
 *
 * Returns: (transfer none) (nullable): the `hb_face_t`
 *   for the given font
 *
 * Since: 1.44
 */
hb_face_t *
pango_fc_font_map_get_hb_face (PangoFcFontMap *fcfontmap,
                               PangoFcFont    *fcfont)
{
  PangoFcFontFaceData *data;

  data = pango_fc_font_map_get_font_face_data (fcfontmap, fcfont->font_pattern);

  if (!data->hb_face)
    {
      hb_blob_t *blob;

      blob = hb_blob_create_from_file (data->filename);
      data->hb_face = hb_face_create (blob, data->id);
      hb_blob_destroy (blob);
    }

  return data->hb_face;
}

static gboolean
pango_fc_font_map_add_font_file (PangoFontMap  *fontmap,
                                 const char    *filename,
                                 GError       **error)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (fontmap);
  FcConfig *config;

  if (fcfontmap->priv->config)
    config = fcfontmap->priv->config;
  else
    config = FcConfigGetCurrent ();

  if (!FcConfigAppFontAddFile (config, (FcChar8 *) filename))
    {
      g_set_error (error, G_FILE_ERROR, G_FILE_ERROR_FAILED,
                   "Adding font %s to fontconfig configuration failed",
                   filename);
      return FALSE;
    }

  if (config != fcfontmap->priv->config)
    pango_fc_font_map_set_config (fcfontmap, config);
  else
    {
      g_clear_pointer (&fcfontmap->priv->fonts, FcFontSetDestroy);
      pango_fc_font_map_config_changed (fcfontmap);
    }

  return TRUE;
}
