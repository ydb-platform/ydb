/*
 * Copyright © 2021 Benjamin Otte
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 *
 * Authors: Benjamin Otte <otte@gnome.org>
 */


#include "config.h"

#include "gtkjsonprinterprivate.h"

typedef struct _GtkJsonBlock GtkJsonBlock;

typedef enum {
  GTK_JSON_BLOCK_TOPLEVEL,
  GTK_JSON_BLOCK_OBJECT,
  GTK_JSON_BLOCK_ARRAY,
} GtkJsonBlockType;

struct _GtkJsonBlock
{
  GtkJsonBlockType type;
  gsize n_elements; /* number of elements already written */
};

struct _GtkJsonPrinter
{
  GtkJsonPrinterFlags flags;
  char *indentation;

  GtkJsonPrinterWriteFunc write_func;
  gpointer user_data;
  GDestroyNotify user_destroy;

  GtkJsonBlock *block; /* current block */
  GtkJsonBlock *blocks; /* blocks array */
  GtkJsonBlock *blocks_end; /* blocks array */
  GtkJsonBlock blocks_preallocated[128]; /* preallocated */
};

static void
gtk_json_printer_push_block (GtkJsonPrinter   *self,
                             GtkJsonBlockType  type)
{
  self->block++;
  if (self->block == self->blocks_end)
    {
      gsize old_size = self->blocks_end - self->blocks;
      gsize new_size = old_size + 128;

      if (self->blocks == self->blocks_preallocated)
        {
          self->blocks = g_new (GtkJsonBlock, new_size);
          memcpy (self->blocks, self->blocks_preallocated, sizeof (GtkJsonBlock) * G_N_ELEMENTS (self->blocks_preallocated));
        }
      else
        {
          self->blocks = g_renew (GtkJsonBlock, self->blocks, new_size);
        }
      self->blocks_end = self->blocks + new_size;
      self->block = self->blocks + old_size;
    }

  self->block->type = type;
  self->block->n_elements = 0;
}
                     
static void
gtk_json_printer_pop_block (GtkJsonPrinter *self)
{
  g_assert (self->block > self->blocks);
  self->block--;
}

GtkJsonPrinter *
gtk_json_printer_new (GtkJsonPrinterWriteFunc write_func,
                      gpointer                data,
                      GDestroyNotify          destroy)
{
  GtkJsonPrinter *self;

  g_return_val_if_fail (write_func, NULL);

  self = g_slice_new0 (GtkJsonPrinter);

  self->flags = 0;
  self->indentation = g_strdup ("  ");

  self->write_func = write_func;
  self->user_data = data;
  self->user_destroy = destroy;

  self->blocks = self->blocks_preallocated;
  self->blocks_end = self->blocks + G_N_ELEMENTS (self->blocks_preallocated);
  self->block = self->blocks;
  self->block->type = GTK_JSON_BLOCK_TOPLEVEL;

  return self;
}

void
gtk_json_printer_free (GtkJsonPrinter *self)
{
  g_return_if_fail (self != NULL);

  g_free (self->indentation);

  if (self->user_destroy)
    self->user_destroy (self->user_data);

  if (self->blocks != self->blocks_preallocated)
    g_free (self->blocks);

  g_slice_free (GtkJsonPrinter, self);
}

static gboolean
gtk_json_printer_has_flag (GtkJsonPrinter      *self,
                           GtkJsonPrinterFlags  flag)
{
  return (self->flags & flag) ? TRUE : FALSE;
}
                           
gsize
gtk_json_printer_get_depth (GtkJsonPrinter *self)
{
  return self->block - self->blocks;
}

gsize
gtk_json_printer_get_n_elements (GtkJsonPrinter *self)
{
  return self->block->n_elements;
}

void
gtk_json_printer_set_flags (GtkJsonPrinter      *self,
                            GtkJsonPrinterFlags  flags)
{
  g_return_if_fail (self != NULL);

  self->flags = flags;
}

GtkJsonPrinterFlags
gtk_json_printer_get_flags (GtkJsonPrinter *self)
{
  g_return_val_if_fail (self != NULL, 0);

  return self->flags;
}

void
gtk_json_printer_set_indentation (GtkJsonPrinter *self,
                                  gsize           amount)
{
  g_return_if_fail (self != NULL);

  g_free (self->indentation);

  self->indentation = g_malloc (amount + 1);
  memset (self->indentation, ' ', amount);
  self->indentation[amount] = 0;
}

gsize
gtk_json_printer_get_indentation (GtkJsonPrinter *self)
{
  g_return_val_if_fail (self != NULL, 2);

  return strlen (self->indentation);
}

static void
gtk_json_printer_write (GtkJsonPrinter *self,
                        const char     *s)
{
  self->write_func (self, s, self->user_data);
}

static char *
gtk_json_printer_escape_string (GtkJsonPrinter *self,
                                const char     *str)
{
  GString *string;

  string = g_string_new (NULL);
  string = g_string_append_c (string, '"');

  for (; *str != '\0'; str = g_utf8_next_char (str))
    {
      switch (*str)
        {
          case '"':
            g_string_append (string, "\\\"");
            break;
          case '\\':
            g_string_append (string, "\\\\");
            break;
          case '\b':
            g_string_append (string, "\\b");
            break;
          case '\f':
            g_string_append (string, "\\f");
            break;
          case '\n':
            g_string_append (string, "\\n");
            break;
          case '\r':
            g_string_append (string, "\\r");
            break;
          case '\t':
            g_string_append (string, "\\t");
            break;
          default:
            if ((int) *str < 0x20 || (int) *str >= 0x80)
              {
                if ((guint) *str < 0x20 || gtk_json_printer_has_flag (self, GTK_JSON_PRINTER_ASCII))
                  g_string_append_printf (string, "\\u%04x", g_utf8_get_char (str));
                else
                  g_string_append_unichar (string, g_utf8_get_char (str));
              }
            else
              g_string_append_c (string, *str);
        }
    }

  string = g_string_append_c (string, '"');
  return g_string_free (string, FALSE);
}

static void
gtk_json_printer_newline (GtkJsonPrinter *self)
{
  gsize depth;

  if (!gtk_json_printer_has_flag (self, GTK_JSON_PRINTER_PRETTY))
    return;

  gtk_json_printer_write (self, "\n");
  for (depth = gtk_json_printer_get_depth (self); depth-->0;)
    gtk_json_printer_write (self, self->indentation);
}

static void
gtk_json_printer_begin_member (GtkJsonPrinter *self,
                               const char     *name)
{
  if (gtk_json_printer_get_n_elements (self) > 0)
    gtk_json_printer_write (self, ",");
  if (self->block->type != GTK_JSON_BLOCK_TOPLEVEL || gtk_json_printer_get_n_elements (self) > 0)
    gtk_json_printer_newline (self);

  self->block->n_elements++;

  if (name)
    {
      char *escaped = gtk_json_printer_escape_string (self, name);
      gtk_json_printer_write (self, escaped);
      g_free (escaped);
      if (gtk_json_printer_has_flag (self, GTK_JSON_PRINTER_PRETTY))
        gtk_json_printer_write (self, " : ");
      else
        gtk_json_printer_write (self, ":");
    }
}

void
gtk_json_printer_add_boolean (GtkJsonPrinter *self,
                              const char     *name,
                              gboolean        value)
{
  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  gtk_json_printer_write (self, value ? "true" : "false");
}

void
gtk_json_printer_add_number (GtkJsonPrinter *self,
                             const char     *name,
                             double          value)
{
  char buf[G_ASCII_DTOSTR_BUF_SIZE];

  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  g_ascii_dtostr (buf, G_ASCII_DTOSTR_BUF_SIZE, value);
  gtk_json_printer_write (self, buf);
}

void
gtk_json_printer_add_integer (GtkJsonPrinter *self,
                              const char     *name,
                              int             value)
{
  char buf[128];

  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  g_snprintf (buf, sizeof (buf), "%d", value);
  gtk_json_printer_write (self, buf);
}

void
gtk_json_printer_add_string (GtkJsonPrinter *self,
                             const char     *name,
                             const char     *s)
{
  char *escaped;

  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));
  g_return_if_fail (s != NULL);

  gtk_json_printer_begin_member (self, name);
  escaped = gtk_json_printer_escape_string (self, s);
  gtk_json_printer_write (self, escaped);
  g_free (escaped);
}

void
gtk_json_printer_add_null (GtkJsonPrinter *self,
                           const char     *name)
{
  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  gtk_json_printer_write (self, "null");
}

void
gtk_json_printer_start_object (GtkJsonPrinter *self,
                               const char     *name)
{
  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  gtk_json_printer_write (self, "{");
  gtk_json_printer_push_block (self, GTK_JSON_BLOCK_OBJECT);
}

void
gtk_json_printer_start_array (GtkJsonPrinter *self,
                              const char     *name)
{
  g_return_if_fail (self != NULL);
  g_return_if_fail ((self->block->type == GTK_JSON_BLOCK_OBJECT) == (name != NULL));

  gtk_json_printer_begin_member (self, name);
  gtk_json_printer_write (self, "[");
  gtk_json_printer_push_block (self, GTK_JSON_BLOCK_ARRAY);
}

void
gtk_json_printer_end (GtkJsonPrinter *self)
{
  const char *bracket;
  gboolean empty;

  g_return_if_fail (self != NULL);

  switch (self->block->type)
    {
    case GTK_JSON_BLOCK_OBJECT:
      bracket = "}";
      break;
    case GTK_JSON_BLOCK_ARRAY:
      bracket = "]";
      break;
    case GTK_JSON_BLOCK_TOPLEVEL:
    default:
      g_return_if_reached ();
    }

  empty = gtk_json_printer_get_n_elements (self) == 0;
  gtk_json_printer_pop_block (self);

  if (!empty)
    {
      gtk_json_printer_newline (self);
    }
  gtk_json_printer_write (self, bracket);
}

