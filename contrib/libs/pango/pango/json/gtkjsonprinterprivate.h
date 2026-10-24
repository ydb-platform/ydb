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


#ifndef __GTK_JSON_PRINTER_H__
#define __GTK_JSON_PRINTER_H__

#include <glib.h>

G_BEGIN_DECLS

typedef struct _GtkJsonPrinter GtkJsonPrinter;

typedef enum {
  GTK_JSON_PRINTER_PRETTY = (1 << 0),
  GTK_JSON_PRINTER_ASCII  = (1 << 1),
} GtkJsonPrinterFlags;

typedef void            (* GtkJsonPrinterWriteFunc)             (GtkJsonPrinter         *printer,
                                                                 const char             *s,
                                                                 gpointer                user_data);
             

GtkJsonPrinter *        gtk_json_printer_new                    (GtkJsonPrinterWriteFunc write_func,
                                                                 gpointer                data,
                                                                 GDestroyNotify          destroy);
void                    gtk_json_printer_free                   (GtkJsonPrinter         *self);

void                    gtk_json_printer_set_flags              (GtkJsonPrinter         *self,
                                                                 GtkJsonPrinterFlags     flags);
GtkJsonPrinterFlags     gtk_json_printer_get_flags              (GtkJsonPrinter         *self);
void                    gtk_json_printer_set_indentation        (GtkJsonPrinter         *self,
                                                                 gsize                   amount);
gsize                   gtk_json_printer_get_indentation        (GtkJsonPrinter         *self);

gsize                   gtk_json_printer_get_depth              (GtkJsonPrinter         *self);
gsize                   gtk_json_printer_get_n_elements         (GtkJsonPrinter         *self);

void                    gtk_json_printer_add_boolean            (GtkJsonPrinter         *self,
                                                                 const char             *name,
                                                                 gboolean                value);
void                    gtk_json_printer_add_number             (GtkJsonPrinter         *self,
                                                                 const char             *name,
                                                                 double                  value);
void                    gtk_json_printer_add_integer            (GtkJsonPrinter         *self,
                                                                 const char             *name,
                                                                 int                     value);
void                    gtk_json_printer_add_string             (GtkJsonPrinter         *self,
                                                                 const char             *name,
                                                                 const char             *s);
void                    gtk_json_printer_add_null               (GtkJsonPrinter         *self,
                                                                 const char             *name);

void                    gtk_json_printer_start_object           (GtkJsonPrinter         *self,
                                                                 const char             *name);
void                    gtk_json_printer_start_array            (GtkJsonPrinter         *self,
                                                                 const char             *name);
void                    gtk_json_printer_end                    (GtkJsonPrinter         *self);


G_END_DECLS

#endif /* __GTK_JSON_PRINTER_H__ */
