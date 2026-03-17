/* GLIB - Library of useful routines for C programming
 * Copyright (C) 2018-2019  Patrick Griffis, James Westman
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
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include <Foundation/Foundation.h>
#include "gutils.h"
#include "gstrfuncs.h"

void load_user_special_dirs_macos (gchar **table);

static gchar *
find_folder (NSSearchPathDirectory type)
{
  gchar *filename;
  NSString *path;
  NSArray *paths;

  paths = NSSearchPathForDirectoriesInDomains (type, NSUserDomainMask, YES);
  path = [paths firstObject];
  if (path == nil)
    {
      return NULL;
    }

  filename = g_strdup ([path UTF8String]);

  return filename;
}

void
load_user_special_dirs_macos(gchar **table)
{
  table[G_USER_DIRECTORY_DESKTOP] = find_folder (NSDesktopDirectory);
  table[G_USER_DIRECTORY_DOCUMENTS] = find_folder (NSDocumentDirectory);
  table[G_USER_DIRECTORY_DOWNLOAD] = find_folder (NSDownloadsDirectory);
  table[G_USER_DIRECTORY_MUSIC] = find_folder (NSMusicDirectory);
  table[G_USER_DIRECTORY_PICTURES] = find_folder (NSPicturesDirectory);
  table[G_USER_DIRECTORY_PUBLIC_SHARE] = find_folder (NSSharedPublicDirectory);
  table[G_USER_DIRECTORY_TEMPLATES] = NULL;
  table[G_USER_DIRECTORY_VIDEOS] = find_folder (NSMoviesDirectory);
}
