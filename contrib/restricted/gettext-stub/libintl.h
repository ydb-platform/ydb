/*

gettext-stub - light stub to replace gettext
Made 2008 by Lars Stoltenow <penma@penma.de>
Idea by Maximilian Gass <mxey@cloudconnected.org>

This product is released under the terms of the WTFPL. Read
http://sam.zoy.org/wtfpl for more details.

*/

#ifndef _LIBINTL_H
#define _LIBINTL_H 1

#define __USE_GNU_GETTEXT 1
#define __GNU_GETTEXT_SUPPORTED_REVISION(major) 0

#define gettext(msgid) (msgid)
#define dgettext(domain,msgid) (msgid)
#define __dgettext dgettext

#define dcgettext(domain,msgid,cat) (msgid)
#define __dcgettext dcgettext

#define ngettext(msgid1,msgid2,n) ((n) == 1 ? (msgid1) : (msgid2))

#define dngettext(domain,msgid1,msgid2,n) (ngettext((msgid1),(msgid2),(n)))

#define dcngettext(domain,msgid1,msgid2,n,cat) (ngettext((msgid1),(msgid2),(n)))

#define textdomain(domain) ("C")
#define bindtextdomain(domain,dir) (NULL)
#define bind_textdomain_codeset(domain,code) (NULL)

#endif
