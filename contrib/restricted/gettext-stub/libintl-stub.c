/*

gettext-stub - light stub to replace gettext
Made 2008 by Lars Stoltenow <penma@penma.de>
Idea by Maximilian Gass <mxey@cloudconnected.org>

This product is released under the terms of the WTFPL. Read
http://sam.zoy.org/wtfpl for more details.

*/

char *gettext(const char *msgid) { return msgid; }
char *dgettext(const char *domain, const char *msgid) { return msgid; }
char *dcgettext(const char *domain, const char *msgid, const char *category) { return msgid; }

char *ngettext(const char *msgid1, const char *msgid2, unsigned long int n) { return msgid1; }
char *dngettext(const char *domain, const char *msgid1, const char *msgid2, unsigned long int n) { return msgid1; }
char *dcngettext(const char *domain, const char *msgid1, const char *msgid2, unsigned long int n, int category) { return msgid1; }

char *textdomain(const char *domain) { return "C"; }
char *bindtextdomain(const char *domainname, const char *dirname) { return (char*)0; }
char *bind_textdomain_codeset(const char *domain, const char *codeset) { return (char*)0; }
