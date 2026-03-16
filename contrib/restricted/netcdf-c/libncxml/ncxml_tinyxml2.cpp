/* Copyright 2018-2018 University Corporation for Atmospheric  Research/Unidata. */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <string.h>
#include "tinyxml2.h"
#include "ncxml.h"

#ifndef nulldup
#define nulldup(s) ((s)?strdup(s):NULL)
#endif

using namespace tinyxml2;

#ifdef _MSC_VER /*Do not use _WIN32 since this is a visual studio issue */
#define XMLDocument tinyxml2::XMLDocument
#endif

static int ncxml_initialized = 0;

void
ncxml_initialize(void)
{
    ncxml_initialized = 1;
}

void
ncxml_finalize(void)
{
    ncxml_initialized = 0;
}

ncxml_doc_t
ncxml_parse(char* contents, size_t len)
{
    XMLError err = XML_SUCCESS;
    XMLDocument* doc = new XMLDocument(); /* the resulting document tree */
    doc->Parse(contents,len);
    if((err = doc->ErrorID()))
        return NULL;
    return (ncxml_doc_t)doc;
}

void
ncxml_free(ncxml_doc_t doc0)
{
    XMLDocument* doc = (XMLDocument*)doc0;
    delete doc;
 }

ncxml_t
ncxml_root(ncxml_doc_t doc0)
{
    XMLDocument* doc = (XMLDocument*)doc0;    
    return (ncxml_t)doc->RootElement();
}

/* Element name */
const char*
ncxml_name(ncxml_t xml0)
{
    XMLNode* xml = (XMLNode*)xml0;
    XMLElement* e = (xml?xml->ToElement():NULL);
    return (e?e->Name():NULL);
}

char*
ncxml_attr(ncxml_t xml0, const char* key)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    const char* value = NULL;
    char* s = NULL;

    value = xml->Attribute(key);
    s = nulldup((char*)value);
    return s;
}

/* First child by name */
ncxml_t
ncxml_child(ncxml_t xml0, const char* name)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    XMLNode* child = NULL;

    child = xml->FirstChildElement(name);
    return (child?(ncxml_t)child:NULL);
}

ncxml_t
ncxml_next(ncxml_t xml0, const char* name)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    XMLNode* next = NULL;
    next = xml->NextSiblingElement(name);
    return (next?(ncxml_t)next:NULL);
}

char*
ncxml_text(ncxml_t xml0)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    const char* txt = NULL;
    char* s = NULL;
    if(xml == NULL) return NULL;
    txt = xml->GetText();
    s = nulldup((char*)txt);
    return s;
}

/* Nameless versions of child and next */
ncxml_t
ncxml_child_first(ncxml_t xml0)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    XMLNode* child = NULL;

    if(xml == NULL) return NULL;
    child = xml->FirstChild();
    return (child?(ncxml_t)child:NULL);
}

ncxml_t
ncxml_child_next(ncxml_t xml0)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    XMLNode* child = NULL;

    if(xml == NULL) return NULL;
    child = xml->NextSiblingElement();
    return (child?(ncxml_t)child:NULL);
}

int
ncxml_attr_pairs(ncxml_t xml0, char*** pairsp)
{
    XMLNode* n = (XMLNode*)xml0;
    XMLElement* xml = (n?n->ToElement():NULL);
    const XMLAttribute* attr = NULL;
    int i,count = 0;
    char** pairs = NULL;

    if(xml == NULL) return 0;
    /* First count */
    for(attr=xml->FirstAttribute();attr;attr=attr->Next()) count++;
    /* Allocate */
    pairs = (char**)malloc(sizeof(char*)*((2*count)+1));
    if(pairs == NULL) return 0;
    /* Collect */
    for(i=0,attr=xml->FirstAttribute();attr;i+=2,attr=attr->Next()) {
	const char* value;
        pairs[i] = nulldup((char*)attr->Name());
        value = attr->Value();
	pairs[i+1] = nulldup((char*)value);
    }
    pairs[2*count] = NULL;
    if(pairsp) *pairsp = pairs;
    return 1;
}
