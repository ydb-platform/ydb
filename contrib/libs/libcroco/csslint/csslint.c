/* -*- Mode: C; indent-tabs-mode:nil; c-basic-offset: 8-*- */

/*
 * csslint.c : a small tester program for libcroco.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of version 2.1 of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 * Initial Author: Gael Chamoulaud.
 * Main programmer: Dodji Seketeli
 * Contributor: Rob Buis
 * See COPYRGIGHTS file for copyright information.
 */

#include "libcroco.h"
#include <libxml/xpath.h>

#include <glib.h>
#include <string.h>

/**
 *The options data structure.
 *The variable of this data structure are set
 *during the  parsing the command line by the
 *parse_command_line() function.
 */
struct Options {
        gboolean show_version;
        gboolean use_cssom;
        gboolean display_help;
        gboolean evaluate;
        gboolean dump_location;
        gchar *author_sheet_path;
        gchar *user_sheet_path;
        gchar *ua_sheet_path;
        gchar *xml_path;
        gchar *xpath;
        gchar **css_files_list;
};

struct SacContext {
        gint level;

};

static enum CRStatus sac_parse_and_display_locations (guchar * a_file_uri);

static void parse_cmd_line (int a_argc, char **a_argv,
                            struct Options *a_options);

static void display_version (void);

static void display_usage (void);

static enum CRStatus cssom_parse (guchar * a_file_uri);

static enum CRStatus get_and_dump_node_style (xmlNode * a_node,
                                              CRSelEng * a_sel_eng,
                                              CRCascade * a_cascade);

static enum CRStatus evaluate_selectors (gchar * a_xml_path,
                                         gchar * a_author_sheet_path,
                                         gchar * a_user_sheet_path,
                                         gchar * a_ua_sheet_path,
                                         gchar * a_xpath);

/**
 *Parses the command line.
 *@param a_argc the argc parameter of the main routine.
 *@param the argv parameter of the main routine.
 *@param a_options out parameter the parsed options.
 */
static void
parse_cmd_line (int a_argc, char **a_argv, struct Options *a_options)
{
        int i = 0;

        g_return_if_fail (a_options);

        if (a_argc <= 1) {
                display_usage ();
        }

        for (i = 1; i < a_argc; i++) {
                if (a_argv[i][0] != '-')
                        break;

                if ((!strcmp (a_argv[i], "-version")) ||
                    (!strcmp (a_argv[i], "-v"))) {
                        a_options->show_version = TRUE;
                } else if (!strcmp (a_argv[i], "--evaluate") ||
                           !strcmp (a_argv[i], "-e")) {
                        for (i++; i < a_argc; i++) {
                                if (!strcmp (a_argv[i], "--author-sheet")) {
                                        if (a_options->author_sheet_path) {
                                                display_usage ();
                                                exit (-1);
                                        }
                                        i++;
                                        if (i >= a_argc
                                            || a_argv[i][0] == '-') {
                                                g_print ("--author-sheet should be followed by a path to the sheet\n");
                                                display_usage ();
                                                exit (-1);
                                        }
                                        a_options->author_sheet_path =
                                                a_argv[i];
                                } else if (!strcmp
                                           (a_argv[i], "--user-sheet")) {
                                        if (a_options->user_sheet_path) {
                                                display_usage ();
                                                exit (-1);
                                        }
                                        i++;
                                        if (i >= a_argc
                                            || a_argv[i][0] == '-') {
                                                g_print ("--user-sheet should be followed by a path to the sheet\n");
                                                display_usage ();
                                                exit (-1);
                                        }
                                        a_options->user_sheet_path =
                                                a_argv[i];
                                } else if (!strcmp (a_argv[i], "--ua-sheet")) {
                                        if (a_options->ua_sheet_path) {
                                                display_usage ();
                                                exit (-1);
                                        }
                                        i++;
                                        if (i >= a_argc
                                            || a_argv[i][0] == '-') {
                                                g_print ("--ua-sheet should be followed by a path to the sheet\n");
                                                display_usage ();
                                                exit (-1);
                                        }
                                        a_options->ua_sheet_path = a_argv[i];
                                } else if (!strcmp (a_argv[i], "--xml")) {
                                        i++;
                                        if (i >= a_argc
                                            || a_argv[i][0] == '-') {
                                                g_print ("--xml should be followed by a path to the xml document\n");
                                                display_usage ();
                                                exit (-1);
                                        }
                                        a_options->xml_path = a_argv[i];
                                } else if (!strcmp (a_argv[i], "--xpath")) {
                                        i++;
                                        if (i >= a_argc
                                            || a_argv[i][0] == '-') {
                                                g_print ("--xpath should be followed by an xpath expresion\n");
                                                display_usage ();
                                                exit (-1);
                                        }
                                        a_options->xpath = a_argv[i];
                                } else {
                                        break;
                                }
                        }
                        if (!a_options->author_sheet_path
                            && !a_options->user_sheet_path &&
                            !a_options->ua_sheet_path) {
                                g_print ("Error: you must specify at least one stylesheet\n");
                                display_usage ();
                                exit (-1);

                        }
                        if (!a_options->xpath) {
                                g_printerr
                                        ("Error: you must specify an xpath expression using the --xpath option\n");
                                display_usage ();
                                exit (-1);

                        }
                        a_options->evaluate = TRUE;
                } else if (!strcmp (a_argv[i], "--dump-location")) {
                        a_options->dump_location = TRUE;
                        a_options->use_cssom = FALSE;
                } else if (!strcmp (a_argv[i], "--help") ||
                           !strcmp (a_argv[i], "-h")) {
                        a_options->display_help = TRUE;
                } else {
                        display_usage ();
                        exit (-1);
                }
        }

        if (i >= a_argc) {
                a_options->css_files_list = NULL;
        } else {
                if (a_argv[i][0] == '-') {
                        display_usage ();
                        exit (-1);
                }
                a_options->css_files_list = &a_argv[i];
        }
}

/**
 *Displays the version text.
 *@param a_argc the argc variable passed to the main function.
 *@param a_argv the argv variable passed to the main function.
 */
static void
display_version (void)
{
        g_print ("%s\n", LIBCROCO_VERSION);
}

/**
 *Displays the usage text.
 *@param a_argc the argc variable passed to the main function.
 *@param a_argv the argv variable passed to the main function.
 */
static void
display_usage (void)
{
        g_print ("Usage: csslint <path to a css file>\n");
        g_print ("\t| csslint -v|--version\n");
        g_print ("\t| csslint --dump-location <path to a css file>\n");
        g_print ("\t| csslint <--evaluate | -e> [--author-sheet <path> --user-sheet <path> --ua-sheet <path>\n\t   ] --xml <path> --xpath <xpath expression>\n");
}

/**
 *The test of the cr_input_read_byte() method.
 *Reads the each byte of a_file_uri using the
 *cr_input_read_byte() method. Each byte is send to
 *stdout.
 *@param a_file_uri the file to read.
 *@return CR_OK upon successfull completion of the
 *function, an error code otherwise.
 */
static enum CRStatus
cssom_parse (guchar * a_file_uri)
{
        enum CRStatus status = CR_OK;
        CROMParser *parser = NULL;
        CRStyleSheet *stylesheet = NULL;

        g_return_val_if_fail (a_file_uri, CR_BAD_PARAM_ERROR);

        parser = cr_om_parser_new (NULL);
        status = cr_om_parser_parse_file (parser,
                                          a_file_uri, CR_ASCII, &stylesheet);
        if (status == CR_OK && stylesheet) {
                cr_stylesheet_dump (stylesheet, stdout);
                g_print ("\n");
                cr_stylesheet_destroy (stylesheet);
        }
        cr_om_parser_destroy (parser);

        return status;
}

static enum CRStatus
get_and_dump_node_style (xmlNode * a_node,
                         CRSelEng * a_sel_eng, CRCascade * a_cascade)
{
        CRPropList *prop_list = NULL,
                *pair = NULL,
                *prev_pair = NULL;
        enum CRStatus status = CR_OK;

        g_return_val_if_fail (a_node && a_sel_eng && a_cascade,
                              CR_BAD_PARAM_ERROR);

        status = cr_sel_eng_get_matched_properties_from_cascade
                (a_sel_eng, a_cascade, a_node, &prop_list);
        if (status != CR_OK) {
                g_printerr ("Error: unable to run the selection engine\n");
                return CR_OK;
        }
        g_print ("Properties of xml element %s are:\n", a_node->name);
        for (pair = prop_list; pair; pair = cr_prop_list_get_next (pair)) {
                CRDeclaration *decl = NULL;

                cr_prop_list_get_decl (pair, &decl);
                if (decl) {
                        prev_pair = cr_prop_list_get_prev (pair);
                        if (prev_pair) {
                                g_print ("\n");
                                prev_pair = NULL;
                        }
                        cr_declaration_dump_one (decl, stdout, 2);
                        decl = NULL;
                }
        }
        g_print ("\n=====================\n\n");

        if (prop_list) {
                cr_prop_list_destroy (prop_list);
                prop_list = NULL;
        }

        return CR_OK;
}

static enum CRStatus
evaluate_selectors (gchar * a_xml_path,
                    gchar * a_author_sheet_path,
                    gchar * a_user_sheet_path,
                    gchar * a_ua_sheet_path, gchar * a_xpath)
{
        CRSelEng *sel_eng = NULL;
        xmlDoc *xml_doc = NULL;
        xmlXPathContext *xpath_context = NULL;
        xmlXPathObject *xpath_object = NULL;
        CRStyleSheet *author_sheet = NULL,
                *user_sheet = NULL,
                *ua_sheet = NULL;
        CRCascade *cascade = NULL;
        xmlNode *cur_node = NULL;
        gint i = 0;
        enum CRStatus status = CR_OK;

        g_return_val_if_fail (a_xml_path && a_xpath, CR_BAD_PARAM_ERROR);

        xml_doc = xmlParseFile (a_xml_path);
        if (!xml_doc) {
                g_printerr ("Error: Could not parse file %s\n", a_xml_path);
                return CR_ERROR;
        }
        if (a_author_sheet_path) {
                status = cr_om_parser_simply_parse_file
                        (a_author_sheet_path, CR_ASCII, &author_sheet);
                if (!author_sheet) {
                        g_printerr ("Error: Could not parse author sheet\n");
                }
        }
        if (a_user_sheet_path) {
                status = cr_om_parser_simply_parse_file
                        (a_user_sheet_path, CR_ASCII, &user_sheet);
                if (!user_sheet) {
                        g_printerr ("Error: Could not parse author sheet\n");
                }
        }
        if (a_ua_sheet_path) {
                status = cr_om_parser_simply_parse_file
                        (a_ua_sheet_path, CR_ASCII, &ua_sheet);
                if (!ua_sheet) {
                        g_printerr ("Error: Could not parse ua sheet\n");
                }
        }
        cascade = cr_cascade_new (author_sheet, user_sheet, ua_sheet);
        if (!cascade) {
                g_printerr ("Could not instanciate the cascade\n");
                return CR_ERROR;
        }
        sel_eng = cr_sel_eng_new ();
        if (!sel_eng) {
                g_printerr
                        ("Error: Could not instanciate the selection engine\n");
                return CR_ERROR;
        }
        xpath_context = xmlXPathNewContext (xml_doc);
        if (!xpath_context) {
                g_printerr
                        ("Error: Could not instanciate the xpath context\n");
                return CR_ERROR;
        }
        xpath_object = xmlXPathEvalExpression (a_xpath, xpath_context);
        if (!xpath_object) {
                g_printerr ("Error: Could not evaluate xpath expression\n");
                return CR_ERROR;
        }
        if (xpath_object->type != XPATH_NODESET || !xpath_object->nodesetval) {
                g_printerr
                        ("Error: xpath does not evalualuate to a node set\n");
                return CR_ERROR;
        }

        for (i = 0; i < xpath_object->nodesetval->nodeNr; i++) {
                cur_node = xpath_object->nodesetval->nodeTab[i];
                if (cur_node->type == XML_ELEMENT_NODE) {
                        status = get_and_dump_node_style (cur_node, sel_eng,
                                                          cascade);
                }
        }

        if (xpath_context) {
                xmlXPathFreeContext (xpath_context);
                xpath_context = NULL;
        }
        if (xpath_context) {
                xmlXPathFreeObject (xpath_object);
                xpath_object = NULL;
        }
        if (sel_eng) {
                cr_sel_eng_destroy (sel_eng);
                sel_eng = NULL;
        }
        if (cascade) {
                cr_cascade_destroy (cascade);
        }
        return CR_OK;
}

/***************************
 *SAC related stuff for the
 *line/col annotation stylesheet
 *dumping
 ***************************/
static void
start_document (CRDocHandler * a_this)
{
        struct SacContext *context = NULL;

        g_return_if_fail (a_this);

        context = g_try_malloc (sizeof (struct SacContext));
        if (!context) {
                cr_utils_trace_info ("instanciation of sac context failed");
                return;
        }
        cr_doc_handler_set_ctxt (a_this, context);
}

static void
end_document (CRDocHandler * a_this)
{
        struct SacContext *context = NULL;

        g_return_if_fail (a_this);

        cr_doc_handler_get_ctxt (a_this, (gpointer *) (gpointer) & context);
        if (context) {
                g_free (context);
                context = NULL;
        }
}

static void
charset (CRDocHandler *a_this, 
         CRString *a_charset,
         CRParsingLocation *a_charset_sym_location)
{
        gchar *str = NULL ;

        g_return_if_fail (a_this && a_charset && a_charset_sym_location) ;

        str = (gchar*)cr_string_peek_raw_str (a_charset) ;
        if (str) {
                g_print ("\n\n@charset \"%s\";\n\n", str) ;
                str = NULL ;
        } else {
                return ;
        }
        g_print ("/********************************************\n") ;
        g_print (" * Parsing location information of the @charset rule\n") ;
        g_print (" ********************************************/\n") ;
        if (a_charset_sym_location) {
                str = cr_parsing_location_to_string (a_charset_sym_location, 0) ;
                if (str) {
                        g_print ("  /*@charset*/\n") ;
                        g_print ("  /*%s*/\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }
        str = (gchar*) cr_string_peek_raw_str (a_charset) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                str = NULL ;
        }
        str = cr_parsing_location_to_string (&a_charset->location, 0) ;
        if (str) {
                g_print ("  /*%s*/\n\n", str) ;
                g_free (str) ;
                str = NULL ;
        }
}

static void
import_style (CRDocHandler *a_this,
              GList *a_media_list,
              CRString *a_uri,
              CRString *a_uri_default_ns,
              CRParsingLocation *a_location)
{
        gchar *str = NULL ;
        GString *gstr = NULL ;
        GList *cur_media = NULL ;

        g_return_if_fail (a_this && a_location) ;

        gstr = g_string_new (NULL) ;
        if (!gstr) {
                cr_utils_trace_info ("Out of memory error") ;
                return ;
        }
        for (cur_media = a_media_list ; 
             cur_media ;
             cur_media = g_list_next (cur_media)) {
                str = (gchar*)cr_string_peek_raw_str
                        ((CRString*)cur_media->data) ;
                if (str) {
                        if (g_list_previous (cur_media)) {
                                g_string_append_printf (gstr, ", %s", 
                                                        str) ;
                        } else {
                                g_string_append_printf (gstr, "%s",
                                                        str) ;
                        }
                }
        }
        str = (gchar*)cr_string_peek_raw_str (a_uri) ;
        if (str) {
                g_print ("@import url(\"%s\") ", str) ;
                if (gstr) {
                        g_print ("%s;\n\n", gstr->str) ;
                }
                str = NULL ;
        }
        str = cr_parsing_location_to_string (a_location, 0) ;
        if (str) {
                g_print ("/*****************************************************\n") ;
                g_print (" *Parsing location inforamtion for the @import rule\n") ;
                g_print (" ******************************************************/\n\n") ;
                g_print ("  /*@import*/\n") ;
                g_print ("  /*%s*/\n\n", str) ;
                g_free (str) ;
                str = NULL ;
        }

        str = (gchar*)cr_string_peek_raw_str (a_uri) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                str = cr_parsing_location_to_string 
                        (&a_uri->location, 0) ;
                if (str) {
                        g_print ("  /*%s*/\n\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }

        for (cur_media = a_media_list ; 
             cur_media ;
             cur_media = g_list_next (cur_media)) {
                str = (gchar*)cr_string_peek_raw_str
                        ((CRString*)cur_media->data) ;
                if (str) {
                        g_print ("  /*%s*/\n", str) ;
                }
                str = cr_parsing_location_to_string 
                        (&((CRString*)cur_media->data)->location, 0) ;
                if (str) {
                        g_print ("  /*%s*/\n\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }

        if (gstr) {
                g_string_free (gstr, TRUE) ;
                gstr = NULL ;
        }
}

static void
start_font_face (CRDocHandler *a_this,
                 CRParsingLocation *a_location)
{
        gchar *str = NULL ;

        g_print ("@font-face {\n") ;
        g_print ("/******************************************************\n") ;
        g_print (" Parsing location information for the @font-face rule\n") ;
        g_print (" ******************************************************/\n\n") ;

        g_print ("  /*@font-face*/\n") ;
        if (a_location) {
                str = cr_parsing_location_to_string (a_location, 0) ;
        }
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                g_free (str) ;
                str = NULL ;
        }
}

static void
end_font_face (CRDocHandler *a_this)
{
        g_print ("}\n") ;
}

static void
start_media (CRDocHandler *a_this,
             GList *a_media_list,
             CRParsingLocation *a_location)
{
        GList *cur_media = NULL ;
        gchar *str = NULL ;

        g_print ("@media ") ;
        for (cur_media = a_media_list ;
             cur_media ;
             cur_media = g_list_next (cur_media)) {
                CRString *crstr = cur_media->data ;
                if (crstr) {
                        str = (gchar*)cr_string_peek_raw_str (crstr) ;
                        if (str) {
                                if (cur_media->prev) {
                                        g_print (", %s", str) ;
                                } else {
                                        g_print ("%s", str) ;
                                }
                        }
                }
        }
        /*****************************
         *new print parsing locations
         *****************************/
        g_print ("\n  /*@media*/\n") ;
        if (a_location)
                str = cr_parsing_location_to_string (a_location,
                                                     0) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                g_free (str) ;
                str = NULL ;
        }
        for (cur_media = a_media_list ;
             cur_media ;
             cur_media = g_list_next (cur_media)) {
                CRString *crstr = cur_media->data ;
                if (crstr) {
                        str = (gchar*)cr_string_peek_raw_str (crstr) ;
                        if (str) {
                                g_print ("  /*%s*/\n", str) ;
                                str = cr_parsing_location_to_string 
                                        (&crstr->location, 0) ;
                                if (str) {
                                        g_print ("  /*%s*/\n", str) ;
                                        g_free (str) ;
                                        str = NULL ;
                                }
                        }
                }
        }
        g_print ("\n{\n") ;
}

static void
end_media (CRDocHandler *a_this,
           GList *a_media_list)
{        
        g_print ("\n}\n") ;
}

static void
start_page (CRDocHandler *a_this,
            CRString *a_name,
            CRString *a_pseudo_page,
            CRParsingLocation *a_location)
{
        gchar *str = NULL ;
        
        g_print ("@page ") ;
        if (a_name)
                str = (gchar*)cr_string_peek_raw_str (a_name) ;
        if (str) {
                g_print ("%s ", str) ;
        }
        if (a_pseudo_page)
                str = (gchar*) cr_string_peek_raw_str (a_pseudo_page) ;
        if (str) {
                g_print (":%s", str) ;
                str = NULL ;
        }
        /*************************************
         *print parsing location informations
         ************************************/
        g_print ("\n\n  /*@page*/\n") ;
        if (a_location)
                str = cr_parsing_location_to_string 
                        (a_location, 0) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                g_free (str) ;
                str = NULL ;
        }
        if (a_name)
                str = (gchar*)cr_string_peek_raw_str (a_name) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                str = cr_parsing_location_to_string 
                        (&a_name->location, 0) ;
                if (str) {
                        g_print ("  /*%s*/\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }
        if (a_pseudo_page)
                str = (gchar*) cr_string_peek_raw_str 
                        (a_pseudo_page) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                str = cr_parsing_location_to_string 
                        (&a_pseudo_page->location, 0) ;
                if (str) {
                        g_print ("  /*%s*/\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }
        g_print ("\n{\n") ;        
}

static void
end_page (CRDocHandler *a_this,
          CRString *a_name,
          CRString *pseudo_page)
{
        g_print ("}\n") ;
}

static void
dump_location_annotated_simple_sel (CRSimpleSel *a_this)
{
        CRSimpleSel *cur_simple_sel = a_this ;
        CRAdditionalSel *cur_add_sel = NULL ;
        gchar *str0 = NULL ;

        g_return_if_fail (a_this) ;

        /*first, display the entire simple sel*/
        str0 = cr_simple_sel_one_to_string
                (cur_simple_sel) ;
        if (str0) {
                g_print ("/*%s*/\n", str0) ;
                g_free (str0) ;
                str0 = NULL ;
        }
        g_print ("/*") ;
        cr_parsing_location_dump
                (&cur_simple_sel->location, 0,
                 stdout);
        g_print ("*/\n") ;

        /*now display the details of the simple sel*/
        if (cur_simple_sel->name) {
                str0 = (gchar*) cr_string_peek_raw_str
                        (cur_simple_sel->name) ;
                if (str0) {
                        g_print ("  /*%s*/\n", str0) ;
                        str0 = NULL ;
                }
                str0 = cr_parsing_location_to_string
                        (&cur_simple_sel->name->location,
                         0) ;
                if (str0) {
                        g_print ("  /*%s*/\n", str0) ;
                        g_free (str0) ;
                        str0 = NULL ;
                }
        }
        for (cur_add_sel = cur_simple_sel->add_sel; 
             cur_add_sel;
             cur_add_sel = cur_add_sel->next) {
                str0 = cr_additional_sel_one_to_string 
                        (cur_add_sel) ;
                if (str0) {
                        g_print ("\n  /*%s*/\n", str0) ;
                        g_free (str0) ;
                        str0 = NULL ;
                }
                str0 = cr_parsing_location_to_string 
                        (&cur_add_sel->location, 0) ;
                if (str0) {
                        g_print ("  /*%s*/\n", str0) ;
                        g_free (str0) ;
                        str0 = NULL ;
                }
        }
}

static void
start_selector (CRDocHandler * a_this, CRSelector * a_selector_list)
{
        struct SacContext *context = NULL;
        CRSelector *cur_sel = NULL ;
        CRSimpleSel *cur_simple_sel = NULL ;

        g_return_if_fail (a_this);

        cr_doc_handler_get_ctxt (a_this, (gpointer *) (gpointer) & context);
        if (context) {
                context->level++;
        }
        cr_selector_dump (a_selector_list, stdout);
        g_print (" {\n") ;
        g_print ("/************************************************\n") ;
        g_print (" *Parsing location information of the selector\n") ;
        g_print (" ************************************************/\n") ;
        
        for (cur_sel = a_selector_list; 
             cur_sel ;
             cur_sel = cur_sel->next) {
                for (cur_simple_sel = cur_sel->simple_sel ;
                     cur_simple_sel ;
                     cur_simple_sel = cur_simple_sel->next) {

                        dump_location_annotated_simple_sel 
                                (cur_simple_sel) ;
                }
        }        
}

static void
end_selector (CRDocHandler * a_this,
              CRSelector *a_selector_list)
{
        struct SacContext *context = NULL;

        g_return_if_fail (a_this);

        cr_doc_handler_get_ctxt (a_this, (gpointer *) (gpointer) & context);
        if (context) {
                context->level--;
        }
        g_print (" }\n") ;
}

static void
property (CRDocHandler *a_this,
          CRString *a_name,
          CRTerm *a_expr,
          gboolean a_is_important)
{
        gchar *str = NULL ;
        CRTerm *cur_term = NULL ;
        
        str = (gchar*) cr_string_peek_raw_str (a_name) ;
        if (str) {
                g_print ("\n\n") ;
                g_print ("%s", str) ;
                str = NULL ;
                if (a_expr) {
                        str = cr_term_to_string (a_expr) ;
                        if (str) {
                                g_print (" : %s;\n\n", str) ;
                                g_free (str) ;
                                str = NULL ;
                        }
                } else {
                        g_print (";\n\n") ;
                }
        }

        /*Now dump each part of the property declaration*/

        g_print ("\n") ;
        g_print ("/************************************************\n") ;
        g_print (" *Parsing location information of the property\n") ;
        g_print (" ************************************************/\n") ;

        str = (gchar*) cr_string_peek_raw_str (a_name) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                str = NULL ;
        }
        str = cr_parsing_location_to_string (&a_name->location, 0) ;
        if (str) {
                g_print ("  /*%s*/\n", str) ;
                g_free (str) ;
                str = NULL ;
        }

        for (cur_term = a_expr ;
             cur_term;
             cur_term = cur_term->next) {
                str = cr_term_one_to_string (cur_term) ;
                if (str) {
                        g_print ("  /*%s*/\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
                str = cr_parsing_location_to_string
                        (&cur_term->location, 0) ;
                if (str) {
                        g_print ("  /*%s*/\n", str) ;
                        g_free (str) ;
                        str = NULL ;
                }
        }
}


static enum CRStatus
sac_parse_and_display_locations (guchar * a_file_uri)
{
        enum CRStatus status = CR_OK;
        CRDocHandler *sac_handler = NULL;
        CRParser *parser = NULL;

        g_return_val_if_fail (a_file_uri, CR_BAD_PARAM_ERROR);

        parser = cr_parser_new_from_file (a_file_uri, CR_UTF_8);
        if (!parser) {
                cr_utils_trace_info ("parser instanciation failed");
                return CR_ERROR;
        }
        sac_handler = cr_doc_handler_new ();
        if (!sac_handler) {
                cr_utils_trace_info ("sac handler instanciation failed");
                status = CR_OUT_OF_MEMORY_ERROR;
                goto cleanup;
        }
        sac_handler->start_document = start_document ;
        sac_handler->end_document = end_document ;
        sac_handler->charset = charset ;
        sac_handler->import_style = import_style ;
        sac_handler->start_font_face = start_font_face ;
        sac_handler->end_font_face = end_font_face ;
        sac_handler->start_media = start_media ;
        sac_handler->end_media = end_media ;
        sac_handler->start_page = start_page ;
        sac_handler->end_page = end_page ;
        sac_handler->start_selector = start_selector;
        sac_handler->end_selector = end_selector;
        sac_handler->property = property ;

        cr_parser_set_sac_handler (parser, sac_handler) ;
        status = cr_parser_parse (parser) ;
        
 cleanup:
        if (parser) {
                cr_parser_destroy (parser);
                parser = NULL;
        }
        return status;
}

int
main (int argc, char **argv)
{
        struct Options options;
        enum CRStatus status = CR_OK;

        memset (&options, 0, sizeof (struct Options));
        options.use_cssom = TRUE;
        parse_cmd_line (argc, argv, &options);

        if (options.show_version == TRUE) {
                display_version ();
                return 0;
        }

        if (options.display_help == TRUE) {
                display_usage ();
                return 0;
        }
        if (options.use_cssom == TRUE) {
                if (options.evaluate == TRUE) {
                        status = evaluate_selectors
                                (options.xml_path,
                                 options.author_sheet_path,
                                 options.user_sheet_path,
                                 options.ua_sheet_path, options.xpath);
                } else if (options.css_files_list != NULL) {
                        status = cssom_parse (options.css_files_list[0]);
                }
        } else if (options.dump_location == TRUE) {
                if (options.css_files_list) {
                        status = sac_parse_and_display_locations 
                                (options.css_files_list[0]) ;
                } else {
                        display_usage () ;
                        return -1 ;
                }
        }

        return 0;
}
