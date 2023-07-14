#ifndef __sun
#define _POSIX_C_SOURCE 200809L
#else
#define __EXTENSIONS__ 1
#endif

#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "replxx.h"
#include "util.h"

void modify_callback(char** line, int* cursorPosition, void* ud) {
	char* s = *line;
	char* p = strchr( s, '*' );
	if ( p ) {
		int len = (int)strlen( s );
		char* n = *line = calloc( len * 2, 1 );
		int i = (int)( p - s );
		strncpy(n, s, i);
		n += i;
		strncpy(n, s, i);
		n += i;
		strncpy(n, p + 1, len - i - 1);
		n += ( len - i - 1 );
		strncpy(n, p + 1, len - i - 1);
		*cursorPosition *= 2;
		free( s );
	}
}

void completionHook(char const* context, replxx_completions* lc, int* contextLen, void* ud) {
	char** examples = (char**)( ud );
	size_t i;

	int utf8ContextLen = context_len( context );
	int prefixLen = (int)strlen( context ) - utf8ContextLen;
	*contextLen = utf8str_codepoint_len( context + prefixLen, utf8ContextLen );
	for (i = 0;	examples[i] != NULL; ++i) {
		if (strncmp(context + prefixLen, examples[i], utf8ContextLen) == 0) {
			replxx_add_completion(lc, examples[i]);
		}
	}
}

void hintHook(char const* context, replxx_hints* lc, int* contextLen, ReplxxColor* c, void* ud) {
	char** examples = (char**)( ud );
	int i;
	int utf8ContextLen = context_len( context );
	int prefixLen = (int)strlen( context ) - utf8ContextLen;
	*contextLen = utf8str_codepoint_len( context + prefixLen, utf8ContextLen );
	if ( *contextLen > 0 ) {
		for (i = 0;	examples[i] != NULL; ++i) {
			if (strncmp(context + prefixLen, examples[i], utf8ContextLen) == 0) {
				replxx_add_hint(lc, examples[i]);
			}
		}
	}
}

void colorHook( char const* str_, ReplxxColor* colors_, int size_, void* ud ) {
	int i = 0;
	for ( ; i < size_; ++ i ) {
		if ( isdigit( str_[i] ) ) {
			colors_[i] = REPLXX_COLOR_BRIGHTMAGENTA;
		}
	}
	if ( ( size_ > 0 ) && ( str_[size_ - 1] == '(' ) ) {
		replxx_emulate_key_press( ud, ')' );
		replxx_emulate_key_press( ud, REPLXX_KEY_LEFT );
	}
}

ReplxxActionResult word_eater( int ignored, void* ud ) {
	Replxx* replxx = (Replxx*)ud;
	return ( replxx_invoke( replxx, REPLXX_ACTION_KILL_TO_BEGINING_OF_WORD, 0 ) );
}

ReplxxActionResult upper_case_line( int ignored, void* ud ) {
	Replxx* replxx = (Replxx*)ud;
	ReplxxState state;
	replxx_get_state( replxx, &state );
	int l = (int)strlen( state.text );
#ifdef _WIN32
#define strdup _strdup
#endif
	char* d = strdup( state.text );
#undef strdup
	for ( int i = 0; i < l; ++ i ) {
		d[i] = toupper( d[i] );
	}
	state.text = d;
	state.cursorPosition /= 2;
	replxx_set_state( replxx, &state );
	free( d );
	return ( REPLXX_ACTION_RESULT_CONTINUE );
}

char const* recode( char* s ) {
	char const* r = s;
	while ( *s ) {
		if ( *s == '~' ) {
			*s = '\n';
		}
		++ s;
	}
	return ( r );
}

void split( char* str_, char** data_, int size_ ) {
	int i = 0;
	char* p = str_, *o = p;
	while ( i < size_ ) {
		int last = *p == 0;
		if ( ( *p == ',' ) || last ) {
			*p = 0;
			data_[i ++] = o;
			o = p + 1;
			if ( last ) {
				break;
			}
		}
		++ p;
	}
	data_[i] = 0;
}

int main( int argc, char** argv ) {
#define MAX_EXAMPLE_COUNT 128
	char* examples[MAX_EXAMPLE_COUNT + 1] = {
		"db", "hello", "hallo", "hans", "hansekogge", "seamann", "quetzalcoatl", "quit", "power", NULL
	};
	Replxx* replxx = replxx_init();
	replxx_install_window_change_handler( replxx );

	int quiet = 0;
	char const* prompt = "\x1b[1;32mreplxx\x1b[0m> ";
	int installModifyCallback = 0;
	int installCompletionCallback = 1;
	int installHighlighterCallback = 1;
	int installHintsCallback = 1;
	int indentMultiline = 0;
	while ( argc > 1 ) {
		-- argc;
		++ argv;
#ifdef __REPLXX_DEBUG__
		if ( !strcmp( *argv, "--keycodes" ) ) {
			replxx_debug_dump_print_codes();
			exit(0);
		}
#endif
		switch ( (*argv)[0] ) {
			case 'b': replxx_set_beep_on_ambiguous_completion( replxx, (*argv)[1] - '0' ); break;
			case 'c': replxx_set_completion_count_cutoff( replxx, atoi( (*argv) + 1 ) );   break;
			case 'e': replxx_set_complete_on_empty( replxx, (*argv)[1] - '0' );            break;
			case 'd': replxx_set_double_tab_completion( replxx, (*argv)[1] - '0' );        break;
			case 'h': replxx_set_max_hint_rows( replxx, atoi( (*argv) + 1 ) );             break;
			case 'H': replxx_set_hint_delay( replxx, atoi( (*argv) + 1 ) );                break;
			case 's': replxx_set_max_history_size( replxx, atoi( (*argv) + 1 ) );          break;
			case 'P': replxx_set_preload_buffer( replxx, recode( (*argv) + 1 ) );          break;
			case 'I': replxx_set_immediate_completion( replxx, (*argv)[1] - '0' );         break;
			case 'u': replxx_set_unique_history( replxx, (*argv)[1] - '0' );               break;
			case 'w': replxx_set_word_break_characters( replxx, (*argv) + 1 );             break;
			case 'm': replxx_set_no_color( replxx, (*argv)[1] - '0' );                     break;
			case 'i': replxx_set_ignore_case( replxx, (*argv)[1] - '0' );                  break;
			case 'n': indentMultiline = (*argv)[1] - '0';                                  break;
			case 'B': replxx_enable_bracketed_paste( replxx );                             break;
			case 'p': prompt = recode( (*argv) + 1 );                                      break;
			case 'q': quiet = atoi( (*argv) + 1 );                                         break;
			case 'M': installModifyCallback = atoi( (*argv) + 1 );                         break;
			case 'C': installCompletionCallback = 0;                                       break;
			case 'S': installHighlighterCallback = 0;                                      break;
			case 'N': installHintsCallback = 0;                                            break;
			case 'x': split( (*argv) + 1, examples, MAX_EXAMPLE_COUNT );                   break;
		}

	}

	replxx_set_indent_multiline( replxx, indentMultiline );
	const char* file = "./replxx_history.txt";

	replxx_history_load( replxx, file );
	if ( installModifyCallback ) {
		replxx_set_modify_callback( replxx, modify_callback, 0 );
	}
	if ( installCompletionCallback ) {
		replxx_set_completion_callback( replxx, completionHook, examples );
	}
	if ( installHighlighterCallback ) {
		replxx_set_highlighter_callback( replxx, colorHook, replxx );
	}
	if ( installHintsCallback ) {
		replxx_set_hint_callback( replxx, hintHook, examples );
	}
	replxx_bind_key( replxx, '.', word_eater, replxx );
	replxx_bind_key( replxx, REPLXX_KEY_F2, upper_case_line, replxx );

	printf("starting...\n");

	while (1) {
		char const* result = NULL;
		do {
			result = replxx_input( replxx, prompt );
		} while ( ( result == NULL ) && ( errno == EAGAIN ) );

		if (result == NULL) {
			printf("\n");
			break;
		} else if (!strncmp(result, "/history", 9)) {
			/* Display the current history. */
			int index = 0;
			int size = replxx_history_size( replxx );
			ReplxxHistoryScan* hs = replxx_history_scan_start( replxx );
			ReplxxHistoryEntry he;
			for ( ; replxx_history_scan_next( replxx, hs, &he ) == 0; ++index ) {
				replxx_print( replxx, "%4d: %s\n", index, he.text );
			}
			replxx_history_scan_stop( replxx, hs );
		} else if (!strncmp(result, "/unique", 8)) {
			replxx_set_unique_history( replxx, 1 );
		} else if (!strncmp(result, "/eb", 4)) {
			replxx_enable_bracketed_paste( replxx );
		} else if (!strncmp(result, "/db", 4)) {
			replxx_disable_bracketed_paste( replxx );
		}
		if (*result != '\0') {
			replxx_print( replxx, quiet ? "%s\n" : "thanks for the input: %s\n", result );
			replxx_history_add( replxx, result );
		}
	}
	replxx_history_save( replxx, file );
	printf( "Exiting Replxx\n" );
	replxx_end( replxx );
}

