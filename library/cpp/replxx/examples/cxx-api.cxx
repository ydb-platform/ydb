#include <vector>
#include <unordered_map>
#include <string>
#include <regex>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <utility>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>

#include "replxx.hxx"
#include "util.h"

using Replxx = replxx::Replxx;
using namespace replxx::color;

class Tick {
	typedef std::vector<char32_t> keys_t;
	std::thread _thread;
	int _tick;
	int _promptState;
	bool _alive;
	keys_t _keys;
	bool _tickMessages;
	bool _promptFan;
	Replxx& _replxx;
public:
	Tick( Replxx& replxx_, std::string const& keys_, bool tickMessages_, bool promptFan_ )
		: _thread()
		, _tick( 0 )
		, _promptState( 0 )
		, _alive( false )
		, _keys( keys_.begin(), keys_.end() )
		, _tickMessages( tickMessages_ )
		, _promptFan( promptFan_ )
		, _replxx( replxx_ ) {
	}
	void start() {
		_alive = true;
		_thread = std::thread( &Tick::run, this );
	}
	void stop() {
		_alive = false;
		_thread.join();
	}
	void run() {
		std::string s;
		static char const PROMPT_STATES[] = "-\\|/";
		while ( _alive ) {
			if ( _tickMessages ) {
				_replxx.print( "%d\n", _tick );
			}
			if ( _tick < static_cast<int>( _keys.size() ) ) {
				_replxx.emulate_key_press( _keys[_tick] );
			}
			if ( ! _tickMessages && ! _promptFan && ( _tick >= static_cast<int>( _keys.size() ) ) ) {
				break;
			}
			if ( _promptFan ) {
				for ( int i( 0 ); i < 4; ++ i ) {
					char prompt[] = "\x1b[1;32mreplxx\x1b[0m[ ]> ";
					prompt[18] = PROMPT_STATES[_promptState % 4];
					++ _promptState;
					_replxx.set_prompt( prompt );
					std::this_thread::sleep_for( std::chrono::milliseconds( 250 ) );
				}
			} else {
				std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
			}
			++ _tick;
		}
	}
};

// prototypes
Replxx::completions_t hook_completion(std::string const& context, int& contextLen, std::vector<std::string> const& user_data, bool);
Replxx::hints_t hook_hint(std::string const& context, int& contextLen, Replxx::Color& color, std::vector<std::string> const& user_data, bool);
typedef std::vector<std::pair<std::string, Replxx::Color>> syntax_highlight_t;
typedef std::unordered_map<std::string, Replxx::Color> keyword_highlight_t;
void hook_color( std::string const& str, Replxx::colors_t& colors, syntax_highlight_t const&, keyword_highlight_t const& );
void hook_modify( std::string& line, int& cursorPosition, Replxx* );

bool eq( std::string const& l, std::string const& r, int s, bool ic ) {
	if ( static_cast<int>( l.length() ) < s ) {
		return false;
	}
	if ( static_cast<int>( r.length() ) < s ) {
		return false;
	}
	bool same( true );
	for ( int i( 0 ); same && ( i < s ); ++ i ) {
		same = ( ic && ( towlower( l[i] ) == towlower( r[i] ) ) ) || ( l[i] == r[i] );
	}
	return same;
}

Replxx::completions_t hook_completion(std::string const& context, int& contextLen, std::vector<std::string> const& examples, bool ignoreCase) {
	Replxx::completions_t completions;
	int utf8ContextLen( context_len( context.c_str() ) );
	int prefixLen( static_cast<int>( context.length() ) - utf8ContextLen );
	if ( ( prefixLen > 0 ) && ( context[prefixLen - 1] == '\\' ) ) {
		-- prefixLen;
		++ utf8ContextLen;
	}
	contextLen = utf8str_codepoint_len( context.c_str() + prefixLen, utf8ContextLen );

	std::string prefix { context.substr(prefixLen) };
	if ( prefix == "\\pi" ) {
		completions.push_back( "π" );
	} else {
		for (auto const& e : examples) {
			bool lowerCasePrefix( std::none_of( prefix.begin(), prefix.end(), iswupper ) );
			if ( eq( e, prefix, static_cast<int>( prefix.size() ), ignoreCase && lowerCasePrefix ) ) {
				Replxx::Color c( Replxx::Color::DEFAULT );
				if ( e.find( "brightred" ) != std::string::npos ) {
					c = Replxx::Color::BRIGHTRED;
				} else if ( e.find( "red" ) != std::string::npos ) {
					c = Replxx::Color::RED;
				}
				completions.emplace_back(e.c_str(), c);
			}
		}
	}

	return completions;
}

Replxx::hints_t hook_hint(std::string const& context, int& contextLen, Replxx::Color& color, std::vector<std::string> const& examples, bool ignoreCase) {
	Replxx::hints_t hints;

	// only show hint if prefix is at least 'n' chars long
	// or if prefix begins with a specific character

	int utf8ContextLen( context_len( context.c_str() ) );
	int prefixLen( static_cast<int>( context.length() ) - utf8ContextLen );
	contextLen = utf8str_codepoint_len( context.c_str() + prefixLen, utf8ContextLen );
	std::string prefix { context.substr(prefixLen) };

	if (prefix.size() >= 2 || (! prefix.empty() && prefix.at(0) == '.')) {
		bool lowerCasePrefix( std::none_of( prefix.begin(), prefix.end(), iswupper ) );
		for (auto const& e : examples) {
			if ( eq( e, prefix, prefix.size(), ignoreCase && lowerCasePrefix ) ) {
				hints.emplace_back(e.c_str());
			}
		}
	}

	// set hint color to green if single match found
	if (hints.size() == 1) {
		color = Replxx::Color::GREEN;
	}

	return hints;
}

inline bool is_kw( char ch ) {
	return isalnum( ch ) || ( ch == '_' );
}

void hook_color( std::string const& context, Replxx::colors_t& colors, syntax_highlight_t const& regex_color, keyword_highlight_t const& word_color ) {
	// highlight matching regex sequences
	for (auto const& e : regex_color) {
		size_t pos {0};
		std::string str = context;
		std::smatch match;

		while(std::regex_search(str, match, std::regex(e.first))) {
			std::string c{ match[0] };
			std::string prefix( match.prefix().str() );
			pos += utf8str_codepoint_len( prefix.c_str(), static_cast<int>( prefix.length() ) );
			int len( utf8str_codepoint_len( c.c_str(), static_cast<int>( c.length() ) ) );

			for (int i = 0; i < len; ++i) {
				colors.at(pos + i) = e.second;
			}

			pos += len;
			str = match.suffix();
		}
	}
	bool inWord( false );
	int wordStart( 0 );
	int wordEnd( 0 );
	int colorOffset( 0 );
	auto dohl = [&](int i) {
		inWord = false;
		std::string intermission( context.substr( wordEnd, wordStart - wordEnd ) );
		colorOffset += utf8str_codepoint_len( intermission.c_str(), intermission.length() );
		int wordLen( i - wordStart );
		std::string keyword( context.substr( wordStart, wordLen ) );
		bool bold( false );
		if ( keyword.substr( 0, 5 ) == "bold_" ) {
			keyword = keyword.substr( 5 );
			bold = true;
		}
		bool underline( false );
		if ( keyword.substr( 0, 10 ) == "underline_" ) {
			keyword = keyword.substr( 10 );
			underline = true;
		}
		keyword_highlight_t::const_iterator it( word_color.find( keyword ) );
		Replxx::Color color = Replxx::Color::DEFAULT;
		if ( it != word_color.end() ) {
			color = it->second;
		}
		if ( bold ) {
			color = replxx::color::bold( color );
		}
		if ( underline ) {
			color = replxx::color::underline( color );
		}
		for ( int k( 0 ); k < wordLen; ++ k ) {
			Replxx::Color& c( colors.at( colorOffset + k ) );
			if ( color != Replxx::Color::DEFAULT ) {
				c = color;
			}
		}
		colorOffset += wordLen;
		wordEnd = i;
	};
	for ( int i( 0 ); i < static_cast<int>( context.length() ); ++ i ) {
		if ( !inWord ) {
			if ( is_kw( context[i] ) ) {
				inWord = true;
				wordStart = i;
			}
		} else if ( inWord && !is_kw( context[i] ) ) {
			dohl(i);
		}
		if ( ( context[i] != '_' ) && ispunct( context[i] ) ) {
			wordStart = i;
			dohl( i + 1 );
		}
	}
	if ( inWord ) {
		dohl(context.length());
	}
}

void hook_modify( std::string& currentInput_, int&, Replxx* rx ) {
	char prompt[64];
	snprintf( prompt, 64, "\x1b[1;32mreplxx\x1b[0m[%lu]> ", currentInput_.length() );
	rx->set_prompt( prompt );
}

Replxx::ACTION_RESULT message( Replxx& replxx, std::string s, char32_t ) {
	replxx.invoke( Replxx::ACTION::CLEAR_SELF, 0 );
	replxx.print( "%s\n", s.c_str() );
	replxx.invoke( Replxx::ACTION::REPAINT, 0 );
	return ( Replxx::ACTION_RESULT::CONTINUE );
}

int main( int argc_, char** argv_ ) {
	// words to be completed
	std::vector<std::string> examples {
		".help", ".history", ".quit", ".exit", ".clear", ".prompt ",
		"hello", "world", "db", "data", "drive", "print", "put",
		"color_black", "color_red", "color_green", "color_brown", "color_blue",
		"color_magenta", "color_cyan", "color_lightgray", "color_gray",
		"color_brightred", "color_brightgreen", "color_yellow", "color_brightblue",
		"color_brightmagenta", "color_brightcyan", "color_white",
		"determinANT", "determiNATION", "deterMINE", "deteRMINISM", "detERMINISTIC", "deTERMINED",
		"star", "star_galaxy_cluser_supercluster_observable_universe",
	};

	// highlight specific words
	// a regex string, and a color
	// the order matters, the last match will take precedence
	using cl = Replxx::Color;
	keyword_highlight_t word_color {
		// single chars
		{"`", cl::BRIGHTCYAN},
		{"'", cl::BRIGHTBLUE},
		{"\"", cl::BRIGHTBLUE},
		{"-", cl::BRIGHTBLUE},
		{"+", cl::BRIGHTBLUE},
		{"=", cl::BRIGHTBLUE},
		{"/", cl::BRIGHTBLUE},
		{"*", cl::BRIGHTBLUE},
		{"^", cl::BRIGHTBLUE},
		{".", cl::BRIGHTMAGENTA},
		{"(", cl::BRIGHTMAGENTA},
		{")", cl::BRIGHTMAGENTA},
		{"[", cl::BRIGHTMAGENTA},
		{"]", cl::BRIGHTMAGENTA},
		{"{", cl::BRIGHTMAGENTA},
		{"}", cl::BRIGHTMAGENTA},

		// color keywords
		{"color_black", cl::BLACK},
		{"color_red", cl::RED},
		{"color_green", cl::GREEN},
		{"color_brown", cl::BROWN},
		{"color_blue", cl::BLUE},
		{"color_magenta", cl::MAGENTA},
		{"color_cyan", cl::CYAN},
		{"color_lightgray", cl::LIGHTGRAY},
		{"color_gray", cl::GRAY},
		{"color_brightred", cl::BRIGHTRED},
		{"color_brightgreen", cl::BRIGHTGREEN},
		{"color_yellow", cl::YELLOW},
		{"color_brightblue", cl::BRIGHTBLUE},
		{"color_brightmagenta", cl::BRIGHTMAGENTA},
		{"color_brightcyan", cl::BRIGHTCYAN},
		{"color_white", cl::WHITE},

		// commands
		{"help", cl::BRIGHTMAGENTA},
		{"history", cl::BRIGHTMAGENTA},
		{"quit", cl::BRIGHTMAGENTA},
		{"exit", cl::BRIGHTMAGENTA},
		{"clear", cl::BRIGHTMAGENTA},
		{"prompt", cl::BRIGHTMAGENTA},
	};
	syntax_highlight_t regex_color {
		// numbers
		{"[\\-|+]{0,1}[0-9]+", cl::YELLOW}, // integers
		{"[\\-|+]{0,1}[0-9]*\\.[0-9]+", cl::YELLOW}, // decimals
		{"[\\-|+]{0,1}[0-9]+e[\\-|+]{0,1}[0-9]+", cl::YELLOW}, // scientific notation

		// strings
		{"\".*?\"", cl::BRIGHTGREEN}, // double quotes
		{"\'.*?\'", cl::BRIGHTGREEN}, // single quotes
	};
	static int const MAX_LABEL_NAME( 32 );
	char label[MAX_LABEL_NAME];
	for ( int r( 0 ); r < 6; ++ r ) {
		for ( int g( 0 ); g < 6; ++ g ) {
			for ( int b( 0 ); b < 6; ++ b ) {
				snprintf( label, MAX_LABEL_NAME, "rgb%d%d%d", r, g, b );
				word_color.insert( std::make_pair( label, replxx::color::rgb666( r, g, b ) ) );
				for ( int br( 0 ); br < 6; ++ br ) {
					for ( int bg( 0 ); bg < 6; ++ bg ) {
						for ( int bb( 0 ); bb < 6; ++ bb ) {
							snprintf( label, MAX_LABEL_NAME, "fg%d%d%dbg%d%d%d", r, g, b, br, bg, bb );
							word_color.insert(
								std::make_pair(
									label,
									rgb666( r, g, b ) | replxx::color::bg( rgb666( br, bg, bb ) )
								)
							);
						}
					}
				}
			}
		}
	}
	for ( int gs( 0 ); gs < 24; ++ gs ) {
		snprintf( label, MAX_LABEL_NAME, "gs%d", gs );
		word_color.insert( std::make_pair( label, grayscale( gs ) ) );
		for ( int bgs( 0 ); bgs < 24; ++ bgs ) {
			snprintf( label, MAX_LABEL_NAME, "gs%dgs%d", gs, bgs );
			word_color.insert( std::make_pair( label, grayscale( gs ) | bg( grayscale( bgs ) ) ) );
		}
	}
	Replxx::Color colorCodes[] = {
		Replxx::Color::BLACK, Replxx::Color::RED, Replxx::Color::GREEN, Replxx::Color::BROWN, Replxx::Color::BLUE,
		Replxx::Color::CYAN, Replxx::Color::MAGENTA, Replxx::Color::LIGHTGRAY, Replxx::Color::GRAY, Replxx::Color::BRIGHTRED,
		Replxx::Color::BRIGHTGREEN, Replxx::Color::YELLOW, Replxx::Color::BRIGHTBLUE, Replxx::Color::BRIGHTCYAN,
		Replxx::Color::BRIGHTMAGENTA, Replxx::Color::WHITE
	};
	for ( Replxx::Color bg : colorCodes ) {
		for ( Replxx::Color fg : colorCodes ) {
			snprintf( label, MAX_LABEL_NAME, "c_%d_%d", static_cast<int>( fg ), static_cast<int>( bg ) );
			word_color.insert( std::make_pair( label, fg | replxx::color::bg( bg ) ) );
		}
	}

	bool tickMessages( false );
	bool promptFan( false );
	bool promptInCallback( false );
	bool indentMultiline( false );
	bool bracketedPaste( false );
	bool ignoreCase( false );
	std::string keys;
	std::string prompt;
	int hintDelay( 0 );
	while ( argc_ > 1 ) {
		-- argc_;
		++ argv_;
		switch ( (*argv_)[0] ) {
			case ( 'm' ): tickMessages = true; break;
			case ( 'F' ): promptFan = true; break;
			case ( 'P' ): promptInCallback = true; break;
			case ( 'I' ): indentMultiline = true; break;
			case ( 'i' ): ignoreCase = true; break;
			case ( 'k' ): keys = (*argv_) + 1; break;
			case ( 'd' ): hintDelay = std::stoi( (*argv_) + 1 ); break;
			case ( 'h' ): examples.push_back( (*argv_) + 1 ); break;
			case ( 'p' ): prompt = (*argv_) + 1; break;
			case ( 'B' ): bracketedPaste = true; break;
		}
	}

	// init the repl
	Replxx rx;
	Tick tick( rx, keys, tickMessages, promptFan );
	rx.install_window_change_handler();

	// the path to the history file
	std::string history_file_path {"./replxx_history.txt"};

	// load the history file if it exists
	/* scope for ifstream object for auto-close */ {
		std::ifstream history_file( history_file_path.c_str() );
		rx.history_load( history_file );
	}

	// set the max history size
	rx.set_max_history_size(128);

	// set the max number of hint rows to show
	rx.set_max_hint_rows(3);

	// set the callbacks
	using namespace std::placeholders;
	rx.set_completion_callback( std::bind( &hook_completion, _1, _2, cref( examples ), ignoreCase ) );
	rx.set_highlighter_callback( std::bind( &hook_color, _1, _2, cref( regex_color ), cref( word_color ) ) );
	rx.set_hint_callback( std::bind( &hook_hint, _1, _2, _3, cref( examples ), ignoreCase ) );
	if ( promptInCallback ) {
		rx.set_modify_callback( std::bind( &hook_modify, _1, _2, &rx ) );
	}

	// other api calls
	rx.set_word_break_characters( " \n\t.,-%!;:=*~^'\"/?<>|[](){}" );
	rx.set_completion_count_cutoff( 128 );
	rx.set_hint_delay( hintDelay );
	rx.set_double_tab_completion( false );
	rx.set_complete_on_empty( true );
	rx.set_beep_on_ambiguous_completion( false );
	rx.set_no_color( false );
	rx.set_indent_multiline( indentMultiline );
	if ( bracketedPaste ) {
		rx.enable_bracketed_paste();
	}
	rx.set_ignore_case( ignoreCase );

	// showcase key bindings
	rx.bind_key_internal( Replxx::KEY::BACKSPACE,                      "delete_character_left_of_cursor" );
	rx.bind_key_internal( Replxx::KEY::DELETE,                         "delete_character_under_cursor" );
	rx.bind_key_internal( Replxx::KEY::LEFT,                           "move_cursor_left" );
	rx.bind_key_internal( Replxx::KEY::RIGHT,                          "move_cursor_right" );
	rx.bind_key_internal( Replxx::KEY::UP,                             "line_previous" );
	rx.bind_key_internal( Replxx::KEY::DOWN,                           "line_next" );
	rx.bind_key_internal( Replxx::KEY::meta( Replxx::KEY::UP ),        "history_previous" );
	rx.bind_key_internal( Replxx::KEY::meta( Replxx::KEY::DOWN ),      "history_next" );
	rx.bind_key_internal( Replxx::KEY::PAGE_UP,                        "history_first" );
	rx.bind_key_internal( Replxx::KEY::PAGE_DOWN,                      "history_last" );
	rx.bind_key_internal( Replxx::KEY::HOME,                           "move_cursor_to_begining_of_line" );
	rx.bind_key_internal( Replxx::KEY::END,                            "move_cursor_to_end_of_line" );
	rx.bind_key_internal( Replxx::KEY::TAB,                            "complete_line" );
	rx.bind_key_internal( Replxx::KEY::control( Replxx::KEY::LEFT ),   "move_cursor_one_word_left" );
	rx.bind_key_internal( Replxx::KEY::control( Replxx::KEY::RIGHT ),  "move_cursor_one_word_right" );
	rx.bind_key_internal( Replxx::KEY::control( Replxx::KEY::UP ),     "hint_previous" );
	rx.bind_key_internal( Replxx::KEY::control( Replxx::KEY::DOWN ),   "hint_next" );
	rx.bind_key_internal( Replxx::KEY::control( Replxx::KEY::ENTER ),  "commit_line" );
	rx.bind_key_internal( Replxx::KEY::control( 'R' ),                 "history_incremental_search" );
	rx.bind_key_internal( Replxx::KEY::control( 'W' ),                 "kill_to_begining_of_word" );
	rx.bind_key_internal( Replxx::KEY::control( 'U' ),                 "kill_to_begining_of_line" );
	rx.bind_key_internal( Replxx::KEY::control( 'K' ),                 "kill_to_end_of_line" );
	rx.bind_key_internal( Replxx::KEY::control( 'Y' ),                 "yank" );
	rx.bind_key_internal( Replxx::KEY::control( 'L' ),                 "clear_screen" );
	rx.bind_key_internal( Replxx::KEY::control( 'D' ),                 "send_eof" );
	rx.bind_key_internal( Replxx::KEY::control( 'C' ),                 "abort_line" );
	rx.bind_key_internal( Replxx::KEY::control( 'T' ),                 "transpose_characters" );
#ifndef _WIN32
	rx.bind_key_internal( Replxx::KEY::control( 'V' ),                 "verbatim_insert" );
	rx.bind_key_internal( Replxx::KEY::control( 'Z' ),                 "suspend" );
#endif
	rx.bind_key_internal( Replxx::KEY::meta( Replxx::KEY::BACKSPACE ), "kill_to_whitespace_on_left" );
	rx.bind_key_internal( Replxx::KEY::meta( 'p' ),                    "history_common_prefix_search" );
	rx.bind_key_internal( Replxx::KEY::meta( 'n' ),                    "history_common_prefix_search" );
	rx.bind_key_internal( Replxx::KEY::meta( 'd' ),                    "kill_to_end_of_word" );
	rx.bind_key_internal( Replxx::KEY::meta( 'y' ),                    "yank_cycle" );
	rx.bind_key_internal( Replxx::KEY::meta( 'u' ),                    "uppercase_word" );
	rx.bind_key_internal( Replxx::KEY::meta( 'l' ),                    "lowercase_word" );
	rx.bind_key_internal( Replxx::KEY::meta( 'c' ),                    "capitalize_word" );
	rx.bind_key_internal( 'a',                                         "insert_character" );
	rx.bind_key_internal( Replxx::KEY::INSERT,                         "toggle_overwrite_mode" );
	rx.bind_key( Replxx::KEY::F1, std::bind( &message, std::ref( rx ), "<F1>", _1 ) );
	rx.bind_key( Replxx::KEY::F2, std::bind( &message, std::ref( rx ), "<F2>", _1 ) );
	rx.bind_key( Replxx::KEY::F3, std::bind( &message, std::ref( rx ), "<F3>", _1 ) );
	rx.bind_key( Replxx::KEY::F4, std::bind( &message, std::ref( rx ), "<F4>", _1 ) );
	rx.bind_key( Replxx::KEY::F5, std::bind( &message, std::ref( rx ), "<F5>", _1 ) );
	rx.bind_key( Replxx::KEY::F6, std::bind( &message, std::ref( rx ), "<F6>", _1 ) );
	rx.bind_key( Replxx::KEY::F7, std::bind( &message, std::ref( rx ), "<F7>", _1 ) );
	rx.bind_key( Replxx::KEY::F8, std::bind( &message, std::ref( rx ), "<F8>", _1 ) );
	rx.bind_key( Replxx::KEY::F9, std::bind( &message, std::ref( rx ), "<F9>", _1 ) );
	rx.bind_key( Replxx::KEY::F10, std::bind( &message, std::ref( rx ), "<F10>", _1 ) );
	rx.bind_key( Replxx::KEY::F11, std::bind( &message, std::ref( rx ), "<F11>", _1 ) );
	rx.bind_key( Replxx::KEY::F12, std::bind( &message, std::ref( rx ), "<F12>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F1 ), std::bind( &message, std::ref( rx ), "<S-F1>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F2 ), std::bind( &message, std::ref( rx ), "<S-F2>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F3 ), std::bind( &message, std::ref( rx ), "<S-F3>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F4 ), std::bind( &message, std::ref( rx ), "<S-F4>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F5 ), std::bind( &message, std::ref( rx ), "<S-F5>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F6 ), std::bind( &message, std::ref( rx ), "<S-F6>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F7 ), std::bind( &message, std::ref( rx ), "<S-F7>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F8 ), std::bind( &message, std::ref( rx ), "<S-F8>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F9 ), std::bind( &message, std::ref( rx ), "<S-F9>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F10 ), std::bind( &message, std::ref( rx ), "<S-F10>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F11 ), std::bind( &message, std::ref( rx ), "<S-F11>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::F12 ), std::bind( &message, std::ref( rx ), "<S-F12>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F1 ), std::bind( &message, std::ref( rx ), "<C-F1>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F2 ), std::bind( &message, std::ref( rx ), "<C-F2>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F3 ), std::bind( &message, std::ref( rx ), "<C-F3>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F4 ), std::bind( &message, std::ref( rx ), "<C-F4>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F5 ), std::bind( &message, std::ref( rx ), "<C-F5>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F6 ), std::bind( &message, std::ref( rx ), "<C-F6>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F7 ), std::bind( &message, std::ref( rx ), "<C-F7>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F8 ), std::bind( &message, std::ref( rx ), "<C-F8>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F9 ), std::bind( &message, std::ref( rx ), "<C-F9>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F10 ), std::bind( &message, std::ref( rx ), "<C-F10>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F11 ), std::bind( &message, std::ref( rx ), "<C-F11>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::F12 ), std::bind( &message, std::ref( rx ), "<C-F12>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::TAB ), std::bind( &message, std::ref( rx ), "<S-Tab>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::HOME ), std::bind( &message, std::ref( rx ), "<C-Home>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::HOME ), std::bind( &message, std::ref( rx ), "<S-Home>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::END ), std::bind( &message, std::ref( rx ), "<C-End>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::END ), std::bind( &message, std::ref( rx ), "<S-End>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::PAGE_UP ), std::bind( &message, std::ref( rx ), "<C-PgUp>", _1 ) );
	rx.bind_key( Replxx::KEY::control( Replxx::KEY::PAGE_DOWN ), std::bind( &message, std::ref( rx ), "<C-PgDn>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::LEFT ), std::bind( &message, std::ref( rx ), "<S-Left>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::RIGHT ), std::bind( &message, std::ref( rx ), "<S-Right>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::UP ), std::bind( &message, std::ref( rx ), "<S-Up>", _1 ) );
	rx.bind_key( Replxx::KEY::shift( Replxx::KEY::DOWN ), std::bind( &message, std::ref( rx ), "<S-Down>", _1 ) );
	rx.bind_key( Replxx::KEY::meta( '\r' ), std::bind( &message, std::ref( rx ), "<M-Enter>", _1 ) );

	// display initial welcome message
	std::cout
		<< "Welcome to Replxx\n"
		<< "Press 'tab' to view autocompletions\n"
		<< "Type '.help' for help\n"
		<< "Type '.quit' or '.exit' to exit\n\n";

	// set the repl prompt
	if ( prompt.empty() ) {
		prompt = "\x1b[1;32mreplxx\x1b[0m> ";
	}

	// main repl loop
	if ( ! keys.empty() || tickMessages || promptFan ) {
		tick.start();
	}
	for (;;) {
		// display the prompt and retrieve input from the user
		char const* cinput{ nullptr };

		do {
			cinput = rx.input(prompt);
		} while ( ( cinput == nullptr ) && ( errno == EAGAIN ) );

		if (cinput == nullptr) {
			break;
		}

		// change cinput into a std::string
		// easier to manipulate
		std::string input {cinput};

		if (input.empty()) {
			// user hit enter on an empty line

			continue;

		} else if (input.compare(0, 5, ".quit") == 0 || input.compare(0, 5, ".exit") == 0) {
			// exit the repl

			rx.history_add(input);
			break;

		} else if (input.compare(0, 5, ".help") == 0) {
			// display the help output
			std::cout
				<< ".help\n\tdisplays the help output\n"
				<< ".quit\n\texit the repl\n"
				<< ".exit\n\texit the repl\n"
				<< ".clear\n\tclears the screen\n"
				<< ".history\n\tdisplays the history output\n"
				<< ".prompt <str>\n\tset the repl prompt to <str>\n";

			rx.history_add(input);
			continue;

		} else if (input.compare(0, 7, ".prompt") == 0) {
			// set the repl prompt text
			auto pos = input.find(" ");
			if (pos == std::string::npos) {
				std::cout << "Error: '.prompt' missing argument\n";
			} else {
				prompt = input.substr(pos + 1) + " ";
			}

			rx.history_add(input);
			continue;

		} else if (input.compare(0, 8, ".history") == 0) {
			// display the current history
			Replxx::HistoryScan hs( rx.history_scan() );
			for ( int i( 0 ); hs.next(); ++ i ) {
				std::cout << std::setw(4) << i << ": " << hs.get().text() << "\n";
			}

			rx.history_add(input);
			continue;

		} else if (input.compare(0, 6, ".merge") == 0) {
			history_file_path = "replxx_history_alt.txt";

			rx.history_add(input);
			continue;

		} else if (input.compare(0, 5, ".save") == 0) {
			history_file_path = "replxx_history_alt.txt";
			std::ofstream history_file( history_file_path.c_str() );
			rx.history_save( history_file );
			continue;

		} else if (input.compare(0, 6, ".clear") == 0) {
			// clear the screen
			rx.clear_screen();

			rx.history_add(input);
			continue;

		} else {
			// default action
			// echo the input

			rx.print( "%s\n", input.c_str() );

			rx.history_add( input );
			continue;
		}
	}
	if ( ! keys.empty() || tickMessages || promptFan ) {
		tick.stop();
	}

	// save the history
	rx.history_sync( history_file_path );
	if ( bracketedPaste ) {
		rx.disable_bracketed_paste();
	}
	if ( bracketedPaste || promptInCallback || promptFan ) {
		std::cout << "\n" << prompt;
	}

	std::cout << "\nExiting Replxx\n";

	return 0;
}
