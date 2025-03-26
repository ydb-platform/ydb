self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.87.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-kqWpyd5pKbNPmYh6nu5rUIVDbGSYP/c3ZjoW3ggn0qQ=";
		};
	};
}
