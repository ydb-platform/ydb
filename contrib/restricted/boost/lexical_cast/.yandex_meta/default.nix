self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.89.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-EVw+1e43ZAA8J2QUHkwkR5Z3pUCKnMZkxKBs5XltRbo=";
		};
	};
}
