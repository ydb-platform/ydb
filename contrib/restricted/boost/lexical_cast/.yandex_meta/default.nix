self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.92.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-7kafDO9cNkz5+TChxIZctM13IekNZH2/QBL2MeeXyIo=";
		};
	};
}
