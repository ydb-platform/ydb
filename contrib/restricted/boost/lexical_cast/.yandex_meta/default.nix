self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.90.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-5Jzp7cx9rA1wxRbk08gXlyoKwfSATexOOHLy+SihKB4=";
		};
	};
}
