self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.91.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-R8yxXanq7qBmN+IO4UVdIOUDyq3wnALSonTn2qGI/ng=";
		};
	};
}
