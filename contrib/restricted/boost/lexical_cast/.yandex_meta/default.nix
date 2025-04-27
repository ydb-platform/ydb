self: super: with self; rec {
	boost_lexical_cast = stdenv.mkDerivation rec {
		pname = "boost_lexical_cast";
		version = "1.88.0";

		src = fetchFromGitHub {
			owner = "boostorg";
			repo = "lexical_cast";
			rev = "boost-${version}";
			hash = "sha256-gG8lZALzuowQ6YYC8HAEpcQOlhCKLds17A7afOlfxGE=";
		};
	};
}
