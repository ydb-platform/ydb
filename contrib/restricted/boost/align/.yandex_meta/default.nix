self: super: with self; {
  boost_align = stdenv.mkDerivation rec {
    pname = "boost_align";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "align";
      rev = "boost-${version}";
      hash = "sha256-pmYhhmJk69bAemvm2RNW9wSzUnaSMYNZz5QHZmRO7YY=";
    };
  };
}
