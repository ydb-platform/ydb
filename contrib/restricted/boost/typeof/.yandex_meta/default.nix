self: super: with self; {
  boost_typeof = stdenv.mkDerivation rec {
    pname = "boost_typeof";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "typeof";
      rev = "boost-${version}";
      hash = "sha256-4dxXZiFD8EYspB6Cdm+qEAhnenhgx9Hs0cC/Rwy+DQc=";
    };
  };
}
