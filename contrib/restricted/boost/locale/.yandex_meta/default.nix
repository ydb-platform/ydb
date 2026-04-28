self: super: with self; {
  boost_locale = stdenv.mkDerivation rec {
    pname = "boost_locale";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "locale";
      rev = "boost-${version}";
      hash = "sha256-OUEZUTqCouIEg8vFOoLcAl8LwERO3rBKd4f0z6bG+t8=";
    };
  };
}
