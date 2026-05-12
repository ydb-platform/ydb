self: super: with self; {
  boost_variant = stdenv.mkDerivation rec {
    pname = "boost_variant";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant";
      rev = "boost-${version}";
      hash = "sha256-3Q75TYRc8nrDNg9M+xGK2flEau6xvCsnUYRBo1l0sDw=";
    };
  };
}
