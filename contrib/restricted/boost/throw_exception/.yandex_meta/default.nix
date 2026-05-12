self: super: with self; {
  boost_throw_exception = stdenv.mkDerivation rec {
    pname = "boost_throw_exception";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "throw_exception";
      rev = "boost-${version}";
      hash = "sha256-jRYmeNE1W7KykpCgsVqCm5OcgCd/3N6N/70+XHprtYE=";
    };
  };
}
