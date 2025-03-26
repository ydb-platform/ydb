self: super: with self; {
  boost_preprocessor = stdenv.mkDerivation rec {
    pname = "boost_preprocessor";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "preprocessor";
      rev = "boost-${version}";
      hash = "sha256-6H4tdVQMO3rSNuPjIDKhmHX0LMyMXoejNThmcO7lGGg=";
    };
  };
}
