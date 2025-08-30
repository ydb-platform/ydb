self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-UvgPOuVibz9GEpzDOOGgTvSunYDvErQfqsb9xvj+xic=";
    };
  };
}
