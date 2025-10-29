self: super: with self; {
  boost_conversion = stdenv.mkDerivation rec {
    pname = "boost_conversion";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "conversion";
      rev = "boost-${version}";
      hash = "sha256-95L3qltl5redVxlqe7oYcWUsDlpN6U2lZdi2YJpTgnQ=";
    };
  };
}
