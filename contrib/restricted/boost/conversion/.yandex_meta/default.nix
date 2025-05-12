self: super: with self; {
  boost_conversion = stdenv.mkDerivation rec {
    pname = "boost_conversion";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "conversion";
      rev = "boost-${version}";
      hash = "sha256-mCKzZxY1OiK9B/s5CJQjY47C4mnC8OtQf9hAeECpBjE=";
    };
  };
}
