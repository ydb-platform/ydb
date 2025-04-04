self: super: with self; {
  boost_numeric_conversion = stdenv.mkDerivation rec {
    pname = "boost_numeric_conversion";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "numeric_conversion";
      rev = "boost-${version}";
      hash = "sha256-UIji0KD/9hsqipL13aFur4DlvXcIsKCy8HuvLwTFQQ8=";
    };
  };
}
