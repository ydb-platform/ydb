self: super: with self; {
  boost_charconv = stdenv.mkDerivation rec {
    pname = "boost_charconv";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "charconv";
      rev = "boost-${version}";
      hash = "sha256-DmxF/0Ja79+gWxYJvWoMCmBmxoPZyor+nijbuYDnWVk=";
    };
  };
}
