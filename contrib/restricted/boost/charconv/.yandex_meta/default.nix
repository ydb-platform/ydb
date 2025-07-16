self: super: with self; {
  boost_charconv = stdenv.mkDerivation rec {
    pname = "boost_charconv";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "charconv";
      rev = "boost-${version}";
      hash = "sha256-/BT/McmmdetjDtaqwOKvmid6MthUqfzwfv1V/FXjqP8=";
    };
  };
}
