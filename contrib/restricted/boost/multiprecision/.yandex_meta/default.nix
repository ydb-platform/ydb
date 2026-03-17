self: super: with self; {
  boost_multiprecision = stdenv.mkDerivation rec {
    pname = "boost_multiprecision";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multiprecision";
      rev = "boost-${version}";
      hash = "sha256-R/ghlkAeFGev7SdgLQwp4/9r8krjH8Wav4I5gzWUgTQ=";
    };
  };
}
