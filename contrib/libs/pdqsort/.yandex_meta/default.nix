self: super: with self; {
  pdqsort = stdenv.mkDerivation rec {
    pname = "pdqsort";
    version = "2021-03-14";

    src = fetchFromGitHub {
      owner = "orlp";
      repo = "pdqsort";
      rev = "b1ef26a55cdb60d236a5cb199c4234c704f46726";
      hash = "sha256-xn3Jjn/jxJBckpg1Tx3HHVAWYPVTFMiDFiYgB2WX7Sc=";
    };
  };
}
