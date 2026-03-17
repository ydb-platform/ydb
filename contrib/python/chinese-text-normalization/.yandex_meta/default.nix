self: super: with self; {
  chinese-text-normalization = stdenv.mkDerivation rec {
    pname = "chinese-text-normalization";
    version = "2023-03-18";

    src = fetchFromGitHub {
      owner = "speechio";
      repo = "chinese_text_normalization";
      rev = "5d81cefce281edad9b5e0d9f36aa8c44d2e73c4e";
      hash = "sha256-rl7o6ayJyD6x1eoRavPHoXA44rjJDIcxJWLfERbzDf0=";
    };
  };
}
