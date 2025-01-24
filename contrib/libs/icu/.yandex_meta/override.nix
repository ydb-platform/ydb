self: super: with self; rec {
  version = "76.1";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${lib.replaceChars [ "." ] [ "-" ] version}/icu4c-${lib.replaceChars [ "." ] [ "_" ] version}-src.tgz";
    hash = "sha256-36y0a/5HR0EEcs4+EUS/KKEC/uqk44dbrJtMbPMPTz4=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
