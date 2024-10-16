self: super: with self; rec {
  version = "75.1";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${lib.replaceChars [ "." ] [ "-" ] version}/icu4c-${lib.replaceChars [ "." ] [ "_" ] version}-src.tgz";
    hash = "sha256-y5aN8+TS6H6LEcSaXQHHh70TuVRSgPxmQvgmUnYYyu8=";
  };
}
