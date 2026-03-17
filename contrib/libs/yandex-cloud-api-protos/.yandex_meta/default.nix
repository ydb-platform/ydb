self: super: with self; {
  yandex-cloud-api-protos = stdenv.mkDerivation rec {
    pname = "yandex-cloud-api-protos";
    version = "2026-03-12";

    src = fetchFromGitHub {
      owner = "yandex-cloud";
      repo = "cloudapi";
      rev = "9ec583f7e43991ed5bfda8bc6820c2f5a8d60302";
      hash = "sha256-YK9wadUUdsxLcDKJi6dYMfVIFX9GBykWKLYSNsy7YSM=";
    };
  };
}
