pkgs: attrs: with pkgs; rec {
  pname = "libev";
  version = "4.33";

  src = fetchurl {
    url = "http://dist.schmorp.de/libev/Attic/${pname}-${version}.tar.gz";
    hash = "sha256-UH63uNEBX77FuTXzTr7RW/NGvtBKEauCuO7oSMQgWuo=";
  };
}
